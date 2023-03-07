package com.pristine.dataload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompDataDAOLIG;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompDataLIGRollup {
	private static Logger logger = Logger.getLogger("CompDataLIG");
	private ItemDAO itemdao = new ItemDAO();
	public static void main(String[] args) {
		if (args.length == 1 && args[0].equals("-h")) {
			logger.debug("Insufficient Arguments - CompDataLIGRollup [scheduleId]");
			System.exit(1);
		}
		PropertyConfigurator.configure("log4j-lig.properties");
		int scheduleId = -1; 
		if( args.length == 1)
			scheduleId = Integer.parseInt(args[0]);

		PropertyManager.initialize("analysis.properties");

		Connection conn = null;
		try {
			conn = DBManager.getConnection();

			CompDataLIGRollup compDataLIG = new CompDataLIGRollup();
			if( scheduleId > 0 ){
				compDataLIG.LIGLevelRollUp(conn, scheduleId);
			}
			else{
				int chainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID","-1"));
				if( chainId <= 0){
					logger.info( "Invalid Chain Id, Subscriber Chain Id needs to be set" );
					System.exit(-1);
				}
				String startDate = DateUtil.getWeekStartDate(0);
				String endDate  = DateUtil.getWeekEndDate();
				ScheduleDAO schDAO = new ScheduleDAO();
				logger.info( "Processing for Chain = " + chainId + ", StartDate/EndDate = " + startDate + "-" + endDate);

				ArrayList <ScheduleInfoDTO> schList = schDAO.getSchedulesByChain(conn,chainId, -1, startDate, endDate);
				logger.info( "# of Schedules Processed = " +schList.size());
				//do the processing in a loop
				for( ScheduleInfoDTO schedule:schList){
					compDataLIG.LIGLevelRollUp(conn, schedule.getScheduleId());
					PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
				}
			}
				
		} catch (GeneralException ge) {
			logger.error("Error in Price change Stats Calcs", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}

		try {
			if (PropertyManager.getProperty("DATALOAD.COMMIT", "")
					.equalsIgnoreCase("TRUE")) {
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
			} else {
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Comp Data LIG");
			}
		} catch (GeneralException ge) {
			logger.error("Error in commit", ge);
			System.exit(1);
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	public void LIGLevelRollUp(Connection conn, int scheduleId)
			throws GeneralException {

		
		logger.info("Doing LIG Level Rollup for " + scheduleId);

		CompetitiveDataDAO compDataDao = new CompetitiveDataDAO(conn);
		// Get items in the current in the current schedule
		ArrayList<CompetitiveDataDTO> compDataList = compDataDao.getCompData(
				conn, scheduleId, -1, -1, true);
		logger.info("No of Items to Analyze:" + compDataList.size());

		//*** Delete LIG Data for the schedule
		
		CompDataDAOLIG compDataLigDao = new CompDataDAOLIG();
		
		compDataLigDao.deleteDataForSchedule(conn, scheduleId, Constants.NULLID);
		
		HashMap<Integer, HashMap<Float, Integer>> processedLirList = new HashMap<>();
	
		HashMap<Integer, CompetitiveDataDTO> ligDataMap = new HashMap<Integer, CompetitiveDataDTO>();
		Set<Integer> lirIdSet = new HashSet<Integer>();
		HashMap<Integer, HashMap<Float,CompetitiveDataDTO>> tempCompDtoMap = new HashMap<>();
		for(CompetitiveDataDTO compData : compDataList){
			lirIdSet.add(compData.lirId);
		}
		HashMap<Integer, Integer> lirIdItemCodeMap =  itemdao.getLIRInfo(conn, lirIdSet);
		
		for (CompetitiveDataDTO compData : compDataList) {
			int count = 0;
			HashMap<Float, Integer> temp = new HashMap<>();
			HashMap<Float,CompetitiveDataDTO> tempMap= new HashMap<>();
	
			if (compData.lirId > 0 && lirIdItemCodeMap.containsKey(compData.lirId)
					&& lirIdItemCodeMap.get(compData.lirId) != null) {
				if (processedLirList.containsKey(compData.lirId)) {
					HashMap<Float, Integer> priceMap = processedLirList.get(compData.lirId);

					if (priceMap.containsKey(compData.regPrice)) {
						count = priceMap.get(compData.regPrice);
						priceMap.put(compData.regPrice, count + 1);
						processedLirList.put(compData.lirId, priceMap);
					} else {
						priceMap.put(compData.regPrice, count + 1);
						processedLirList.put(compData.lirId, priceMap);
					}
					tempMap=tempCompDtoMap.get(compData.lirId);
					tempMap.put(compData.regPrice, compData);
					tempCompDtoMap.put(compData.lirId,tempMap);
				} else {
					temp.put(compData.regPrice, count + 1);
					List<CompetitiveDataDTO> existingdata= new ArrayList<>();
					existingdata.add(compData);
					tempMap.put(compData.regPrice, compData);
					tempCompDtoMap.put(compData.lirId,tempMap);
					processedLirList.put(compData.lirId, temp);
				}

			}
		}
		//Select most common price for LIG,in case of tie select min price for LIG 
		setLIGPrices(lirIdItemCodeMap, processedLirList, ligDataMap, tempCompDtoMap, scheduleId);
		lirIdItemCodeMap.clear();
		tempCompDtoMap.clear();
		processedLirList.clear();
		setZonePriceDiffFlagForLig(ligDataMap, compDataList);
		compDataLigDao.setupLIGData(conn, ligDataMap);
		
		logger.info("Completed LIG Level Rollup for " + scheduleId);
	}

	/**
	 * This method sets diff zone price flag for Lig item.  
	 * @param ligDataMap
	 * @param compDataList
	 */
	private void setZonePriceDiffFlagForLig(HashMap<Integer, CompetitiveDataDTO> ligDataMap, 
			ArrayList<CompetitiveDataDTO> compDataList){
		HashMap<Integer, ArrayList<CompetitiveDataDTO>> compDataMap = new HashMap<>();
		for(CompetitiveDataDTO compData: compDataList){
			if(compDataMap.get(compData.lirId) == null){
				ArrayList<CompetitiveDataDTO> tempList = new ArrayList<>();
				tempList.add(compData);
				compDataMap.put(compData.lirId, tempList);
			}else{
				ArrayList<CompetitiveDataDTO> tempList = compDataMap.get(compData.lirId);
				tempList.add(compData);
				compDataMap.put(compData.lirId, tempList);
			}
		}
		for(Map.Entry<Integer, CompetitiveDataDTO> entry: ligDataMap.entrySet()){
			CompetitiveDataDTO ligData = entry.getValue();
			ArrayList<CompetitiveDataDTO> ligMembers = compDataMap.get(ligData.lirId);
			boolean isZonePriceDiff = false;
			for(CompetitiveDataDTO ligMember: ligMembers){
				if(ligMember.isZonePriceDiff){
					isZonePriceDiff = true;
					//If anyone of lig member has different zone price, set zone price diff flag to lig item.
					break;
				}
			}
			ligData.isZonePriceDiff = isZonePriceDiff;
		}
		
		
	}
	
	/**
	 * Function to use most common price for LIG
	 * @param lirIdItemCodeMap
	 * @param processedLirList
	 * @param ligDataMap
	 * @param tempCompDtoMap
	 * @param scheduleId
	 */
	private void setLIGPrices(HashMap<Integer, Integer> lirIdItemCodeMap,
			HashMap<Integer, HashMap<Float, Integer>> processedLirList, HashMap<Integer, CompetitiveDataDTO> ligDataMap,
			HashMap<Integer, HashMap<Float, CompetitiveDataDTO>> tempCompDtoMap, int scheduleId) {

		for (Entry<Integer, HashMap<Float, Integer>> processMap : processedLirList.entrySet()) {

			int lirItemCode = -1;
			float mostCommonCompPrice = 0;
			int maxCount = 0;

			lirItemCode = lirIdItemCodeMap.get(processMap.getKey());
			CompetitiveDataDTO compData = null;
			HashMap<Float, CompetitiveDataDTO> compDataMap =null;
			if (lirItemCode != -1) {
				Integer key = processMap.getKey();
				if (tempCompDtoMap.containsKey(key)) {
					 compDataMap = tempCompDtoMap.get(key);
				}
				for (Map.Entry<Float, Integer> priceValues : processMap.getValue().entrySet()) {

					if (mostCommonCompPrice == 0 && maxCount == 0 && priceValues.getKey() != 0.0) {
						mostCommonCompPrice = priceValues.getKey();
						maxCount = priceValues.getValue();
					} else if (priceValues.getValue() > maxCount && priceValues.getKey() != 0.0) {
						mostCommonCompPrice = priceValues.getKey();
						maxCount = priceValues.getValue();
					} else if (priceValues.getValue() == maxCount) {
						mostCommonCompPrice = Math.min(mostCommonCompPrice, priceValues.getKey());
					}
				}
				if (compDataMap != null) {
					compData = compDataMap.get(mostCommonCompPrice);
					compData.regPrice = mostCommonCompPrice;
					ligDataMap.put(lirItemCode, compData);
				}
			} else {
				logger.warn("setLIGPrices() Lig Item Code not found for retLir Id: " + processMap.getKey());
			}
		}

	}
}