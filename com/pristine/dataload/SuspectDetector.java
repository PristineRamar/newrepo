package com.pristine.dataload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SuspectDetector {
	private static Logger  logger = Logger.getLogger("SuspectDetector");
	private CompetitiveDataDAO compDataDao;
	private HashMap <Integer, Float> lastCheckItemMap = new HashMap <Integer, Float>();
	private HashMap <Integer, Float> lastCheckLirIdMap = new HashMap <Integer, Float>();
	private static double allowedPercentVariation = 0.2;
	public static void main(String[] args) {
		
		int scheduleId = -1;
		if( args.length == 1)
			scheduleId = Integer.parseInt(args[0]);
		else if ( args.length >1)
			logger.error("Insufficient Arguments - SuspectDetector [scheduleId]");
		PropertyConfigurator.configure("log4j.properties");

		PropertyManager.initialize("analysis.properties");
		
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
		
			SuspectDetector sd = new SuspectDetector();
			
			if( scheduleId > 0 ){
				sd.markSupectItems(conn, scheduleId );
				PristineDBUtil.commitTransaction(conn, "SuspectDetector");
			}
			else{
				ScheduleDAO schDAO = new ScheduleDAO();
				int excludeChainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID","-1"));
				allowedPercentVariation  = Double.parseDouble(PropertyManager.getProperty("NON_SUSPECT_VARIATION","0.2"));
				ArrayList <Integer> schList = schDAO.getCurrentDaySchedules(conn, excludeChainId);
				
				
				logger.info( "# of Schedules to Process = " +schList.size());
				//do the processing in a loop
				for( int currentSchId:schList){
					sd.markSupectItems(conn, currentSchId );
					PristineDBUtil.commitTransaction(conn, "SuspectDetector");
					//PristineDBUtil.rollbackTransaction(conn, "SuspectDetector");
				}
			}
		}catch(GeneralException ge){
			logger.error("Error in Price change Stats Calcs", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}
		catch(Exception e){
			logger.error("Java Exception ", e);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}

	}
	
	public void markSupectItems( Connection conn, int scheduleId) throws GeneralException {
		
		logger.info("Identifying Suspect for " + scheduleId);
		
		//Clear data from prev stores
		lastCheckItemMap.clear();
		lastCheckLirIdMap.clear();
			
		//Find the list of schedules for the Previous 6 months
		ScheduleDAO schDao = new ScheduleDAO ();
		ArrayList<Integer> prevScheduleList = schDao.getPreviousSchedules(conn, scheduleId);
		if( prevScheduleList.isEmpty()){
			logger.info("No Previous schedule for the current list");
			return;
		}
		//Create a CSV schedule list
		String prevSchedules = "";
		for ( int prevSchId : prevScheduleList){
			prevSchedules = prevSchedules +  prevSchId + ",";
		}
		if( compDataDao == null )
			compDataDao = new CompetitiveDataDAO (conn);
		
		setupLastCheckData(conn, prevScheduleList);
		
		//Get items in the current in the current schedule
		ArrayList<CompetitiveDataDTO> compDataList= compDataDao.getCompData(conn, scheduleId, -1, -1, true);
		int suspectCount = 0;
		
		ArrayList <CompetitiveDataDTO> suspectCheckIdList = new ArrayList <CompetitiveDataDTO> ();  
		
		logger.info("No of Items to Analyze:" +compDataList.size());
		for ( CompetitiveDataDTO compData : compDataList){
		
			
			boolean isSuspect = false;
			if( lastCheckItemMap.containsKey(compData.itemcode)){
				isSuspect = isPriceSuspect(compData, compData.itemcode, lastCheckItemMap);
				if( isSuspect){
					compData.comment  = "current price = " + compData.regPrice + ", prev Item Price = " + 
						lastCheckItemMap.get(compData.itemcode) + ", prev Check Id = " + compData.checkItemId + ", ";
				}
			}else if( compData.lirId > 0 && lastCheckLirIdMap.containsKey(compData.lirId)){
				isSuspect = isPriceSuspect(compData, compData.lirId, lastCheckLirIdMap);
				if( isSuspect){
					compData.comment  =  "current price = " + compData.regPrice + ", prev LIR Price = " + 
						lastCheckLirIdMap.get(compData.lirId) + ", prev Check Id = " + compData.checkItemId +  ", ";
				}
			}
			
			if( isSuspect){
				suspectCount++;
				logger.debug(compData.comment);
				suspectCheckIdList.add(compData);
			}
		}
		//Do the update for Suspect
		int updateCount = compDataDao.updateSuspectFlag(conn, suspectCheckIdList);
		logger.info("# of Suspects, # of Updates " + suspectCount + ", " + updateCount);
		logger.info("Suspect Analysis successfully completed for " + scheduleId);
		
	}
	
	private boolean isPriceSuspect(CompetitiveDataDTO compData, int keyForMap, HashMap<Integer, Float> lastCheckPriceMap) {
		boolean isSuspect = false;
		float lastPrice = lastCheckPriceMap.get(keyForMap);
		double lowRangePrice = (1.0 - allowedPercentVariation)* lastPrice;
		double highRangePrice = (1.0 + allowedPercentVariation)* lastPrice;
		if( compData.itemNotFound.equals("N") &&  compData.priceNotFound.equals("N") ){
			if( compData.regPrice < lowRangePrice || compData.regPrice > highRangePrice)
				isSuspect = true;
		}
		return isSuspect ;
	}

	private void setupLastCheckData(Connection conn, ArrayList<Integer> prevScheduleList)  throws GeneralException {
		
		
		int schCount = 0; 
		for( int prevSchId:prevScheduleList){
			
			schCount++;
			ArrayList<CompetitiveDataDTO> prevCheckCompDataList= compDataDao.getCompData(conn, prevSchId, -1, -1, true);
			for ( CompetitiveDataDTO compData : prevCheckCompDataList){
				
				if( !lastCheckItemMap.containsKey(compData.itemcode) && compData.itemNotFound.equals("N") && compData.priceNotFound.equals("N") )
					lastCheckItemMap.put(compData.itemcode, compData.regPrice);
				
				if( compData.lirId > 0 && !lastCheckLirIdMap.containsKey(compData.lirId) && compData.itemNotFound.equals("N") && compData.priceNotFound.equals("N") )
					lastCheckLirIdMap.put(compData.lirId, compData.regPrice);
			}
		}
	}
		
}
