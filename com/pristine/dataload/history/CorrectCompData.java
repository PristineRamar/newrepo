/**
 * This class inserts competition price data for Item_Codem, if there are 
 * multiple Item_Code for same UPC
 * @author Janani
 *
 */
package com.pristine.dataload.history;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dataload.CompDataLIGRollup;
import com.pristine.dataload.CompDataPISetup;
import com.pristine.dataload.PriceChangeStats;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CorrectCompData {
	private static Logger logger = Logger.getLogger("CorrectCompData");
	
	private Connection conn = null;
	private String startDate = null;
	private String endDate = null;
	
	public CorrectCompData(String startDate, String endDate){
		PropertyManager.initialize("analysis.properties");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
		this.startDate = startDate;
		this.endDate = endDate;
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-correct-comp-data.properties");
		String startDate = null;
		String endDate = null;
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith("START_DATE")){
					startDate = arg.substring("START_DATE=".length());
				}else if(arg.startsWith("END_DATE")){
					endDate = arg.substring("END_DATE=".length());
				}
			}
		}
		if(startDate == null || endDate == null){
			logger.error("Insufficient Arguments - START_DATE and END_DATE");
			System.exit(1);
		}
		CorrectCompData dataCorrection = new CorrectCompData(startDate, endDate);
		dataCorrection.correctCompDataForMultipleItemsWithSameUPC();
	}

	private void correctCompDataForMultipleItemsWithSameUPC() {
		logger.debug("Data correction initiated for schedules between " + startDate + " and " + endDate);
		try{
			ScheduleDAO schDAO = new ScheduleDAO();
			CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn);
			ItemDAO itemDAO = new ItemDAO();
			List<Integer> scheduleList = schDAO.getSchedulesForNonSubscriber(conn, startDate, endDate);
			List<CompetitiveDataDTO> insertList = new ArrayList<CompetitiveDataDTO>();
			logger.info("Number of schedules to correct data " + scheduleList.size());
			for(Integer scheduleId : scheduleList){
				logger.debug("Processing for schedule " + scheduleId);
				// Get competition data and store it with UPC as key
				HashMap<String, CompetitiveDataDTO> compDataMap = compDataDAO.getCompData(conn, scheduleId);
				Set<String> upcSet = new HashSet<String>();
				for(CompetitiveDataDTO compData : compDataMap.values()){
					upcSet.add(compData.upc);
				}
				
				// Get item_Code for upc
				HashMap<String, HashMap<String, Integer>> upcItemCodesMap = itemDAO.getItemCode(conn, upcSet);
				
				for(Map.Entry<String, CompetitiveDataDTO> entry : compDataMap.entrySet()){
					int itemCodeInCompData = entry.getValue().itemcode;
					HashMap<String, Integer> allItemCodesForUPC = upcItemCodesMap.get(entry.getKey());
					for(Integer itemCode : allItemCodesForUPC.values()){
						// Copy and add records for item_code that doesnt exist in competitive_data
						if(itemCode != itemCodeInCompData){
							logger.debug("Data inserted for item code " + itemCode);
							CompetitiveDataDTO tDTO = new CompetitiveDataDTO();
							tDTO.copy(entry.getValue());
							tDTO.itemcode = itemCode;
							insertList.add(tDTO);
						}
					}
				}
				
				// Insert Data
				compDataDAO.insertCompetitiveData(insertList);
				logger.debug("Processing ends for schedule " + scheduleId);
			}
			
			PriceChangeStats pcs = new PriceChangeStats();
			for(Integer scheduleId : scheduleList){
				pcs.calculatePriceChangeStats(conn, scheduleId,0);
			}
			
			String loadLIGValue = PropertyManager.getProperty("DATALOAD.ROLLUPLIG", "FALSE");
			String priceIndexFlagUpdate = PropertyManager.getProperty("DATALOAD.UPDATEPRICEINDEXFLAG", "FALSE");
			
			if (loadLIGValue.equalsIgnoreCase("true")) {
				// LIG Rollup
				CompDataLIGRollup ligRollup = new CompDataLIGRollup();
				for(Integer scheduleId : scheduleList){
					ligRollup.LIGLevelRollUp(conn, scheduleId);
				}
				
				// PI Update
				CompDataPISetup piDataSetup = new CompDataPISetup();
				for(Integer scheduleId : scheduleList){
					piDataSetup.setupSchedule(conn, scheduleId);
				}
			}
			
			try {
				if (priceIndexFlagUpdate.equalsIgnoreCase("true")) {
					for(Integer scheduleId : scheduleList){
						itemDAO.updatePriceIndexFlag(conn, scheduleId);
						logger.info("Price Index Flag Updated for Schedule "
								+ scheduleId);
					}
				}
			} catch (Exception e) {
				logger.error("Error in setting Price Index Enabled Flag ",
						e);
			}
			
			PristineDBUtil.commitTransaction(conn, "Competition Data Correction Complete");
			PristineDBUtil.close(conn);
		}catch(GeneralException ge){
			logger.error("Error when correcting data " + ge);
		}
	}
}
