package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RevenueCorrectionDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CTDRevenueCorrection {
	static Logger logger = Logger.getLogger("CTDRevenueCorrection");
	
	Connection _conn = null;
	String _weekStartNo = null;
	String _weekEndNo = null;
	String _calYear = null;
	
	boolean _updateRevenue = false;
	boolean _updateVisitCount = true;
	
	/**
	 * Class Constructor
	 * Process 1 : Input Validation 
	 * Process 2 : Get Database Connection 
	 * @param weekStartNo	Week Start Number
	 * @param weekEndNo		Week End Number	
	 */
	public CTDRevenueCorrection(String weekStartNo, String weekEndNo, String calYear) {
		
		/*if (weekStartNo == null) {
			logger.error("Week start number missing in input");
			System.exit(1);
		}
		if (weekEndNo == null) {
			logger.error("Week end number missing in input");
			System.exit(1);
		}
		if (calYear == null) {
			logger.error("Calendar Year missing in input");
			System.exit(1);
		}*/
		
		if(weekStartNo != null && weekEndNo != null && calYear == null){
			logger.error("Calendar Year missing in input");
			System.exit(1);
		}
		
		_weekStartNo = weekStartNo;
		_weekEndNo = weekEndNo;
		_calYear = calYear;
		
		// Changes for updating Revenue, Visit Count based on configuration parameters
		String revenueCorrectionParam = PropertyManager.getProperty(Constants.WEEKLY_CORRECTION, "");
		String param[] = revenueCorrectionParam.split(",");
		List<String> paramList = new ArrayList<String>(Arrays.asList(param));
		if(paramList.contains(Constants.REVENUVE_CORRECTION))
			_updateRevenue = true;
		if(paramList.contains(Constants.VISITCOUNT_CORRECTION))
			_updateVisitCount = true;
		
		try {
			// Initialize the db connection
			_conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Connection Failed");
			logger.error(exe);
			System.exit(1);
		}
	}

	/**
	 * Main method
	 * Argument 1 : Week Start Number
	 * Argument 2 : Week End Number
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-revenue-correction.properties");
		PropertyManager.initialize("analysis.properties");
		logCommand(args);
		
		String weekStartNo = null;
		String weekEndNo = null;
		String calYear = null;
		
		for (int iP = 0; iP < args.length; iP++) {
			String arg = args[iP];

			// get starting week number
			if (arg.startsWith("WEEKSTARTNO")) {
				weekStartNo = arg.substring("WEEKSTARTNO=".length());
			}
			// get end week number
			if (arg.startsWith("WEEKENDNO")) {
				weekEndNo = arg.substring("WEEKENDNO=".length());
			}
			// get end week number
			if (arg.startsWith("CALYEAR")) {
				calYear = arg.substring("CALYEAR=".length());
			}
		}		
		
		CTDRevenueCorrection revenueCorrection = new CTDRevenueCorrection(weekStartNo, weekEndNo, calYear);
		revenueCorrection.processCTDData();
	}
	
	private void processCTDData() {
		logger.info("CTD Correction starts");
		
		RevenueCorrectionDAO revenueCorrectionDAO = new RevenueCorrectionDAO();
		try{
			if(_weekStartNo == null)
				_weekStartNo = String.valueOf(1);
			
			if(_weekEndNo == null){
				String weekEndDate = DateUtil.getWeekEndDate(DateUtil.toDate(DateUtil.getWeekEndDate()), "MM/dd/yy");
				RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
				RetailCalendarDTO calendarDTO = retailCalendarDAO.getWeekNo(_conn, weekEndDate);
				_weekEndNo = String.valueOf(calendarDTO.getActualNo());
				_calYear = String.valueOf(calendarDTO.getCalYear());
			}
			
			logger.info("Week End No for CTD Correction: " + _weekEndNo);
			logger.info("Calendar year for CTD Correction: " + _calYear);
			logger.info("Revenue update required: " + _updateRevenue);
			logger.info("Visit Count update required: " + _updateVisitCount);
			
			// Populate map containing week calendar id as key and day calendar id of the corresponding week's end date as value
			long startTime = System.currentTimeMillis();
			HashMap<Integer, Integer> calendarMap = revenueCorrectionDAO.getWeeklyCalendarIdMapping(_conn, _weekStartNo, _weekEndNo, _calYear);
			long endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve calendar ids - " + (endTime - startTime) + "ms");
			logger.info("No of weeks for which calendar id was retrieved - " + calendarMap.size());
			
			// Retrieve finance department data for all weeks for which calendar id was retrieved
			startTime = System.currentTimeMillis();
			HashMap<Integer, HashMap<String, Double[]>> financeDeptMap = revenueCorrectionDAO.getFinanceDeptWeeklyData(_conn, calendarMap.keySet(), false);
			HashMap<Integer, HashMap<String, Double[]>> financeDeptRolledupMap = revenueCorrectionDAO.getFinanceDeptWeeklyData(_conn, calendarMap.keySet(), true);
			HashMap<Integer, HashMap<String, Double[]>> storeDataMap = revenueCorrectionDAO.getStoreLevelWeeklyData(_conn, calendarMap.keySet(), false);
			HashMap<Integer, HashMap<String, Double[]>> storeDataRolledupMap = revenueCorrectionDAO.getStoreLevelWeeklyData(_conn, calendarMap.keySet(), true);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve data for all weeks - " + (endTime - startTime) + "ms");			
			logger.info("No of weeks for which data is present - " + financeDeptMap.size());
			
			// Update finance department data for week to date
			startTime = System.currentTimeMillis();
			if(_updateRevenue){
				revenueCorrectionDAO.updateWeekToDateData(_conn, financeDeptMap, calendarMap, false);
				revenueCorrectionDAO.updateWeekToDateData(_conn, financeDeptRolledupMap, calendarMap, true);
			}
			revenueCorrectionDAO.updateWeekToDateDataForStore(_conn, storeDataMap, calendarMap, false, _updateRevenue, _updateVisitCount);
			revenueCorrectionDAO.updateWeekToDateDataForStore(_conn, storeDataRolledupMap, calendarMap, true, _updateRevenue, _updateVisitCount);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to update week to date data - " + (endTime - startTime) + "ms");			
			PristineDBUtil.commitTransaction(_conn, "CTD Correction Commit");
			
			// Retrieve period to weeks mapping
			startTime = System.currentTimeMillis();
			HashMap<Integer, TreeSet<Integer>> periodToWeeksMap = revenueCorrectionDAO.getCalendarIdMapping(_conn, Constants.CALENDAR_PERIOD, _weekStartNo, _weekEndNo, _calYear);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve periodsToWeeksMap - " + (endTime - startTime) + "ms");
			
			// Update finance department data for period to date
			startTime = System.currentTimeMillis();
			if(_updateRevenue){
				revenueCorrectionDAO.updateCTDDataForFD(_conn, financeDeptMap, periodToWeeksMap, calendarMap, Constants.CTD_PERIOD, false);
				revenueCorrectionDAO.updateCTDDataForFD(_conn, financeDeptRolledupMap, periodToWeeksMap, calendarMap, Constants.CTD_PERIOD, true);
			}
			revenueCorrectionDAO.updateCTDDataForStore(_conn, storeDataMap, periodToWeeksMap, calendarMap, Constants.CTD_PERIOD, false, _updateRevenue, _updateVisitCount);
			revenueCorrectionDAO.updateCTDDataForStore(_conn, storeDataRolledupMap, periodToWeeksMap, calendarMap, Constants.CTD_PERIOD, true, _updateRevenue, _updateVisitCount);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to update period to date data - " + (endTime - startTime) + "ms");
			
			// Retrieve quarter to weeks mapping
			periodToWeeksMap = null;
			startTime = System.currentTimeMillis();
			HashMap<Integer, TreeSet<Integer>> quarterToWeeksMap = revenueCorrectionDAO.getCalendarIdMapping(_conn, Constants.CALENDAR_QUARTER, _weekStartNo, _weekEndNo, _calYear);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve periodsToWeeksMap - " + (endTime - startTime) + "ms");
			
			// Update finance department data for quarter to date
			startTime = System.currentTimeMillis();
			if(_updateRevenue){
				revenueCorrectionDAO.updateCTDDataForFD(_conn, financeDeptMap, quarterToWeeksMap, calendarMap, Constants.CTD_QUARTER, false);
				revenueCorrectionDAO.updateCTDDataForFD(_conn, financeDeptRolledupMap, quarterToWeeksMap, calendarMap, Constants.CTD_QUARTER, true);
			}
			revenueCorrectionDAO.updateCTDDataForStore(_conn, storeDataMap, quarterToWeeksMap, calendarMap, Constants.CTD_QUARTER, false, _updateRevenue, _updateVisitCount);
			revenueCorrectionDAO.updateCTDDataForStore(_conn, storeDataRolledupMap, quarterToWeeksMap, calendarMap, Constants.CTD_QUARTER, true, _updateRevenue, _updateVisitCount);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to update quarter to date data - " + (endTime - startTime) + "ms");
						
			// Retrieve year to weeks mapping
			startTime = System.currentTimeMillis();
			quarterToWeeksMap = null;
			HashMap<Integer, TreeSet<Integer>> yearToWeeksMap = revenueCorrectionDAO.getCalendarIdMapping(_conn, Constants.CALENDAR_YEAR, _weekStartNo, _weekEndNo, _calYear);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve periodsToWeeksMap - " + (endTime - startTime) + "ms");						
			
			// Update finance department data for year to date
			startTime = System.currentTimeMillis();
			if(_updateRevenue){
				revenueCorrectionDAO.updateCTDDataForFD(_conn, financeDeptMap, yearToWeeksMap, calendarMap, Constants.CTD_YEAR, false);
				revenueCorrectionDAO.updateCTDDataForFD(_conn, financeDeptRolledupMap, yearToWeeksMap, calendarMap, Constants.CTD_YEAR, true);
			}
			revenueCorrectionDAO.updateCTDDataForStore(_conn, storeDataMap, yearToWeeksMap, calendarMap, Constants.CTD_YEAR, false, _updateRevenue, _updateVisitCount);
			revenueCorrectionDAO.updateCTDDataForStore(_conn, storeDataRolledupMap, yearToWeeksMap, calendarMap, Constants.CTD_YEAR, true, _updateRevenue, _updateVisitCount);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to update year to date data - " + (endTime - startTime) + "ms");
			
			PristineDBUtil.commitTransaction(_conn, "CTD Correction Commit");
		}catch(GeneralException exception){
			logger.error("Error in CTD data processing - " + exception);
		}
	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Sales Forecast Loader ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
}
