/*
 * Title: Weekly Revenue Correction for Finance Department
 *
 *****************************************************************************************************
 * Modification History
 *----------------------------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *----------------------------------------------------------------------------------------------------
 * Version 0.1	 08-29-2012	    Janani Ramkumar	     Initial  Version 
 * ***************************************************************************************************
 */

package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RevenueCorrectionDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.ExcelParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class WeeklyRevenueCorrection extends PristineFileParser{
	static Logger logger = Logger.getLogger("WeeklyRevenueCorrection");
	Connection _conn = null; // hold the database connection
	String _rootPath = null; // hold the input file path
	String _relativePath = null; // hold the input file name
	String _weekNo = null; // hold the input week number
	String _calendarYear = null; // hold the input calendar year
	int _sNumberLength = 0;
	
	boolean _updateRevenue = false;
	boolean _updateVisitCount = true;
	
	/**
	 * Class Constructor: 
	 * Process 1 : Input Validation 
	 * Process 2 : Get Database Connection 
	 * Argument 1 : File Name (test.xls) 
	 * Argument 2 : Sheet Number (6) 
	 * Argument 3 : Week Number (1)
	 * Argument 4 : Calendar Year (2012)
	 * 
	 * @catch general Exception
	 */
	public WeeklyRevenueCorrection(String relativePath, String weekNo, String calendarYear)
			throws GeneralException {

		if (relativePath == null) {
			logger.error("Invalid Relative path: Relative Path missing in input");
			System.exit(1);
		}
		
		/*if (weekNo == null) {
			logger.error("Invalid Week Number : Week Number Missing in Input");
			System.exit(1);
		}
		
		if (calendarYear == null) {
			logger.error("Invalid Calendar Year : Calendar Year Missing in Input");
			System.exit(1);
		}*/
		
		if(weekNo != null && calendarYear == null){
			logger.error("Invalid Calendar Year : Calendar Year Missing in Input");
			System.exit(1);
		}
		
		PropertyManager.initialize("analysis.properties");

		// get the base file path from properties file
		_rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");

		// read the store number length from property file
		_sNumberLength = Integer.valueOf(PropertyManager.getProperty("DATALOADER.STORENUMBERLENGTH", "4"));

		// Check the base path availability
		if (_rootPath.equalsIgnoreCase("")) {
			logger.error("Invalid configuration: Excel file path missing");
			System.exit(1);
		}
		_relativePath = relativePath;
		_weekNo = weekNo;
		_calendarYear = calendarYear;
		
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
		} catch (Exception exe) {
			logger.error("Connection Failed");
			logger.error(exe);
			System.exit(1);

		}
	}
	
	/**
	 * Main Method
	 * Argument 1 : Relative File Path
	 * Argument 2 : Sheet Index 
	 * Argument 3 : Week Number
	 * call the class constructor and initialize the db connection
	 * 
	 * @catch Exception
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-revenue-correction.properties");
		
		logCommand(args);
		String relativePath = null; // hold the input file name
		String weekNo = null; // hold the input week no
		String calendarYear = null; // hold the input calendar year
		
		WeeklyRevenueCorrection revenueCorrection = null;
		try {

			// get the file path and file name from user
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the file name
				if (arg.startsWith("RELATIVEPATH")) {
					relativePath = arg.substring("RELATIVEPATH=".length());
				}
				// get the starting column number
				if (arg.startsWith("WEEKNO")) {
					weekNo = arg.substring("WEEKNO=".length());
				}
				// get the starting column number
				if (arg.startsWith("CALYEAR")) {
					calendarYear = arg.substring("CALYEAR=".length());
				}
			}

			// call the class constructor
			revenueCorrection = new WeeklyRevenueCorrection(relativePath, weekNo, calendarYear);

			revenueCorrection.processWeeklyFiles();
			
		} catch (GeneralException exe) {
			logger.error(exe.getMessage());
			PristineDBUtil.rollbackTransaction(revenueCorrection._conn,	"Weekly Revenue Correction Rollback");
		} finally {
			try {
			PristineDBUtil.close(revenueCorrection._conn);
			} catch (Exception exe) {
				logger.error(exe.getMessage());
			}
		}
	}
	
	public void processWeeklyFiles() throws GeneralException{
		ExcelParser objParseExcel = new ExcelParser();
		
		// Changes to allow processing of zip files
		//getzip files
		ArrayList<String> zipFileList = getZipFiles(_relativePath);
		
		//Start with -1 so that if any regular files are present, they are processed first
		int curZipFileCount = -1;
		boolean processZipFile = false;
		
		String zipFilePath = _rootPath + "/" + _relativePath;
			
		do {
			ArrayList<String> fileList = null;
			boolean commit = true;

			try {
				if( processZipFile)
					PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
				// Changes to allow processing of zip files - Ends
						
				fileList = objParseExcel.getFiles(_rootPath,_relativePath);
					
				if (fileList.size() > 0) {
						
					for (int fL = 0; fL < fileList.size(); fL++) {
						String fileName = fileList.get(fL);
						logger.info("Processing File: " + fileName);		
						if(_weekNo == null || _calendarYear == null){
							String date = objParseExcel.getDateInWeeklyReport(fileName);
							RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
							RetailCalendarDTO calendarDTO = calendarDAO.getWeekNo(_conn, date);
							_weekNo = String.valueOf(calendarDTO.getActualNo());
							_calendarYear = String.valueOf(calendarDTO.getCalYear());
						}
						logger.info("Processing for Week: " + _weekNo);
						logger.info("Processing for Year: " + _calendarYear);
						
						// Method used to read the excel
						HashMap<String, Double[]> revenueMap = readExcel(fileName, Constants.WEEKLY_REVENUE_CORRECTION_FD);
						HashMap<String, Double[]> storeDataMap = readExcel(fileName, Constants.WEEKLY_REVENUE_CORRECTION_STORE);
						
						if(!(revenueMap.size() > 0 && storeDataMap.size() > 0)){
							logger.info("No records found for weekly correction");
						}else{
							processRecords(revenueMap, storeDataMap);
						}
					}
				}else{
					logger.error("File Not avilable in Relative path");
				}
						 
			} catch (GeneralException exe) {
				PristineDBUtil.rollbackTransaction(_conn,"Rollback Weekly Correction");
				commit = false;
				logger.error("Error in weekly correction" + exe);
								 
			}
					
			// Changes to allow processing of zip files
			if( processZipFile){
			   	PrestoUtil.deleteFiles(fileList);
			   	fileList.clear();
			   	fileList.add(zipFileList.get(curZipFileCount));
			}
				
			String archivePath = getRootPath() + "/" + _relativePath + "/";
				    
			if( commit ){
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			}
			else{
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			}
			curZipFileCount++;
			processZipFile = true;
		}while (curZipFileCount < zipFileList.size());
		// Changes to allow processing of zip files - Ends

	}
	
	/**
	 * Processes and updates weekly revenue for finance departments
	 * @param revenueMap	Map containing store numbers as key and Array of revenues for finance department as value
	 * @param weekNo		Week Number to be processed
	 * @throws GeneralException
	 */
	private void processRecords(HashMap<String, Double[]> revenueMap, HashMap<String, Double[]> storeDataMap) throws GeneralException{
		RevenueCorrectionDAO revenueCorrDAO = new RevenueCorrectionDAO();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		
		// Get calendar id for the given day
		RetailCalendarDTO calendarDTO = revenueCorrDAO.getCalendarId(_conn, _weekNo, _calendarYear);
		int calendarId = calendarDTO.getCalendarId();
		// Get Chain Id
		String chainId = retailPriceDAO.getChainId(_conn);
		
		// Get StoreId for Store Numbers
		HashMap<String, Integer> storeIdMap = revenueCorrDAO.getStoreIds(_conn, chainId);
		
		// Update revenue for finance departments
		if(_updateRevenue){
			long startTime = System.currentTimeMillis();
			revenueCorrDAO.updateWeeklyRevenueForFinanceDept(_conn, calendarId, storeIdMap, revenueMap);
			long endTime = System.currentTimeMillis();
			logger.info("Time taken to update revenue for finance dept- " + (endTime - startTime) + " ms");
		}
		
		// Update revenue for stores
		long startTime = System.currentTimeMillis();
		revenueCorrDAO.updateWeeklyVisitCount(_conn, calendarId, storeIdMap, storeDataMap, _updateRevenue, _updateVisitCount);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to update weekly data for store- " + (endTime - startTime) + " ms");
		
		PristineDBUtil.commitTransaction(_conn, "Weekly Correction Commit");
		
		HashMap<String, Integer> productIdMap = revenueCorrDAO.getProductGroup(_conn, Constants.FINANCEDEPARTMENT);
		
		// Weekly Rollup starts
		// District Level Rollup begins
		startTime = System.currentTimeMillis();
		HashMap<Integer, HashSet<String>> districtIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_STORE, calendarDTO.getStartDate(), false);
		HashMap<Integer, HashSet<String>> identicalDistrictIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_STORE, calendarDTO.getStartDate(), true);
		
		HashMap<Integer, Double[]> fdRevenueMapForDistrictRollup = revenueCorrDAO.rollupStoreDataForFD(revenueMap, districtIdMap);
		HashMap<Integer, Double[]> fdRevenueMapForDistrictRollupId = revenueCorrDAO.rollupStoreDataForFD(revenueMap, identicalDistrictIdMap);
		HashMap<Integer, Double[]> storeMapForDistrictRollup = revenueCorrDAO.rollupStoreData(storeDataMap, districtIdMap);
		HashMap<Integer, Double[]> storeMapForDistrictRollupId = revenueCorrDAO.rollupStoreData(storeDataMap, identicalDistrictIdMap);
		
		if(_updateRevenue){
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForDistrictRollup, productIdMap, Constants.DISTRICT_LEVEL_ID, false);
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForDistrictRollupId, productIdMap, Constants.DISTRICT_LEVEL_ID, true);
		}
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForDistrictRollup, Constants.DISTRICT_LEVEL_ID, false, _updateRevenue, _updateVisitCount);
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForDistrictRollupId, Constants.DISTRICT_LEVEL_ID, true, _updateRevenue, _updateVisitCount);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for district level rollup- " + (endTime - startTime) + " ms");
		PristineDBUtil.commitTransaction(_conn, "District Rollup Correction Commit");
		// District Level Rollup Ends
		
		// Region Level Rollup begins
		startTime = System.currentTimeMillis();
		HashMap<Integer, HashSet<String>> regionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_REGION, Constants.ROLLUP_COLUMNNAME_REGION, Constants.ROLLUP_COLUMNNAME_STORE, calendarDTO.getStartDate(), false);
		HashMap<Integer, HashSet<String>> identicalRegionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_REGION, Constants.ROLLUP_COLUMNNAME_REGION, Constants.ROLLUP_COLUMNNAME_STORE, calendarDTO.getStartDate(), true);
		HashMap<Integer, Double[]> fdRevenueMapForRegionRollup = revenueCorrDAO.rollupStoreDataForFD(revenueMap, regionIdMap);
		HashMap<Integer, Double[]> fdRevenueMapForRegionRollupId = revenueCorrDAO.rollupStoreDataForFD(revenueMap, identicalRegionIdMap);
		HashMap<Integer, Double[]> storeMapForRegionRollup = revenueCorrDAO.rollupStoreData(storeDataMap, regionIdMap);
		HashMap<Integer, Double[]> storeMapForRegionRollupId = revenueCorrDAO.rollupStoreData(storeDataMap, identicalRegionIdMap);
		
		if(_updateRevenue){
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForRegionRollup, productIdMap, Constants.REGION_LEVEL_ID, false);
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForRegionRollupId, productIdMap, Constants.REGION_LEVEL_ID, true);
		}
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForRegionRollup, Constants.REGION_LEVEL_ID, false, _updateRevenue, _updateVisitCount);
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForRegionRollupId, Constants.REGION_LEVEL_ID, true, _updateRevenue, _updateVisitCount);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for region level rollup - " + (endTime - startTime) + " ms");
		PristineDBUtil.commitTransaction(_conn, "Region Rollup Correction Commit");
		// Region Level Rollup Ends
		
		// Divison Level Rollup Begins
		startTime = System.currentTimeMillis();
		HashMap<Integer, HashSet<String>> divisionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DIVISION, Constants.ROLLUP_COLUMNNAME_DIVISION, Constants.ROLLUP_COLUMNNAME_STORE, calendarDTO.getStartDate(), false);
		HashMap<Integer, HashSet<String>> identicalDivisionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DIVISION, Constants.ROLLUP_COLUMNNAME_DIVISION, Constants.ROLLUP_COLUMNNAME_STORE, calendarDTO.getStartDate(), true);
		HashMap<Integer, Double[]> fdRevenueMapForDivisionRollup = revenueCorrDAO.rollupStoreDataForFD(revenueMap, divisionIdMap);
		HashMap<Integer, Double[]> fdRevenueMapForDivisionRollupId = revenueCorrDAO.rollupStoreDataForFD(revenueMap, identicalDivisionIdMap);
		HashMap<Integer, Double[]> storeMapForDivisionRollup = revenueCorrDAO.rollupStoreData(storeDataMap, divisionIdMap);
		HashMap<Integer, Double[]> storeMapForDivisionRollupId = revenueCorrDAO.rollupStoreData(storeDataMap, identicalDivisionIdMap);
		
		if(_updateRevenue){
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForDivisionRollup, productIdMap, Constants.DIVISION_LEVEL_ID, false);
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForDivisionRollupId, productIdMap, Constants.DIVISION_LEVEL_ID, true);
		}
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForDivisionRollup, Constants.DIVISION_LEVEL_ID, false, _updateRevenue, _updateVisitCount);
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForDivisionRollupId, Constants.DIVISION_LEVEL_ID, true, _updateRevenue, _updateVisitCount);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for division level rollup - " + (endTime - startTime) + " ms");
		PristineDBUtil.commitTransaction(_conn, "Division Rollup Correction Commit");
		// Division Level Rollup Ends
		
		// Chain Level Rollup Starts
		startTime = System.currentTimeMillis();
		HashMap<Integer, Double[]> fdRevenueMapForChainRollup = revenueCorrDAO.rollUpChainLevelDataForFD(fdRevenueMapForDivisionRollup);
		HashMap<Integer, Double[]> fdRevenueMapForChainRollupId = revenueCorrDAO.rollUpChainLevelDataForFD(fdRevenueMapForDivisionRollupId);
		HashMap<Integer, Double[]> storeMapForChainRollup = revenueCorrDAO.rollupChainLevelData(storeMapForDivisionRollup);
		HashMap<Integer, Double[]> storeMapForChainRollupId = revenueCorrDAO.rollupChainLevelData(storeMapForDivisionRollupId);
		
		if(_updateRevenue){
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForChainRollup, productIdMap, Constants.CHAIN_LEVEL_ID, false);
			revenueCorrDAO.updateRolledupWeeklyRevenue(_conn, calendarId, fdRevenueMapForChainRollupId, productIdMap, Constants.CHAIN_LEVEL_ID, true);
		}
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForChainRollup, Constants.CHAIN_LEVEL_ID, false, _updateRevenue, _updateVisitCount);
		revenueCorrDAO.updateWeeklyRollupVisitCount(_conn, calendarId, storeMapForChainRollupId, Constants.CHAIN_LEVEL_ID, true, _updateRevenue, _updateVisitCount);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for chain level rollup - " + (endTime - startTime) + " ms");
		// Chain Level Rollup Ends
		
		PristineDBUtil.commitTransaction(_conn, "Chain Rollup Correction Commit");
	}
	
	/**
	 * Reads and parses input from excel
	 * @return	HashMap containing Store Number as key and revenue of financial departments as an array of Double
	 * @throws GeneralException
	 */
	private HashMap<String, Double[]> readExcel(String fileName, String processingMode) throws GeneralException {
		
		ExcelParser objParseExcel = new ExcelParser();
		HashMap<String, Double[]> revenueMap = null;
		
		int rowNumber = 0;

		revenueMap = objParseExcel.weeklyReportExcelParser(fileName, _sNumberLength, rowNumber, processingMode);
					 
		logger.info("No of records in the file - " + revenueMap.size());

		return revenueMap;
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

	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
		
	}
}
