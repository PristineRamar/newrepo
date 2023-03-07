/*
 * Title: Daily Revenue Correction
 *
 *****************************************************************************************************
 * Modification History
 *----------------------------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *----------------------------------------------------------------------------------------------------
 * Version 0.1	 08-28-2012	    Janani Ramkumar	     Initial  Version 
 * ***************************************************************************************************
 */

package com.pristine.dataload.tops;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RevenueCorrectionDAO;
import com.pristine.dto.ForecastDto;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.ExcelParser;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class DailyRevenueCorrection {
	
	static Logger logger = Logger.getLogger("DailyRevenueCorrection");
	Connection _conn = null; // hold the database connection
	String _rootPath = null; // hold the input file path
	String _relativePath = null; // hold the input file name
	String _sheetNo = null; // hold the input sheet number
	Date _startDate = null; // hold the input start date
	Date _endDate = null; // hold the input end date
	int _startColumn = 0; // hold the input start column
	int _sNumberLength = 0;

	/**
	 * Class Constructor: 
	 * Process 1 : Input Validation 
	 * Process 2 : Get Database Connection 
	 * Argument 1 : File Name (test.xls) 
	 * Argument 2 : Sheet Number (1) 
	 * Argument 3 : Start Column (0)
	 * Argument 4 : Start Date Argument 6 : End Date
	 * @param endDate 
	 * @param startDate 
	 * 
	 * @catch general Exception
	 */
	public DailyRevenueCorrection(String relativePath, String sheetNo,
			String startColumn, Date startDate, Date endDate)
			throws GeneralException {

		if (relativePath == null) {
			logger.error("Invalid Relative path: Relative Path missing in input");
			System.exit(1);
		}

		if (sheetNo == null) {
			logger.error("Invalid Sheet Number : Excel Sheet Number Missing In Input");
			System.exit(1);
		}
		
		if (startColumn == null) {
			logger.error("Invalid Start Column : Start Column Missing in Input");
			System.exit(1);
		}
		
		if( startDate == null){
			logger.error(" Start Date missing ................");
			System.exit(1);
		}
		
		if( endDate == null){
			logger.error(" End Date is missing..................");
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
		_sheetNo = sheetNo;
		_startColumn = Integer.valueOf(startColumn);
		_startDate = startDate;
		_endDate = endDate;
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
	 * Argument 2 : Sheet Number 
	 * Argument 3 : File Type
	 * Argument 4 : Start column 
	 * Argument 5 : Input date 
	 * call the class constructor and initialize the db connection
	 * @throws ParseException 
	 * 
	 * @catch Exception
	 */
	public static void main(String[] args) throws ParseException {
		PropertyConfigurator.configure("log4j-revenue-correction.properties");
		
		logCommand(args);
		String relativePath = null; // hold the input file name
		String sheetNo = null; // hold the input excel sheet number
		String startColumn = null; // hold the input start Column (full process
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		Date endDate = null; // hold the end date
		Date startDate = null;
	 

		DailyRevenueCorrection revenueCorrection = null;
		try {

			// get the file path and file name from user
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the file name
				if (arg.startsWith("RELATIVEPATH")) {
					relativePath = arg.substring("RELATIVEPATH=".length());
				}
				// get the sheet number
				if (arg.startsWith("SHEETINDEX")) {
					sheetNo = arg.substring("SHEETINDEX=".length());
				}


				// get the starting column number
				if (arg.startsWith("STARTCOLUMN")) {
					startColumn = arg.substring("STARTCOLUMN=".length());
				}
				
				if (arg.startsWith("STARTDATE")) {
					String Inputdate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("Start Date Parsing Error, check Input");
					}
				}

				/*
				 * Get the End date if process is for date range This date is
				 * valid only if start date is specified in input
				 */
				if (arg.startsWith("ENDDATE")) {
					String Inputdate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("End Date Parsing Error, checkInput");
					}
				}
					
			}

			// call the class constructor
			revenueCorrection = new DailyRevenueCorrection(relativePath, sheetNo, startColumn ,startDate , endDate);

			// Method used to read the excel
			List<ForecastDto> forecastDTOList = revenueCorrection.readExcel();

			RetailCalendarDAO objCalDao = new RetailCalendarDAO();
			
			
			List<RetailCalendarDTO> dayCalList = objCalDao.dayCalendarList(
					revenueCorrection._conn, startDate, endDate,
					Constants.CALENDAR_DAY);
			
			for (int ii = 0; ii < dayCalList.size(); ii++) {

				RetailCalendarDTO objCalDto = dayCalList.get(ii);
				
				Date processDate = DateUtil.toDate(DateUtil.getWeekEndDate(DateUtil.toDate(objCalDto.getStartDate(), "yyyy-MM-dd")));
				
				revenueCorrection.processRecords(forecastDTOList,
						objCalDto.getStartDate(), objCalDto.getCalendarId(), processDate);
			}
			 
			
			
		} catch (GeneralException exe) {
			logger.error(exe.getMessage());
			PristineDBUtil.rollbackTransaction(revenueCorrection._conn,	"Daily Revenue Correction Rollback");
		} finally {
			try {
			PristineDBUtil.close(revenueCorrection._conn);
			} catch (Exception exe) {
				logger.error(exe.getMessage());
			}

		}
	}
	
	/**
	 * This method processes and updates daily revenue for a store
	 * @param forecastDTOList	List of ForecastDTO
	 */
	private void processRecords(List<ForecastDto> forecastDTOList, String inputDate , int calendar_id, Date processDate) throws GeneralException{
		// Get day of week
		int dayOfWeek = DateUtil.getdayofWeek(inputDate,"yyyy-MM-dd");
		
		
		RevenueCorrectionDAO revenueCorrDAO = new RevenueCorrectionDAO();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		
		// Get Chain Id
		String chainId = retailPriceDAO.getChainId(_conn);
		
		// Get StoreId for Store Numbers
		HashMap<String, Integer> storeIdMap = revenueCorrDAO.getStoreIds(_conn, chainId);
		
		// Get store wise revenue for a day
		HashMap<Integer, Object> storeRevenueMap = getRevenueForStores(storeIdMap, forecastDTOList, dayOfWeek, processDate);
				
		// Update daily revenue
		long startTime = System.currentTimeMillis();
		revenueCorrDAO.updateDailyRevenue(_conn, calendar_id, storeRevenueMap);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to update revenue - " + (endTime - startTime) + " ms");
		
		String startDate = DateUtil.dateToString(_startDate, Constants.APP_DATE_FORMAT);
		
		// Get district wise revenue for a day
		HashMap<Integer, HashSet<String>> districtIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_STOREID, startDate, false);
		HashMap<Integer, HashSet<String>> identicalDistrictIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_DISTRICT, Constants.ROLLUP_COLUMNNAME_STOREID, startDate, true);
		HashMap<Integer, Double> districtRevenueMap = rollupDailyRevenue(districtIdMap, storeRevenueMap);
		HashMap<Integer, Double> identicalDistrictRevenueMap = rollupDailyRevenue(identicalDistrictIdMap, storeRevenueMap);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, districtRevenueMap, Constants.DISTRICT_LEVEL_ID, calendar_id, false);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, identicalDistrictRevenueMap, Constants.DISTRICT_LEVEL_ID, calendar_id, true);
		
		// Get region wise revenue for a day
		HashMap<Integer, HashSet<String>> regionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_REGION, Constants.ROLLUP_COLUMNNAME_REGION, Constants.ROLLUP_COLUMNNAME_STOREID, startDate, false);
		HashMap<Integer, HashSet<String>> identicalRegionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_REGION, Constants.ROLLUP_COLUMNNAME_REGION, Constants.ROLLUP_COLUMNNAME_STOREID, startDate, true);
		HashMap<Integer, Double> regionRevenueMap = rollupDailyRevenue(regionIdMap, storeRevenueMap);
		HashMap<Integer, Double> identicalRegionRevenueMap = rollupDailyRevenue(identicalRegionIdMap, storeRevenueMap);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, regionRevenueMap, Constants.REGION_LEVEL_ID, calendar_id, false);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, identicalRegionRevenueMap, Constants.REGION_LEVEL_ID, calendar_id, true);
		
		// Get division wise revenue for a day
		HashMap<Integer, HashSet<String>> divisionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DIVISION, Constants.ROLLUP_COLUMNNAME_DIVISION, Constants.ROLLUP_COLUMNNAME_STOREID, startDate, false);
		HashMap<Integer, HashSet<String>> identicalDivisionIdMap = revenueCorrDAO.retrieveRollupLevelId(_conn, Constants.ROLLUP_TABLENAME_DIVISION, Constants.ROLLUP_COLUMNNAME_DIVISION, Constants.ROLLUP_COLUMNNAME_STOREID, startDate, true);
		HashMap<Integer, Double> divisionRevenueMap = rollupDailyRevenue(divisionIdMap, storeRevenueMap);
		HashMap<Integer, Double> identicalDivisionRevenueMap = rollupDailyRevenue(identicalDivisionIdMap, storeRevenueMap);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, divisionRevenueMap, Constants.DIVISION_LEVEL_ID, calendar_id, false);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, identicalDivisionRevenueMap, Constants.DIVISION_LEVEL_ID, calendar_id, true);
		
		// Get chain wise revenue for a day
		HashMap<Integer, Double> chainRevenueMap = rollupDailyRevenueAtChain(divisionRevenueMap);
		HashMap<Integer, Double> identicalChainRevenueMap = rollupDailyRevenueAtChain(identicalDivisionRevenueMap);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, chainRevenueMap, Constants.CHAIN_LEVEL_ID, calendar_id, false);
		revenueCorrDAO.updateDailyRollupRevenue(_conn, identicalChainRevenueMap, Constants.CHAIN_LEVEL_ID, calendar_id, true);
		
		PristineDBUtil.commitTransaction(_conn, "Daily Revenue Correction Commit");
	}
	
	private HashMap<Integer, Double> rollupDailyRevenue(HashMap<Integer, HashSet<String>> districtIdMap, HashMap<Integer, Object> storeRevenueMap){
		logger.debug("Inside rollupDailyRevenue() of Daily Revenue Correction");
		
		HashMap<Integer, Double> rolledupRevenueMap = new HashMap<Integer, Double>();
		
		for (Map.Entry<Integer, HashSet<String>> entry : districtIdMap.entrySet()){
			double revenue = 0;
			for(String store : entry.getValue()){
				Integer storeId = Integer.parseInt(store);
				if(storeRevenueMap.get(storeId) != null)
					revenue = revenue + (Double)storeRevenueMap.get(storeId);
			}
			
			rolledupRevenueMap.put(entry.getKey(), revenue);
		}
		
		return rolledupRevenueMap;
	}
	
	private HashMap<Integer, Double> rollupDailyRevenueAtChain(HashMap<Integer, Double> revenueMap) throws GeneralException{
		logger.debug("Inside rollupDailyRevenueAtChain() of Daily Revenue Correction");
		
		HashMap<Integer, Double> rolledupRevenueMap = new HashMap<Integer, Double>();
		
		double revenue = 0;
		for(Double storeRevenue : revenueMap.values()){
			revenue = revenue + storeRevenue;
		}
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		Integer chainId = Integer.parseInt(retailPriceDAO.getChainId(_conn));
		rolledupRevenueMap.put(chainId, revenue);

		return rolledupRevenueMap;
	}
	
	/**
	 * Returns store and its revenue on a day
	 * @param forecastDTOList	List of Forecast DTO
	 * @param dayOfWeek			Day of Week
	 * @param processDate		Week End Date
	 * @return Map containing store as key and its revenue as value
	 */
	private HashMap<Integer, Object> getRevenueForStores(HashMap<String, Integer> storeIdMap, List<ForecastDto> forecastDTOList, int dayOfWeek, Date processDate) {
		HashMap<Integer, Object> revenueMap = new HashMap<Integer, Object>();
		for(ForecastDto forecastDTO : forecastDTOList){
			if(forecastDTO.getProcessDate().compareTo(processDate) == 0){
				if(storeIdMap.get(forecastDTO.getStoreNumber()) != null){
					revenueMap.put(storeIdMap.get(forecastDTO.getStoreNumber()), getRevenue(forecastDTO, dayOfWeek));
				}else{
					logger.info("Store not found in database for store number - " + forecastDTO.getStoreNumber());
				}
			}
		}
		return revenueMap;
	}
	
	/**
	 * Returns appropriate revenue for a store
	 * @param forecastDTO	Forecast DTO
	 * @param dayOfWeek		Day of week
	 * @return Revenue Object
	 */
	private Object getRevenue(ForecastDto forecastDTO, int dayOfWeek){
		Object wog = null;
		switch(dayOfWeek){
			case 1:
				wog = forecastDTO.getSundayWog();
				break;
			case 2:
				wog = forecastDTO.getMondayWog();
				break;
			case 3:
				wog = forecastDTO.getTuesdayWog();
				break;
			case 4:
				wog = forecastDTO.getWednesdayWog();
				break;
			case 5:
				wog = forecastDTO.getThursdayWog();
				break;
			case 6:
				wog = forecastDTO.getFridayWog();
				break;
			case 7:
				wog = forecastDTO.getSaterdayWog();
				break;
		}
		return wog;
	}
	
	/**
	 * Reads and parses input from excel
	 * @return	List of ForecastDTO
	 * @throws GeneralException
	 */
	private List<ForecastDto> readExcel() throws GeneralException {
		
		ExcelParser objParseExcel = new ExcelParser();
		ArrayList<String> fileList = null;
		List<ForecastDto> foreCastList = null;
		try {
			fileList = objParseExcel.getFiles(_rootPath,_relativePath);
		
		int rowNumber = 0;

			
			if (fileList.size() > 0) {
				for (int fL = 0; fL < fileList.size(); fL++) {
					String fileName = fileList.get(fL);
					
					logger.info(" Processing File Name..... " + fileName);
					
					 
					foreCastList = objParseExcel.forecastExcelParser(fileName,
								_sheetNo, _startColumn, _startDate, _endDate,
								_sNumberLength, rowNumber, Constants.DAILY_REVENUE_CORRECTION_MODE);
					 
	
					logger .info("No of records in the file - " + foreCastList.size());
					
					if( foreCastList.size() > 0){
						logger.info("Excel file reading process complete");
						
						/*String archivePath = _rootPath + "/" + _relativePath + "/";
						PrestoUtil.moveFiles(fileList, archivePath+ Constants.COMPLETED_FOLDER);*/
					}
					else{
						logger .info("No records found");
						System.exit(-1);
					}
				}
			}else{
				logger.error("No files available in the relative path");
				System.exit(-1);
			}
			 
		} catch (GeneralException exe) {
			/*String archivePath = _rootPath + "/" + _relativePath + "/";
			PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);*/
			PristineDBUtil.rollbackTransaction(_conn,"Daily Revenue Correction Rollback");
			logger.error("Error in processing " + exe);
					 
		}
		return foreCastList;
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