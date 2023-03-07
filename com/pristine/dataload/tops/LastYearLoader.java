/*
  * Title: TOPS   Last Year LastYear Loader
  *
  *************************************************************************************
  * Modification History
  *------------------------------------------------------------------------------
  * Version		  Date		    Name			     Remarks
  *------------------------------------------------------------------------------
  * Version 0.1	 04-05-2012	    Dinesh Kumar V	     Initial  Version 
  * Version 0.2   21-05-2012     Dinesh Kumar V       add the new dto for total revenue.
  * Version 0.2   28-08-2012     Dinesh Kumar V		 chain support code added
  **************************************************************************************
*/

package com.pristine.dataload.tops;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.LastYearDAO;
import com.pristine.dto.ForecastDto;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.ExcelParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LastYearLoader {

	static Logger logger 	= Logger.getLogger("LastYearLoader");
	Connection _conn 		= null; // hold the database connection
	String _rootPath 		= null; // hold the input file path
	String _relativePath 	= null; // hold the input file name
	String _sheetName 		= null; // hold the input sheet Name
	Date _processYear 		= null; // hold the processing year
	Date _startdate 		= null; // hold the input start date
	Date _endDate 			= null; // hold the input end date
	int _startColumn 		= 0; // hold the input start column
	int _sNumebrLength 		= 0;

	/*
	 * Class Constructor: Process 1 : Input Validation Process 2 : Get Database
	 * Connection Argument 1 : File Name (test.xls) Argument 2 : Sheet Name
	 * (test) Argument 3 : File Type (WG / WOG) Argument 4 : Start Column (10)
	 * Argument 5 : Start Date Argument 6 : End Date
	 * 
	 * @catch general Exception
	 */
	public LastYearLoader(String relativePath, String sheetName,
			String startColumn, Date startDate, Date endDate)
			throws GeneralException {

		if (relativePath == null) {
			logger.error("Invalid Relative path: Relative Path missing in input");
			System.exit(1);
		}

		if (sheetName == null) {
			logger.error("Invalid Sheet Name : Excel Sheet Name Missing In Input");
			System.exit(1);
		}

		if (startColumn == null) {
			logger.error("Invalid Start Column : Start Column Missing in Input");
			System.exit(1);
		}
		if (startDate == null) {
			logger.error("Invalid Start Date: Start date missing in Input");
			System.exit(1);
		}

		/*
		 * // Check given file is xls or not String fileExtension =
		 * fileName.substring(fileName.lastIndexOf("."),
		 * fileName.length()).trim(); if
		 * (!fileExtension.toUpperCase().equalsIgnoreCase(".XLS")) {
		 * logger.error("File Format Error"); System.exit(1); }
		 */
		PropertyManager.initialize("analysis.properties");

		// get the base file path from properties file
		_rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");

		// read the store number length from property file
		_sNumebrLength = Integer.valueOf(PropertyManager.getProperty(
				"DATALOADER.STORENUMBERLENGTH", "4"));

		// Check the base path availability
		if (_rootPath.equalsIgnoreCase("")) {
			logger.error("Invalid configuration: Excel file path missing");
			System.exit(1);
		}
		_relativePath = relativePath;
		_sheetName = sheetName;
		_startColumn = Integer.valueOf(startColumn);
		_startdate = startDate;
		_endDate = endDate;
		try {

			// Initialize the db connection
			_conn = DBManager.getConnection();
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Connection Failed", exe);

		}
	}

	/*
	 * Main Method of the batch.method mainly used get the initial arguments .
	 * Argument 1 : File Name Argument 2 : Sheet Number Argument 3 : File Type
	 * Argument 4 : Start column Argument 5 : Start date Argument 6 : End Date
	 * call the class constructor and initialize the db connection
	 * 
	 * @catch Exception
	 */

	public static void main(String[] args) throws GeneralException {

		PropertyConfigurator.configure("log4j-LastYear-loader.properties");
		logger.info("Lastyear Loader Enter into process");
		logCommand(args);
		String relativePath = null; // hold the input file name
		String sheetName = null; // hold the input excl sheet number
		String startColumn = null; // hold the input start Column (full process
									// / partial process )
		Date startDate = null; // hold the input start date (Process start date)
		Date endDate = null; // hold the input end date (Process end date)

		LastYearLoader lastYearLoader = null;
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy"); // Date
																	// format

		try {

			// get the file path and file name from user
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the file name
				// get the file name
				if (arg.startsWith("RELATIVEPATH")) {
					relativePath = arg.substring("RELATIVEPATH=".length());
				}
				// get the sheet number
				if (arg.startsWith("SHEETINDEX")) {
					sheetName = arg.substring("SHEETINDEX=".length());
				}

				// get the starting column number
				if (arg.startsWith("STARTCOLUMN")) {
					startColumn = arg.substring("STARTCOLUMN=".length());
				}

				// get the process start date
				if (arg.startsWith("STARTDATE")) {
					String date = arg.substring("STARTDATE=".length());
					if (date != null) {
						startDate = dateFormat.parse(date);
					}
				}

				// get the process end date (optional)
				if (arg.startsWith("ENDDATE")) {
					String date = arg.substring("ENDDATE=".length());
					if (date != null) {
						endDate = dateFormat.parse(date);
					}

				}

			}

			// call the class constructor
			lastYearLoader = new LastYearLoader(relativePath, sheetName,
					startColumn, startDate, endDate);

			// Method used to read the excel and insert the values into
			// salesforeCast table
			lastYearLoader.readExcel();

			PristineDBUtil.commitTransaction(lastYearLoader._conn,
					"Process Commited");

		} catch (Exception exe) {
			logger.error(exe);
			PristineDBUtil.rollbackTransaction(lastYearLoader._conn,
					"Sales foreCast Loder Rollback");
		} finally {
			try {
				PristineDBUtil.close(lastYearLoader._conn);
			} catch (Exception exe) {
				logger.error(exe.getMessage());
			}

		}

	}

	/*
	 * Method used read the excel and add the excel values into list Argument 1
	 * : File Name Argument 2 : File Path Argument 3 : Sheet Number Argument 4 :
	 * Connection
	 * 
	 * @catch FileNotFoundException ,Exception
	 */

	private void readExcel() throws GeneralException {

		// Check the file availability

		ExcelParser objParseExcel = new ExcelParser();

		ArrayList<String> fileList = null;

		try {
			fileList = objParseExcel.getFiles(_rootPath, _relativePath);

			int rowNumber = 0;

			if (fileList.size() > 0) {

				for (int fL = 0; fL < fileList.size(); fL++) {
					String fileName = fileList.get(fL);

					logger.info(" Processing File Name..... " + fileName);
					List<ForecastDto> lastYearList = null;

					lastYearList = objParseExcel.forecastExcelParser(fileName,
							_sheetName, _startColumn, _startdate, _endDate,
							_sNumebrLength, rowNumber, "LASTYEARLOADER");

					logger.info("Excel file reading process completed");

					// call the process method to process the excel records
					// and insert the records into table
					// get the calendar id for given process date ( First
					// Process
					// Day );
					// Get store id for given store number

					processSalesForeCastDaily(lastYearList);

					String archivePath = _rootPath + "/" + _relativePath + "/";
					PrestoUtil.moveFiles(fileList, archivePath
							+ Constants.COMPLETED_FOLDER);
				}
			} else {
				logger.error("File Not avilable in Relative path......");
			}

		} catch (GeneralException exe) {
			String archivePath = _rootPath + "/" + _relativePath + "/";
			PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			PristineDBUtil.rollbackTransaction(_conn,
					"Sales foreCast Loder Rollback");
			logger.error(" Error in processing..... " + exe);
			throw new GeneralException("Error in processing..... ", exe);

		}

	}

	/*
	 * Method mainly used to iterate the SalesforeCast list. get the calendar id
	 * for given process date get the store Id number for given store . finally
	 * call the insert method to insert the process ( Batch Insert ). Argument 1
	 * : List<ForecastDto> List
	 * 
	 * @ throws GeneralException , SqlException
	 */

	private void processSalesForeCastDaily(List<ForecastDto> foreCastList)
			throws GeneralException {

		// object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// object for CompStroreDao
		CompStoreDAO objCompStoreDao = new CompStoreDAO();

		int count = 0;

		// Object for SalesForcastDao
		LastYearDAO objLastYearDao = new LastYearDAO();
		try {

			PreparedStatement insertPsmt = _conn
					.prepareStatement(objLastYearDao
							.forecastInsert("SALES_AGGR_DAILY"));

			PreparedStatement updatePsmt = _conn
					.prepareStatement(objLastYearDao.forecastUpdate());

			// hold the Day calendar Id list
			HashMap<String, Integer> calendarList = new HashMap<String, Integer>();

			// hold the CompStrId list

			logger.info(" Get Locationid list process starts");
			HashMap<String, Integer> locationList = objCompStoreDao
					.getCompStrId(_conn);
			logger.info(" Get Locationid process Ends .... List Size .."
					+ locationList.size());

			HashMap<String, String> previousForecast = null;

			logger.info(" LastYear Store Level records process starts");

			for (int ii = 0; ii < foreCastList.size(); ii++) {

				ForecastDto objForCastDto = foreCastList.get(ii);

				// logger.info(" Get process Year.... " +
				// objForCastDto.getProcessDate());

				if (ii == 0) {
					_processYear = objForCastDto.getProcessDate();
				}

				// Check the calendar list availability
				if (!calendarList.containsKey(objForCastDto.getProcessDate()
						+ "_" + 1)) {
					// get the calendar List for a given week
				 calendarList = objCalendarDao.excelBasedCalendarList(_conn,
						 				objForCastDto.getProcessDate(),
						 				Constants.CALENDAR_DAY, 0, _processYear);

				previousForecast = objLastYearDao.getPreviousForecast(
							_conn, calendarList, Constants.STORE_LEVEL_ID);
				}

				if (locationList.containsKey(objForCastDto.getStoreNumber())) {
				
					objForCastDto.setLocationId(locationList.get(objForCastDto
														.getStoreNumber()));
					
																	
					String storeStatus =  objLastYearDao.getStoreStatus(_conn , objForCastDto.getProcessDate() ,
								objForCastDto.getStoreNumber() , Constants.CALENDAR_WEEK);

					// call the addSqlbatch method to add the values into batch
					objLastYearDao.addDayBatch(insertPsmt, updatePsmt,
							previousForecast, calendarList, objForCastDto , storeStatus);
				}
			}

			try {
				// call the execute batch method to insert the daily records.
				count = objLastYearDao.excuteBatch(insertPsmt,
						"Last Year Day Level Insert");
				count = objLastYearDao.excuteBatch(updatePsmt,
						"Last Year Level Update");
			} catch (GeneralException gex) {
				throw new GeneralException(" Process Error...........", gex);
			}

			if (count != 0) {

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");

				logger.info("Last Year Daily Records inserted sucussfully");

				logger.info(" Delete Previous  Daily Rollup Aggregation ");
				objLastYearDao.deleteDailyForecast(_conn, _processYear);
				logger.info(" Delete Process Ends");

				// used to insert the district / region / division / chain level
				// forecast data

				logger.info(" Lastyear Daily Rollup starts");
				dailyLastYear(objLastYearDao);
				logger.debug("Last Year Daily Rollup Ends");

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");

				logger.info(" Delete Previous Week,Period,Quarter,year aggregation");
				objLastYearDao.deleteWeeklyLastYear(_conn, _processYear);
				logger.info(" Delete Process Ends");

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");

				logger.info(" Lastyear weekly Aggregation Records starts");
				// method used to insert the weekly forecast data
				weeklyLastYear(objCalendarDao, objLastYearDao);
				logger.debug("Lastyear Weekly Aggregation Ends");

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");
				// method used to insert the period forecast data
				periodLastYear(objCalendarDao, objLastYearDao);

				logger.debug("Last Year Period Records Insertd Sucussfully");

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");
				// method used to get the quarter forecast data
				quarterLastYear(objCalendarDao, objLastYearDao);
				logger.debug("Last Year Quarter Records Insertd sucussfully");

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");
				// method used to get the year forecast date
				yearLastYear(objCalendarDao, objLastYearDao);
				logger.debug("Last Year Year Records Insertd sucussfully");

				PristineDBUtil
						.commitTransaction(_conn, "Commit the transation");
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Process Error In daily", exe);
		}

	}

	/*
	 * Method mainly used to get the quarter forecast records for store /
	 * district / region / division level and insert the forecast records
	 * Argument 1 : RetailCalendarDAO Argument 2 : SalesForecastDAO Argument 3 :
	 * Calendar quarter constant Argument 4 : Store / District / Region /
	 * Division Level Id Argument 5 : Ctd week constant
	 * 
	 * @catch Exception
	 * 
	 * @throws GeneralException
	 */

	private boolean yearLastYear(RetailCalendarDAO objCalendarDao,
			LastYearDAO objSalesForecastDao) throws ParseException,
			GeneralException {

		boolean insertFlag = false;
		// logger.debug("Year Forecast Starts");
		// for year aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.STORE_LEVEL_ID,
				Constants.CTD_YEAR, "SALES_AGGR", "SALES_AGGR_DAILY",
				"LastYear Store Level year Aggrgetion");
		
		// for district
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.DISTRICT_LEVEL_ID,
				Constants.CTD_YEAR, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear District Level year Aggrgetion");

		// for region
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.REGION_LEVEL_ID,
				Constants.CTD_YEAR, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Region Level year Aggrgetion");

		// for division
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.DIVISION_LEVEL_ID,
				Constants.CTD_YEAR, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Division Level year Aggrgetion");
		
		// for chain
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.CHAIN_LEVEL_ID,
				Constants.CTD_YEAR, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear chain Level year Aggrgetion");
		
		// logger.debug("Year Forecast Ends");
		return insertFlag;
	}

	private boolean quarterLastYear(RetailCalendarDAO objCalendarDao,
			LastYearDAO objSalesForecastDao) throws ParseException,
			GeneralException {

		boolean insertFlag = false;

		// logger.debug("Quarter Forecast Starts");
		// for quarter aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.STORE_LEVEL_ID,
				Constants.CTD_QUARTER, "SALES_AGGR", "SALES_AGGR_DAILY",
				"LastYear store Level Quarter Aggrgetion");
		
		// for district
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.DISTRICT_LEVEL_ID,
				Constants.CTD_QUARTER, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear District Level Quarter Aggrgetion");

		// for region
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.REGION_LEVEL_ID,
				Constants.CTD_QUARTER, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Region Level Quarter Aggrgetion");

		// for division
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.DIVISION_LEVEL_ID,
				Constants.CTD_QUARTER, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Division Level Quarter Aggrgetion");

		// for chain
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.CHAIN_LEVEL_ID,
				Constants.CTD_QUARTER, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Chain Level Quarter Aggrgetion");
		
		// logger.debug("Quarter Forecast Ends");
		return insertFlag;
	}

	/*
	 * Method mainly used to get the period forecast records for store /
	 * district / region / division level and insert the forecast records
	 * Argument 1 : RetailCalendarDAO Argument 2 : SalesForecastDAO Argument 3 :
	 * Calendar period constant Argument 4 : Store / District / Region /
	 * Division Level Id Argument 5 : Ctd week constant
	 * 
	 * @catch Exception
	 * 
	 * @throws GeneralException
	 */

	private boolean periodLastYear(RetailCalendarDAO objCalendarDao,
			LastYearDAO objSalesForecastDao) throws ParseException,
			GeneralException {

		boolean insertFlag = false;

		// for period aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.STORE_LEVEL_ID,
				Constants.CTD_PERIOD, "SALES_AGGR", "SALES_AGGR_DAILY",
				"LastYear Store Level Period Aggrgetion");
		
		// for district
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.DISTRICT_LEVEL_ID,
				Constants.CTD_PERIOD, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear District Level Period Aggrgetion");
		
		// for region
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.REGION_LEVEL_ID,
				Constants.CTD_PERIOD, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Region Level Period Aggrgetion");

		// for division 
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.DIVISION_LEVEL_ID,
				Constants.CTD_PERIOD, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear Division Level Period Aggrgetion");
		
		// for chain
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.CHAIN_LEVEL_ID,
				Constants.CTD_PERIOD, "SALES_AGGR_ROLLUP",
				"SALES_AGGR_DAILY_ROLLUP",
				"LastYear chain Level Period Aggrgetion");

		// logger.debug("Period Forecast Ends");
		return insertFlag;
	}

	/*
	 * Method mainly used to get the weekly forecast records for store /
	 * district / region / division level and insert the forecast records
	 * Argument 1 : RetailCalendarDAO Argument 2 : SalesForecastDAO Argument 3 :
	 * Calendar Week constant Argument 4 : Store / District / Region / Division
	 * Level Id Argument 5 : Ctd week constant
	 * 
	 * @catch Exception
	 * 
	 * @throws GeneralException
	 */

	private void weeklyLastYear(RetailCalendarDAO objCalendarDao,
			LastYearDAO objSalesForecastDao) throws GeneralException {

		// logger.debug("Weekly Forecast");

		try {

			// for week aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.STORE_LEVEL_ID,
					Constants.CTD_WEEK, "SALES_AGGR_WEEKLY",
					"SALES_AGGR_DAILY",
					"Lastyear store level weekly aggregation");
			
			// for district aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.DISTRICT_LEVEL_ID,
					Constants.CTD_WEEK, "SALES_AGGR_WEEKLY_ROLLUP",
					"SALES_AGGR_DAILY_ROLLUP",
					"Lastyear district level weekly aggregation");

			// for region aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.REGION_LEVEL_ID,
					Constants.CTD_WEEK, "SALES_AGGR_WEEKLY_ROLLUP",
					"SALES_AGGR_DAILY_ROLLUP",
					"Lastyear region level Weekly aggregation");

			// for division  aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.DIVISION_LEVEL_ID,
					Constants.CTD_WEEK, "SALES_AGGR_WEEKLY_ROLLUP",
					"SALES_AGGR_DAILY_ROLLUP",
					"LastYear Division Level Weekly Aggrgetion");
			
			// for chain  aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.CHAIN_LEVEL_ID,
					Constants.CTD_WEEK, "SALES_AGGR_WEEKLY_ROLLUP",
					"SALES_AGGR_DAILY_ROLLUP",
					"LastYear Chain Level Weekly Aggrgetion");

		} catch (Exception exe) {
			logger.debug(exe);
			throw new GeneralException("Weekly Forecast Error", exe);
		}

		// logger.debug("Weekly Forecast Ends");

	}

	/*
	 * Method used to get the district , Region , Division Level forecast
	 * records , and insert the records in table Argument 1 : SalesForecastDAO
	 * Argument 2 : processYear Argument 3 : Calendar Day Constants Argument 4 :
	 * Store Level Id , District Level Id, Region Level Id , Division Level Id
	 * 
	 * @catch Exception
	 * 
	 * @throws GeneralException
	 */

	private void dailyLastYear(LastYearDAO objLastYearDao)
			throws GeneralException {
		
		// Obejct for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();

		// logger.debug("Forecast Daily Rollup Process");
		try {
			// get the district based lastYearData
			List<ForecastDto> lastYearDistrict = objLastYearDao
					.GetDistrictForecast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.STORE_LEVEL_ID);
			
			// get identical based last YearData
			HashMap<String, Double> identicalDistrictMap = objLastYearDao.getIdenticalDistrict(
								_conn , _processYear, Constants.CALENDAR_DAY , Constants.STORE_LEVEL_ID);
			

			// insert the process
			objLastYearDao.lastYearRollup(_conn, lastYearDistrict,
					Constants.DISTRICT_LEVEL_ID,
					"LastYear Daily Level District Aggregation" , identicalDistrictMap);

			// Get the region level aggregation records
			List<ForecastDto> lastYearRegion = objLastYearDao
					.getRegionForeCast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.DISTRICT_LEVEL_ID);

			// insert the process
			objLastYearDao.lastYearRollup(_conn, lastYearRegion,
					Constants.REGION_LEVEL_ID,
					"LastYear Daily Level Region Aggregation" , null);

			// get the division level aggregation records
			List<ForecastDto> lastYearDivision = objLastYearDao
						.getDivisionForeCast(_conn, _processYear,
								Constants.CALENDAR_DAY, Constants.REGION_LEVEL_ID);

			// insert the process
			objLastYearDao.lastYearRollup(_conn, lastYearDivision,
					Constants.DIVISION_LEVEL_ID,
					"LastYear Daily Level Division Aggregation" ,null);
			
			int compChainId = objStoreDao.getCompChainId(_conn);
			
			// get the chain level aggregation
			List<ForecastDto> lastYearChain = objLastYearDao.getChainForecast(
					_conn, _processYear, Constants.CALENDAR_DAY,
					Constants.DIVISION_LEVEL_ID, compChainId);
			
			// insert the process
			objLastYearDao.lastYearRollup(_conn, lastYearChain,
					Constants.CHAIN_LEVEL_ID,
					"LastYear Daily Level Chain Aggregation" ,null);

		} catch (GeneralException exe) {
			logger.error(exe);
			throw new GeneralException("forecastRollup Process Error", exe);
		}

	}

	/*
	 * Method used insert the forecast data in weekly level and ctd level
	 * Process 1 : Get the weekly calendar List for a given year Process 2 : get
	 * daily calendar Id for a given week Process 3 : Delete the previous sales
	 * forecast data for a given week process 4 : get salesForecastData For a
	 * given day Process 5 : Insert the process
	 * 
	 * @throws GeneralException
	 */

	private boolean processSalesForeCast(RetailCalendarDAO objCalendarDao,
			LastYearDAO objSalesForecastDao, String calendarConstant,
			int locationLevelId, int ctdConstant, String insertTableName,
			String selectTableName, String methodName) throws GeneralException,
			ParseException {

		// Object for weeklyCalendarList
		List<RetailCalendarDTO> weeklyCalendarList = new ArrayList<RetailCalendarDTO>();

		int processId = 0;
		int tempLocationId = 0; // hold the process store id
		int tempLocationLevelId = 0; // hold the process location level id
		int processCalendarId = 0;
		double temptotRevenue = 0;
		int currentLastCalendar = 0;
		double tempId_totRevene = 0;

		try {

			// get the weekly calendar List for a process year
			weeklyCalendarList = objCalendarDao.forecastCalendarList(_conn,
					calendarConstant, _processYear, "FORECAST");

			ForecastDto objForeCastInsertDto = new ForecastDto();

			PreparedStatement ctdpsmt = _conn
					.prepareStatement(objSalesForecastDao.forecastCtdInsert());

			PreparedStatement forecastpsmt = _conn
					.prepareStatement(objSalesForecastDao.forecastInsertWeek(insertTableName , locationLevelId));
			
			// hold the store status
			String storeStatus = "";

			for (int ii = 0; ii < weeklyCalendarList.size(); ii++) {

				RetailCalendarDTO objWeeklyCalendar = weeklyCalendarList
						.get(ii);

				/*
				 * logger.debug("Processing  calendar Id " +
				 * objWeeklyCalendar.getCalendarId());
				 */

				// get the start date and end date
				ComputeDateRanges(objWeeklyCalendar.getStartDate(),
						objWeeklyCalendar.getEndDate());
				
						
				// get the daily calendar List for a given mode (Week / Period /
				// Quarter /Year)
				List<RetailCalendarDTO> dailyCalendarList = objCalendarDao
						.dayCalendarList(_conn, _startdate, _endDate,
								Constants.CALENDAR_DAY);

				RetailCalendarDTO objCalDto = dailyCalendarList
						.get(dailyCalendarList.size() - 1);
				currentLastCalendar = objCalDto.getCalendarId();
				
				HashMap<String ,String> storeStatusMap = null;
				
				if (locationLevelId == Constants.STORE_LEVEL_ID) {
					storeStatusMap = objSalesForecastDao.getStoreStatus(_conn,
							_startdate, _endDate, Constants.CALENDAR_WEEK,
							calendarConstant);
				}

				// Get the daily forecast details for a given calendar id list
				List<ForecastDto> dailyForecastList = objSalesForecastDao
						.getDailySalesForecastDetails(_conn, dailyCalendarList,
								locationLevelId, selectTableName);
				

				for (int jj = 0; jj < dailyForecastList.size(); jj++) {

					ForecastDto objForecastDto = dailyForecastList.get(jj);
					
					if( locationLevelId ==5 &&  storeStatusMap.containsKey(String.valueOf(objForecastDto.getLocationId()))){
						
						storeStatus =  storeStatusMap.get(String.valueOf(objForecastDto.getLocationId()));
					}
					
					// Assign the location id and location_level id to temp values
					if (tempLocationId == 0 && tempLocationLevelId == 0
							&& processCalendarId == 0) {
						tempLocationId = objForecastDto.getLocationId();
						tempLocationLevelId = objForecastDto.getLocationLevelId();
						processCalendarId = objWeeklyCalendar.getCalendarId();
					}

					// Check the location Id change
					if (tempLocationId == objForecastDto.getLocationId()
							&& tempLocationLevelId == objForecastDto
									.getLocationLevelId()
							&& processCalendarId == objWeeklyCalendar
									.getCalendarId()) {

						if (processId == 0) {

							temptotRevenue += objForecastDto.gettotRevenue();
							tempId_totRevene += objForecastDto.getIdTotRevenue();

							forecastDetails(objForeCastInsertDto,
									processCalendarId, temptotRevenue,
									objForecastDto, ctdConstant,
									currentLastCalendar, tempId_totRevene);

							// insert the sales forecast ctd table
							objSalesForecastDao.addSqlBatch(ctdpsmt,
									objForeCastInsertDto, "Ctd" , insertTableName , storeStatus);

							/*
							 * objSalesForecastDao.insertSalesForeCastCtd(_conn,
							 * objForeCastInsertDto);
							 */
							processId = 1;

						} else {
							temptotRevenue += objForecastDto.gettotRevenue();
							tempId_totRevene += objForecastDto.getIdTotRevenue();
							
							forecastDetails(objForeCastInsertDto,
									processCalendarId, temptotRevenue,
									objForecastDto, ctdConstant,
									currentLastCalendar , tempId_totRevene);
							
							// update the forecast ctd table
							objSalesForecastDao.addSqlBatch(ctdpsmt,
									objForeCastInsertDto, "Ctd" , insertTableName , storeStatus);
							/*
							 * objSalesForecastDao.insertSalesForeCastCtd(_conn,
							 * objForeCastInsertDto);
							 */
						}

					} else {

						// Insert the SalesForecast table
						objSalesForecastDao.addSqlBatch(forecastpsmt,
								objForeCastInsertDto, "Week" , insertTableName , storeStatus);
						objForeCastInsertDto = new ForecastDto();
						tempLocationId = objForecastDto.getLocationId();
						tempLocationLevelId = objForecastDto
								.getLocationLevelId();

						temptotRevenue = objForecastDto.gettotRevenue();
						tempId_totRevene = objForecastDto.getIdTotRevenue();
						processCalendarId = objWeeklyCalendar.getCalendarId();

						forecastDetails(objForeCastInsertDto,
								processCalendarId, temptotRevenue,
								objForecastDto, ctdConstant,
								currentLastCalendar , tempId_totRevene);

						objSalesForecastDao.addSqlBatch(ctdpsmt,
								objForeCastInsertDto, "Ctd",insertTableName , storeStatus);
						// insert the forecast ctd table
						/*
						 * objSalesForecastDao.insertSalesForeCastCtd(_conn,
						 * objForeCastInsertDto);
						 */

					}

				}

			}

			// Insert the SalesForecast table
			objSalesForecastDao.addSqlBatch(forecastpsmt, objForeCastInsertDto,
					"Week" , insertTableName , storeStatus);
			objSalesForecastDao.excuteBatch(forecastpsmt, methodName);
			//objSalesForecastDao.excuteBatch(ctdpsmt, methodName);
		} catch (Exception exe) {
			logger.error(exe);
			exe.printStackTrace();
			throw new GeneralException(
					"Insert the week/Period/Quarter/Year Error", exe);

		}

		return false;

	}

	/*
	 * Method mainly used to assign the sum of forecast records into Dto
	 * Argument 1 : ForecastDto Argument 2 : processCalendarId Argument 3 :
	 * Forecast with gas Argument 4 : Forecast Without gas Argument 5 : Ctd
	 * Constant
	 */

	private void forecastDetails(ForecastDto objForeCastInsertDto,
			int processCalendarId, double totRevenue,
			ForecastDto objForecastDto, int ctdConstant, int currentLastCalendar, double tempId_totRevene) {

		objForeCastInsertDto.setCalendarId(processCalendarId);
		objForeCastInsertDto.settotRevenue(totRevenue);
		objForeCastInsertDto.setIdTotRevenue(tempId_totRevene);
		objForeCastInsertDto
				.setForecastCtdId(objForecastDto.getForecastCtdId());
		objForeCastInsertDto.setLocationId(objForecastDto.getLocationId());
		objForeCastInsertDto.setLocationLevelId(objForecastDto
				.getLocationLevelId());
		objForeCastInsertDto.setCtdType(ctdConstant);
		objForeCastInsertDto.setLastCalendarId(currentLastCalendar);

	}

	/*
	 * ****************************************************************
	 * Method to construct the given date with start time and end time Argument
	 * 1: Date ****************************************************************
	 */
	private void ComputeDateRanges(String startdate, String Enddate)
			throws GeneralException, ParseException {

		Calendar cal = Calendar.getInstance();
		// find the date with start time
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		Date s_date = (Date) formatter.parse(startdate);
		cal.setTime(s_date);
		_startdate = cal.getTime();

		Date e_date = (Date) formatter.parse(Enddate);
		cal.setTime(e_date);
		_endDate = cal.getTime();

	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Product Group Loader ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}
