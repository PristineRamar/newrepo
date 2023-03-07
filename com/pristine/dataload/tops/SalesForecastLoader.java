/*
 * Title: TOPS  Product Group Loader
 *
 *****************************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *--------------------------------------------------------------------------------------------------------
 * Version 0.1	 19-04-2012	    Dinesh Kumar V	     Initial  Version 
 * Version 0.2   01-05-2012     Dinesh Kumar V       Add the batch insert 
 * Version 0.3   02-05-2012     Dinesh Kumar V       Add the new input condition and update process		  
 * Version 0.4   24-07-2012     Dinesh Kumar V		 add identical and Non identical based total revenue
 * Version 0.4   18-08-2012     Dinesh Kumar V		 Adding Chain support code....
 * **********************************************************************************************************
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
import com.pristine.dao.salesanalysis.SalesForecastDAO;
import com.pristine.dto.ForecastDto;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.ExcelParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SalesForecastLoader extends PristineFileParser{

	static Logger logger         = Logger.getLogger("SalesforeCastLoader");
	private Connection _conn     = null; // hold the database connection
	private String _rootPath     = null; // hold the input file path
	private String _relativePath = null; // hold the input file name
	private String _sheetName    = null; // hold the input sheet Name
	private String _fileType     = null; // hold the input file type
	private Date _processYear    = null; // hold the processing year
	private Date _startdate      = null; // hold the input start date
	private Date _endDate        = null; // hold the input end date
	private int _startColumn     = 0; // hold the input start column
	private int _sNumebrLength   = 0; // hold the store number length from property
	private boolean _overrideValidations = false;
	
	/*
	 * Class Constructor: 
	 * Process 1 : Input Validation 
	 * Process 2 : Get Database Connection 
	 * Argument 1 : File Name (test.xls) 
	 * Argument 2 : Sheet Name (test) 
	 * Argument 3 : File Type (WG / WOG) 
	 * Argument 4 : Start Column (10)
	 * Argument 5 : Start Date Argument 6 : End Date
	 * 
	 * @catch general Exception
	 */
	public SalesForecastLoader(String relativePath, String sheetName,
			String fileType, String startColumn, Date startDate, Date endDate, Boolean overrideValidations)
			throws GeneralException {

		if (relativePath == null) {
			logger.error("Invalid Relative path: Relative Path missing in input");
			System.exit(1);
		}

		if (sheetName == null) {
			logger.error("Invalid Sheet Name : Excel Sheet Name Missing In Input");
			System.exit(1);
		}
		if (fileType == null) {
			logger.error("Invalid File Type : File type missing in input");
			System.exit(1);
		}
		if (startColumn == null) {
			logger.error("Invalid Start Column : Start Column Missing in Input");
			System.exit(1);
		}
		/*if (startDate == null) {
			logger.error("Invalid Start Date: Start date missing in Input");
			System.exit(1);
		}*/

		
		PropertyManager.initialize("analysis.properties");

		// get the base file path from properties file
		_rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");

		// read the store number length from property file
		_sNumebrLength = Integer.valueOf(PropertyManager.getProperty("DATALOADER.STORENUMBERLENGTH", "4"));

		// Check the base path availability
		if (_rootPath.equalsIgnoreCase("")) {
			logger.error("Invalid configuration: Excel file path missing");
			System.exit(1);
		}
		_relativePath = relativePath;
		_sheetName = sheetName;
		_fileType = fileType;
		_startColumn = Integer.valueOf(startColumn);
		_startdate = startDate;
		_endDate = endDate;
		_overrideValidations = overrideValidations;
		try {

			// Initialize the db connection
			_conn = DBManager.getConnection();
		} catch (Exception exe) {
			logger.error("Connection Failed");
			logger.error(exe);
			System.exit(1);

		}
	}

	/*
	 * Main Method of the batch.method mainly used get the initial arguments .
	 * Argument 1 : File Name 
	 * Argument 2 : Sheet Number 
	 * Argument 3 : File Type
	 * Argument 4 : Start column 
	 * Argument 5 : Start date 
	 * Argument 6 : End Date
	 * call the class constructor and initialize the db connection
	 * 
	 * @catch Exception
	 */

	public static void main(String[] args) throws GeneralException {
		PropertyConfigurator.configure("log4j-Forecast-loader.properties");
		logger.info("Sales Forecast loader starts");
		logCommand(args);
		String relativePath = null; // hold the input file name
		String sheetName = null; // hold the input excl sheet number
		String fileType = null; // hold the input file type (Wg / Wog / full
								// file(Wg and wog)
		String startColumn = null; // hold the input start Column (full process
									// / partial process )
		Date startDate = null; // hold the input start date (Process start date)
		Date endDate = null; // hold the input end date (Process end date)

		SalesForecastLoader productLoader = null;
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy"); // Date
																	// format
		Boolean overrideValidations = false;
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
					sheetName = arg.substring("SHEETINDEX=".length());
				}

				// get the file type (with gas file / without gas file)
				if (arg.startsWith("FILETYPE")) {
					fileType = arg.substring("FILETYPE=".length());
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
				
				if (arg.startsWith("OVERRIDEVALIDATIONS")) {
					overrideValidations = true;
				}
			}

			// call the class constructor
			productLoader = new SalesForecastLoader(relativePath, sheetName,
					fileType, startColumn, startDate, endDate, overrideValidations);
			
			// Method used to read the excel and insert the values into
			// salesforeCast table
			productLoader.readExcel();
			PristineDBUtil.commitTransaction(productLoader._conn, "Commit Sales Forecast Loading");
			
		} catch (Exception exe) {
			logger.error(exe.getMessage());
			PristineDBUtil.rollbackTransaction(productLoader._conn,	"Rollback Sales Forecast Loading");
		} finally {
			try {
				PristineDBUtil.close(productLoader._conn);
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
			
				int rowNumber = 0;
	
				
				if (fileList.size() > 0) {
				
				for (int fL = 0; fL < fileList.size(); fL++) {
					String fileName = fileList.get(fL);
					
					logger.info(" Processing Forecast File: " + fileName);
					logger.info(" Processing on Date: " + DateUtil.getCurrentDateTime());
					
					// Changes to automate this program
					if(_startdate == null || _endDate == null){
						Date endDate = objParseExcel.getLatestDateInFile(fileName, _sheetName);
						if(endDate != null){
							String startDate = DateUtil.getWeekStartDate(endDate, 0);
							_startdate = DateUtil.toDate(startDate);
							_endDate = endDate;
						}
					}
					// Changes to automate this program - Ends
					
					if(_startdate != null){
						logger.info(" Processing for Week: " + _startdate + " " + _endDate);
						Date currentDate = DateUtil.toDate(DateUtil.getWeekStartDate(0));
						if(currentDate.compareTo(_startdate) > 2 || currentDate.compareTo(_startdate) < 2){
							if(!_overrideValidations){
								logger.error("Process date does not fall in the threshold of +/- 2 weeks from current date.");
								logger.error("Override validations using OVERRIDEVALIDATIONS input argument");
								continue;
							}
						}
						
						List<ForecastDto> foreCastList = null;
						 
						foreCastList = objParseExcel.forecastExcelParser(fileName,
									_sheetName, _startColumn, _startdate, _endDate,
									_sNumebrLength, rowNumber, "FORECASTLOADER");
						 
		
						logger .info("No of records read from the forecast file: " + foreCastList.size());
						
						if( foreCastList.size() > 0){
							logger.info("Forecast file reading process complete");
		
							isForecastProcessed(_startdate, _endDate);
							
							// call the process method to process the excel records
							// and insert the records into table
							// get the calendar id for given process date ( First Process
							// Day );
							// Get store id for given store number
							processSalesForeCastDaily(foreCastList);
							
							
							// String archivePath = _rootPath + "/" + _relativePath + "/";
							// PrestoUtil.moveFiles(fileList, archivePath+ Constants.COMPLETED_FOLDER);
						}
						else{
							logger .info(" No records found...");
						}
					}
				}
				}/*else{
					logger.error("File Not avilable in Relative path......");
				}*/
				 
			}catch (GeneralException exe) {
				// String archivePath = _rootPath + "/" + _relativePath + "/";
				// PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				PristineDBUtil.rollbackTransaction(_conn,"Sales foreCast Loder Rollback");
				commit = false;
				logger.error(" Error in processing..... " + exe);
						 
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
	 * Check if forecast records are processed already for the given start and end date
	 * and logs accordingly
	 * @param _startdate
	 * @param _endDate
	 */
	private void isForecastProcessed(Date _startdate, Date _endDate) throws GeneralException{
		RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
		SalesForecastDAO salesForecastDAO = new SalesForecastDAO();
		
		List<RetailCalendarDTO> calendarDTOList = calendarDAO.dayCalendarList(_conn, _startdate, _endDate, Constants.CALENDAR_DAY);
		List<ForecastDto> forecastDTOList = salesForecastDAO.getDailySalesForecastDetails(_conn, calendarDTOList, Constants.STORE_LEVEL_ID);
		if(forecastDTOList != null && forecastDTOList.size() > 0){
			logger.warn("Forecast data was processed already. Reprocessing forecast data");
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
	 

		// Object for SalesForcastDao
		SalesForecastDAO objSalesForecastDao = new SalesForecastDAO();

		HashMap<String, String> previousForecast = null;
		try {

			PreparedStatement insertPsmt = _conn.prepareStatement(objSalesForecastDao.forecastInsert("STORE"));

			PreparedStatement updatePsmt = _conn.prepareStatement(objSalesForecastDao.forecastUpdate(_fileType));

			// hold the Day calendar Id list
			HashMap<String, Integer> calendarList = new HashMap<String, Integer>();

			// hold the CompStrId list
			// logger.info("Get Locationid List Starts");
			HashMap<String, Integer> compStrId = objCompStoreDao.getCompStrId(_conn);
			logger.info("No of stores retrieved: " + compStrId.size());

			logger.info("Forecast Record Processing at daily level starts");
			for (int ii = 0; ii < foreCastList.size(); ii++) {

				ForecastDto objForCastDto = foreCastList.get(ii);

				if (ii == 0) {
					_processYear = objForCastDto.getProcessDate();
				}
				
				//logger.info(" Process Date.... " +objForCastDto.getProcessDate()+"....store Number"+objForCastDto.getStoreNumber() ) ;

				// Check the calendar list availability
				if (!calendarList.containsKey(objForCastDto.getProcessDate()+ "_" + 1)) {
					// get the calendar List for a given week
					calendarList = objCalendarDao.excelBasedCalendarList(_conn,
							objForCastDto.getProcessDate(),
							Constants.CALENDAR_DAY, 0, _processYear);

					previousForecast = objSalesForecastDao.getPreviousForecast(
							_conn, calendarList, Constants.STORE_LEVEL_ID);
				}

				if (compStrId.containsKey(objForCastDto.getStoreNumber())) {
					
					objForCastDto.setLocationId(compStrId.get(objForCastDto
							.getStoreNumber()));

					// call the addSqlbatch method to add the values into batch
					objSalesForecastDao.addDayBatch(insertPsmt, updatePsmt,
							previousForecast, calendarList, objForCastDto,
							_fileType);
				}
			}
			
		 
			
			try {
				
				// logger.info(" Insert Preparement size...." + insertPsmt.getMaxRows());
			
				// call the execute batch method to insert the daily records.
				objSalesForecastDao.excuteBatch(insertPsmt,	"Day Level Forecast Insert");
				
				objSalesForecastDao.excuteBatch(updatePsmt,	"Day Level Forecast Update");
			} catch (GeneralException e) {
				 logger.error("Error in insert/update of forecast at daily level: " + e);
				 throw new GeneralException("Error in insert/update of forecast at daily level: " + e);
			}
			
			PristineDBUtil.commitTransaction(_conn, "Day Level Forecast data commit");
			
			logger.debug("Delete Previous Records Start.........");
			objSalesForecastDao.deleteForecast(_conn, _processYear);
		
			// used to insert the district / region / division / chain leve//
			// forecast data

			logger.info("Daily rollup of forecast records starts ");
			dailyForecast(objSalesForecastDao);

			// method used to insert the weekly forecast data

			logger.info("Forecast data processing at Weekly level starts");
			weeklyForeCast(objCalendarDao, objSalesForecastDao);
			//logger.info("Weekly Forecast Ends");
			
			PristineDBUtil.commitTransaction(_conn, "Week Level Forecast Data commit");

			// method used to insert the period forecast data

			logger.info("Forecast data processing at Period level starts");
			periodForecast(objCalendarDao, objSalesForecastDao);

			//logger.info("Period Forecast Ends");
			
			PristineDBUtil.commitTransaction(_conn, "Period Level Forecast Data commit");

			// method used to get the quarter forecast data

			logger.info("Forecast data processing at Quarter level starts");
			quarterForecast(objCalendarDao, objSalesForecastDao);

			//logger.info("Quarter Forecast Ends");
			
			PristineDBUtil.commitTransaction(_conn, "Quarter Level Forecast Data commit");

			// method used to get the year forecast date

			logger.info("Forecast data processing at Year level starts");
			yearForecast(objCalendarDao, objSalesForecastDao);
			
			PristineDBUtil.commitTransaction(_conn, "Year Level Forecast Data commit");

			//logger.info("Year Forecast Ends");

		} catch (Exception exe) {
			logger.error("Error in processing forecast data: " + exe);
			throw new GeneralException("Error in processing forecast data: " + exe);
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

	private boolean yearForecast(RetailCalendarDAO objCalendarDao,
			SalesForecastDAO objSalesForecastDao) throws ParseException,
			GeneralException {

		boolean insertFlag = false;

		// for year aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.STORE_LEVEL_ID,
				Constants.CTD_YEAR, "Year Store Forecast");
		
		// for district process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.DISTRICT_LEVEL_ID,
				Constants.CTD_YEAR, "Year District Forecast");
		
		// for region process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.REGION_LEVEL_ID,
				Constants.CTD_YEAR, "Year Region Forecast");
		
		// for division process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_YEAR, Constants.DIVISION_LEVEL_ID,
				Constants.CTD_YEAR, "Year Division Forecast"); 
		
		// for Chain process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_YEAR, Constants.CHAIN_LEVEL_ID,
					Constants.CTD_YEAR, "Year Chain Forecast"); 

		return insertFlag;
	}

	private boolean quarterForecast(RetailCalendarDAO objCalendarDao,
			SalesForecastDAO objSalesForecastDao) throws ParseException,
			GeneralException {

		boolean insertFlag = false;

		// for quarter aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.STORE_LEVEL_ID,
				Constants.CTD_QUARTER, "Quarter Store Forecast");
		
		// for district process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.DISTRICT_LEVEL_ID,
				Constants.CTD_QUARTER, "Quarter District Forecast");

		// for region process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.REGION_LEVEL_ID,
				Constants.CTD_QUARTER, "Quarter Region Forecast");

		// for division process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.DIVISION_LEVEL_ID,
				Constants.CTD_QUARTER, "Quarter Divison Forecast");

		// for chain process
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_QUARTER, Constants.CHAIN_LEVEL_ID,
				Constants.CTD_QUARTER, "Quarter Chain Forecast");
		
		//PristineDBUtil.commitTransaction(_conn, "Commit the Quarter transation");

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

	private boolean periodForecast(RetailCalendarDAO objCalendarDao,
			SalesForecastDAO objSalesForecastDao) throws ParseException,
			GeneralException {

		boolean insertFlag = false;

		// for period aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.STORE_LEVEL_ID,
				Constants.CTD_PERIOD, "Period Store Forecast");
		
		// for disitrict aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.DISTRICT_LEVEL_ID,
				Constants.CTD_PERIOD, "Period District Forecast");

		// for region aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.REGION_LEVEL_ID,
				Constants.CTD_PERIOD, "Period Region Forecast");

		// for division aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.DIVISION_LEVEL_ID,
				Constants.CTD_PERIOD, "Period Divison Forecast");

		// for chain aggregation
		insertFlag = processSalesForeCast(objCalendarDao, objSalesForecastDao,
				Constants.CALENDAR_PERIOD, Constants.CHAIN_LEVEL_ID,
				Constants.CTD_PERIOD, "Period Chain Forecast");

		
			
		//PristineDBUtil.commitTransaction(_conn, "Commit the Quarter transation");

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

	private void weeklyForeCast(RetailCalendarDAO objCalendarDao,
			SalesForecastDAO objSalesForecastDao) throws GeneralException {

		try {

			// for week aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.STORE_LEVEL_ID,
					Constants.CTD_WEEK, "Weekly Store Forecast");

			// District aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.DISTRICT_LEVEL_ID,
					Constants.CTD_WEEK, "Weekly District Forecast");

			// Region Aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.REGION_LEVEL_ID,
					Constants.CTD_WEEK, "Weekly Region Forecast");

			// division aggregation
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.DIVISION_LEVEL_ID,
					Constants.CTD_WEEK, "Weekly Division Forecast"); 
			
			// code added for chain aggregation .... 28-08-2012
			processSalesForeCast(objCalendarDao, objSalesForecastDao,
					Constants.CALENDAR_WEEK, Constants.CHAIN_LEVEL_ID,
					Constants.CTD_WEEK, "Weekly Chain Forecast");
			

		} catch (Exception exe) {
			logger.error("Error in forecast data processing at weekly level " + exe);
			throw new GeneralException("Error in forecast data processing at weekly level " + exe);
		}

	}


	/*
	 * Method used to aggregate the forecast data in district , region ,
	 * division ,chain level
	 * Argument 1 : objSalesForecastDao
	 * Process 1 : get the District Level aggregation from store
	 * Process 2 : get the identical level aggregation from store
	 * Process 3 : Insert the district level aggregation
	 * process 4 : get the region level aggregation from district
	 * Process 5 : Insert the region level aggregation
	 * Process 6 : get the division Level aggregation from region
	 * Process 7 : Insert the division aggregation
	 * Process 8 : get the chain level aggregation 
	 * Process 9 : Insert the process
	 * @throws GeneralException 
	 */
	

	private void dailyForecast(SalesForecastDAO objSalesForecastDao)
			throws GeneralException {

		// Object for CompStoreDAO  
		CompStoreDAO objCompStoreDao = new CompStoreDAO();
		
		try {
			// get the district forecast
			List<ForecastDto> districtForecast = objSalesForecastDao
					.GetDistrictForecast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.STORE_LEVEL_ID);

			// get the identical forecast details
			HashMap<String, ForecastDto> identicalDistrictForecast = objSalesForecastDao
					.GetidenticalDistrictForecast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.STORE_LEVEL_ID);

			// insert the process
			objSalesForecastDao.forecastRollup(_conn, objSalesForecastDao,
					districtForecast, identicalDistrictForecast,
					Constants.DISTRICT_LEVEL_ID, "Daily District Forecast");

			// Get the region level aggregation records
			List<ForecastDto> regionForecast = objSalesForecastDao
					.getRegionForeCast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.DISTRICT_LEVEL_ID);

			// insert the process
			objSalesForecastDao.forecastRollup(_conn, objSalesForecastDao,
					regionForecast, null, Constants.REGION_LEVEL_ID,
					"Daily Region Level");

			// get the division level aggregation records
			List<ForecastDto> DivisionForecast = objSalesForecastDao
					.getDivisionForeCast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.REGION_LEVEL_ID);

			// insert the process
			objSalesForecastDao.forecastRollup(_conn, objSalesForecastDao,
					DivisionForecast, null, Constants.DIVISION_LEVEL_ID,
					"Daily Division Level");
			
			// get the chain id from table..
			int compChainId = objCompStoreDao.getCompChainId(_conn);
			
			
			// get the chain level aggregated records
			List<ForecastDto> chainForecast = objSalesForecastDao
					.getChainForecast(_conn, _processYear,
							Constants.CALENDAR_DAY, Constants.DIVISION_LEVEL_ID , compChainId);
			
			// insert the process
			objSalesForecastDao.forecastRollup(_conn, objSalesForecastDao,
					chainForecast, null, Constants.CHAIN_LEVEL_ID,
					"Daily Chain Level");
			
			
		} catch (Exception exe) {
			logger.error("Error in forecast data processing at daily rollup level: " + exe);
			throw new GeneralException("Error in forecast data processing at daily rollup level: " + exe);
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
			SalesForecastDAO objSalesForecastDao, String calendarConstant,
			int locationLevelId, int ctdConstant, String methodName)
			throws GeneralException {

		// Object for weeklyCalendarList
		List<RetailCalendarDTO> weeklyCalendarList = new ArrayList<RetailCalendarDTO>();

		int processId = 0;
		int tempLocationId = 0; // hold the process store id
		int tempLocationLevelId = 0; // hold the process location level id
		int processCalendarId = 0;
		double tempForeCastWg = 0; // hold the forecast with gas
		double tempForeCastWog = 0; // hold the forecast without gas
		double tempIdForeCastWg = 0; // hold the forecast with gas
		double tempIdForeCastWog = 0; // hold the forecast without gas

		try {

			// get the weekly calendar List for a process year
			weeklyCalendarList = objCalendarDao.forecastCalendarList(_conn,
					calendarConstant, _processYear, "FORECAST");

			ForecastDto objForeCastInsertDto = new ForecastDto();

			PreparedStatement ctdpsmt = _conn.prepareStatement(objSalesForecastDao.forecastCtdInsert());

			PreparedStatement forecastpsmt = _conn.prepareStatement(objSalesForecastDao.forecastInsertWeek());

			for (int ii = 0; ii < weeklyCalendarList.size(); ii++) {

				RetailCalendarDTO objWeeklyCalendar = weeklyCalendarList
						.get(ii);

				logger.debug("Processing  calendar Id "	+ objWeeklyCalendar.getCalendarId());

				// get the start date and end date
				ComputeDateRanges(objWeeklyCalendar.getStartDate(),	objWeeklyCalendar.getEndDate());

				// get the daily calendar List for a given mode (Week / Period /
				// Quarter /Year)
				List<RetailCalendarDTO> dailyCalendarList = objCalendarDao
						.dayCalendarList(_conn, _startdate, _endDate,
								Constants.CALENDAR_DAY);
				// Get the daily forecast details for a given calendar id list
				List<ForecastDto> dailyForecastList = objSalesForecastDao
						.getDailySalesForecastDetails(_conn, dailyCalendarList,
								locationLevelId);

				for (int jj = 0; jj < dailyForecastList.size(); jj++) {

					ForecastDto objForecastDto = dailyForecastList.get(jj);
					// Assign the location id and location_level id to temp
					// values
					if (tempLocationId == 0 && tempLocationLevelId == 0	&& processCalendarId == 0) {
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

							tempForeCastWg += objForecastDto.getForecastWg();
							tempForeCastWog += objForecastDto.getForeCastWog();
							tempIdForeCastWg += objForecastDto.getIdForecastWg();
							tempIdForeCastWog += objForecastDto.getIdForecastWog();

							forecastDetails(objForeCastInsertDto,
									processCalendarId, tempForeCastWg,
									tempForeCastWog, objForecastDto,
									ctdConstant, tempIdForeCastWg,
									tempIdForeCastWog);

							// insert the sales forecast ctd table
							objSalesForecastDao.addSqlBatch(ctdpsmt,objForeCastInsertDto, null, "Ctd", null);

							/*
							 * objSalesForecastDao.insertSalesForeCastCtd(_conn,
							 * objForeCastInsertDto);
							 */
							processId = 1;

						} else {
							tempForeCastWg += objForecastDto.getForecastWg();
							tempForeCastWog += objForecastDto.getForeCastWog();
							tempIdForeCastWg += objForecastDto.getIdForecastWg();
							tempIdForeCastWog += objForecastDto.getIdForecastWog();

							forecastDetails(objForeCastInsertDto,
									processCalendarId, tempForeCastWg,
									tempForeCastWog, objForecastDto,
									ctdConstant ,tempIdForeCastWg ,tempIdForeCastWog);
							// update the forecast ctd table
							objSalesForecastDao.addSqlBatch(ctdpsmt,
									objForeCastInsertDto, null, "Ctd", null);
							/*
							 * objSalesForecastDao.insertSalesForeCastCtd(_conn,
							 * objForeCastInsertDto);
							 */
						}

					} else {

						// Insert the SalesForecast table
						objSalesForecastDao.addSqlBatch(forecastpsmt,
								objForeCastInsertDto, null, "Week", null);
						objForeCastInsertDto = new ForecastDto();
						tempLocationId = objForecastDto.getLocationId();
						tempLocationLevelId = objForecastDto.getLocationLevelId();
						tempForeCastWg = objForecastDto.getForecastWg();
						tempForeCastWog = objForecastDto.getForeCastWog();
						tempIdForeCastWg  = objForecastDto.getIdForecastWg();
						tempIdForeCastWog = objForecastDto.getIdForecastWog();
						processCalendarId = objWeeklyCalendar.getCalendarId();

						forecastDetails(objForeCastInsertDto,
								processCalendarId, tempForeCastWg,
								tempForeCastWog, objForecastDto, ctdConstant,
								tempIdForeCastWg, tempIdForeCastWog);

						objSalesForecastDao.addSqlBatch(ctdpsmt,
								objForeCastInsertDto, null, "Ctd", null);
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
					null, "Week", null);
			objSalesForecastDao.excuteBatch(forecastpsmt, methodName);
			//objSalesForecastDao.excuteBatch(ctdpsmt, methodName + " Ctd");
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException(	"Insert the week/Period/Quarter/Year Error", exe);
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
			int processCalendarId, double foreCastWg, double foreCastWog,
			ForecastDto objForecastDto, int ctdConstant, double tempIdForeCastWg, double tempIdForeCastWog) {

		objForeCastInsertDto.setCalendarId(processCalendarId);
		objForeCastInsertDto.setForecastWg(foreCastWg);
		objForeCastInsertDto.setForeCastWog(foreCastWog);
		objForeCastInsertDto.setForecastCtdId(objForecastDto.getForecastCtdId());
		objForeCastInsertDto.setLocationId(objForecastDto.getLocationId());
		objForeCastInsertDto.setLocationLevelId(objForecastDto.getLocationLevelId());
		objForeCastInsertDto.setIdForecastWg(tempIdForeCastWg);
		objForeCastInsertDto.setIdForecastWog(tempIdForeCastWog);
		objForeCastInsertDto.setCtdType(ctdConstant);

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
