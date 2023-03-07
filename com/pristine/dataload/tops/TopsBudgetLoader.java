/*
 * Title: TOPS   Tops Budget Loader
 *
 *************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 05-06-2012	    Dinesh Kumar V	     Initial  Version 
 * Version 0.2   19-07-2012     Dinesh Kumar V		 Adding Calendar to Date Process
 * Version 0.3   24-07-2012     Dinesh Kumar V		 Adding Identical and non identical process 
 **************************************************************************************
 */

package com.pristine.dataload.tops;

import java.sql.Connection;
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
import com.pristine.dao.salesanalysis.BudgetDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.BudgetDto;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.ExcelParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class TopsBudgetLoader {
	
	private static Logger logger 	 = Logger.getLogger("TopsBudgetLoader");
	private String _rootpath 		 = null; 	// hold the base path of tops
	private String _relativePath 	 = null; 	// hold the input filename
	private int _processYear 		 = 0;        // hold the input process year
	private Date _startdate 		 = null;     // hold the input start date
	private Date _endDate 			 = null;     // hold the input end date
	private Connection _conn 		 = null; 	// hold the input Db connection
	private int _compChainId 		 = 0;       // hold the chain id 
	

	/*
	 * Class Constructor 
	 * Argument 1 : Relative path 
	 * Argument 2 : Start Period
	 * Argument 3 : End Period
	 * @catch GendralException
	 */
	public TopsBudgetLoader(String relativePath, int processYear) {

		// Check the input file name avilbility
		if (relativePath == null || relativePath == "") {
			logger.error(" Input is Missing : RELATIVEPATH.......");
			System.exit(1);
		}

		if (processYear == 0) {
			logger.error(" Input is Missing : PROCESSYEAR........ ");
			System.exit(1);
		}

		// intialize the base properties file
		PropertyManager.initialize("analysis.properties");
		// get the base file path from properties file
		_rootpath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");

		// Check the base path availability
		if (_rootpath.equalsIgnoreCase("")) {
			logger.error("Invalid configuration: Excel file path missing.......");
			System.exit(1);
		}

		// read the base file
		_relativePath = relativePath;
		_processYear = processYear;

		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Connection Failed........... " +exe);
			System.exit(1);
		}

	}

	/*
	 * Main method for the batch 
	 * Argument 1 : RELATIVE PATH 
	 * Argument 2 : START PERIOD 
	 * Argument 3 : END PERIOD
	 * @catch Exception
	 */

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-budget-loader.properties");
		logger.info(" ..................Tops Budget loader process starts");
		logCommand(args);

		String relativePath = null; // hold the input file relative path.
		int processYear = 0; // hold the input processing year
		TopsBudgetLoader budgetLoader = null;

		try {

			// get the input arguments
			for (int ii = 0; ii < args.length; ii++) {

				String arg = args[ii];

				if (arg.startsWith("RELATIVEPATH")) {
					relativePath = arg.substring("RELATIVEPATH=".length());
				}

				// get the process year from command line
				if (arg.startsWith("PROCESSYEAR")) {
					try {
						processYear = Integer.parseInt(arg
								.substring("PROCESSYEAR=".length()));
					} catch (Exception exe) {
						logger.error(" Input Error : PROCESSYEAR ");
						System.exit(1);
					}
				}

			}

			budgetLoader = new TopsBudgetLoader(relativePath, processYear);

			// *******************Process
			// Method**********************************
			boolean commitFlag = budgetLoader.processBudgetFile();
			// ****************************************************

			if (commitFlag) {

				// process commited
				PristineDBUtil.commitTransaction(budgetLoader._conn,
						"Budget Process Commited");
			} else {

				// Process Rollbacked
				PristineDBUtil.rollbackTransaction(budgetLoader._conn,
						"Budget Process Rollbacked");

			}

			logger.info("...................Tops Budget Loader Process Ends");
		} catch (GeneralException exe) {

			PristineDBUtil.rollbackTransaction(budgetLoader._conn,
					"Budget Process Rollbacked");
			logger.error("Main Method Error" + exe);
		}

		finally {
			try {
				PristineDBUtil.close(budgetLoader._conn);
			} catch (Exception exe) {
				logger.error(" Error While closing the connection " + exe);
			}
		}

	}

	 
	/*
	 *  Method used to read the excel file in period level
	 *  Argument : Class Level Variables
	 *  Process 1 : Read the Excel Using "budgetExcelParser" Method
	 *  Process 2 : Get the Period / Quarter / Year Calendar List
	 *  Process 3 : call the "insertAccounstDetails" to insert the excel values
	 * 
	 */
	
	
	
	private boolean processBudgetFile() throws GeneralException {

		logger.info("..... Loading budgetfile method starts");

		RetailCalendarDAO objRetailDao = new RetailCalendarDAO();

		BudgetDao objBudgetDao = new BudgetDao();

		boolean commitFlag = false;

		// object for parse excel filec
		ExcelParser excelParser = new ExcelParser();

		ArrayList<String> fileList = excelParser.getFiles(_rootpath,
				_relativePath);

		try {

			// get the excel file list from relative path

			if (fileList.size() > 0) {

				// get the period calendar List for given year
				HashMap<String, Integer> periodCalendarMap = objRetailDao
						.excelBasedCalendarList(_conn, null,
								Constants.CALENDAR_PERIOD, _processYear,null);

				List<RetailCalendarDTO> periodCalendarList = objRetailDao
						.forecastCalendarList(_conn, Constants.CALENDAR_PERIOD,
								_processYear, "BUDGET");

				// get the quarter calendar List for given year
				List<RetailCalendarDTO> quarterCalendarList = objRetailDao
						.forecastCalendarList(_conn,
								Constants.CALENDAR_QUARTER, _processYear,
								"BUDGET");

				// get the year calendar list for given year
				List<RetailCalendarDTO> yearCalendarList = objRetailDao
						.forecastCalendarList(_conn, Constants.CALENDAR_YEAR,
								_processYear, "BUDGET");

				// itreate through the file list
				for (int fL = 0; fL < fileList.size(); fL++) {

					String fileName = fileList.get(fL);

					logger.info(" Processing  File Name is............"
							+ fileName);

					// call the budget excel parser method
					// method will read the excel (based on excel format ., if
					// it is format change means code does not work)
					// and after reading the all sheets in given excel ,the data
					// will be groupd and added into BudgetDto

					HashMap<String, BudgetDto> budgetDetails = excelParser
							.budgetExcelParser(fileName, periodCalendarMap,
									"BUDGET");

					logger.info(" Total No of rows " + budgetDetails.size());

					if (budgetDetails.size() > 0) {

						logger.info(" Insert process starts.....");

						// ************************************************************************
						// Calling the insert method
						commitFlag = periodBudget(objBudgetDao, budgetDetails,
								periodCalendarMap);
						commitFlag = quarterbudget(objBudgetDao,
								quarterCalendarList);
						commitFlag = yearbudget(objBudgetDao, yearCalendarList);
						//commitFlag = ctdProcess(periodCalendarList,
						//		quarterCalendarList, yearCalendarList);
						// ***************************************************************************
					}

				}

				if (commitFlag) {
					String archivePath = _rootpath + "/" + _relativePath + "/";
					PrestoUtil.moveFiles(fileList, archivePath
							+ Constants.COMPLETED_FOLDER);
				} else {
					String archivePath = _rootpath + "/" + _relativePath + "/";
					PrestoUtil.moveFiles(fileList, archivePath
							+ Constants.COMPLETED_FOLDER);
				}

			} else {
				logger.error("File Not avilable in Relative path......");
			}

		} catch (Exception exe) {
			logger.error("processbudgetFile Method......" + exe);
			String archivePath = _rootpath + "/" + _relativePath + "/";
			PrestoUtil.moveFiles(fileList, archivePath
					+ Constants.COMPLETED_FOLDER);
			throw new GeneralException("loadbudget Method.....", exe);

		}

		logger.info(".......loadbudgetfile  Method Ends");

		return commitFlag;

	}
	

	/**************************************************************************************************
	 * Method used to insert the Period Level budget details
	 * Argument 1 : BudgetDetails HashMap (From Excel)
	 * Argument 2 : Calendar Map (For Period)
 	 * Process 1 : Get the Previous budget details for updation or insertion
 	 * Process 2 : Insert the finance level budget details
	**********************************************************************************************/
	
	
	private boolean periodBudget(BudgetDao objBudgetDao,
			HashMap<String, BudgetDto> budgetDetails,
			HashMap<String, Integer> periodCalendarMap) throws GeneralException {

		// object for CompStroreDao
		CompStoreDAO objCompStoreDao = new CompStoreDAO();

		HashMap<String, Integer> storeMap = objCompStoreDao.getCompStrId(_conn);
		
		// get the chain id
		_compChainId = objCompStoreDao.getCompChainId(_conn);

		logger.info(" Store Map Size ...." + storeMap.size());

		// itreate the account map
		boolean commit = false;
		try {

			// fetch the previous records for updation.........
			HashMap<String, String> previousBudget = objBudgetDao
					.getPreviousBudgetDetails(_conn, _processYear,
							Constants.CALENDAR_PERIOD,
							Constants.FINANCEDEPARTMENT);

			logger.info(" Store Level Period Record insert begins.....");

			// insert the store level budget details
			commit = objBudgetDao.budgetInsert(_conn, budgetDetails,
					periodCalendarMap, storeMap, Constants.STORE_LEVEL_ID,
					"FINANCE", 0, previousBudget, null, "NONIDENTICAL");

			objBudgetDao.deletePreviousAggregation(_conn, _processYear);

			HashMap<String, BudgetDto> periodbudget = objBudgetDao
					.getbudgetDetails(_conn, _processYear, null, null,
							Constants.CALENDAR_PERIOD, "PERIOD",
							Constants.STORE_LEVEL_ID);

			logger.info(" Period (Store) records size ....."
					+ periodbudget.size());

			commit = objBudgetDao.budgetInsert(_conn, periodbudget, null, null,
					Constants.STORE_LEVEL_ID, "PERIOD", 0, null, null,
					"NONIDENTICAL");

			PristineDBUtil.commitTransaction(_conn, "Process Committed");

			logger.info(" District Level Period  Record insert begins.....");

			HashMap<String, BudgetDto> ditrictPeriodbudget = objBudgetDao
					.getdistrictPeriodbudget(_conn, _processYear, null, null,
							Constants.CALENDAR_PERIOD,
							Constants.STORE_LEVEL_ID, "PERIOD", "");

			HashMap<String, BudgetDto> IdenticaldistrictPeriodbudget = objBudgetDao
					.getdistrictPeriodbudget(_conn, _processYear, null, null,
							Constants.CALENDAR_PERIOD,
							Constants.STORE_LEVEL_ID, "PERIOD", "IDENTICAL");

			logger.info(" Period (District) records size ....."
					+ ditrictPeriodbudget.size());

			commit = objBudgetDao.budgetInsert(_conn, ditrictPeriodbudget,
					null, null, Constants.DISTRICT_LEVEL_ID, "DISTRICTPERIOD",
					0, null, IdenticaldistrictPeriodbudget, "IDENTICAL");

			PristineDBUtil.commitTransaction(_conn, "Process Committed");

			logger.info(" Region Level Period Record insert begins.....");

			HashMap<String, BudgetDto> regionPeriodbudget = objBudgetDao
					.getregionPeriodbudget(_conn, _processYear, null, null,
							Constants.CALENDAR_PERIOD,
							Constants.DISTRICT_LEVEL_ID, "PERIOD");

			logger.info(" Period (Region) records size ....."
					+ regionPeriodbudget.size());

			commit = objBudgetDao.budgetInsert(_conn, regionPeriodbudget, null,
					null, Constants.REGION_LEVEL_ID, "DISTRICTPERIOD", 0, null,
					null, "NONIDENTICAL");

			PristineDBUtil.commitTransaction(_conn, "Process Committed");

			logger.info(" Divison Level Period Record insert begins.....");

			HashMap<String, BudgetDto> divisionPeriodbudget = objBudgetDao
					.getDivisionPeriodbudget(_conn, _processYear, null, null,
							Constants.CALENDAR_PERIOD,
							Constants.REGION_LEVEL_ID, "PERIOD");

			logger.info(" Period (Divison) records size ....."
					+ divisionPeriodbudget.size());

			commit = objBudgetDao.budgetInsert(_conn, divisionPeriodbudget,
					null, null, Constants.DIVISION_LEVEL_ID, "DISTRICTPERIOD",
					0, null, null, "NONIDENTICAL");

			PristineDBUtil.commitTransaction(_conn, "Process Committed");
			
			logger.info(" Chain Level Period Record insert begins.....");

			HashMap<String, BudgetDto> chainPeriodbudget = objBudgetDao
					.getChainPeriodbudget(_conn, _processYear, null, null,
							Constants.CALENDAR_PERIOD,
							Constants.DIVISION_LEVEL_ID, "PERIOD" ,_compChainId);
			
			logger.info(" Period (Chain) records size ....."
					+ chainPeriodbudget.size());

			commit = objBudgetDao.budgetInsert(_conn, chainPeriodbudget,
					null, null, Constants.CHAIN_LEVEL_ID, "DISTRICTPERIOD",
					0, null, null, "NONIDENTICAL");

			PristineDBUtil.commitTransaction(_conn, "Process Committed");
			// method used to insert the quarter level account records

		} catch (Exception exe) {
			logger.error(" Error while inserting the budget data", exe);
			throw new GeneralException("Error while inserting the budget data",
					exe);
		}

		return commit;
	}

	
	/*
	 * Method used to process the quarter level budget records and insert the
	 * values into table 
	 * Argument 1 : objbudgetDao 
	 * Argument 2 : _quarterCalendarList 
	 * return commit flag
	 * @catch Exception
	 * @throws GeneralException
	 */

	private boolean quarterbudget(BudgetDao objbudgetDao,
			List<RetailCalendarDTO> quarterCalendarList)
			throws GeneralException {

		boolean commit = false;
		try {

			// itreate the quarterCalendar List
			for (int qC = 0; qC < quarterCalendarList.size(); qC++) {

				RetailCalendarDTO objCalendarDto = quarterCalendarList.get(qC);

				logger.info(" Processing Quarter Calendar Id "
						+ objCalendarDto.getCalendarId());

				computeProcessDate(objCalendarDto.getStartDate(),
						objCalendarDto.getEndDate());

				logger.info(" Store Level Quarter Record insert begins.....");

				HashMap<String, BudgetDto> quarterbudget = objbudgetDao
						.getbudgetDetails(_conn, _processYear, _startdate,
								_endDate, Constants.CALENDAR_PERIOD, "QUARTER",
								Constants.STORE_LEVEL_ID);

				logger.info(" Quarter (Store) records size ....."
						+ quarterbudget.size());

				commit = objbudgetDao.budgetInsert(_conn, quarterbudget, null,
						null, Constants.STORE_LEVEL_ID, "QUARTER",
						objCalendarDto.getCalendarId(), null, null,
						"NONIDENTICAL");

				logger.info(" District Level Quarter Record insert begins.....");

				HashMap<String, BudgetDto> ditrictPeriodbudget = objbudgetDao
						.getdistrictPeriodbudget(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.STORE_LEVEL_ID, "QUARTER",
								"NONIDENTICAL");

				logger.info(" Quarter (District) records size ....."
						+ ditrictPeriodbudget.size());

				HashMap<String, BudgetDto> IdenticaldistrictPeriodbudget = objbudgetDao
						.getdistrictPeriodbudget(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.STORE_LEVEL_ID, "QUARTER",
								"IDENTICAL");

				commit = objbudgetDao.budgetInsert(_conn, ditrictPeriodbudget,
						null, null, Constants.DISTRICT_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, IdenticaldistrictPeriodbudget, "IDENTICAL");

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" Region Level Quarter Record insert begins.....");

				HashMap<String, BudgetDto> regionPeriodbudget = objbudgetDao
						.getregionPeriodbudget(_conn, _processYear, _startdate,
								_endDate, Constants.CALENDAR_PERIOD,
								Constants.DISTRICT_LEVEL_ID, "QUARTER");

				logger.info(" Quarter (Region) records size ....."
						+ regionPeriodbudget.size());

				commit = objbudgetDao.budgetInsert(_conn, regionPeriodbudget,
						null, null, Constants.REGION_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, null, "NONIDENTICAL");

				logger.info(" Divsion Level Quarter Record insert begins.....");

				HashMap<String, BudgetDto> divisionPeriodbudget = objbudgetDao
						.getDivisionPeriodbudget(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.REGION_LEVEL_ID, "QUARTER");

				logger.info(" Quarter (Disvison) records size ....."
						+ divisionPeriodbudget.size());

				commit = objbudgetDao.budgetInsert(_conn, divisionPeriodbudget,
						null, null, Constants.DIVISION_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, null, "NONIDENTICAL");

				logger.info(" Chain Level Quarter Record insert begins.....");

				HashMap<String, BudgetDto> chainPeriodbudget = objbudgetDao
						.getChainPeriodbudget(_conn, _processYear, _startdate,
								_endDate, Constants.CALENDAR_PERIOD,
								Constants.DIVISION_LEVEL_ID, "QUARTER",
								_compChainId);

				logger.info(" Quarter (Chain) records size ....."
						+ divisionPeriodbudget.size());

				commit = objbudgetDao.budgetInsert(_conn, chainPeriodbudget,
						null, null, Constants.CHAIN_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, null, "NONIDENTICAL");

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

			}

		} catch (Exception e) {

			logger.error(" processQuarterbudget method error ", e);
			throw new GeneralException(" processQuarterbudget method error ", e);

		}

		return commit;
	}

	/*
	 * Method used to process the year budget 
	 * Argument 1 : objbudgetDao 
	 * Argument 2 : List<RetailCalendarDTO>
	 * @catch Exception
	 * @throws gendral exception
	 */

	private boolean yearbudget(BudgetDao objbudgetDao,
			List<RetailCalendarDTO> yearCalendarList) throws GeneralException {

		boolean commit = false;
		try {

			// itreate the year calendar list
			for (int yC = 0; yC < yearCalendarList.size(); yC++) {

				RetailCalendarDTO objCalendarDto = yearCalendarList.get(yC);

				logger.info(" Processing Year Calendar Id "
						+ objCalendarDto.getCalendarId());

				logger.info(" Store Level Year Record insert begins.....");

				// Year budget Process for store

				HashMap<String, BudgetDto> yearbudget = objbudgetDao
						.getbudgetDetails(_conn, _processYear, null, null,
								Constants.CALENDAR_QUARTER, "YEAR",
								Constants.STORE_LEVEL_ID);

				commit = objbudgetDao.budgetInsert(_conn, yearbudget, null,
						null, Constants.STORE_LEVEL_ID, "QUARTER",
						objCalendarDto.getCalendarId(), null, null,
						"NONIDENTICAL");

				logger.info(" District Level Year Record insert begins.....");

				// District Level Year Record insert begins.

				HashMap<String, BudgetDto> ditrictPeriodbudget = objbudgetDao
						.getdistrictPeriodbudget(_conn, _processYear, null,
								null, Constants.CALENDAR_QUARTER,
								Constants.STORE_LEVEL_ID, "YEAR",
								"NONIDENTICAL");

				HashMap<String, BudgetDto> IdenticaldistrictPeriodbudget = objbudgetDao
						.getdistrictPeriodbudget(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_QUARTER,
								Constants.STORE_LEVEL_ID, "YEAR", "IDENTICAL");

				commit = objbudgetDao.budgetInsert(_conn, ditrictPeriodbudget,
						null, null, Constants.DISTRICT_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, IdenticaldistrictPeriodbudget, "IDENTICAL");

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" region Level Year Record insert begins.....");

				// region Level Year Record insert begins....

				HashMap<String, BudgetDto> regionPeriodbudget = objbudgetDao
						.getregionPeriodbudget(_conn, _processYear, null, null,
								Constants.CALENDAR_QUARTER,
								Constants.DISTRICT_LEVEL_ID, "YEAR");

				commit = objbudgetDao.budgetInsert(_conn, regionPeriodbudget,
						null, null, Constants.REGION_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, null, "NONIDENTICAL");

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" Divison Level Year Record insert begins.....");

				// Divison Level Year Record insert begins.....

				HashMap<String, BudgetDto> divisionPeriodbudget = objbudgetDao
						.getDivisionPeriodbudget(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_QUARTER,
								Constants.REGION_LEVEL_ID, "YEAR");

				commit = objbudgetDao.budgetInsert(_conn, divisionPeriodbudget,
						null, null, Constants.DIVISION_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, null, "NONIDENTICAL");

				PristineDBUtil.commitTransaction(_conn, "Process Committed");
				
				logger.info(" Divison Level Year Record insert begins.....");

				// Divison Level Year Record insert begins.....

				HashMap<String, BudgetDto> chainPeriodbudget = objbudgetDao
						.getChainPeriodbudget(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_QUARTER,
								Constants.DIVISION_LEVEL_ID, "YEAR",_compChainId);
				

				commit = objbudgetDao.budgetInsert(_conn, chainPeriodbudget,
						null, null, Constants.CHAIN_LEVEL_ID,
						"DISTRICTQUARTER", objCalendarDto.getCalendarId(),
						null, null, "NONIDENTICAL");

				PristineDBUtil.commitTransaction(_conn, "Process Committed");
			}

		} catch (Exception exe) {
			logger.error(" processYearbudget Method Error...." + exe);
			throw new GeneralException(" processYearbudget Method Error...."
					+ exe);
		}

		return commit;
	}
	
	/*
	 * Method used to process calendar to date business
	 * First Level : Period Level == > Insert Level Period/Quarter/Year
	 * Second Level : Quarter Level ==> Insert Level Quarter/Year
	 *  
	 */
	
	private boolean ctdProcess(List<RetailCalendarDTO> periodCalendarList,
			List<RetailCalendarDTO> quarterCalendarList,
			List<RetailCalendarDTO> yearCalendarList) throws GeneralException,
			ParseException {

		BudgetDao objBudgetDao = new BudgetDao();

		RetailCalendarDAO objRetailCalendarDao = new RetailCalendarDAO();

		boolean processFlag = false;

		try {
			for (int ii = 0; ii < periodCalendarList.size(); ii++) {

				RetailCalendarDTO objRetailcalDto = periodCalendarList.get(ii);

				//List<BudgetDto> periodCtdList = objBudgetDao.getCtdRevenue(
				//		_conn, objRetailcalDto.getCalendarId(), 0, "PERIOD",
				//		Constants.CTD_PERIOD);

				//processFlag = objBudgetDao.budgetCtdInsert(_conn, periodCtdList, null);

			}

			for (int ii = 0; ii < periodCalendarList.size(); ii++) {

				RetailCalendarDTO objRetailcalDto = periodCalendarList.get(ii);

				HashMap<String, Integer> currentCtdMap = objBudgetDao
						.getCurrentCtd(_conn, objRetailcalDto.getCalendarId());

				//List<BudgetDto> periodCtdList = objBudgetDao.getCtdRevenue(
				//		_conn, objRetailcalDto.getCalendarId(), ii, "YEAR",
				//		Constants.CTD_YEAR);

				//processFlag = objBudgetDao.budgetCtdInsert(_conn, periodCtdList, currentCtdMap);

			}

			PristineDBUtil.commitTransaction(_conn, "Comitted");

			for (int ii = 0; ii < quarterCalendarList.size(); ii++) {

				RetailCalendarDTO objRetailcalDto = quarterCalendarList.get(ii);

				//List<BudgetDto> periodCtdList = objBudgetDao.getCtdRevenue(
				//		_conn, objRetailcalDto.getCalendarId(), 0, "PERIOD",
				//		Constants.CTD_QUARTER);

				//processFlag = objBudgetDao.budgetCtdInsert(_conn, periodCtdList, null);

				computeProcessDate(objRetailcalDto.getStartDate(),
						objRetailcalDto.getEndDate());

				List<RetailCalendarDTO> querterBasedPeriodCalendar = objRetailCalendarDao
						.RowTypeBasedCalendarList(_conn, _startdate, _endDate,
								Constants.CALENDAR_PERIOD);

				for (int jj = 0; jj < querterBasedPeriodCalendar.size(); jj++) {

					RetailCalendarDTO objCalDto = querterBasedPeriodCalendar
							.get(jj);

					HashMap<String, Integer> currentCtdMap = objBudgetDao
							.getCurrentCtd(_conn, objCalDto.getCalendarId());

					//List<BudgetDto> periodCtdPeriodList = objBudgetDao
					//		.getCtdRevenue(_conn, objCalDto.getCalendarId(),
					//				jj, "YEAR", Constants.CTD_QUARTER);

					//processFlag = objBudgetDao.budgetCtdInsert(_conn, periodCtdPeriodList, currentCtdMap);

				}

			}

			for (int ii = 0; ii < quarterCalendarList.size(); ii++) {

				RetailCalendarDTO objRetailcalDto = quarterCalendarList.get(ii);

				HashMap<String, Integer> currentCtdMap = objBudgetDao
						.getCurrentCtd(_conn, objRetailcalDto.getCalendarId());

				//List<BudgetDto> periodCtdList = objBudgetDao.getCtdRevenue(
				//		_conn, objRetailcalDto.getCalendarId(), ii, "YEAR",
				//		Constants.CTD_YEAR);

				//processFlag = objBudgetDao.budgetCtdInsert(_conn, periodCtdList, currentCtdMap);

			}

			PristineDBUtil.commitTransaction(_conn, "Comitted");

			for (int ii = 0; ii < yearCalendarList.size(); ii++) {

				RetailCalendarDTO objRetailcalDto = yearCalendarList.get(ii);

				//List<BudgetDto> periodCtdList = objBudgetDao.getCtdRevenue(
				//		_conn, objRetailcalDto.getCalendarId(), 0, "PERIOD",
				//		Constants.CTD_YEAR);

				//processFlag = objBudgetDao.budgetCtdInsert(_conn, periodCtdList, null);

			}

			PristineDBUtil.commitTransaction(_conn, "Comitted");

		} catch (GeneralException e) {
			logger.error(e);
			throw new GeneralException(" Error In ctd Processing.... ", e);
		}

		return processFlag;
	}
	
	

	private void computeProcessDate(String quarterstartDate,
			String quarterEndDate) throws ParseException {

		Calendar cal = Calendar.getInstance();
		// find the date with start time
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		Date date = (Date) formatter.parse(quarterstartDate);
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_startdate = cal.getTime();
		Date edate = (Date) formatter.parse(quarterEndDate);
		cal.setTime(edate);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_endDate = cal.getTime();

	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Tops budget Loader ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}
