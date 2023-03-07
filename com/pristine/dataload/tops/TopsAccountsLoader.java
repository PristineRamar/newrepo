/*
 * Title: TOPS   Tops Accounts Loader
 *
 *****************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 12-06-2012	    Dinesh Kumar V	     Initial  Version 
 * Version 0.2   21-06-2012     Dinesh Kumar V		 Adding identical/non identical process
 * Version 0.3   25-06-2012     Dinesh Kumar V		 Adding Ctd based accounts prorcess
 * Version 0.4   18-08-2012     Dinesh Kumar V		 Chain support code added
 *******************************************************************************************
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
import com.pristine.dao.salesanalysis.AccountsDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.BudgetDto;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.ExcelParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class TopsAccountsLoader {

	private static Logger logger 	= Logger.getLogger("TopsAccountsLoader");
	private  Connection _conn 		= null; // hold the input Db connection
	private String _rootpath 		= null; // hold the base path of tops
	private String _relativePath 	= null; // hold the input filename
	private	int _processYear 		= 0; // hold the input process year
	private Date _startdate 		= null; // hold the input start date
	private Date _endDate 			= null; // hold the input end date
	private int _compChainId        = 0;  // hold the presto chain id

	
	/*
	 * Class Constructor 
	 * Argument 1 : Relative path 
	 * Argument 2 : Start Period
	 * Argument 3 : End Period
	 * @catch GendralException
	 */
	public TopsAccountsLoader(String relativePath,  int processYear) {

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
			logger.error("Connection Failed");
			logger.error(exe);
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

		PropertyConfigurator.configure("log4j-accounts-loader.properties");
		logger.info(" ..................Tops accounts loader process starts");

		String relativePath = null; // hold the input file relative path.
		int processYear = 0; // hold the input processing year
		TopsAccountsLoader accountsLoader = null;
		logCommand(args);
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

			accountsLoader = new TopsAccountsLoader(relativePath, processYear);
			accountsLoader.processAccountsFile();

			logger.info("...................Tops accounts Loader Process Ends");
		} catch (GeneralException exe) {
			PristineDBUtil.rollbackTransaction(accountsLoader._conn,
					"Process Rollbacked");
			logger.error("Main Method Error" + exe);
		}

		finally {
			try {

				PristineDBUtil.close(accountsLoader._conn);

			} catch (Exception exe) {
				logger.error(" Error While closing the connection " + exe);
			}
		}

	}

	/*
	 * Method used to read the budget excel and insert the excel cell values
	 * into DB Call Method 1 : ParseBudgetExcel Method Call Method 2 :
	 * insertMethod Argument 1 : File Name
	 * 
	 * @catch Exception
	 * 
	 * @throws Gendral Exception
	 */
	private void processAccountsFile() throws GeneralException {

		logger.info("..... Loading accountsFile method starts");

		RetailCalendarDAO objRetailDao = new RetailCalendarDAO();

		// object for parse excel filec
		ExcelParser excelParser = new ExcelParser();
		ArrayList<String> fileList = excelParser.getFiles(_rootpath,_relativePath);
		
		try {

			// get the excel file list from relative path

			if (fileList.size() > 0) {
				
				// get the day calendar list
				HashMap<String, Integer> dayCalendarMap = objRetailDao
						.excelBasedCalendarList(_conn, null,
								Constants.CALENDAR_PERIOD, _processYear,null);

				// get the period calendar list
				List<RetailCalendarDTO> periodCalendarList = objRetailDao
						.forecastCalendarList(_conn,
								Constants.CALENDAR_PERIOD, _processYear,
								"BUDGET");
				
				// get the quarter calendar list
				List<RetailCalendarDTO> quarterCalendarList = objRetailDao
						.forecastCalendarList(_conn,
								Constants.CALENDAR_QUARTER, _processYear,
								"BUDGET");

				// get the year calendar lsit
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

					HashMap<String, BudgetDto> accountsDetails = excelParser
							.budgetExcelParser(fileName, dayCalendarMap,"ACCOUNTS");

					logger.info(" Total No of rows " + accountsDetails.size());

					if (accountsDetails.size() > 0) {
						logger.debug(" Insert process starts.....");
						insertAccounstDetails(accountsDetails,
								dayCalendarMap, quarterCalendarList,
								yearCalendarList,periodCalendarList);
					}

				}
				PristineDBUtil.commitTransaction(_conn, "Process Committed");
				String archivePath = _rootpath + "/" + _relativePath + "/";
				PrestoUtil.moveFiles(fileList, archivePath
						+ Constants.COMPLETED_FOLDER);
			} else {
				logger.error("File Not avilable in Relative path......");
			}

		} catch (Exception exe) {
			logger.error("processaccountsFile Method......" + exe);
			PristineDBUtil.rollbackTransaction(_conn, "Process Rollbacked");
			String archivePath = _rootpath + "/" + _relativePath + "/";
			PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			throw new GeneralException("loadaccounts Method.....", exe);

		}

		logger.debug(".......loadaccountsfile  Method Ends");

	}

	/*
	 *  Method mainly used to insert the excel values into table 
	 * Argument 1 : Excel Details 
	 * Argument 2 : Calendar Map 
	 * Method call 1 : Itreate the accountsdetail map and add into preparedstatment batch 
	 * Method call 2 : Execute the batch
	 * @catch Exception ,SqlException
	 * @throws gendralexception
	 */
	private boolean insertAccounstDetails(
			HashMap<String, BudgetDto> accountsDetails,
			HashMap<String, Integer> calendarMap,
			List<RetailCalendarDTO> quarterCalendarList,
			List<RetailCalendarDTO> yearCalendarList,
			List<RetailCalendarDTO> periodCalendarList) throws GeneralException {

		// Object for AccountDao
		AccountsDao objAccountsDao = new AccountsDao();

		// object for CompStroreDao
		CompStoreDAO objCompStoreDao = new CompStoreDAO();

		HashMap<String, Integer> storeMap = objCompStoreDao.getCompStrId(_conn);

		logger.debug(" Store Map Size ...." + storeMap.size());

		// itreate the account map
		boolean commit = false;
		try {

			// insert the finance level accounts details

			logger.info(" Finance Department accounts insert process begins....");

			HashMap<String, Integer> periodCtdMap = objAccountsDao
					.getPeriodCtdMap(_conn, _processYear, "SALES_AGGR_DAILY",
							Constants.CALENDAR_PERIOD);

			// insert the store level accounts details
			commit = objAccountsDao.accountsInsert(_conn, accountsDetails,
					calendarMap, storeMap, Constants.STORE_LEVEL_ID, "FINANCE",
					0, periodCtdMap);

			PristineDBUtil.commitTransaction(_conn, "Process Committed");
			
			// process used to get the compchain_id
			_compChainId = objCompStoreDao.getCompChainId(_conn);

			commit = processPeriodAccounts(objAccountsDao, calendarMap,
					periodCtdMap);

			commit = processQuarterAccounts(objAccountsDao, quarterCalendarList);

			commit = processYearAccounts(objAccountsDao, yearCalendarList);

			//commit = ctdProcess(periodCalendarList, quarterCalendarList);

		} catch (Exception exe) {
			logger.error(" Error while inserting the accounts data", exe);
			throw new GeneralException(
					"Error while inserting the accounts data", exe);
		}

		return commit;
	}

	

	/**
	 * @param objAccountsDao
	 * @param calendarMap
	 * @param periodCtdMap
	 * @return
	 * @throws GeneralException
	 */
	private boolean processPeriodAccounts(AccountsDao objAccountsDao,
			HashMap<String, Integer> calendarMap,
			HashMap<String, Integer> periodCtdMap) throws GeneralException {

		boolean commit = false;

		logger.info(" Store Level Period accounts insert process begins....");
		
		// get the period level aggregation
		HashMap<String, BudgetDto> periodAccounts = objAccountsDao
				.getAccountsDetails(_conn, _processYear, null, null,
						Constants.CALENDAR_PERIOD, "PERIOD");

		logger.info(" Store Level Period record Count ...."
				+ periodAccounts.size());

		// Insert the period process
		commit = objAccountsDao.periodInsert(_conn, periodAccounts,
				Constants.STORE_LEVEL_ID, periodCtdMap);

		PristineDBUtil.commitTransaction(_conn, "Process Committed");

		logger.info(" District Level Period accounts insert process begins....");

		HashMap<String, Integer> summaryCtdMap = objAccountsDao
				.getPeriodCtdMap(_conn, _processYear,
						"SALES_AGGR_DAILY_ROLLUP", Constants.CALENDAR_PERIOD);

		HashMap<String, BudgetDto> ditrictPeriodAccounts = objAccountsDao
				.getdistrictPeriodAccounts(_conn, _processYear, null, null,
						Constants.CALENDAR_PERIOD, Constants.STORE_LEVEL_ID,
						"PERIOD");

		logger.info(" Get the Identical store details......");

		HashMap<String, BudgetDto> identicalDetails = objAccountsDao
				.getIdenticalDetails(_conn, _processYear,
						Constants.CALENDAR_PERIOD, Constants.STORE_LEVEL_ID);

		logger.info(" District Level Period record count...."
				+ ditrictPeriodAccounts.size());

		commit = objAccountsDao.districtInsert(_conn, ditrictPeriodAccounts,
				Constants.DISTRICT_LEVEL_ID, "DISTRICT", identicalDetails,
				summaryCtdMap, 0, Constants.CTD_PERIOD);

		PristineDBUtil.commitTransaction(_conn, "Process Committed");
				
		logger.info(" Region Level Period accounts insert process begins....");

		HashMap<String, BudgetDto> regionPeriodAccounts = objAccountsDao
				.getregionPeriodAccounts(_conn, _processYear, null, null,
						Constants.CALENDAR_PERIOD, Constants.DISTRICT_LEVEL_ID,
						"PERIOD");

		logger.info(" Region Level Period record count..... "
				+ regionPeriodAccounts.size());

		commit = objAccountsDao.districtInsert(_conn, regionPeriodAccounts,
				Constants.REGION_LEVEL_ID, "REGION", null, summaryCtdMap, 0,
				Constants.CTD_PERIOD);

		PristineDBUtil.commitTransaction(_conn, "Process Committed");

		logger.info(" Divison Level Period accounts insert process begins....");

		HashMap<String, BudgetDto> divisionPeriodAccounts = objAccountsDao
				.getDivisionPeriodAccounts(_conn, _processYear, null, null,
						Constants.CALENDAR_PERIOD, Constants.REGION_LEVEL_ID,
						"PERIOD");

		logger.info(" Division Level period record count ......"
				+ divisionPeriodAccounts.size());

		commit = objAccountsDao.districtInsert(_conn, divisionPeriodAccounts,
				Constants.DIVISION_LEVEL_ID, "REGION", null, summaryCtdMap, 0,
				Constants.CTD_PERIOD);

		PristineDBUtil.commitTransaction(_conn, "Process Committed");
		
		logger.info(" Chain Level Period accounts insert process begins....");
				
		HashMap<String, BudgetDto> chainPeriodAccounts = objAccountsDao
				.getChainPeriodAccounts(_conn, _processYear, null, null,
						Constants.CALENDAR_PERIOD, Constants.DIVISION_LEVEL_ID,
						"PERIOD", _compChainId);
		
		logger.info(" Chain Level period record count ......"
				+ divisionPeriodAccounts.size());

		commit = objAccountsDao.districtInsert(_conn, chainPeriodAccounts,
				Constants.CHAIN_LEVEL_ID, "REGION", null, summaryCtdMap, 0,
				Constants.CTD_PERIOD);
		
		PristineDBUtil.commitTransaction(_conn, "Process Committed");
		
		return commit;
	}
	
	
	
	/*
	 * Method used to process the quarter level accounts records and insert the
	 * values into table 
	 * Argument 1 : objAccountsDao 
	 * Argument 2:_quarterCalendarList return commit flag
	 * @catch Exception
	 * @throws gendral exception
	 */

	private boolean processQuarterAccounts(AccountsDao objAccountsDao,
			List<RetailCalendarDTO> quarterCalendarList)
			throws GeneralException {

		boolean commit = false;
		try {

			// itreate the quarterCalendar List
			for (int qC = 0; qC < quarterCalendarList.size(); qC++) {

				// itreate the calendar Id
				RetailCalendarDTO objCalendarDto = quarterCalendarList.get(qC);

				logger.info(" Processing Quarter Calendar Id "
						+ objCalendarDto.getCalendarId());

				// Convert the sql date to util date format
				computeProcessDate(objCalendarDto.getStartDate(),
						objCalendarDto.getEndDate());

				logger.info(" Store Level Quarter accounts insert begins.....");

				// get the Quarter level Ctd id from "Sales_aggr_daily"
				// table........
				HashMap<String, Integer> summaryCtdMap = objAccountsDao
						.getPeriodCtdMap(_conn, _processYear,
								"SALES_AGGR_DAILY", Constants.CALENDAR_QUARTER);

				// get the quarter level accounts records from sales_aggr_rollup
				// table
				HashMap<String, BudgetDto> quarterAccounts = objAccountsDao
						.getAccountsDetails(_conn, _processYear, _startdate,
								_endDate, Constants.CALENDAR_PERIOD, "QUARTER");

				// insert the quarter level accounts records
				commit = objAccountsDao.quarterInsert(_conn, quarterAccounts,
						Constants.STORE_LEVEL_ID, "STORE", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_QUARTER);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" District level quarter record count ..............");

				summaryCtdMap = objAccountsDao.getPeriodCtdMap(_conn,
						_processYear, "SALES_AGGR_DAILY_ROLLUP",
						Constants.CALENDAR_QUARTER);

				HashMap<String, BudgetDto> ditrictPeriodAccounts = objAccountsDao
						.getDistrictQuarterAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.DISTRICT_LEVEL_ID, "QUARTER");

				commit = objAccountsDao.districtInsert(_conn,
						ditrictPeriodAccounts, Constants.DISTRICT_LEVEL_ID,
						"DISTRICTQUARTER", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_QUARTER);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" Region Level quarter accounts insert begins......");

				HashMap<String, BudgetDto> regionPeriodAccounts = objAccountsDao
						.getregionPeriodAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.DISTRICT_LEVEL_ID, "QUARTER");

				logger.info(" Region level quarter record count ...."
						+ regionPeriodAccounts.size());

				commit = objAccountsDao.districtInsert(_conn,
						regionPeriodAccounts, Constants.REGION_LEVEL_ID,
						"REGION", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_QUARTER);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");
				logger.info(" Division Level quarter accounts begins.....");

				HashMap<String, BudgetDto> divisionPeriodAccounts = objAccountsDao
						.getDivisionPeriodAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.REGION_LEVEL_ID, "QUARTER");

				commit = objAccountsDao.districtInsert(_conn,
						divisionPeriodAccounts, Constants.DIVISION_LEVEL_ID,
						"REGION", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_QUARTER);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");
				
				
				logger.info(" Chain Level quarter accounts begins.....");

				HashMap<String, BudgetDto> chainPeriodAccounts = objAccountsDao
						.getChainPeriodAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_PERIOD,
								Constants.DIVISION_LEVEL_ID, "QUARTER" ,_compChainId);

				commit = objAccountsDao.districtInsert(_conn,
						chainPeriodAccounts, Constants.CHAIN_LEVEL_ID,
						"REGION", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_QUARTER);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");
			}

		} catch (Exception e) {
			logger.error(" processQuarterAccounts method error ", e);
			throw new GeneralException(" processQuarterAccounts method error ",	e);
		}
		return commit;
	}
	


	private boolean processYearAccounts(AccountsDao objAccountsDao,
			List<RetailCalendarDTO> yearCalendarList) throws GeneralException {

		boolean commit = false;
		try {

			// itreate the year calendar list
			for (int yC = 0; yC < yearCalendarList.size(); yC++) {

				RetailCalendarDTO objCalendarDto = yearCalendarList.get(yC);

				logger.info(" Processing Year Calendar Id "
						+ objCalendarDto.getCalendarId());

				computeProcessDate(objCalendarDto.getStartDate(),
						objCalendarDto.getEndDate());

				logger.info(" store Level year accounts begins.....");

				HashMap<String, Integer> summaryCtdMap = objAccountsDao
						.getPeriodCtdMap(_conn, _processYear,
								"SALES_AGGR_DAILY", Constants.CALENDAR_YEAR);

				HashMap<String, BudgetDto> quarterAccounts = objAccountsDao
						.getAccountsDetails(_conn, _processYear, null, null,
								Constants.CALENDAR_QUARTER, "YEAR");

				commit = objAccountsDao.quarterInsert(_conn, quarterAccounts,
						Constants.STORE_LEVEL_ID, "STORE", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_YEAR);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" District Level year accounts begins.....");

				summaryCtdMap = objAccountsDao.getPeriodCtdMap(_conn,
						_processYear, "SALES_AGGR_DAILY_ROLLUP",
						Constants.CALENDAR_YEAR);

				HashMap<String, BudgetDto> ditrictPeriodAccounts = objAccountsDao
						.getDistrictQuarterAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_QUARTER,
								Constants.DISTRICT_LEVEL_ID, "YEAR");

				commit = objAccountsDao.districtInsert(_conn,
						ditrictPeriodAccounts, Constants.DISTRICT_LEVEL_ID,
						"DISTRICTQUARTER", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_YEAR);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" Region Level year accounts begins.....");

				HashMap<String, BudgetDto> regionPeriodAccounts = objAccountsDao
						.getregionPeriodAccounts(_conn, _processYear, null,
								null, Constants.CALENDAR_QUARTER,
								Constants.DISTRICT_LEVEL_ID, "YEAR");

				commit = objAccountsDao.districtInsert(_conn,
						regionPeriodAccounts, Constants.REGION_LEVEL_ID,
						"REGION", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_YEAR);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

				logger.info(" Division Level year accounts begins.....");

				HashMap<String, BudgetDto> divisionPeriodAccounts = objAccountsDao
						.getDivisionPeriodAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_QUARTER,
								Constants.REGION_LEVEL_ID, "YEAR");

				commit = objAccountsDao.districtInsert(_conn,
						divisionPeriodAccounts, Constants.DIVISION_LEVEL_ID,
						"REGION", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_YEAR);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");
				
				logger.info(" Chain Level year accounts begins.....");

				HashMap<String, BudgetDto> chainPeriodAccounts = objAccountsDao
						.getChainPeriodAccounts(_conn, _processYear,
								_startdate, _endDate,
								Constants.CALENDAR_QUARTER,
								Constants.DIVISION_LEVEL_ID, "YEAR" ,_compChainId);

				commit = objAccountsDao.districtInsert(_conn,
						chainPeriodAccounts, Constants.CHAIN_LEVEL_ID,
						"REGION", null, summaryCtdMap,
						objCalendarDto.getCalendarId(), Constants.CTD_YEAR);

				PristineDBUtil.commitTransaction(_conn, "Process Committed");

			}

		} catch (Exception exe) {
			logger.error("processYearAccounts Method Error...." ,exe);
			throw new GeneralException("processYearAccounts Method Error....",exe);
		}

		return commit;
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
	
	private boolean ctdProcess(List<RetailCalendarDTO> periodCalendarList,
			List<RetailCalendarDTO> quarterCalendarList)
			throws GeneralException {
		
		boolean insertFlag = false;
		
		AccountsDao objAccountsDao = new AccountsDao();
		
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();
		
		
		try {
			// Method Process the period Calendar to date
			
			for (int ii = 0; ii < periodCalendarList.size(); ii++) {

				RetailCalendarDTO objCalendarDto = periodCalendarList.get(ii);

				List<BudgetDto> ctdstoreRevenue = objAccountsDao
						.getrevenueforCtd(_conn,
								objCalendarDto.getCalendarId(), ii,
								"SALES_AGGR");

				for (int jj = 0; jj < ctdstoreRevenue.size(); jj++) {

					BudgetDto objBudgetDto = ctdstoreRevenue.get(jj);

					String ctdId = objAccountsDao.getCtdid(_conn,
							objCalendarDto.getCalendarId(), "SALES_AGGR",
							objBudgetDto.getLocationId(),
							objBudgetDto.getLocationLevelId(),
							objBudgetDto.getProcessproductId(),
							objBudgetDto.getProcessproductLevelId());
					
					objAccountsDao.upDateCtd(_conn,
							objBudgetDto.getStoreBudget(),
							objBudgetDto.getIdenticalBudget(), ctdId,
							Constants.CTD_YEAR);

					PristineDBUtil.commitTransaction(_conn, "Commit");
					
			}
				
				
				List<BudgetDto> ctdDistrictRevenue = objAccountsDao
						.getrevenueforCtd(_conn,
								objCalendarDto.getCalendarId(), ii,
								"SALES_AGGR_ROLLUP");

				for (int kk = 0; kk < ctdDistrictRevenue.size(); kk++) {

					BudgetDto objBudgetDto = ctdDistrictRevenue.get(kk);

					String ctdId = objAccountsDao.getCtdid(_conn,
							objCalendarDto.getCalendarId(),
							"SALES_AGGR_ROLLUP", objBudgetDto.getLocationId(),
							objBudgetDto.getLocationLevelId(),
							objBudgetDto.getProcessproductId(),
							objBudgetDto.getProcessproductLevelId());

					objAccountsDao.upDateCtd(_conn,
							objBudgetDto.getStoreBudget(),
							objBudgetDto.getIdenticalBudget(), ctdId,
							Constants.CTD_YEAR);

					PristineDBUtil.commitTransaction(_conn, "Commit");
					
					insertFlag =true;

				}
			}
			
			
			for (int qC = 0; qC < quarterCalendarList.size(); qC++) {
				
				
				RetailCalendarDTO objQuarterCalendar = quarterCalendarList.get(qC);
				
				computeProcessDate(objQuarterCalendar.getStartDate(),objQuarterCalendar.getEndDate());
								
				List<RetailCalendarDTO> querterBasedPeriodCalendar = objCalendarDao
						.RowTypeBasedCalendarList(_conn, _startdate, _endDate,
								Constants.CALENDAR_PERIOD);
				
				
				for (int ii = 0; ii < querterBasedPeriodCalendar.size(); ii++) {

					RetailCalendarDTO objCalendarDto = querterBasedPeriodCalendar.get(ii);

					List<BudgetDto> ctdstoreRevenue = objAccountsDao
							.getrevenueforCtd(_conn,
									objCalendarDto.getCalendarId(), ii,
									"SALES_AGGR");

					for (int jj = 0; jj < ctdstoreRevenue.size(); jj++) {

						BudgetDto objBudgetDto = ctdstoreRevenue.get(jj);

						String ctdId = objAccountsDao.getCtdid(_conn,
								objCalendarDto.getCalendarId(), "SALES_AGGR",
								objBudgetDto.getLocationId(),
								objBudgetDto.getLocationLevelId(),
								objBudgetDto.getProcessproductId(),
								objBudgetDto.getProcessproductLevelId());
						
						objAccountsDao.upDateCtd(_conn,
								objBudgetDto.getStoreBudget(),
								objBudgetDto.getIdenticalBudget(), ctdId,
								Constants.CTD_QUARTER);

						PristineDBUtil.commitTransaction(_conn, "Commit");
						
				}
					
					
					List<BudgetDto> ctdDistrictRevenue = objAccountsDao
							.getrevenueforCtd(_conn,
									objCalendarDto.getCalendarId(), ii,
									"SALES_AGGR_ROLLUP");

					for (int kk = 0; kk < ctdDistrictRevenue.size(); kk++) {

						BudgetDto objBudgetDto = ctdDistrictRevenue.get(kk);

						String ctdId = objAccountsDao.getCtdid(_conn,
								objCalendarDto.getCalendarId(),
								"SALES_AGGR_ROLLUP",
								objBudgetDto.getLocationId(),
								objBudgetDto.getLocationLevelId(),
								objBudgetDto.getProcessproductId(),
								objBudgetDto.getProcessproductLevelId());

						objAccountsDao.upDateCtd(_conn,
								objBudgetDto.getStoreBudget(),
								objBudgetDto.getIdenticalBudget(), ctdId,
								Constants.CTD_QUARTER);

						PristineDBUtil.commitTransaction(_conn, "Commit");
						
						insertFlag =true;

					}
				
		} 
				
			}
		}catch (ParseException e) {
			 logger.error(e);
			 throw new GeneralException("Error in Period Calendar to Date..... " , e);
		} catch (GeneralException e) {
			 logger.error(e);
			 throw new GeneralException("Error in Period Calendar to Date..... " , e);
		}

		
		
		return insertFlag;
	}
	
	
	
	
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Tops Accounts Loader ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
}

