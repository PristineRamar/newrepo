/*
 * title: TOPS SUMMARY YEAR MOVEMENT
 *
 *******************************************************************************
 * modification history
 *------------------------------------------------------------------------------
 * version		date		name			remarks
 *------------------------------------------------------------------------------
 * Version 0.1	12/04/2012	Dinesh kumar	initial version 
 * Version 0.2  06/08/2012  Dinesh Kumar    Movement By Volume code added 
 *******************************************************************************
 */
package com.pristine.dataload.salesanalysis;

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
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationCtdDao;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


public class SummaryYear {
	
	static Logger logger = Logger.getLogger("SummaryYearly");
	
	// global variables
	private Date _startDate 		= null;                 // hold the start date
	private Date _endDate 			= null;                 // hold the end date
	private Date _processDate       = null;                 // hold the input process date
	private String _storeNumber 	= "";                   // hold the input store number
	private String _districtNumber 	= "";                   // hold the input district number
	private String _reCalculation 	= "";                   // hold the input recalculation flag
	private Connection _conn 		= null;                 // hold the database connection
	private int _yearCalendarId 	= 0;                    // hold the Year Calendar Id
	private String _repaitFlag = null; // hold the repair flag value
	
	/*
	 * ****************************************************************
	 * Class constructor 
	 * Argument 1 : ProcessDate 
	 * Argument 2 : Store Number
	 * Argument 3 : District Number
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryYear(Date processDate, String storeNo, String districtNo,
			String reCalculation, String repairFlag) {
		
		// Validate the Input fields
		if (storeNo == "" && districtNo == "") {
			logger.error(" Store Numebr / District Id is missing in Input");
			System.exit(1);
		}

		//If process date is null then set currentDate - 1 as default 
		if (processDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			processDate = cal.getTime();
		}
		
		if (districtNo != "") {
			storeNo = "";
		}

		if (reCalculation != "") {
			_reCalculation = reCalculation;
		}
		
		// process added for repair flag
		if (repairFlag == null) {
			logger.error(" RepairFlag missing in input..");
			System.exit(1);
		}
		
		PropertyManager.initialize("analysis.properties");

		// get the dbconnection
		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error(" Connection Failed...." + gex);
			System.exit(1);
		}
		
		// Find the Year Start date for given input
		_storeNumber    = storeNo;
		_districtNumber = districtNo;
		_processDate    = processDate;
		_repaitFlag = repairFlag;

	}
	 

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Store Number 
	 * Argument 2: District Number 
	 * Argument 3: Process Date
	 * Argument 4: Recalculation Flag
	 * If the District or Store Number is mandatory. 
	 * If both are are specified then consider district alone
	 * ****************************************************************
	 */
	
	public static void main(String[] args) throws GeneralException {
		
	// Initialize the summary-Year log4j
	PropertyConfigurator.configure("log4j-summary-year.properties");

	logger.info("*** Summary Year Process Begins ***");
	logCommand(args);
	Date processDate 			= null; 		// hold the input start date
	String storeNo 				= ""; 			// hold the input store number
	String districtNo 			= ""; 			// hold the input district number
	String reCalculation 		= ""; 			// hold the recalculation (Yes/No)
	DateFormat dateFormat 		= new SimpleDateFormat("MM/dd/yyyy"); // date format
	SummaryYear summaryYear = null;
	String repairFlag = null; // hold the input repair process

	try {
	   for (int iP = 0; iP < args.length; iP++) {
		String arg = args[iP];

		if (arg.startsWith("PROCESSDATE")) {
		String inputDate = arg.substring("PROCESSDATE=".length());
		try {
			processDate = dateFormat.parse(inputDate);
		} catch (ParseException pxe) {
		 logger.error("The given process date is invalid");
		 System.exit(1);
		}
		}
		// Get the Store number from command line
		if (arg.startsWith("STORE")) {
			storeNo = arg.substring("STORE=".length());
		}

		// Get the District id from command line
		if (arg.startsWith("DISTRICT")) {
	    	 districtNo = arg.substring("DISTRICT=".length());
		}

		if (arg.startsWith("RECALCULATION")) {
					reCalculation = arg.substring("RECALCULATION=".length());
				}
				
				// get the repair process need or not from command line
				if (arg.startsWith("REPAIRFLAG")) {
					repairFlag = arg.substring("REPAIRFLAG=".length());
				}
				
			}

			// call the Constructor
			summaryYear = new SummaryYear(processDate, storeNo, districtNo,
					reCalculation ,repairFlag);

			// call the process Period data
			summaryYear.processYearData();

		} catch (Exception exe) {
			logger.error(" Summary Year Process Error.... " + exe);
			throw new GeneralException("Summary Year Process Error.... " + exe);
		} finally {
			logger.info("*** Summary Year Process Ends ***");			
			PristineDBUtil.close(summaryYear._conn);
		}

	}
	
	/*
	 * ****************************************************************
	 * get the Year and Day calendar id for a given process date
	 * get Store-id for a given Store number / District number
	 * Arguemnt 1  : process Date
	 * Argument 2  : Store Number / District Numebr
	 * @catch GeneralException
	 * ****************************************************************
	 */
	
	private void processYearData() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for SalesAggregationBusiness
		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();

		// object for Sales Aggregation Dao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();

		// Object for CompStoreDao
		CompStoreDAO objCompStoreDao = new CompStoreDAO();

		// Object for SalesAggregationCtdDao
		SalesAggregationCtdDao objCtdDao = new SalesAggregationCtdDao();

		try {	
			
			// hold the week calendar id
			RetailCalendarDTO objRetailCal = null;

			// hold the day calendar id based on week calendar id
			List<RetailCalendarDTO> dailyCalendarList = null;

			

			// input is district then get the all store number
			 List<SummaryDataDTO> storeList = objCompStoreDao
							.getStoreNumebrs(_conn, _districtNumber,
									_storeNumber);

			for (int sN = 0; sN < storeList.size(); sN++) {

				SummaryDataDTO dataDto = storeList.get(sN);

				// if Recalculation mode is Yes then delete the previous  aggregation
												
				if (_repaitFlag.equalsIgnoreCase("FULL")) {

					// If repair mode FULL .. do the repair aggregation
					// Check the repair process need or not for given store
					RetailCalendarDTO repairCalendarDates = objCalendarDao
							.getRepairCalendarDates(_conn,
									dataDto.getLocationId(),
									Constants.STORE_LEVEL_ID,
									"SALES_AGGR_DAILY_TEMP");

					if (repairCalendarDates != null) {

						// get the week calendar id list.....
						List<RetailCalendarDTO> repairWeekCalendarList = objCalendarDao
								.getRepairCalendarList(_conn,
										repairCalendarDates.getStartDate(),
										_processDate, Constants.CALENDAR_YEAR);
						int weekLoopSize = repairWeekCalendarList.size();

						for (int rWC = 0; rWC < repairWeekCalendarList.size(); rWC++) {
							RetailCalendarDTO objWeekCalDto = repairWeekCalendarList
									.get(rWC);

							_yearCalendarId = objWeekCalDto.getCalendarId();
							logger.debug("Process End Date:" + _processDate);
							// check week calendar id had more than one calendar
							// id

							dailyCalendarList = new ArrayList<RetailCalendarDTO>();

							dailyCalendarList = objCalendarDao
									.getRepairDayCalendarList(_conn,
											weekLoopSize, rWC,
											repairCalendarDates.getStartDate(),
											_processDate, _yearCalendarId);
							
							// If condition added to supress ArrayIndexOutOfBoundsException
							if(dailyCalendarList != null && dailyCalendarList.size() > 0){
								RetailCalendarDTO objRepairStartDate = dailyCalendarList
										.get(0);
	
								RetailCalendarDTO objRepairEndDate = dailyCalendarList
										.get(dailyCalendarList.size() - 1);
	
								if (objCalendarDao.checkWeekStartDate(_conn,
										objRepairStartDate.getStartDate(),
										Constants.CALENDAR_YEAR)) {
	
									int lastUpdatedCalendarId = objRepairStartDate
											.getCalendarId() - 1;
	
									// get the previous aggregation details for
									// repair process
									HashMap<String, SummaryDataDTO> dailyTempAggregation = objSalesDao
											.getTempSalesAggregation(_conn,
													dataDto.getLocationId(),
													Constants.STORE_LEVEL_ID,
													"SALES_AGGR",
													"SALES_AGGR_DAILY_TEMP",
													dailyCalendarList,
													_yearCalendarId);
	
									// update the previous aggregation details into
									// SALES_AGGR_WEEKLY table.
									objSalesDao.updateSalesAggr(_conn,
											dailyTempAggregation,
											lastUpdatedCalendarId, _yearCalendarId,
											"SALES_AGGR", "REPAIR", null);
	
									// delete the previous aggregation In Sales
									// Table
									//logger.debug("Delete the existing CTD data");
									//objCtdDao.deletePreviousCtdAggregation(_conn,
									//		dataDto.getLocationId(),
									//		Constants.STORE_LEVEL_ID,
									//		objRepairStartDate.getStartDate(),
									//		objRepairEndDate.getStartDate(),
									//		"SALES_AGGR_DAILY_TEMP",
									//		Constants.CTD_YEAR);
	
									//objCtdDao.deletePreviousCtdAggregation(_conn,
									//		dataDto.getLocationId(),
									//		Constants.STORE_LEVEL_ID,
									//		objRepairStartDate.getStartDate(),
									//		objRepairEndDate.getStartDate(),
									//		"SALES_AGGR_DAILY", Constants.CTD_YEAR);
								} else {
	
									// delete the previous aggregation In Sales
									// Table
									//logger.debug("Delete the existing CTD data");
									//objCtdDao.deletePreviousCtdAggregation(_conn,
									//		dataDto.getLocationId(),
									//		Constants.STORE_LEVEL_ID,
									//		objRepairStartDate.getStartDate(),
									//		objRepairEndDate.getStartDate(),
									//		"SALES_AGGR_DAILY", Constants.CTD_YEAR);
	
									logger.debug("Delete the existing Weekly data");
									objSalesDao.deletePreviousSalesAggregation(
											_conn, _yearCalendarId,
											dataDto.getLocationId(),
											Constants.STORE_LEVEL_ID, "SALES_AGGR");
	
								}
							}else{
								logger.warn("RepairDayCalendarList is null or empty");
							}
							// Repair process ends

							// get the last year aggregation id
							HashMap<String, Long> lastSummaryList = objSalesDao
									.getLastYearSalesAggrList(_conn,
											objWeekCalDto.getlstCalendarId(),
											dataDto.getLocationId(),
											Constants.STORE_LEVEL_ID,
											"SALES_AGGR", "SALES_AGGR_ID");

							// call the Year aggregation process
							getYearAggregation(dataDto, dailyCalendarList,
									businessLogic, objCalendarDao, objSalesDao,
									objCtdDao, lastSummaryList);

						}

					}
					else {
						processYear(objRetailCal, objCalendarDao,
								dailyCalendarList, objSalesDao, dataDto,
								objCtdDao, businessLogic);
					}


				}
					
				else {

					processYear(objRetailCal, objCalendarDao,
							dailyCalendarList, objSalesDao, dataDto,
							objCtdDao, businessLogic);
				}
					}
			 
		} catch (Exception exe) {
			exe.printStackTrace();
			logger.error(" Summary Year Process Error.... " + exe);
			throw new GeneralException("Summary Year Process Error.... " + exe);
		}

	}
	
	
	private void processYear(RetailCalendarDTO objRetailCal,
			RetailCalendarDAO objCalendarDao,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesAggregationDao objSalesDao, SummaryDataDTO dataDto,
			SalesAggregationCtdDao objCtdDao,
			SalesaggregationbusinessV2 businessLogic) throws GeneralException {

		try {
			// get Week calendar Id for a given week
			objRetailCal = objCalendarDao.week_period_quarter_yearCalendarList(
					_conn, _processDate, Constants.CALENDAR_YEAR);

			computeProcessDate(objRetailCal.getStartDate(),
					objRetailCal.getCalendarId());

			// get the day calendar list
			dailyCalendarList = objCalendarDao.dayCalendarList(_conn,
					_startDate, _endDate, Constants.CALENDAR_DAY);
			logger.debug("Year Calendar Id......." + _yearCalendarId);

			if (_reCalculation != "" && _reCalculation.equalsIgnoreCase("YES")) {

				logger.debug("Recalculation : Delete the Previous Aggregation ");

				// Delete Previous Aggregation : For Period
				objSalesDao.deletePreviousSalesAggregation(_conn,
						_yearCalendarId, dataDto.getLocationId(),
						Constants.STORE_LEVEL_ID, "SALES_AGGR");

				// Delete Previous Aggregation : For Ctd
				//objCtdDao.deletePreviousCtdAggregation(_conn,
				//		dataDto.getLocationId(), Constants.STORE_LEVEL_ID,
				//		objRetailCal.getStartDate(), objRetailCal.getEndDate(),
				//		"SALES_AGGR_DAILY", Constants.CTD_YEAR);
			}

			// get Last year summaryDaily List
			HashMap<String, Long> lastSummaryList = objSalesDao
					.getLastYearSalesAggrList(_conn,
							objRetailCal.getlstCalendarId(),
							dataDto.getLocationId(), Constants.STORE_LEVEL_ID,
							"SALES_AGGR", "SALES_AGGR_ID");

			// call the Year aggregation process
			getYearAggregation(dataDto, dailyCalendarList, businessLogic,
					objCalendarDao, objSalesDao, objCtdDao, lastSummaryList);
		} catch (ParseException e) {
			throw new GeneralException(
					" Error While Processing Year Aggregation Proces....", e);
		} catch (GeneralException e) {
			throw new GeneralException(
					" Error While Processing Year Aggregation Proces....", e);
		}

	}

	/*
	 * ****************************************************************
	 * Method used to aggregate the Year level records  
	 * First Test the previous Year level aggregation for a given Year and store
	 * Then get the Daily aggregation , sum up Quarter and week
	 * Arguemnt 1  : SummaryDataDTO dataDto
	 * Argument 2  : List<RetailCalendarDTO> weeklyCalendarList
	 * Argument 3  : Salesaggregationbusiness businessLogic
	 * Argument 4  : RetailCalendarDAO objCalendarDao
	 * Argument 5  : SalesAggregationDao objSalesDao
	 * Argument 2  : Store Number / District Numebr
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void getYearAggregation(SummaryDataDTO dataDto,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesaggregationbusinessV2 businessLogic,
			RetailCalendarDAO objCalendarDao, SalesAggregationDao objSalesDao,
			SalesAggregationCtdDao objCtdDao,
			HashMap<String, Long> lastSummaryList) throws GeneralException {
		
		HashMap<String, SummaryDataDTO> quarterMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> productMap = new HashMap<String, SummaryDataDTO>();
		try {

		// get last aggregation calendar id
		int LastAggrCalendarId = objSalesDao.getLastAggerCalendarId(_conn,
					_yearCalendarId, dataDto.getLocationId(),
					Constants.STORE_LEVEL_ID, "SALES_AGGR");
		logger.debug("Last Calendar Id:" + LastAggrCalendarId);
		
		if (LastAggrCalendarId != 0) {

		//  get previous aggregated records from sales_aggr table for a given quarter
		quarterMap = objSalesDao.getSalesAggregation(_conn,
						_yearCalendarId, dataDto.getLocationId(),
						"SALES_AGGR" );

		}
		for (int cL = 0; cL < dailyCalendarList.size(); cL++) {
			RetailCalendarDTO calendarDto = dailyCalendarList.get(cL);

			// For First Day Process
			if (cL == 0 && quarterMap.size() != 0)
				productMap = quarterMap;

			if (LastAggrCalendarId < calendarDto.getCalendarId()) {

				logger.info("Processing Calendar Id:"	
										+ calendarDto.getCalendarId());

				// get daily aggregation for processing date
				HashMap<String, SummaryDataDTO> dailyMap = 
					objSalesDao.getSalesAggregation(_conn, 
					calendarDto.getCalendarId(), dataDto.getLocationId(), 
														"SALES_AGGR_DAILY");

				if (dailyMap.size() != 0) {
					
					String store_status = objSalesDao.getStoreStatus(_conn,
							calendarDto.getStartDate(), dataDto,
							Constants.CALENDAR_WEEK);

					if (productMap.size() != 0) {

						// call the business logic method to sum up the daily and
						// weekly records
						productMap = businessLogic.sumupAggregation(
							dailyMap, productMap, calendarDto.getCalendarId());

						// updated weekly records .
						objSalesDao.updateSalesAggr(_conn, productMap,
								calendarDto.getCalendarId(), _yearCalendarId,"SALES_AGGR" ,"ACTUAL" ,store_status);

						// Insert New Weekly Records
							objSalesDao.insertSalesAggr(_conn, dailyMap,
									calendarDto.getCalendarId(),
									_yearCalendarId, "SALES_AGGR",
									lastSummaryList , store_status);
						
						// Insert Ctd Process
						//objCtdDao.insertSalesCtd(_conn, productMap,Constants.CTD_YEAR);

						} else {

						// Insert Summary Weekly Records
							objSalesDao.insertSalesAggr(_conn, dailyMap,
									calendarDto.getCalendarId(),
									_yearCalendarId, "SALES_AGGR",
									lastSummaryList , store_status);

						// Insert Ctd Process
						//objCtdDao.insertSalesCtd(_conn, dailyMap,Constants.CTD_YEAR);
						productMap = dailyMap;
					}
				}
				
					objSalesDao.deleteTempAggragation(_conn,
							dataDto.getLocationId(),
							calendarDto.getCalendarId(),
							Constants.STORE_LEVEL_ID, "SALES_AGGR_DAILY_TEMP");

			}
			// commit the transaction
			PristineDBUtil.commitTransaction(_conn, "Commit the Year Transation");
		}
		
		}catch(Exception exe){
			logger.error(exe);
		    PristineDBUtil.rollbackTransaction(_conn, "Rollback the Year Transation");
			throw new GeneralException("Year Insertion Failed");
			
		}

	}
		
 
	
	
	private void computeProcessDate(String startDate, int periodCalendarId) throws ParseException {
	
		Calendar cal = Calendar.getInstance();
		// find the date with start time
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		Date date = (Date) formatter.parse(startDate);
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_startDate = cal.getTime();
		_endDate = _processDate;
		_yearCalendarId = periodCalendarId;
		
	}


	/**
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
	 private static void logCommand (String[] args) {
		StringBuffer sb = new StringBuffer("Command: SummaryYear ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	   }

}
