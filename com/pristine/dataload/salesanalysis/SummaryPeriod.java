/*
 * TITLE: TOPS SUMMARY PERIOD MOVEMENT
 *
 *******************************************************************************
 * MODIFICATION HISTORY
 *------------------------------------------------------------------------------
 * VERSION		DATE		NAME			REMARKS
 *------------------------------------------------------------------------------
 * Version 0.1	09/04/2012	Dinesh kumar	Initial Version 
 * Version 0.2  17/06/2012  John Britto     Comments and log added
 * Version 0.3  06/08/2012  Dinesh Kumar V  Movement By Volume added
 * vERSION 0.4  02/09/2012  Dinesh Kumar V	Repair and margin calculation added
 * Version 0.5  23/01/2013  John Britto     Log re-factored
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

public class SummaryPeriod {

	static Logger logger = Logger.getLogger("SummaryPeriod");

	// global variables
	private Date _startDate = null; // hold the start date
	private Date _endDate = null; // hold the end date
	private Date _processDate = null; // hold the input process date
	private String _storeNumber = ""; // hold the input store number
	private String _districtNumber = ""; // hold the input district number
	private String _reCalculation = ""; // hold the input recalculation flag
	private Connection _conn = null; // hold the database connection
	private int _periodCalendarId = 0; // hold the Period calendar id
	private String _repairFlag = null; // hold the repair flag value

	/*
	 * ****************************************************************
	 * Class constructor Argument 1 : ProcessDate Argument 2 : Store Number
	 * Argument 3 : District Number
	 * 
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryPeriod(Date processDate, String storeNo, String districtNo,
			String reCalculation, String repairFlag) {

		PropertyManager.initialize("analysis.properties");

		// Validate the Input fields
		if (storeNo == "" && districtNo == "") {
			logger.error(" Store Numebr / District Id is missing in Input");
			System.exit(1);
		}

		// If process date is null then set currentDate - 1 as default
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

		if (repairFlag.equalsIgnoreCase("FULL")){
			_repairFlag = "FULL";
			logger.info("Process mode:FULL, Process all the non-processed days");
		}
		else{
			_repairFlag = "NORMAL";
			logger.info("Process mode:NORMAL, Process data only for specific days");
		}
		
		
		

		// get the DB connection
		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error(" Connection Failed.... " + gex);
			System.exit(1);
		}

		// Find the Period Start date for given input
		_storeNumber = storeNo;
		_districtNumber = districtNo;
		_processDate = processDate;

		// logger.debug(" Constructor Ends ");
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument Argument
	 * 1: Store Number Argument 2: District Number Argument 3: Process Date
	 * Argument 4: Recalculation Flag If the District or Store Number is
	 * mandatory. If both are are specified then consider district alone
	 * ****************************************************************
	 */

	public static void main(String[] args) throws GeneralException {

		// Initialize the summary-Period log4j
		PropertyConfigurator.configure("log4j-summary-period.properties");

		logger.info("*** Sales Aggregation Period Process begins ***");
		logCommand(args);
		Date processDate = null; // hold the input start date
		String storeNo = ""; // hold the input store number
		String districtNo = ""; // hold the input district number
		String reCalculation = ""; // hold the recalculation (Yes/No)
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy"); // date format
		SummaryPeriod summaryPeriod = null;
		// variable added for repair process
		String repairFlag = null; // hold the input repair process

		try {
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the start date
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
			summaryPeriod = new SummaryPeriod(processDate, storeNo, districtNo,
					reCalculation, repairFlag);

			// call the process Period data
			summaryPeriod.processPeriodData();

		} catch (Exception exe) {
			logger.error(" Period Aggregation Error.... ", exe);
			throw new GeneralException(" Period Aggregation Error.... ", exe);
		} finally {
			logger.info("*** Sales Aggregation Period Process ends ***");
			PristineDBUtil.close(summaryPeriod._conn);
		}

	}

	/*
	 * ****************************************************************
	 * get the period and Day calendar id for a given process date get Store-id
	 * for a given Store number / District number Arguemnt 1 : process Date
	 * Argument 2 : Store Number / District Numebr
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void processPeriodData() throws GeneralException {

		// logger.debug("processPeriodData Starts ");

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

			// hold the Period calendar id
			RetailCalendarDTO objRetailCal = null;

			// hold the day calendar id based on Period calendar id
			List<RetailCalendarDTO> dailyCalendarList = null;

			// input is district then get the all store number
			List<SummaryDataDTO> storeList = objCompStoreDao.getStoreNumebrs(
					_conn, _districtNumber, _storeNumber);

			for (int sN = 0; sN < storeList.size(); sN++) {

				SummaryDataDTO storeDataDto = storeList.get(sN);

				logger.info("Processing at Store level. Store No:"
											+ storeDataDto.getstoreNumber());

				// if recalculation  mode is Yes, delete the previous aggr data
				logger.debug("Recalc Required:" + _reCalculation);
				
								
				if (_repairFlag.equalsIgnoreCase("FULL")) {

					// If repair mode FULL .. do the repair aggregation
					// Check the repair process need or not for given store
					RetailCalendarDTO repairCalendarDates = objCalendarDao
							.getRepairCalendarDates(_conn,
									storeDataDto.getLocationId(),
									Constants.STORE_LEVEL_ID,
									"SALES_AGGR_DAILY_TEMP");

					if (repairCalendarDates != null) {

						logger.debug("Process Date:" + _processDate);

						// get the Period calendar id list.....
						List<RetailCalendarDTO> repairPeriodCalendar = objCalendarDao
								.getRepairCalendarList(_conn,
										repairCalendarDates.getStartDate(),
										_processDate, Constants.CALENDAR_PERIOD);

						int periodLoopSize = repairPeriodCalendar.size();

						logger.info("Period calendar List Size:" 
															+ periodLoopSize);

						for (int rWC = 0; rWC < repairPeriodCalendar.size(); rWC++) {
							RetailCalendarDTO objPeriodCalDto = repairPeriodCalendar
									.get(rWC);

							_periodCalendarId = objPeriodCalDto.getCalendarId();
							
							// check Period calendar id had more than one calendar  id
							dailyCalendarList = new ArrayList<RetailCalendarDTO>();

							dailyCalendarList = objCalendarDao
									.getRepairDayCalendarList(_conn,
											periodLoopSize, rWC,
											repairCalendarDates.getStartDate(),
											_processDate, _periodCalendarId);
							
							// If condition added to supress ArrayIndexOutOfBoundsException
							if(dailyCalendarList != null && dailyCalendarList.size() > 0){
								RetailCalendarDTO objRepairStartDate = dailyCalendarList
										.get(0);
	
								RetailCalendarDTO objRepairEndDate = dailyCalendarList
										.get(dailyCalendarList.size() - 1);
	
								if (objCalendarDao.checkWeekStartDate(_conn,
										objRepairStartDate.getStartDate(),
										Constants.CALENDAR_PERIOD)) {
	
									int lastUpdatedCalendarId = objRepairStartDate
											.getCalendarId() - 1;
	
									// get the previous aggregation details for repair process
									HashMap<String, SummaryDataDTO> dailyTempAggregation = objSalesDao
											.getTempSalesAggregation(_conn,
													storeDataDto.getLocationId(),
													Constants.STORE_LEVEL_ID,
													"SALES_AGGR",
													"SALES_AGGR_DAILY_TEMP",
													dailyCalendarList,
													_periodCalendarId);
	
									// update the previous aggregation details into SALES_AGGR table.
									objSalesDao.updateSalesAggr(_conn,
											dailyTempAggregation,
											lastUpdatedCalendarId,
											_periodCalendarId, "SALES_AGGR",
											"REPAIR", "");
	
									// delete the previous aggregation In Sales  Table
									//logger.debug("Delete the existing Temp CTD data");
									//objCtdDao.deletePreviousCtdAggregation(_conn,
									//		storeDataDto.getLocationId(),
									//		Constants.STORE_LEVEL_ID,
									//		objRepairStartDate.getStartDate(),
									//		objRepairEndDate.getStartDate(),
									//		"SALES_AGGR_DAILY_TEMP",
									//		Constants.CTD_PERIOD);
									
									//logger.debug("Delete the existing New CTD data");
									//objCtdDao.deletePreviousCtdAggregation(_conn,
									//		storeDataDto.getLocationId(),
									//		Constants.STORE_LEVEL_ID,
									//		objRepairStartDate.getStartDate(),
									//		objRepairEndDate.getStartDate(),
									//		"SALES_AGGR_DAILY",
									//		Constants.CTD_PERIOD);
									
								} else {
									// delete the previous aggregation In Sales Table
									//logger.debug("Delete the existing CTD data");
									//objCtdDao.deletePreviousCtdAggregation(_conn,
									//		storeDataDto.getLocationId(),
									//		Constants.STORE_LEVEL_ID,
									//		objRepairStartDate.getStartDate(),
									//		objRepairEndDate.getStartDate(),
									//		"SALES_AGGR_DAILY",
									//		Constants.CTD_PERIOD);
	
									logger.debug("Delete the existing Period data");
									objSalesDao.deletePreviousSalesAggregation(
											_conn, _periodCalendarId,
											storeDataDto.getLocationId(),
											Constants.STORE_LEVEL_ID, "SALES_AGGR");
	
								}
							}else{
								logger.warn("RepairDayCalendarList is null or empty");
							}
							
							// get Last year summaryDaily List
							logger.debug("Get Last Year Summary Data ID..");
							HashMap<String, Long> lastSummaryList = objSalesDao
									.getLastYearSalesAggrList(_conn,
											objPeriodCalDto.getlstCalendarId(),
											storeDataDto.getLocationId(),
											Constants.STORE_LEVEL_ID,
											"SALES_AGGR", " SALES_AGGR_ID");

							// call the weekly aggregation process
							getPeriodAggregation(storeDataDto, dailyCalendarList,
									businessLogic, objSalesDao, objCtdDao,
									lastSummaryList);

						}

					}

					else {
						processPeriod(objRetailCal, objCalendarDao,
								dailyCalendarList, objCtdDao, storeDataDto,
								objSalesDao, businessLogic);
					}

				}

				else {
					processPeriod(objRetailCal, objCalendarDao,
							dailyCalendarList, objCtdDao, storeDataDto,
							objSalesDao, businessLogic);
				}
			}
			 

		} catch (Exception exe) {
			logger.error(" Period Aggregation Error.....", exe);
			throw new GeneralException(" Period Aggregation Error.....", exe);

		}

	}

	private void processPeriod(RetailCalendarDTO objRetailCal,
			RetailCalendarDAO objCalendarDao,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesAggregationCtdDao objCtdDao, SummaryDataDTO storeDataDto,
			SalesAggregationDao objSalesDao,
			SalesaggregationbusinessV2 businessLogic) throws GeneralException {

		try {
			// get Period calendar Id for a given week
			objRetailCal = objCalendarDao.week_period_quarter_yearCalendarList(
					_conn, _processDate, Constants.CALENDAR_PERIOD);

			computeProcessDate(objRetailCal.getStartDate(),
					objRetailCal.getCalendarId());

			// get the day calendar list
			dailyCalendarList = objCalendarDao.dayCalendarList(_conn,
					_startDate, _endDate, Constants.CALENDAR_DAY);
			logger.debug(" Period Calendar Id......." + _periodCalendarId);

			if (_reCalculation != "" && _reCalculation.equalsIgnoreCase("YES")) {

				// Delete Previous Aggregation : For Ctd
				//logger.debug("Delete Previous Aggr CTD data");
				//objCtdDao.deletePreviousCtdAggregation(_conn,
				//		storeDataDto.getLocationId(), Constants.STORE_LEVEL_ID,
				//		objRetailCal.getStartDate(), objRetailCal.getEndDate(),
				//		"SALES_AGGR_DAILY", Constants.CTD_PERIOD);

				// Delete Previous Aggregation : For Period
				logger.debug("Delete Previous Aggr Period data");
				objSalesDao.deletePreviousSalesAggregation(_conn,
						_periodCalendarId, storeDataDto.getLocationId(),
						Constants.STORE_LEVEL_ID, "SALES_AGGR");

			}

			logger.debug("Get last Year summery ID");
			HashMap<String, Long> lastSummaryList = objSalesDao
					.getLastYearSalesAggrList(_conn,
							objRetailCal.getlstCalendarId(),
							storeDataDto.getLocationId(),
							Constants.STORE_LEVEL_ID, "SALES_AGGR",
							"SALES_AGGR_ID");

			// call the Period aggregation process
			getPeriodAggregation(storeDataDto, dailyCalendarList,
					businessLogic, objSalesDao, objCtdDao, lastSummaryList);
		} catch (ParseException e) {
			 throw new GeneralException(" Error While Processing Period Aggregation...." , e);
		} catch (GeneralException e) {
			throw new GeneralException(" Error While Processing Period Aggregation...." , e);
		}

	}

	/*
	 * ****************************************************************
	 * Method used to aggregate the period level records First Test the previous
	 * period level aggregation for a given period and store Then get the weekly
	 * aggregation , sum up period and week Arguemnt 1 : SummaryDataDTO dataDto
	 * Argument 2 : List<RetailCalendarDTO> weeklyCalendarList Argument 3 :
	 * Salesaggregationbusiness businessLogic Argument 4 : RetailCalendarDAO
	 * objCalendarDao Argument 5 : SalesAggregationDao objSalesDao Argument 2 :
	 * Store Number / District Numebr
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void getPeriodAggregation(SummaryDataDTO storeDataDto,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesaggregationbusinessV2 businessLogic,
			SalesAggregationDao objSalesDao, SalesAggregationCtdDao objCtdDao,
			HashMap<String, Long> lastSummaryList) throws GeneralException {

		logger.debug("Period Aggregation");
		HashMap<String, SummaryDataDTO> periodMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> productMap = new HashMap<String, SummaryDataDTO>();
		try {

			// get last aggregation calendar id
			int LastAggrCalendarId = objSalesDao.getLastAggerCalendarId(_conn,
					_periodCalendarId, storeDataDto.getLocationId(),
					Constants.STORE_LEVEL_ID, "SALES_AGGR");
			logger.debug("Last Aggregated Calendar Id " + LastAggrCalendarId);

			if (LastAggrCalendarId != 0) {

				// get previous aggregated records from sales_aggr table for a
				// given period
				logger.debug("Get Previous Aggregation Data");
				periodMap = objSalesDao.getSalesAggregation(_conn,
						_periodCalendarId, storeDataDto.getLocationId(),
						"SALES_AGGR");

				logger.debug("Period Map count " + periodMap.size());

			}
			for (int cL = 0; cL < dailyCalendarList.size(); cL++) {

				RetailCalendarDTO calendarDto = dailyCalendarList.get(cL);

				// For First Day Process
				if (cL == 0 && periodMap.size() != 0)
					productMap = periodMap;

				if (LastAggrCalendarId < calendarDto.getCalendarId()) {

					logger.info("Process begins at Calendar level. Calendar Id:"
												+ calendarDto.getCalendarId());

					// get daily aggregation for processing date
					logger.debug("Get the daily aggreation Data");
					HashMap<String, SummaryDataDTO> dailyMap = objSalesDao
							.getSalesAggregation(_conn,
									calendarDto.getCalendarId(),
									storeDataDto.getLocationId(),
									"SALES_AGGR_DAILY");

							if (dailyMap.size() != 0) {
								
						// get the store status...
						String store_Status = objSalesDao.getStoreStatus(_conn,
								calendarDto.getStartDate(), storeDataDto,
								Constants.CALENDAR_WEEK);
		
								if (productMap.size() != 0) {
		
									// call the business logic method to sum up the daily
									// and
									// weekly records
									productMap = businessLogic.sumupAggregation(
											dailyMap, productMap,
											calendarDto.getCalendarId());
		
									// updated weekly records .
									logger.debug("Update existing Period Aggregation Data");
									objSalesDao.updateSalesAggr(_conn, productMap,
											calendarDto.getCalendarId(),
											_periodCalendarId, "SALES_AGGR", "ACTUAL" , store_Status);
		
									// Insert New Weekly Records
									logger.debug("Insert New Period Aggregation Data");
									objSalesDao.insertSalesAggr(_conn, dailyMap,
											calendarDto.getCalendarId(),
											_periodCalendarId, "SALES_AGGR",
											lastSummaryList , store_Status);
		
									// Insert Ctd Process
									//logger.debug("Insert WTD Data into CTD");
									//objCtdDao.insertSalesCtd(_conn, productMap, Constants.CTD_PERIOD);
		
								} else {
		
									// Insert Summary Weekly Records
									logger.debug("Insert Period Aggregation Data");
									objSalesDao.insertSalesAggr(_conn, dailyMap,
											calendarDto.getCalendarId(),
											_periodCalendarId, "SALES_AGGR",
											lastSummaryList , store_Status);
		
									// Insert Ctd Process
									//logger.debug("Insert WTD Data into CTD");
									//objCtdDao.insertSalesCtd(_conn, dailyMap, Constants.CTD_PERIOD);
		
									productMap = dailyMap;
								}
								
						}
				}
				// commit the transaction
				PristineDBUtil.commitTransaction(_conn,
						"Commit the Period Transation");
			}
		} catch (Exception exe) {
			logger.error(" Period Aggregation Error....." + exe);
			PristineDBUtil.rollbackTransaction(_conn,
					"Rollback the Period Transation");
			throw new GeneralException("Period Insertion Failed");

		}
	}

	private void computeProcessDate(String startDate, int periodCalendarId)
			throws ParseException {

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
		_periodCalendarId = periodCalendarId;

	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SummaryPeriod ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}
		logger.info(sb.toString());
	}

}
