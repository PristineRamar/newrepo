/*
 * Title: TOPS Summary Year RollUp
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	12/04/2012	Dinesh Kumar	Initial Version 
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
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationCtdDao;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationRollupDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryYearRollup {

	static Logger logger = Logger.getLogger("SummaryYearRollup");

	// global variables
	private Date _processDate = null;
	private Date _startDate = null; // hold the year start date
	private Date _endDate = null; // hold the process end date
	private Connection _conn = null; // hold the database connection
	String _districtId = ""; // hold the input district id
	String _regionId = ""; // hold the input region id
	String _divisionId = ""; // hold the input division id
	String _chainId = ""; // hold the input chain id
	int _yearCalendarId = 0; // hold the year calendar id for given year
	String _reCalculation = ""; // hold the input recalculation flag
	int _locationLevelId = 0; // hold the location level id for given input
	String _locationId = "";
	private String _repairFlag = null; // hold the repair flag value

	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryYearRollup(Date processDate, String districtId,
			String regionId, String divisionId, String chainId,
			String reCalculation, String repairFlag) {

		if (districtId == null && regionId == null && divisionId == null
				&& chainId == null) {
			logger.error(" District/Region/Division/Chain Nedded to process the batch ");
			System.exit(1);
		}

		// If process date is null then set currentDate - 1 as default
		if (processDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			processDate = cal.getTime();
		}

		// Assign the district Id
		if (districtId != null) {

			_locationLevelId = Constants.DISTRICT_LEVEL_ID;
			_districtId = districtId;
			_locationId = districtId;
			logger.info("Summary Year Roll up For District:" + _locationId);
		}

		// Assign the RegionId
		if (regionId != null) {

			_locationLevelId = Constants.REGION_LEVEL_ID;
			_regionId = regionId;
			_locationId = regionId;
			logger.info("Summary Year Roll up For Region:"+ _locationId);
		}

		// Assign the DivisionId
		if (divisionId != null) {

			_locationLevelId = Constants.DIVISION_LEVEL_ID;
			_divisionId = divisionId;
			_locationId = divisionId;
			logger.info("Summary Year Roll up For Division:" + _locationId);
		}

		// Assign the Chain Id
		if (chainId != null) {

			_locationLevelId = Constants.CHAIN_LEVEL_ID;
			_chainId = chainId;
			_locationId = chainId;
			logger.info("Summary Year Roll up For Chain:" + _locationId);
		}

		if (reCalculation != "") {
			_reCalculation = reCalculation;
		}

		// process added for repair flag
		 if (repairFlag.equalsIgnoreCase("FULL")){
			_repairFlag = "FULL";
			logger.info("Process mode:FULL, Process all the non-processed records");
		 }
		 else{
			_repairFlag = "NORMAL";
			logger.info("Process mode:NORMAL, Process data only for specific Calendar Ids");
		 }

		_processDate = processDate;
		_repairFlag = repairFlag;
		PropertyManager.initialize("analysis.properties");
		// get the DB connection
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException gex) {
			logger.error(" Connection Failed...." + gex);
			System.exit(1);
		}

	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Process Date 
	 * Argument 2: Process Id If the StartDate and EndDate is
	 * not specified then process for past period
	 * ****************************************************************
	 */

	public static void main(String[] args) throws GeneralException {
		// Initialize the summary-year log4j
		PropertyConfigurator.configure("log4j-summary-year-rollup.properties");

		logger.info("*** Summary year Rollup Process Begins ***");
		logCommand(args);
		Date processDate = null; // get the input start date
		String districtId = null; // get the input district Id
		String regionId = null; // get the input region Id
		String divisionId = null; // get the input division Id
		String chainId = null; // get the input chain id
		String reCalculation = ""; // hold the recalculation (Yes/No)
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy"); // date format
		String repairFlag = null; // hold the input repair flag
		SummaryYearRollup summaryYearRollup = null;
		try {
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the startdate
				if (arg.startsWith("PROCESSDATE")) {
					String inputDate = arg.substring("PROCESSDATE=".length());
					try {
						processDate = dateFormat.parse(inputDate);
					} catch (ParseException pxe) {
						logger.error("The given Process date is invalid");
						System.exit(1);
					}
				}

				// get the districtId
				if (arg.startsWith("DISTRICT")) {
					districtId = arg.substring("DISTRICT=".length());
				}

				// get the Region Id
				if (arg.startsWith("REGION")) {
					regionId = arg.substring("REGION=".length());
				}

				// get the Division Id
				if (arg.startsWith("DIVISION")) {
					divisionId = arg.substring("DIVISION=".length());
				}

				// get the Chain Id
				if (arg.startsWith("CHAIN")) {
					chainId = arg.substring("CHAIN=".length());
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
			summaryYearRollup = new SummaryYearRollup(processDate, districtId,
					regionId, divisionId, chainId, reCalculation, repairFlag);

			// Process Roll up Starts
			summaryYearRollup.processYearRollupData();
			logger.info("Summary Rollup Process Ends");

		} catch (Exception exe) {
			logger.error(" Error in summary year rollup process..." + exe);
			throw new GeneralException(
					"Error in summary year rollup process..." + exe);
		} finally {
			logger.info("*** Summary year Rollup Process Ends ***");
			PristineDBUtil.close(summaryYearRollup._conn);
		}
	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. Call the aggregation
	 * for calendar list Argument 1 : Start Date Argument 2 : End Date Argument
	 * 3 : Calendar RowType
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void processYearRollupData() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// object for Sales Aggregation Dao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();

		// Object for SalesAggregationCtdDao
		SalesAggregationCtdDao objCtdDao = new SalesAggregationCtdDao();

		SalesAggregationRollupDao objWRDao = new SalesAggregationRollupDao();

		// hold the last year Sales year Roll up Id id
		// key location_level_id and location_id
		// value Sales year Roll up Id
		HashMap<String, Long> lastSummaryList = new HashMap<String, Long>();

		try {

			RetailCalendarDTO objCalendarDto = null;

			List<RetailCalendarDTO> dailyCalendarList = null;

			if (_repairFlag.equalsIgnoreCase("FULL")) {

				// If repair mode FULL .. do the repair aggregation
				// Check the repair process need or not for given store
				RetailCalendarDTO repairCalendarDates = objCalendarDao
						.getRepairCalendarDates(_conn,
								Integer.parseInt(_locationId),
								_locationLevelId,
								"SALES_AGGR_DAILY_ROLLUP_TEMP");

				if (repairCalendarDates != null) {

					logger.info("Process Date:" + _processDate);

					// get the week calendar id list.....
					List<RetailCalendarDTO> repairWeekCalendarList = objCalendarDao
							.getRepairCalendarList(_conn,
									repairCalendarDates.getStartDate(),
									_processDate, Constants.CALENDAR_YEAR);

					int weekLoopSize = repairWeekCalendarList.size();

					logger.info("Period calendar List Size"	+ weekLoopSize);

					for (int rWC = 0; rWC < repairWeekCalendarList.size(); rWC++) {
						RetailCalendarDTO objWeekCalDto = repairWeekCalendarList
								.get(rWC);

						_yearCalendarId = objWeekCalDto.getCalendarId();
						logger.debug("Process End Date:" + _processDate);
						// check week calendar id had more than one calendar
						// id

						dailyCalendarList = new ArrayList<RetailCalendarDTO>();

						dailyCalendarList = objCalendarDao
								.getRepairDayCalendarList(_conn, weekLoopSize,
										rWC,
										repairCalendarDates.getStartDate(),
										_processDate, _yearCalendarId);

						RetailCalendarDTO objRepairStartDate = dailyCalendarList
								.get(0);

						RetailCalendarDTO objRepairEndDate = dailyCalendarList
								.get(dailyCalendarList.size() - 1);

						if (objCalendarDao.checkWeekStartDate(_conn,
								objRepairStartDate.getStartDate(),
								Constants.CALENDAR_YEAR)) {

							int lastUpdatedCalendarId = objRepairStartDate
									.getCalendarId() - 1;

							logger.debug("Repair start Last Calengar Id:"
									+ lastUpdatedCalendarId);

							// get the previous aggregation details for repair
							// process
							HashMap<String, SummaryDataDTO> dailyTempAggregation = objSalesDao
									.getTempSalesAggregation(_conn,
											Integer.parseInt(_locationId),
											_locationLevelId,
											"SALES_AGGR_ROLLUP",
											"SALES_AGGR_DAILY_ROLLUP_TEMP",
											dailyCalendarList, _yearCalendarId);

							// update the previous aggregation details into
							// SALES_AGGR_WEEKLY table.
							objWRDao.updateSalesRollup(_conn,
									dailyTempAggregation,
									lastUpdatedCalendarId, _yearCalendarId,
									"SALES_AGGR_ROLLUP", "REPAIR");

							// delete the previous aggregation in Ctd table
							//objCtdDao.deletePreviousCtdAggregation(_conn,
							//		Integer.valueOf(_locationId),
							//		_locationLevelId,
							//		objRepairStartDate.getStartDate(),
							//		objRepairEndDate.getStartDate(),
							//		"SALES_AGGR_DAILY_ROLLUP_TEMP",
							//		Constants.CTD_YEAR);

							// delete the previous aggregation in Ctd table
							//objCtdDao.deletePreviousCtdAggregation(_conn,
							//		Integer.valueOf(_locationId),
							//		_locationLevelId,
							//		objRepairStartDate.getStartDate(),
							//		objRepairEndDate.getStartDate(),
							//		"SALES_AGGR_DAILY_ROLLUP",
							//		Constants.CTD_YEAR);
						} else {

							// delete the previous aggregation In Sales Table

							// delete the previous aggregation in Sales table
							objSalesDao.deletePreviousSalesAggregation(_conn,
									_yearCalendarId,
									Integer.valueOf(_locationId),
									_locationLevelId, "SALES_AGGR_ROLLUP");

							// delete the previous aggregation in Ctd table
							//objCtdDao.deletePreviousCtdAggregation(_conn,
							//		Integer.valueOf(_locationId),
							//		_locationLevelId,
							//		objRepairStartDate.getStartDate(),
							//		objRepairEndDate.getStartDate(),
							//		"SALES_AGGR_DAILY_ROLLUP_TEMP",
							//		Constants.CTD_YEAR);

						}

						// get Last year summaryDaily List
						lastSummaryList = objSalesDao.getLastYearSalesAggrList(
								_conn, objWeekCalDto.getlstCalendarId(),
								Integer.valueOf(_locationId), _locationLevelId,
								"SALES_AGGR_ROLLUP", "SALES_AGGR_ROLLUP_ID");
						// call the Period aggregation process
						getYearRollupAggregation(dailyCalendarList,
								objCalendarDao, objSalesDao, objCtdDao,
								lastSummaryList);
					}
				}
				else {
					processYearRollup(objCalendarDto, objCalendarDao,
							dailyCalendarList, objSalesDao, objCtdDao,
							lastSummaryList);
				}


			}

			else  {
				
				processYearRollup(objCalendarDto, objCalendarDao,
						dailyCalendarList, objSalesDao, objCtdDao,
						lastSummaryList);

			}

		} catch (Exception exe) {
			logger.error(" Period Rollup Process Error.... " + exe);
			throw new GeneralException(" Period Rollup Process Error.... "
					+ exe);
		}

	}

	private void processYearRollup(RetailCalendarDTO objCalendarDto,
			RetailCalendarDAO objCalendarDao,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesAggregationDao objSalesDao, SalesAggregationCtdDao objCtdDao,
			HashMap<String, Long> lastSummaryList) throws GeneralException {

		try {
			// get Week calendar Id
			objCalendarDto = objCalendarDao
					.week_period_quarter_yearCalendarList(_conn, _processDate,
							Constants.CALENDAR_YEAR);

			computeProcessDate(objCalendarDto.getStartDate(),
					objCalendarDto.getCalendarId());

			// get the day calendar Id
			dailyCalendarList = objCalendarDao.dayCalendarList(_conn,
					_startDate, _endDate, Constants.CALENDAR_DAY);
			logger.debug("Year Calendar Id " + _yearCalendarId);

			if (_reCalculation != "" && _reCalculation.equalsIgnoreCase("YES")) {

				// delete the previous aggregation in Sales table
				objSalesDao.deletePreviousSalesAggregation(_conn,
						_yearCalendarId, Integer.valueOf(_locationId),
						_locationLevelId, "SALES_AGGR_ROLLUP");

				// delete the previous aggregation in Ctd table
				//objCtdDao.deletePreviousCtdAggregation(_conn,
				//		Integer.valueOf(_locationId), _locationLevelId,
				//		objCalendarDto.getStartDate(),
				//		objCalendarDto.getEndDate(), "SALES_AGGR_DAILY_ROLLUP",
				//		Constants.CTD_YEAR);
			}

			// get Last year summaryDaily List
			lastSummaryList = objSalesDao.getLastYearSalesAggrList(_conn,
					objCalendarDto.getlstCalendarId(),
					Integer.valueOf(_locationId), _locationLevelId,
					"SALES_AGGR_ROLLUP", "SALES_AGGR_ROLLUP_ID");
			// call the Period aggregation process
			getYearRollupAggregation(dailyCalendarList, objCalendarDao,
					objSalesDao, objCtdDao, lastSummaryList);
		} catch (NumberFormatException e) {
			throw new GeneralException(
					" Error While Processing Year Aggrgation...", e);
		} catch (ParseException e) {
			throw new GeneralException(
					" Error While Processing Year Aggrgation...", e);
		} catch (GeneralException e) {
			throw new GeneralException(
					" Error While Processing Year Aggrgation...", e);
		}

	}

	/*
	 * ****************************************************************
	 * Aggregate the district , region , division , chain reocrds Sales Quarter
	 * Process Argument 1 : calendarId Argument 2 : SummaryRollupDao object
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void getYearRollupAggregation(
			List<RetailCalendarDTO> dailyCalendarList,
			RetailCalendarDAO objCalendarDao, SalesAggregationDao objSalesDao,
			SalesAggregationCtdDao objCtdDao,
			HashMap<String, Long> lastSummaryList) throws GeneralException {

		// hold the given period aggregation records
		HashMap<String, SummaryDataDTO> yearMap = new HashMap<String, SummaryDataDTO>();

		// hold the sum of period records
		HashMap<String, SummaryDataDTO> productMap = new HashMap<String, SummaryDataDTO>();

		// object for Sales aggregation business
		SalesaggregationbusinessV2 objSalesBusiness = new SalesaggregationbusinessV2();

		// Object for SalesAggregationRollupDao
		SalesAggregationRollupDao objSalesRollupDao = new SalesAggregationRollupDao();

		try {
			// Check Availability in SALES_AGGR_periodLY_ROLLUP
			int LastAggrCalendarId = objSalesDao.getLastAggerCalendarId(_conn,
					_yearCalendarId, Integer.valueOf(_locationId),
					_locationLevelId, "SALES_AGGR_ROLLUP");
			logger.debug("Last Calendar Id Is " + LastAggrCalendarId);

			if (LastAggrCalendarId != 0) {

				// Get the already aggregation records for given period
				yearMap = objSalesRollupDao.rollupAggregation(_conn,
						_yearCalendarId, _locationId, _locationLevelId,
						"SALES_AGGR_ROLLUP");


			}

			for (int cL = 0; cL < dailyCalendarList.size(); cL++) {
				RetailCalendarDTO calendarDto = dailyCalendarList.get(cL);

				// For First Day Process
				if (cL == 0 && yearMap.size() != 0)
					productMap = yearMap;

				if (LastAggrCalendarId < calendarDto.getCalendarId()) {

					logger.info("Processing Calendar Id:"
							+ calendarDto.getCalendarId());

					// get daily aggregation for processing date
					HashMap<String, SummaryDataDTO> dailyMap = objSalesRollupDao
							.rollupAggregation(_conn,
									calendarDto.getCalendarId(), _locationId,
									_locationLevelId, "SALES_AGGR_DAILY_ROLLUP");

					if (dailyMap.size() != 0) {

						if (productMap.size() != 0) {

							// call the business logic method to sum up the daily
							// and periodly records
							productMap = objSalesBusiness.sumupAggregation(
									dailyMap, productMap,
									calendarDto.getCalendarId());

							// updated periodly records .
							objSalesRollupDao.updateSalesRollup(_conn,
									productMap, calendarDto.getCalendarId(),
									_yearCalendarId, "SALES_AGGR_ROLLUP",
									"ACTUAL");

							// Insert New periodly Records
							objSalesRollupDao.insertSalesRollup(_conn,
									dailyMap, calendarDto.getCalendarId(),
									_yearCalendarId, "SALES_AGGR_ROLLUP",
									lastSummaryList);

							// insert Ctd process
							//objCtdDao.insertSalesCtd(_conn, productMap, Constants.CTD_YEAR);
						} else {

							// Insert Summary periodly Records
							objSalesRollupDao.insertSalesRollup(_conn,
									dailyMap, calendarDto.getCalendarId(),
									_yearCalendarId, "SALES_AGGR_ROLLUP",
									lastSummaryList);

							// Insert Ctd Process
							//objCtdDao.insertSalesCtd(_conn, dailyMap, Constants.CTD_YEAR);
							productMap = dailyMap;
						}
					}

					objSalesDao.deleteTempAggragation(_conn,
							Integer.parseInt(_locationId),
							calendarDto.getCalendarId(), _locationLevelId,
							"SALES_AGGR_DAILY_ROLLUP_TEMP");

				}
				PristineDBUtil.commitTransaction(_conn, " Connection Comitted");
			}

		} catch (Exception exe) {
			logger.error(" Error in summary year rollup process..." + exe);
			PristineDBUtil.rollbackTransaction(_conn, " Connection Rollbacked");
			throw new GeneralException(
					"Error in summary year rollup process..." + exe);
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
		_yearCalendarId = periodCalendarId;

	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: Summary Year  Rollup ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}
