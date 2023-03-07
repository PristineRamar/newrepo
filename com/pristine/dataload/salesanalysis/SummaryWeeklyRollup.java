/*
 * Title: TOPS Summary Weekly RollUp
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/02/2012	Dinesh Kumar	Initial Version 
 * version 0.1  05/10/2012  Dinesh Kumar	add identical non identical store process
 * Version 0.2  06/08/2012  Dinesh Kumar    Movement By Volume Code added
 * Version 0.3  02/09/2012  Dinesh Kumar    Margin Calculation and repair process added
 * Version 0.4  23/01/2013  John Britto     Log re-factored
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

public class SummaryWeeklyRollup {

	static Logger logger = Logger.getLogger("SummaryWeeklyRollup");

	// global variables
	private Date _processDate    = null;         // hold the given input process date
	private Date _startDate 	 = null;         // hold the week start date
	private Date _endDate 		 = null;         // hold the process end date
	private Connection _conn     = null;         // hold the database connection
	String _districtId 			 = "";           // hold the input district id
	String _regionId 			 = "";           // hold the input region id
	String _divisionId 			 = "";           // hold the input division id 
	String _chainId 			 = "";           // hold the input chain id
	int _weekCalendarId 		 = 0;            // hold the week calendar id for given week
	String _reCalculation 		 = "";           // hold the input recalculation flag
	int _locationLevelId 		 = 0;            // hold the location level id for given input
	String _locationId 			 = "";           // hold the location Id
	private String _repairFlag   = null;         // hold the repair flag value

	/*
	 * ****************************************************************
	 * Class constructor
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryWeeklyRollup(Date processDate, String districtId,
								String regionId, String divisionId, String chainId,
								String reCalculation, String repairFlag) {

		//If process date is null then set currentDate - 1 as default 
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
			logger.info("Summary Weekly Roll up For District Id:" + _locationId);
		}

		// Assign the RegionId
		if (regionId != null) {

			_locationLevelId = Constants.REGION_LEVEL_ID;
			_regionId = regionId;
			_locationId = regionId;
			logger.info("Summary Weekly Roll up For Region Id:"+ _locationId);
		}

		// Assign the DivisionId
		if (divisionId != null) {

			_locationLevelId = Constants.DIVISION_LEVEL_ID;
			_divisionId = divisionId;
			_locationId = divisionId;
			logger.info("Summary Weekly Rollup For Division Id:" + _locationId);
		}

		// Assign the Chain Id
		if (chainId != null) {

			_locationLevelId = Constants.CHAIN_LEVEL_ID;
			_chainId = chainId;
			_locationId = chainId;
			logger.info("Summary Weekly Rollup For Chain Id:" + _locationId);
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

		// find week start date
		_processDate = processDate;

		PropertyManager.initialize("analysis.properties");

		// get the DB connection
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException gex) {
			logger.error("Error while Connecting DB" + gex);
			System.exit(1);
		}

		logger.debug(" Constructor Ends ");
	}

	
	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument Argument
	 * 1: Start Date Argument 
	 * 2: End Date 
	 * If the StartDate and EndDate is not  specified then process for past week
	 * ****************************************************************
	 */

	public static void main(String[] args) throws GeneralException {
		// Intialize the summary-weekly log4j
		PropertyConfigurator
				.configure("log4j-summary-weekly-rollup.properties");

		logger.info("*** Summary Weekly Rollup Process begins ***");
		logCommand(args);
		Date processDate 						= null; // get the input startdate
		String districtId 						= null; // get the input district Id
		String regionId 						= null; // get the input region Id
		String divisionId 						= null; // get the input division Id
		String chainId 							= null; // get the input chain id
		String reCalculation 					= "";   // hold the recalculation (Yes/No)
		String repairFlag                       = ""; // hold the input repair flag
		DateFormat dateFormat 					= new SimpleDateFormat("MM/dd/yyyy"); // dateformat
		SummaryWeeklyRollup summaryWeeklyRollup = null;
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
				if(arg.startsWith("REPAIRFLAG")){
					repairFlag = arg.substring("REPAIRFLAG=".length());
				}

			}

			if (districtId == null && regionId == null && divisionId == null
					&& chainId == null) {
				logger.error(" District/Region/Division/Chain Nedded to process the batch ");
				System.exit(1);
			}

			// call the Constructor
			summaryWeeklyRollup = new SummaryWeeklyRollup(processDate,
					districtId, regionId, divisionId, chainId, reCalculation,repairFlag);

			// Process Roll up Starts
			summaryWeeklyRollup.processWeeklyRollupData();

		} catch (Exception exe) {
			logger.error("Error in weekly rollup..... " + exe);
			throw new GeneralException("Error in weekly rollup..... " + exe);
		} finally {
			logger.info("*** Summary Weekly Rollup Process ends ***");
			PristineDBUtil.close(summaryWeeklyRollup._conn);
		}
	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range.
	 *  Call the aggregation for calendar list 
	 *  Argument 1 : Start Date 
	 *  Argument 2 : End Date 
	 *  Argument 3 : Calendar RowType
	 *  @catch GeneralException
	 * ****************************************************************
	 */

	private void processWeeklyRollupData() throws GeneralException {

		//logger.debug("processWeeklyData Starts ");

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for Summary Weekly Rollup Dao
		SalesAggregationRollupDao objWRDao = new SalesAggregationRollupDao();
		
		// object for SalesAggregationDao 
		SalesAggregationDao objSalesDao = new SalesAggregationDao();
		
		// Object for SalesAggregationCtdDao
		SalesAggregationCtdDao objCtdDao = new SalesAggregationCtdDao();
		
		// hold the last year summarydaily id 
		// key location_level_id and location_id 
		// value Summary_Daily_Id
		HashMap<String, Long> lastSummaryList = new HashMap<String, Long>(); 

		try {
			
			RetailCalendarDTO objWeekCalendar = null;
			
			List<RetailCalendarDTO> dailyCalendarList = null;
			
						
			if( _repairFlag.equalsIgnoreCase("FULL")){
				
				// If repair mode FULL .. do the repair aggregation
				// Check the repair process need or not for given store
				RetailCalendarDTO repairCalendarDates = objCalendarDao
						.getRepairCalendarDates(_conn,
								Integer.parseInt(_locationId),_locationLevelId,
								"SALES_AGGR_DAILY_ROLLUP_TEMP");
				
				if (repairCalendarDates != null) {

					logger.debug(" Process Date:" + _processDate);

					// get the week calendar id list.....
					List<RetailCalendarDTO> repairWeekCalendarList = objCalendarDao
							.getRepairCalendarList(_conn,
									repairCalendarDates.getStartDate(),
									_processDate, Constants.CALENDAR_WEEK);

					int weekLoopSize = repairWeekCalendarList.size();

					logger.info("Week calendar List Size:" + weekLoopSize);

					for (int rWC = 0; rWC < repairWeekCalendarList.size(); rWC++) {
						RetailCalendarDTO objWeekCalDto = repairWeekCalendarList
								.get(rWC);

						_weekCalendarId = objWeekCalDto.getCalendarId();
						logger.debug(" Process End Date...... " + _processDate);
						// check week calendar id had more than one calendar
						// id

						dailyCalendarList = new ArrayList<RetailCalendarDTO>();

						dailyCalendarList = objCalendarDao
								.getRepairDayCalendarList(_conn, weekLoopSize,
										rWC,
										repairCalendarDates.getStartDate(),
										_processDate, _weekCalendarId);

						RetailCalendarDTO objRepairStartDate = dailyCalendarList
								.get(0);

						RetailCalendarDTO objRepairEndDate = dailyCalendarList
								.get(dailyCalendarList.size() - 1);

						if (objCalendarDao.checkWeekStartDate(_conn,
								objRepairStartDate.getStartDate(),
								Constants.CALENDAR_WEEK)) {

							int lastUpdatedCalendarId = objRepairStartDate
									.getCalendarId() - 1;

							logger.debug(" Repair start Last Calenar is:"
									+ lastUpdatedCalendarId);

							// get the previous aggregation details for repair
							// process
							HashMap<String, SummaryDataDTO> dailyTempAggregation = objSalesDao
									.getTempSalesAggregation(_conn,
											Integer.parseInt(_locationId),
											_locationLevelId,
											"SALES_AGGR_WEEKLY_ROLLUP",
											"SALES_AGGR_DAILY_ROLLUP_TEMP",
											dailyCalendarList, _weekCalendarId);

							// update the previous aggregation details into
							// SALES_AGGR_WEEKLY table.
							objWRDao.updateSalesRollup(_conn,
									dailyTempAggregation,
									lastUpdatedCalendarId, _weekCalendarId,
									"SALES_AGGR_WEEKLY_ROLLUP", "REPAIR");
							
							
							// delete the previous temp aggregation in Ctd table
							//objCtdDao.deletePreviousCtdAggregation(_conn,
							//		Integer.valueOf(_locationId),
							//		_locationLevelId,
							//		objRepairStartDate.getStartDate(),
							//		objRepairEndDate.getStartDate(),
							//		"SALES_AGGR_DAILY_ROLLUP_TEMP",
							//		Constants.CTD_WEEK);
							
							// delete the previous temp aggregation in Ctd table
							//objCtdDao.deletePreviousCtdAggregation(_conn,
							//		Integer.valueOf(_locationId),
							//		_locationLevelId,
							//		objRepairStartDate.getStartDate(),
							//		objRepairEndDate.getStartDate(),
							//		"SALES_AGGR_DAILY_ROLLUP",
							//		Constants.CTD_WEEK);
						} else {

							// delete the previous aggregation In Sales Table

							// delete the previous aggregation in Sales table
							objSalesDao.deletePreviousSalesAggregation(_conn,
									_weekCalendarId,
									Integer.valueOf(_locationId),
									_locationLevelId,
									"SALES_AGGR_WEEKLY_ROLLUP");

							// delete the previous aggregation in Ctd table
							//objCtdDao.deletePreviousCtdAggregation(_conn,
							//		Integer.valueOf(_locationId),
							//		_locationLevelId,
							//		objRepairStartDate.getStartDate(),
							//		objRepairEndDate.getStartDate(),
							//		"SALES_AGGR_DAILY_ROLLUP",
							//		Constants.CTD_WEEK);

						}

						lastSummaryList = objSalesDao.getLastYearSalesAggrList(
								_conn, objWeekCalDto.getlstCalendarId(),
								Integer.valueOf(_locationId), _locationLevelId,
								"SALES_AGGR_WEEKLY_ROLLUP",
								"SALES_AGGR_WEEKLY_ROLLUP_ID");

						// call the weeklyrollup process
						getWeeklyRollupAggregation(dailyCalendarList, objWRDao,
								objSalesDao, objCtdDao, _locationId,
								_locationLevelId, lastSummaryList);
					}
				}
				
				else {
					
					processWeeklyRollup(objWeekCalendar, objCalendarDao,
							dailyCalendarList, objSalesDao, objCtdDao,
							lastSummaryList, objWRDao);
				}
				
			}
			
			
			else  {
				processWeeklyRollup(objWeekCalendar, objCalendarDao,
						dailyCalendarList, objSalesDao, objCtdDao,
						lastSummaryList, objWRDao);
			}
			 
		} catch (Exception exe) {
			logger.error("Error in weekly rollup..... " + exe);
			throw new GeneralException("Error in weekly rollup..... " + exe);
		}
	}

	private void processWeeklyRollup(RetailCalendarDTO objWeekCalendar,
			RetailCalendarDAO objCalendarDao,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesAggregationDao objSalesDao, SalesAggregationCtdDao objCtdDao,
			HashMap<String, Long> lastSummaryList,
			SalesAggregationRollupDao objWRDao) throws GeneralException {

		try {
			// get Week calendar Id
			objWeekCalendar = objCalendarDao
					.week_period_quarter_yearCalendarList(_conn, _processDate,
							Constants.CALENDAR_WEEK);

			computeProcessDate(objWeekCalendar.getStartDate(),
					objWeekCalendar.getCalendarId());

			// get the day calendar Id
			dailyCalendarList = objCalendarDao.dayCalendarList(_conn,
					_startDate, _endDate, Constants.CALENDAR_DAY);
			logger.debug(" Week Calendar Id " + _weekCalendarId);

			// if Recalculation mode is Yes then delete the previous aggregation
			if (_reCalculation != "" && _reCalculation.equalsIgnoreCase("YES")) {

				// delete the previous aggregation in Sales table
				objSalesDao.deletePreviousSalesAggregation(_conn,
						_weekCalendarId, Integer.valueOf(_locationId),
						_locationLevelId, "SALES_AGGR_WEEKLY_ROLLUP");

				// delete the previous aggregation in Ctd table
				//objCtdDao.deletePreviousCtdAggregation(_conn,
				//		Integer.valueOf(_locationId), _locationLevelId,
				//		objWeekCalendar.getStartDate(),
				//		objWeekCalendar.getEndDate(),
				//		"SALES_AGGR_DAILY_ROLLUP", Constants.CTD_WEEK);
			}

			// get Last year summaryDaily List
			lastSummaryList = objSalesDao.getLastYearSalesAggrList(_conn,
					objWeekCalendar.getlstCalendarId(),
					Integer.valueOf(_locationId), _locationLevelId,
					"SALES_AGGR_WEEKLY_ROLLUP", "SALES_AGGR_WEEKLY_ROLLUP_ID");

			// call the weeklyrollup process
			getWeeklyRollupAggregation(dailyCalendarList, objWRDao,
					objSalesDao, objCtdDao, _locationId, _locationLevelId,
					lastSummaryList);
		} catch (NumberFormatException e) {
		  throw new GeneralException(" Error In Process Weekly Rollup" ,e);
		} catch (ParseException e) {
			throw new GeneralException(" Error In Process Weekly Rollup" ,e);			 
		} catch (GeneralException e) {
			throw new GeneralException(" Error In Process Weekly Rollup" ,e);
		}

	}

	/*
	 * ****************************************************************
	 * Method used to get the district level aggregation records 
	 * call the fetchdistrictaggregation method and get the district level records 
	 * call the insert rollup daily method and insert the district level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void getWeeklyRollupAggregation(
			List<RetailCalendarDTO> dailyCalendarList,
			SalesAggregationRollupDao objWRDao,
			SalesAggregationDao objSalesDao, SalesAggregationCtdDao objCtdDao,
			String productId, int _locationLevelId,
			HashMap<String, Long> lastSummaryList) throws GeneralException {
		//logger.debug(" Get Weekly Aggregation Records Starts ");

		HashMap<String, SummaryDataDTO> weekMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> productMap = new HashMap<String, SummaryDataDTO>();
		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		try{
		// Check Avilability in SALES_AGGR_WEEKLY_ROLLUP
		int LastAggrCalendarId = objSalesDao.getLastAggerCalendarId(_conn,
										_weekCalendarId, Integer.valueOf(productId),
										_locationLevelId, "SALES_AGGR_WEEKLY_ROLLUP");
		logger.debug(" Last Calendar Id Is " + LastAggrCalendarId);

		if (LastAggrCalendarId != 0) {

			// Get the alreday aggregation records for given week
			weekMap = objWRDao.rollupAggregation(_conn, _weekCalendarId,
					productId, _locationLevelId, "SALES_AGGR_WEEKLY_ROLLUP");
			logger.debug(" Week Map count --" + weekMap.size());

		}

		for (int cL = 0; cL < dailyCalendarList.size(); cL++) {
			RetailCalendarDTO calendarDto = dailyCalendarList.get(cL);

			// For First Day Process
			if (cL == 0 && weekMap.size() != 0)
				productMap = weekMap;

			if (LastAggrCalendarId < calendarDto.getCalendarId()) {

				logger.info("Processing begins at Calendar level. Calendar Id:" 
												+ calendarDto.getCalendarId());

				// get daily aggregation for processing date
				HashMap<String, SummaryDataDTO> dailyMap = objWRDao
						.rollupAggregation(_conn,
								calendarDto.getCalendarId(), productId,
								_locationLevelId, "SALES_AGGR_DAILY_ROLLUP");
				logger.debug(" Daily Map Size:" + dailyMap.size());

				if (dailyMap.size() != 0) {

					if (productMap.size() != 0) {

						// call the businesslogic method to sum up the daily and  weekly records
						productMap = businessLogic.sumupAggregation(
								dailyMap, productMap, 
								calendarDto.getCalendarId());

						// updated weekly records .
						objWRDao.updateSalesRollup(_conn, productMap,
								calendarDto.getCalendarId(), _weekCalendarId, "SALES_AGGR_WEEKLY_ROLLUP" ,"ACTUAL");

						// Insert New Weekly Records
						objWRDao.insertSalesRollup(_conn, dailyMap,
								calendarDto.getCalendarId(), _weekCalendarId, "SALES_AGGR_WEEKLY_ROLLUP", lastSummaryList);

						// insert Ctd process
						//objCtdDao.insertSalesCtd(_conn, productMap, Constants.CTD_WEEK);
					} else {

						// Insert Summary Weekly Records
						objWRDao.insertSalesRollup(_conn, dailyMap,
								calendarDto.getCalendarId(), _weekCalendarId, "SALES_AGGR_WEEKLY_ROLLUP",lastSummaryList );

						// Insert Ctd Process
						//objCtdDao.insertSalesCtd(_conn, dailyMap, Constants.CTD_WEEK);
						productMap = dailyMap;
					}
				}

			}
			PristineDBUtil.commitTransaction(_conn,"Weekly Rollup Comitted");
		}
		
		}
		catch(Exception exe){
			PristineDBUtil.rollbackTransaction(_conn,"Weekly Rollup Rollbacked");
			logger.error("Error in weekly rollup..... " + exe);
			throw new GeneralException("Error in weekly rollup..... " + exe);  		
			
		}

	}
	
	/*
	 * ****************************************************************
	 *  Method mainly used to construct the date range 
	 * Argument 1 : startDate
	 * Argument 2 : Week Calendar Id
	 * throws startDate
	 * ****************************************************************
	 */
	private void computeProcessDate(String startDate, int weekCalendarId) 
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
		_weekCalendarId = weekCalendarId;
	}
	
	
	
	/**
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args) {
		StringBuffer sb = new StringBuffer("Command: SummaryDaily ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }

}
