/*
' * Title: TOPS Summary Weekly Movement V2
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/03/2012	Dinesh Kumar V	 Initial Version
 * Version 0.2  17/06/2012  John Britto      Comments and log added
 * Version 0.3  06/08/2012  Dinesh Kumar V   Movement By Volume Added
 * Version 0.4  27/08/2012  Dinesh Kumar V	 Repair Process added
 * Version 0.5  23/01/2013  John Britto     Log re-factored
 *******************************************************************************
 */

package com.pristine.dataload.salesanalysis;
import java.sql.Connection;
import java.sql.SQLException;
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

public class SummaryWeeklyV2 {

	static Logger logger = Logger.getLogger("SummaryWeekly");

	// global variables
	private Date _processDate       = null;     // hold the given input date
	private Date _startDate 		= null;     // hold the start date
	private Date _endDate 			= null;     // hold the end date
	private String _storeNumber 	= "";       // hold the input store number
	private String _districtNumber 	= "";       // hold the input district number
	private String _reCalculation 	= "";       // hold the input recalculation flag
	private Connection _conn 		= null;     // hold the database connection
	private int _weekCalendarId 	= 0;        // hold the week calendar id
	private String _repairFlag      = null;     // hold the repair flag value
	

	/*
	 * ****************************************************************
	 * Class constructor 
	 * Argument 1 : ProcessDate 
	 * Argument 2 : Store Number
	 * Argument 3 : District Number
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryWeeklyV2(Date processDate, String storeNo, String districtNo,
											String reCalculation , String repairFlag) {
		PropertyManager.initialize("analysis.properties");
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
		 if (repairFlag.equalsIgnoreCase("FULL")){
			_repairFlag = "FULL";
			logger.info("Process mode:FULL, Process all the non-processed records");
		 }
		 else{
			_repairFlag = "NORMAL";
			logger.info("Process mode:NORMAL, Process data only for specific Calendar Ids");
		 }
		
		
		// Find the week start date for given input
		/*findWeekStartDate(processDate);*/
		_processDate = processDate;
		_storeNumber = storeNo;
		_districtNumber = districtNo;
		
		// get the db connection
		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error(gex.getMessage());
		}
	}
	
	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Store Number 
	 * Argument 2: District Number 
	 * Argument 3: Start Date
	 * Argument 4: End Date 
	 * If the StartDate and EndDate is not specified then  process for past week 
	 * If the District or Store Number is mandatory. 
	 * If both are are specified then consider district alone
	 * ****************************************************************
	 */

	public static void main(String[] args) throws GeneralException {
		// Initialize the summary-weekly log4j
		PropertyConfigurator.configure("log4j-summary-weekly.properties");

		logger.info("*** Sales Aggregation Weekly Process begins ***");
		logCommand (args);
		Date processDate 				= null; // hold the input start date
		String storeNo 					= "";   // hold the input store number
		String districtNo 				= ""; // hold the input district number
		String reCalculation 			= ""; // hold the recalculation (Yes/No)
		DateFormat dateFormat 			= new SimpleDateFormat("MM/dd/yyyy"); // date format
		SummaryWeeklyV2 summaryWeekly 	= null;
		
		// variable added for repair process
		String repairFlag = ""; // hold the input repair process
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
				
				// get the recalculation flag from command line
				if (arg.startsWith("RECALCULATION")) {
					reCalculation = arg.substring("RECALCULATION=".length());
				}
				
				// get the repair process need or not from command line
				if(arg.startsWith("REPAIRFLAG")){
					repairFlag = arg.substring("REPAIRFLAG=".length());
				}
			}

			// call the Constructor
			summaryWeekly = new SummaryWeeklyV2(processDate, storeNo,
													districtNo, reCalculation,
													repairFlag);
			// call the process weekly data
			summaryWeekly.processWeeklyData();
			
		} catch (Exception exe) {
			
			logger.error("Weekly Process Error...... " + exe);
			throw new GeneralException("Weekly Process Error...... " + exe);
			
			
		} finally {
			logger.info("*** Sales Aggregation Weekly Process Ends ***");
			PristineDBUtil.close(summaryWeekly._conn);
		}
	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. 
	 * Call the aggregation for calendar list 
	 * Argument 1 : Start Date 
	 * Argument 2 : End Date 
	 * Argument 3 : Calendar RowType
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void processWeeklyData() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for SalesAggregationDao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();

		// Object for SalesAggregationCtdDao
		SalesAggregationCtdDao objCtdDao = new SalesAggregationCtdDao();

		// Object for Competitor Store
		CompStoreDAO objCompStoreDao = new CompStoreDAO();

		// Object for SalesAggregationBusiness
		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();

		try {

			// hold the week calendar id
			RetailCalendarDTO objRetailCal = null;

			// hold the day calendar id based on week calendar id
			List<RetailCalendarDTO> dailyCalendarList = null;

			// input is district then get the all store number
			List<SummaryDataDTO> storeList = objCompStoreDao.getStoreNumebrs(
					_conn, _districtNumber, _storeNumber);

			for (int sN = 0; sN < storeList.size(); sN++) {
				SummaryDataDTO objStoreDto = storeList.get(sN);
				
				logger.info(" Process Start at Store level Store No:" 
											+ objStoreDto.getLocationId());
				
				// hold the day calendar id based on week calendar id
				if (_repairFlag.equalsIgnoreCase("FULL")) {

					// If repair mode FULL .. do the repair aggregation
					// Check the repair process need or not for given store
					RetailCalendarDTO repairCalendarDates = objCalendarDao
							.getRepairCalendarDates(_conn,
									objStoreDto.getLocationId(),
									Constants.STORE_LEVEL_ID,
									"SALES_AGGR_DAILY_TEMP");

					if (repairCalendarDates != null) {

					logger.debug("Process Date:" + _processDate);

					// get the week calendar id list.....
					List<RetailCalendarDTO> repairWeekCalendarList = objCalendarDao
											.getRepairCalendarList(_conn,
													repairCalendarDates.getStartDate(),
													_processDate, Constants.CALENDAR_WEEK);

					int weekLoopSize = repairWeekCalendarList.size();

					logger.info("Total Days to process:" + weekLoopSize);

					for (int rWC = 0; rWC < repairWeekCalendarList.size(); rWC++) {
							RetailCalendarDTO objWeekCalDto = repairWeekCalendarList
									.get(rWC);

					_weekCalendarId = objWeekCalDto.getCalendarId();
					logger.debug(" Process End Date:"+ _processDate);
					// check week calendar id had more than one calendar id

					dailyCalendarList = new ArrayList<RetailCalendarDTO>();

					dailyCalendarList = objCalendarDao
									.getRepairDayCalendarList(_conn,
											weekLoopSize, rWC,
											repairCalendarDates.getStartDate(),
											_processDate, _weekCalendarId);
					
					// If condition added to suppress ArrayIndexOutOfBoundsException
					if(dailyCalendarList != null && dailyCalendarList.size() > 0){
						
						RetailCalendarDTO objRepairStartDate = dailyCalendarList
									.get(0);

						if (objCalendarDao.checkWeekStartDate(_conn,
									objRepairStartDate.getStartDate(),
									Constants.CALENDAR_WEEK)) {

								int lastUpdatedCalendarId = objRepairStartDate
										.getCalendarId() - 1;

								// get the previous aggregation details for
								// repair process
								HashMap<String, SummaryDataDTO> dailyTempAggregation = objSalesDao
										.getTempSalesAggregation(_conn,
												objStoreDto.getLocationId(),
												Constants.STORE_LEVEL_ID,
												"SALES_AGGR_WEEKLY",
												"SALES_AGGR_DAILY_TEMP",
												dailyCalendarList,
												_weekCalendarId);

								// update the previous aggregation details into
								// SALES_AGGR_WEEKLY table.
								objSalesDao.updateSalesAggr(_conn,
										dailyTempAggregation,
										lastUpdatedCalendarId, _weekCalendarId,
										"SALES_AGGR_WEEKLY", "REPAIR", null);

								// delete the previous aggregation In Sales
								// Table
								//logger.debug("Delete the existing Temp CTD data");
								//objCtdDao.deletePreviousCtdAggregation(_conn,
								//		objStoreDto.getLocationId(),
								//		Constants.STORE_LEVEL_ID,
								//		objRepairStartDate.getStartDate(),
								//		objRepairEndDate.getStartDate(),
								//		"SALES_AGGR_DAILY_TEMP",
								//		Constants.CTD_WEEK);

								//logger.debug("Delete the New Temp CTD data");
								//objCtdDao.deletePreviousCtdAggregation(_conn,
								//		objStoreDto.getLocationId(),
								//		Constants.STORE_LEVEL_ID,
								//		objRepairStartDate.getStartDate(),
								//		objRepairEndDate.getStartDate(),
								//		"SALES_AGGR_DAILY", Constants.CTD_WEEK);

						} else {

								// delete the previous aggregation In Sales
								// Table
								//logger.debug("Delete the existing CTD data");
								//objCtdDao.deletePreviousCtdAggregation(_conn,
								//		objStoreDto.getLocationId(),
								//		Constants.STORE_LEVEL_ID,
								//		objRepairStartDate.getStartDate(),
								//		objRepairEndDate.getStartDate(),
								//		"SALES_AGGR_DAILY", Constants.CTD_WEEK);

								logger.debug("Delete the existing Weekly data");
								objSalesDao.deletePreviousSalesAggregation(
										_conn, _weekCalendarId,
										objStoreDto.getLocationId(),
										Constants.STORE_LEVEL_ID,
										"SALES_AGGR_WEEKLY");

						}
					}else{
						logger.warn("RepairDayCalendarList is null or empty");
					}
							// Repair process ends

							// get Last year summaryDaily List
							logger.debug("Get Last Year Summary Data ID");
							HashMap<String, Long> lastSummaryList = objSalesDao
									.getLastYearSalesAggrList(_conn,
											objWeekCalDto.getlstCalendarId(),
											objStoreDto.getLocationId(),
											Constants.STORE_LEVEL_ID,
											"SALES_AGGR_WEEKLY",
											" SALES_AGGR_WEEKLY_ID");

							// call the weekly aggregation process
							getWeeklyAggregation(objStoreDto,
									dailyCalendarList, businessLogic,
									objSalesDao, objCtdDao, lastSummaryList);

						}

					} else {
						processSummaryWeekly(objRetailCal, objCalendarDao,
								dailyCalendarList, objCtdDao, objStoreDto,
								objSalesDao, businessLogic);
					}
				}

				else {

					processSummaryWeekly(objRetailCal, objCalendarDao,
							dailyCalendarList, objCtdDao, objStoreDto,
							objSalesDao, businessLogic);

					logger.debug(" Process End For Store Number"	+ objStoreDto.getLocationId());

				}
			}

		} catch (Exception exe) {
			logger.error("Weekly Process Error...... ", exe);
			throw new GeneralException("Weekly Process Error...... ", exe);
		}
	}

	private void processSummaryWeekly(RetailCalendarDTO objRetailCal,
			RetailCalendarDAO objCalendarDao,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesAggregationCtdDao objCtdDao, SummaryDataDTO objStoreDto,
			SalesAggregationDao objSalesDao,
			SalesaggregationbusinessV2 businessLogic) throws GeneralException {

		try {
			// get Week calendar Id for a given week
			objRetailCal = objCalendarDao.week_period_quarter_yearCalendarList(
					_conn, _processDate, Constants.CALENDAR_WEEK);
			computeProcessDate(objRetailCal.getStartDate(),
					objRetailCal.getCalendarId());

			// get the day calendar list
			dailyCalendarList = objCalendarDao.dayCalendarList(_conn,
					_startDate, _endDate, Constants.CALENDAR_DAY);
			logger.debug(" Week Calendar Id......." + _weekCalendarId);

			// if Recalculation mode is Yes then delete the previous
			// aggregation
			if (_reCalculation != "" && _reCalculation.equalsIgnoreCase("YES")) {

				// delete the previous aggregation In Sales Table
				//logger.debug("Delete the existing CTD data");
				//objCtdDao.deletePreviousCtdAggregation(_conn,
				//		objStoreDto.getLocationId(), Constants.STORE_LEVEL_ID,
				//		objRetailCal.getStartDate(), objRetailCal.getEndDate(),
				//		"SALES_AGGR_DAILY", Constants.CTD_WEEK);

				logger.debug("Delete the existing Weekly data");
				objSalesDao.deletePreviousSalesAggregation(_conn,
						_weekCalendarId, objStoreDto.getLocationId(),
						Constants.STORE_LEVEL_ID, "SALES_AGGR_WEEKLY");
			}

			logger.debug("Get Last Year Summary Data ID");
			HashMap<String, Long> lastSummaryList = objSalesDao
					.getLastYearSalesAggrList(_conn,
							objRetailCal.getlstCalendarId(),
							objStoreDto.getLocationId(),
							Constants.STORE_LEVEL_ID, "SALES_AGGR_WEEKLY",
							" SALES_AGGR_WEEKLY_ID");

			// call the weekly aggregation process
			getWeeklyAggregation(objStoreDto, dailyCalendarList, businessLogic,
					objSalesDao, objCtdDao, lastSummaryList);
		} catch (ParseException e) {
			throw new GeneralException(" Error In Weekly Processing ...", e);
		} catch (SQLException e) {
			throw new GeneralException(" Error In Weekly Processing ...", e);
		} catch (GeneralException e) {
			throw new GeneralException(" Error In Weekly Processing ...", e);
		}
	}



	/*
	 * ****************************************************************
	 * Get Weekly Aggregation for given week calendar Chech the already
	 * aggregation records. 
	 * Argument 1 : weekCalendarId 
	 * Argument 2 : dataDto
	 * Argument 3 : dailyCalendarList
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private void getWeeklyAggregation(SummaryDataDTO storeDto,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesaggregationbusinessV2 businessLogic,
			SalesAggregationDao objSalesDao, SalesAggregationCtdDao objCtdDao,
			HashMap<String, Long> lastSummaryList) throws SQLException,
			GeneralException {

		//logger.debug(" Get Weekly Aggregation Records Starts ");

		HashMap<String, SummaryDataDTO> weekMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> productMap = new HashMap<String, SummaryDataDTO>();
		
		try{
		// get the last aggregation records from sales_aggr_weekly table
			logger.debug("Get Last Aggregated Calendar Id");
			int LastAggrCalendarId = objSalesDao.getLastAggerCalendarId(_conn,
					_weekCalendarId, storeDto.getLocationId(),
					Constants.STORE_LEVEL_ID, "SALES_AGGR_WEEKLY");
			logger.debug("Last Aggregated Calendar Id" + LastAggrCalendarId);

		if (LastAggrCalendarId != 0) {
			//  Get the weekly aggregation records

			logger.debug("Get Last Weekly Aggregation Data");
			weekMap = objSalesDao.getSalesAggregation(_conn,
						_weekCalendarId, storeDto.getLocationId(),
						"SALES_AGGR_WEEKLY" );
			logger.debug("Week Map count: " + weekMap.size());
		}

		for (int cL = 0; cL < dailyCalendarList.size(); cL++) {
			RetailCalendarDTO calendarDto = dailyCalendarList.get(cL);

			// For First Day Process
			if (cL == 0 && weekMap.size() != 0)
				productMap = weekMap;

			if (LastAggrCalendarId < calendarDto.getCalendarId()) {

				// get daily aggregation for processing date
				logger.debug("Fetch aggregation data for Calendar Id:"
												+ calendarDto.getCalendarId());

				HashMap<String, SummaryDataDTO> dailyMap = 
								objSalesDao.getSalesAggregation(_conn, 
					calendarDto.getCalendarId(), storeDto.getLocationId(), 
													"SALES_AGGR_DAILY" );

				if (dailyMap.size() != 0) {
					
						String store_Status = objSalesDao.getStoreStatus(_conn,
								calendarDto.getStartDate(), storeDto,
								Constants.CALENDAR_WEEK);

					if (productMap.size() != 0) {

						// call the businesslogic method to sum up the daily and
						// weekly records
						productMap = businessLogic.sumupAggregation(
							dailyMap, productMap, calendarDto.getCalendarId());
						
						
						// updated weekly records .
						logger.debug("Update existing Weekly Aggregation Data");
						objSalesDao.updateSalesAggr(_conn, productMap,
								calendarDto.getCalendarId(), _weekCalendarId,
														"SALES_AGGR_WEEKLY" ,"ACTUAL" , store_Status);
						
						
						// Insert New Weekly Records
						logger.debug("Insert New Weekly Aggregation Data");
						objSalesDao.insertSalesAggr(_conn, dailyMap,
							calendarDto.getCalendarId(), _weekCalendarId, 
							"SALES_AGGR_WEEKLY", lastSummaryList , store_Status);

						// insert Ctd process
						//logger.debug("Insert WTD Data into CTD");
						//objCtdDao.insertSalesCtd(_conn, productMap,Constants.CTD_WEEK);
						
					} else {

						// Insert Summary Weekly Records
						logger.debug("Insert Weekly Aggregation Data");
						objSalesDao.insertSalesAggr(_conn, dailyMap,
							calendarDto.getCalendarId(), _weekCalendarId, 
									"SALES_AGGR_WEEKLY", lastSummaryList , store_Status);

						// Insert Ctd Process
						//logger.debug("Insert WTD Data into CTD");
						//objCtdDao.insertSalesCtd(_conn,  dailyMap,Constants.CTD_WEEK);
						productMap = dailyMap;
					}
				}

			}
			// commit the transaction
			PristineDBUtil.commitTransaction(_conn, "Commit the summary Weekly Process");
		}
		
		}catch(Exception exe){
			logger.error(exe.getMessage());
			 PristineDBUtil.rollbackTransaction(_conn, "Rollback the Period Transation");
			 throw new GeneralException("Period Insertion Failed");
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
    private static void logCommand (String[] args)  {
		StringBuffer sb = new StringBuffer("Command: SummaryWeekly ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }
}
