/*
 * Title: TOPS Summary Daily Roll up Process
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/03/2012	Dinesh Kumar	Initial Version 
 * Version 0.2  03/08/2012  Dinesh Kumar    Movement By Volume Code Added
 * Version 0.3  07/08/2012  Dinesh Kumar    Ctd Seq Name changed
 * Version 0.4  14/08/2012  Dinesh Kumar	Repair Process Added
 * Version 0.5  14/08/2012  Dinesh Kumar    Revenue Correction and margin calculation added
 * Version 0.6  23/01/2013  John Britto     Log re-factored
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
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDailyRollupDao;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationStatusDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryDailyRollUp {

	static Logger logger = Logger.getLogger("SummaryRollup");

	// global variables
	private Date _startDate	= null;		// hold the input start date
	private Date _endDate = null;       // hold the input end date
	private Connection _conn = null;    // hold the database connection
	String _districtId = "";           	// hold the district id
	String _regionId = "";           	// hold the region id
	String _divisionId = "";           	// hold the division id
	String  _chainId   = "";           	// hold the chain id
	String _repairFlag = null;          // hold the repair mode flag
	boolean _updateOnlyPLMetrics = false;
	
	/*
	 * ****************************************************************
	 * Class constructor
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryDailyRollUp(Date startDate, Date endDate, String districtId,
		String regionId, String divisionId, String chainId, String repairFlag,
														String updatePLOnly) {
		PropertyManager.initialize("analysis.properties");

		if (districtId == null && regionId == null && divisionId == null
				&& chainId == null) {
			logger.error("Input error, District/Region/Division/Chain needed for aggregation");
			System.exit(1);
		}
		
		if (startDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			startDate = cal.getTime();
		}

		if (endDate == null) {
			endDate = startDate;
		}

		// Assign the district Id
		if (districtId != null ) {
			_districtId = districtId;
			logger.info("Summary Daily Rollup For District:" + districtId);

		}
		// Assign the RegionId
		 if (regionId != null ) {
			_regionId = regionId;
			logger.info("Summary Daily Rollup For Region:" + regionId);
		}
		// Assign the DivisionId
		 if (divisionId != null ) {
			_divisionId = divisionId;
			logger.info("Summary Daily Rollup For Division:" + divisionId);
		}
		// Assign the Chain Id
		 if (chainId != null ) {
			_chainId = chainId;
			logger.info("Summary Daily Rollup For Chain:" + chainId);
		}
		
		 if (repairFlag.equalsIgnoreCase("FULL")){
			 _repairFlag = "FULL";
			logger.info("Process mode:FULL, Process all the non-processed records");
		 }
		 else{
			 _repairFlag = "NORMAL";
			logger.info("Process mode:NORMAL, Process data only for specific Calendar Ids");
		 }
		 
		if (updatePLOnly.equalsIgnoreCase("Y"))
			_updateOnlyPLMetrics = true;
		 
		// assign the inputs to global variables
		_startDate = startDate;
		_endDate  = endDate;

		// get the DB connection
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException gex) {
			logger.error("Error while connecting DB:" + gex);
			logger.info("Summary Daily Roll up process Ends unsucessfully");
			System.exit(1);
		}

	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Start Date 
	 * Argument 2: End Date 
	 * If the StartDate and EndDate is not specified then process for past week
	 * ****************************************************************
	 */

	public static void main(String[] args) throws GeneralException {
		
		// Initialize the summary-weekly log4j
		PropertyConfigurator.configure("log4j-summary-daily-rollup.properties");

		logger.info("Summary Rollup Process Starts");
		logCommand (args);
		Date startDate 			= null; // get the input start date
		Date endDate 			= null; // get the input end date
		String districtId 		= null; // get the input district Id
		String regionId 		= null; // get the input region Id
		String divisionId 		= null; // get the input division Id
		String chainId 			= null; // get the input chain id
		String repairFlag       = ""; // get the input repair flag value
		DateFormat dateFormat 	= new SimpleDateFormat("MM/dd/yyyy"); // date format
		SummaryDailyRollUp summaryRollup = null;
		String updatePLOnly = "N";
		
		try {
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the start date
				if (arg.startsWith("STARTDATE")) {
					String inputDate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(inputDate);
					} catch (ParseException pxe) {
						logger.error("The given start date is invalid");
						System.exit(1);
					}
				}
				// get the end date
				if (arg.startsWith("ENDDATE")) {
					String inputDate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(inputDate);
					} catch (ParseException pxe) {
						logger.error("The given end date is invalid");
						System.exit(1);
					}
				}

				if (arg.startsWith("DISTRICT")) {	// get the districtId
					districtId = arg.substring("DISTRICT=".length());
				}
				else if (arg.startsWith("REGION")) {	// get the Region Id

					regionId = arg.substring("REGION=".length());
				}
				else if (arg.startsWith("DIVISION")) {	// get the Division Id
					divisionId = arg.substring("DIVISION=".length());
				}
				else if (arg.startsWith("CHAIN")) {		// get the Chain Id
					chainId = arg.substring("CHAIN=".length());
				}
				
				// get the repair flag value
				if( arg.startsWith("REPAIRFLAG")){
					 repairFlag = arg.substring("REPAIRFLAG=".length());
				}
				
				if (arg.startsWith("UPDATEPLONLY")) {
					updatePLOnly = arg.substring("UPDATEPLONLY=".length());
				}		
			}

			// call the Constructor
			summaryRollup = new SummaryDailyRollUp(startDate, endDate,
						districtId, regionId, divisionId, chainId, repairFlag, updatePLOnly);
			
			// method to do the daily roll up process :
			summaryRollup.dailyRollupAggregation();
			
			logger.info("Summary Roll up process ends sucessfully");

		} catch (Exception exe) {
			logger.error(" Error in rollup main class..." , exe);
			logger.info("Summary Roll up process ends unsucessfully");
			throw new GeneralException(" Error in rollup main class..." , exe);
		} finally {
			PristineDBUtil.close(summaryRollup._conn);
		}
	}
	

	/*
	 * ***********************************************************************
	 * Get the Calendar Id list for the given date range. Call the aggregation
	 * for calendar list 
	 * Argument 1 : Start Date 
	 * Argument 2 : End Date 
	 * Argument 3 : Calendar RowType
	 * @catch GeneralException
	 * ***********************************************************************
	 */

	private void dailyRollupAggregation() throws SQLException, GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objRetailDao = new RetailCalendarDAO();

		// Object For SalesAggregationDao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();

		// Object For SalesAggregationDao
		SalesAggregationStatusDao objSAStatusDao = new SalesAggregationStatusDao();

		
		// object for SummaryRollUpDao
		SalesAggregationDailyRollupDao objSDRollupDao = new SalesAggregationDailyRollupDao();
		
		int locationLevelId = 0;
		int locationId = 0;
		if (_districtId != ""){
			locationLevelId = Constants.DISTRICT_LEVEL_ID;
			locationId = Integer.parseInt(_districtId);
		}
		else if (_regionId != ""){
			locationLevelId = Constants.REGION_LEVEL_ID;
			locationId = Integer.parseInt(_regionId);
		}
		else if (_divisionId != ""){
			locationLevelId = Constants.DIVISION_LEVEL_ID;
			locationId = Integer.parseInt(_divisionId);
		}
		else if (_chainId != ""){
			locationLevelId = Constants.CHAIN_LEVEL_ID;
			locationId = Integer.parseInt(_chainId);
		}

		List<RetailCalendarDTO> calendarList; 
		try {

			// To check the repair mode enable or not
			if (_repairFlag.equalsIgnoreCase("FULL")) {



				calendarList = objSAStatusDao.getNotProcessedCalendar(_conn, locationLevelId, locationId, Constants.CALENDAR_DAY);
			}
			else{
				calendarList = objRetailDao.dayCalendarList
							(_conn, _startDate, _endDate, Constants.CALENDAR_DAY);
			}
			
			// Check the calendar list availability for further process
			if (calendarList.size() > 0) {
				logger.info("Number of days to process:" + calendarList.size());
				for (int cL = 0; cL < calendarList.size(); cL++) {
					RetailCalendarDTO calendarDto = calendarList.get(cL);
					logger.info("Process begins at Calendar level. Calendar Id:"
												+ calendarDto.getCalendarId());

					// call the roll up aggregation process
					locationLevelAggregation(calendarDto, objSDRollupDao,
							objSalesDao, objRetailDao, objSAStatusDao);
					// commit the process
					PristineDBUtil.commitTransaction(_conn,
							"Commit the Daily Rollup Process");

				}

			} else {
				logger.info("There is no new data to process");
			}

		} catch (Exception exe) {
			logger.error(exe);
			PristineDBUtil.rollbackTransaction(_conn, "RollBack the Process");
			throw new GeneralException("Daily Rollup Error", exe);
		}
	}

	/*
	 * ****************************************************************
	 * Method used to aggregate the location level records call delete roll up
	 * method for delete the previous aggregation 
	 * call the location level aggregation method based on input 
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */

	public void locationLevelAggregation(RetailCalendarDTO calendarDto,
			SalesAggregationDailyRollupDao rollUpDao,
			SalesAggregationDao objSalesDao, RetailCalendarDAO objRetailDao, 
			SalesAggregationStatusDao objSAStatusDao)
			throws GeneralException {

		// logger.info(" Summary Roll up Process ");

		// hold the last year summary daily id
		// key location_level_id and location_id
		// value Summary_Daily_Id
		HashMap<String, Long> lastRollupList = new HashMap<String, Long>();

		boolean insertFlag = false;
		try {
			// DIstrict Level Aggregation

			if (_districtId != "") {

				if (!_updateOnlyPLMetrics){
					// Delete the Previous Aggregation
					logger.debug("Delete Previous District aggrigation data");
					rollUpDao.deleteRollUp(_conn, calendarDto.getCalendarId(),
							_districtId, Constants.DISTRICT_LEVEL_ID);
					
					logger.debug("Delete Previous Sales Aggrigation Status");
					objSAStatusDao.deleteSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.DISTRICT_LEVEL_ID, 
						Integer.parseInt(_districtId));

					if (!_updateOnlyPLMetrics){
						// get the last year roll up Details
						logger.debug("Get last year aggregation IDs");
						lastRollupList = objSalesDao.getLastYearSalesAggrList(_conn,
								calendarDto.getlstCalendarId(),
								Integer.valueOf(_districtId),
								Constants.DISTRICT_LEVEL_ID, "SALES_AGGR_DAILY_ROLLUP",
								"SALES_AGGR_DAILY_ROLLUP_ID");				
					}
				}

				// District Level aggregation
				insertFlag = getDistrictAggregation(calendarDto, rollUpDao,
						lastRollupList, objRetailDao);

				if (!_updateOnlyPLMetrics){
					logger.debug("Insert SA status for District");
					objSAStatusDao.insertSAStatus(_conn, calendarDto.getCalendarId(), 
						Constants.DISTRICT_LEVEL_ID, Integer.parseInt(_districtId));
					
					logger.debug("Update SA status for Store"); 
					objSAStatusDao.updateSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.DISTRICT_LEVEL_ID, 
						Integer.parseInt(_districtId), Constants.CALENDAR_DAY);
				}
			}

			if (_regionId != "") {

				if (!_updateOnlyPLMetrics){
					// Delete the Previous Aggregation
					logger.debug("Delete Previous Region aggrigation data");
					rollUpDao.deleteRollUp(_conn, calendarDto.getCalendarId(),
							_regionId, Constants.REGION_LEVEL_ID);
					
					logger.debug("Delete Previous Sales Aggrigation Status");
					objSAStatusDao.deleteSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.REGION_LEVEL_ID, 
						Integer.parseInt(_regionId));
				
					if (!_updateOnlyPLMetrics){
						// get the last year roll up Details
						logger.debug("Get last year aggregation IDs");
						lastRollupList = objSalesDao
								.getLastYearSalesAggrList(_conn,
										calendarDto.getlstCalendarId(),
										Integer.valueOf(_regionId),
										Constants.REGION_LEVEL_ID,
										"SALES_AGGR_DAILY_ROLLUP",
										"SALES_AGGR_DAILY_ROLLUP_ID");
					}
				}
				
				// Region Level Aggregation
				insertFlag = getRegionAggregation(calendarDto.getCalendarId(),
						rollUpDao, lastRollupList);

				if (!_updateOnlyPLMetrics){
					logger.debug("Insert SA status for Region"); 
					objSAStatusDao.insertSAStatus(_conn, calendarDto.getCalendarId(), 
							Constants.REGION_LEVEL_ID, Integer.parseInt(_regionId));
					
					logger.debug("Update SA status for District"); 
					objSAStatusDao.updateSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.REGION_LEVEL_ID , 
						Integer.parseInt(_regionId), Constants.CALENDAR_DAY);
				}
			}

			// Check Region Insert Status
			if (_divisionId != "") {

				if (!_updateOnlyPLMetrics){
					// Delete the Previous Aggregation
					logger.debug("Delete Previous Division aggrigation data");
					rollUpDao.deleteRollUp(_conn, calendarDto.getCalendarId(),
							_divisionId, Constants.DIVISION_LEVEL_ID);
					
					logger.debug("Delete Previous Sales Aggrigation Status");
					objSAStatusDao.deleteSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.DIVISION_LEVEL_ID, 
						Integer.parseInt(_divisionId));

					if (!_updateOnlyPLMetrics){
						// get the last year roll up Details
						logger.debug("Get last year aggregation IDs");
						lastRollupList = objSalesDao.getLastYearSalesAggrList(_conn,
								calendarDto.getlstCalendarId(),
								Integer.valueOf(_divisionId),
								Constants.DIVISION_LEVEL_ID, "SALES_AGGR_DAILY_ROLLUP",
								"SALES_AGGR_DAILY_ROLLUP_ID");
					}
				}

				// Division Level Aggregation
				insertFlag = getDivisionAggregation(
						calendarDto.getCalendarId(), rollUpDao, lastRollupList);
				
				if (!_updateOnlyPLMetrics){
					logger.debug("Insert SA status for Division"); 
					objSAStatusDao.insertSAStatus(_conn, calendarDto.getCalendarId(), 
							Constants.DIVISION_LEVEL_ID, Integer.parseInt(_divisionId));
					
					logger.debug("Update SA status for Region"); 
					objSAStatusDao.updateSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.DIVISION_LEVEL_ID, 
						Integer.parseInt(_divisionId), Constants.CALENDAR_DAY);
				}
			}

			if (_chainId != "") {

				if (!_updateOnlyPLMetrics){
					// Delete the Previous Aggregation
					logger.debug("Delete Previous Region aggrigation data");
					rollUpDao.deleteRollUp(_conn, calendarDto.getCalendarId(),
							_chainId, Constants.CHAIN_LEVEL_ID);
	
					logger.debug("Delete Previous Sales Aggrigation Status");
					objSAStatusDao.deleteSAStatus(_conn, calendarDto.getCalendarId(), 
							Constants.CHAIN_LEVEL_ID, Integer.parseInt(_chainId));

					if (!_updateOnlyPLMetrics){
						// get the laster roll up Details
						logger.debug("Get last year aggregation IDs");
						lastRollupList = objSalesDao
								.getLastYearSalesAggrList(_conn,
										calendarDto.getlstCalendarId(),
										Integer.valueOf(_chainId),
										Constants.CHAIN_LEVEL_ID,
										"SALES_AGGR_DAILY_ROLLUP",
										"SALES_AGGR_DAILY_ROLLUP_ID");
					}
				}

				// Chain Level Aggregation
				insertFlag = getChainAggregation(calendarDto.getCalendarId(),
						rollUpDao, lastRollupList);

				if (!_updateOnlyPLMetrics){
					logger.debug("Insert SA status for Chain"); 
					objSAStatusDao.insertSAStatus(_conn, calendarDto.getCalendarId(), 
							Constants.CHAIN_LEVEL_ID, Integer.parseInt(_chainId));
					
					logger.debug("Update SA status for Division"); 
					objSAStatusDao.updateSAStatus(_conn, 
						calendarDto.getCalendarId(), Constants.CHAIN_LEVEL_ID, 
						Integer.parseInt(_chainId), Constants.CALENDAR_DAY);
				}
			}

			if (insertFlag) {
				logger.debug(" Process Completed successfully");
			}

		} catch (Exception exe) {
			logger.error(" Daily Rollup Error", exe);
			throw new GeneralException(" Daily Rollup Error", exe);

		}
	}

	/*
	 * ****************************************************************
	 * Method used to get the district level aggregation records call the
	 * fetch district aggregation method and get the district level records .call
	 * the insert roll up daily method and insert the district level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private boolean getDistrictAggregation(RetailCalendarDTO calendarDto,
			SalesAggregationDailyRollupDao rollUpDao,
			HashMap<String, Long> lastRollupList,
			RetailCalendarDAO objRetailDao) throws GeneralException {

		List<SummaryDataDTO> districtList = new ArrayList<SummaryDataDTO>();
		boolean districtFlag = false;

		try {

			// get the district roll up details
			logger.debug("Get current District aggregation data");
			districtList = rollUpDao.getDistrictAggrData(_conn,
					calendarDto.getCalendarId(), _districtId);

			Date processDate = ComputeDateRanges(calendarDto.getStartDate());

			logger.debug("Get last year CalendarId");
			RetailCalendarDTO periodStartDate = objRetailDao
					.week_period_quarter_yearCalendarList(_conn, processDate,
							Constants.CALENDAR_WEEK);

			processDate = ComputeDateRanges(periodStartDate.getStartDate());

			logger.debug("Get Identical Store Data");
			HashMap<String, SummaryDataDTO> identicalMap = rollUpDao
					.getDistrictIdenticalAggrData(_conn,
							calendarDto.getCalendarId(), processDate,
							_districtId);

			logger.debug("Identical Record count:" + identicalMap.size());

			if (districtList.size() != 0) {

				if (!_updateOnlyPLMetrics){
				logger.debug("Insert daily District Data");
				// insert the district level roll up
				districtFlag = rollUpDao.insertRollUpDaily(_conn,
						calendarDto.getCalendarId(), districtList,
						Constants.DISTRICT_LEVEL_ID, lastRollupList,
						identicalMap);
				}
				else{
					logger.debug("Update daily District Data");
					// insert the district level roll up
					districtFlag = rollUpDao.updateRollUpDaily(_conn,
							calendarDto.getCalendarId(), districtList,
							Constants.DISTRICT_LEVEL_ID, lastRollupList,
							identicalMap);
					}
					
				
			} else {
				logger.info("No records found for District:" + _districtId);
			}
		} catch (Exception exe) {
			logger.error(" Daily Rollup Error", exe);
			throw new GeneralException(" Daily Rollup Error", exe);
		}
		return districtFlag;

	}

	/*
	 * ****************************************************************
	 * Method used to get the region level aggregation records call the
	 * fetch region aggregation method and get the region level records call the
	 * insert roll up daily method and insert the region level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private boolean getRegionAggregation(int calendarId,
			SalesAggregationDailyRollupDao rollUpDao,
			HashMap<String, Long> lastRollupList) throws GeneralException {

		List<SummaryDataDTO> regionList = new ArrayList<SummaryDataDTO>();
		boolean regionFlag = false;

		try {
			// logger.info(" Process Region Id  " + _regionId);

			// get the Region Roll up details
			logger.debug("Get current Region aggregation data");
			regionList = rollUpDao.getRegionAggrData(_conn, calendarId,
					_regionId, Constants.DISTRICT_LEVEL_ID);
			if (regionList.size() != 0) {

				
				if (!_updateOnlyPLMetrics){
					// insert the region level roll up
					logger.debug("Insert daily Region data");
					regionFlag = rollUpDao.insertRollUpDaily(_conn, calendarId,
							regionList, Constants.REGION_LEVEL_ID, lastRollupList,
							null);
				}
				else{
					// insert the region level roll up
					logger.debug("Update daily Region data");
					regionFlag = rollUpDao.updateRollUpDaily(_conn, calendarId,
							regionList, Constants.REGION_LEVEL_ID, lastRollupList,
							null);
				}
			} else {
				logger.info("No records found for Region:" + _regionId);
			}

		} catch (Exception exe) {
			logger.error("Daily roll up error:", exe);
			throw new GeneralException(" Daily Rollup Error", exe);
		}
		return regionFlag;
	}

	/*
	 * ****************************************************************
	 * Method used to get the division level aggregation records call the
	 * fetch division aggregation method and get the division level records call
	 * the insert roll up daily method and insert the division level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private boolean getDivisionAggregation(int calendarId,
			SalesAggregationDailyRollupDao rollUpDao,
			HashMap<String, Long> lastRollupList) throws GeneralException {

		List<SummaryDataDTO> divisionList = new ArrayList<SummaryDataDTO>();
		boolean divisionFlag = false;
		try {
			// logger.info(" Process Division Id  " + _divisionId);

			// get the Division Roll up Details
			logger.debug("Get current Division aggregation data");
			divisionList = rollUpDao.getDivisionAggrData(_conn, calendarId,
					_divisionId, Constants.REGION_LEVEL_ID);
			if (divisionList.size() != 0) {
				if (!_updateOnlyPLMetrics){
					logger.debug("Insert daily Division data");
					divisionFlag = rollUpDao.insertRollUpDaily(_conn, calendarId,
							divisionList, Constants.DIVISION_LEVEL_ID,
							lastRollupList, null);
				}
				else{
					logger.debug("Insert daily Division data");
					divisionFlag = rollUpDao.updateRollUpDaily(_conn, calendarId,
							divisionList, Constants.DIVISION_LEVEL_ID,
							lastRollupList, null);
				}
				
			} else {
				logger.info("No Records Found For Division:" + _divisionId);
			}
		} catch (Exception exe) {
			logger.error(" Daily Rollup Error:", exe);
			throw new GeneralException(" Daily Rollup Error", exe);
		}
		return divisionFlag;
	}

	/*
	 * ****************************************************************
	 * Method used to get the chain level aggregation records call the
	 * fetch chain aggregation method and get the chain level records call the
	 * insert roll up daily method and insert the chain level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */

	private boolean getChainAggregation(int calendarId,
			SalesAggregationDailyRollupDao rollUpDao,
			HashMap<String, Long> lastRollupList) throws GeneralException {

		List<SummaryDataDTO> chainList = new ArrayList<SummaryDataDTO>();
		boolean chainFlag = false;
		try {
			// logger.info(" Process Chain Id  " + _chainId);

			// get the Chain Roll up Details
			logger.debug("Get Chain aggregation data");
			chainList = rollUpDao.getChainAggrData(_conn, calendarId, _chainId,
					Constants.DIVISION_LEVEL_ID);

			if (chainList.size() != 0) {
				if (!_updateOnlyPLMetrics){
				logger.debug("Insert daily Chain data");
				chainFlag = rollUpDao.insertRollUpDaily(_conn, calendarId,
						chainList, Constants.CHAIN_LEVEL_ID, lastRollupList,
						null);
				}
				else{
					logger.debug("Insert daily Chain data");
					chainFlag = rollUpDao.updateRollUpDaily(_conn, calendarId,
							chainList, Constants.CHAIN_LEVEL_ID, lastRollupList,
							null);					
				}
					
			} else {
				logger.info("No Records Found For Chain:" + _chainId);
			}
		} catch (Exception exe) {
			logger.error("Error while aggregating daily Chain data:", exe);
			throw new GeneralException(" Daily Rollup Error", exe);
		}
		return chainFlag;
	}

	private Date ComputeDateRanges(String startdate) throws GeneralException,
			ParseException {

		// find the date with start time
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		Date date = (Date) formatter.parse(startdate);

		return date;

	}

	/**
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args)
    {
		StringBuffer sb = new StringBuffer("Command: SummaryDailyRollup ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }

}
