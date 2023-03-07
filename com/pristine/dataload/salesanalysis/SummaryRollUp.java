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
import com.pristine.dao.salesanalysis.SalesAggregationDaoV3;
import com.pristine.dao.salesanalysis.SalesAggregationRollupDaoV3;
import com.pristine.dao.salesanalysis.SalesAggregationStatusDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.PeriodCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class SummaryRollUp {

	static Logger logger = Logger.getLogger("SummaryRollup");

	// global variables
	private Date _processDate       = null;     // hold the given input date
	private Date _startDate	= null;				// hold the input start date
	private Date _endDate = null;       		// hold the input end date
	private Connection _conn = null;    		// hold the database connection
	private String _calMode		    = null;     // hold the mode (week,period,quarter,year)
	private String _tableName	    = null;     // hold the name of table
	int _locationLevelID = 0;
	int _locationID = 0;
	private boolean _cascadeEnabled = false;    // by default the cascade mode is disabled, and all the tables depend on daily.
	private String _quarterSource	= "" ;
	private boolean _updateOnlyPLMetrics = false;
	
	/*  PROCESS FLOW ALONG WITH LOCATION LEVELS(ONLY FOR CASCADE MODE)
	 * 
		Daily 5   -> Daily 4 -> Daily 3 -> Daily 2 -> Daily 1
		 |				|
		 V				v
		Weekly 5     Weekly 4 -> Weekly 3 -> Weekly 2 -> Weekly 1
		 |				|
		 V				v
		Period 5     Period 4 -> Period 3 -> Period 2 -> Period 1
		 |				|
		 V				v
		Quarter 5    Quarter 4 -> Quarter 3 -> Quarter 2 -> Quarter 1
		 |				|
		 V				v
		Year 5       Year 4 -> Year 3 -> Year 2 -> Year 1
	 */
	
	/*
	 * ****************************************************************
	 * Class constructor
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryRollUp(Date processDate, String calMode, String districtId,
		String regionId, String divisionId, String chainId, String updatePLOnly){
		PropertyManager.initialize("analysis.properties");

		if (districtId == null && regionId == null && divisionId == null
				&& chainId == null) {
			logger.error("Input error, District/Region/Division/Chain needed for aggregation");
			System.exit(1);
		}
		
		if(calMode.equalsIgnoreCase(Constants.CALENDAR_DAY)){
			_tableName = "sales_aggr_daily_rollup";
			_calMode = Constants.CALENDAR_DAY;
		} else if(calMode.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
			_tableName = "sales_aggr_weekly_rollup";
			_calMode = Constants.CALENDAR_WEEK;
		} else {
			_tableName = "sales_aggr_rollup";
			_calMode = calMode;
		}

		
		// Assign the district Id
		if (districtId != null ) {
			_locationID = Integer.parseInt(districtId);
			_locationLevelID = Constants.DISTRICT_LEVEL_ID;
			logger.info("Summary Rollup For District:" + districtId);
		}
		// Assign the RegionId
		if (regionId != null ) {
			_locationID = Integer.parseInt(regionId);
			_locationLevelID = Constants.REGION_LEVEL_ID;
			logger.info("Summary Rollup For Region:" + regionId);
		}
		// Assign the DivisionId
		 if (divisionId != null ) {
			_locationID = Integer.parseInt(divisionId);
			_locationLevelID = Constants.DIVISION_LEVEL_ID;
			logger.info("Summary Rollup For Division:" + divisionId);
		}
		// Assign the Chain Id
		 if (chainId != null ) {
			_locationID = Integer.parseInt(chainId);
			_locationLevelID = Constants.CHAIN_LEVEL_ID;
			logger.info("Summary Rollup For Chain:" + chainId);
		}
		 
		// assign the inputs to global variables
		 _processDate = processDate;
		 _calMode = calMode;

		if (updatePLOnly.equalsIgnoreCase("Y"))
			_updateOnlyPLMetrics = true;		 
		 
		// Get the cascade mode
		String cascadeEnb = PropertyManager.getProperty("SALES_ANALYSIS.DATALOAD.CASCADEMODE","false").trim();
		if(cascadeEnb.equalsIgnoreCase("TRUE"))	_cascadeEnabled = true;
		
		// if cascade enabled see if the Quarter source override required.
		if(_cascadeEnabled){
			String qs = PropertyManager.getProperty("SALES_ANALYSIS.DATALOAD.QUARTERSOURCE","P").trim();
			_quarterSource = qs;
		}
		
		// get the DB connection
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException gex) {
			logger.error("Error while connecting DB:" + gex);
			logger.info("Summary Rollup process Ends unsucessfully");
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
		boolean isValidInpDate  = true;
		Date processDate		= null; // get the input date
		String districtId 		= null; // get the input district Id
		String regionId 		= null; // get the input region Id
		String divisionId 		= null; // get the input division Id
		String chainId 			= null; // get the input chain id
		String mode				= ""; // calendar mode
		DateFormat dateFormat 	= new SimpleDateFormat("MM/dd/yyyy"); // date format
		SummaryRollUp summaryRollup = null;
		String updatePLOnly = "N";
		
		try {
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the start date
				if (arg.startsWith("PROCESSDATE")) {
					String inputDate = arg.substring("PROCESSDATE=".length());
					try {
						processDate = dateFormat.parse(inputDate);
					} catch (ParseException pxe) {
						//logger.error("The given process date is invalid");
						//System.exit(1);
						isValidInpDate = false;
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
				
				// Get the Calendar Mode from command line
				if (arg.startsWith("CALMODE")) {
					mode = arg.substring("CALMODE=".length()).toUpperCase().trim();
				}
				
				if (arg.startsWith("UPDATEPLONLY")) {
					updatePLOnly = arg.substring("UPDATEPLONLY=".length());
				}					
			}
			
			// Initialize the summary log4j
			if(mode.equalsIgnoreCase("W"))
				PropertyConfigurator.configure("log4j-summary-weekly-rollup.properties");
			else if(mode.equalsIgnoreCase("P"))
				PropertyConfigurator.configure("log4j-summary-period-rollup.properties");
			else if(mode.equalsIgnoreCase("Q"))
				PropertyConfigurator.configure("log4j-summary-quarter-rollup.properties");
			else if(mode.equalsIgnoreCase("Y"))
				PropertyConfigurator.configure("log4j-summary-year-rollup.properties");
			else
				PropertyConfigurator.configure("log4j-summary-daily-rollup.properties");

			// Input Date validation
			if(!isValidInpDate){
				logger.error("Input error, the given process date is invalid");
				System.exit(1);
			}
			
			logger.info("Summary Rollup Process Starts");
			
			// call the Constructor
			summaryRollup = new SummaryRollUp(processDate, mode,
						districtId, regionId, divisionId, chainId, updatePLOnly);
			
			// method to do the daily roll up process :
			summaryRollup.processDailyData();
			
			logger.info("Summary Roll up process ends sucessfully");

		} catch (Exception exe) {
			logger.error(" Error in rollup main class..." , exe);
			logger.info("Summary Roll up process ends unsucessfully");
			throw new GeneralException(" Error in rollup main class..." , exe);
		} finally {
			PristineDBUtil.close(summaryRollup._conn);
		}
	}
	
	private void processDailyData() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();
		
		// Object for Sales Aggregation Status DAO
		SalesAggregationStatusDao objStatus = new SalesAggregationStatusDao();

		try {
			// hold the day calendar id based on week calendar id
			List<PeriodCalendarDTO> dailyCalendarList = null;

			logger.info("---------------------------------------------");
			logger.info("Process Starts for calendar mode " + _calMode);
			
			// Object for SalesAggregationBusiness
			SalesAggregationRollupDaoV3 objSA = new SalesAggregationRollupDaoV3();

			// get the calendar list
			if( _processDate != null ){
				dailyCalendarList = new ArrayList<PeriodCalendarDTO>();
				dailyCalendarList = objCalendarDao.getPeriodForDate(_conn, _processDate, _calMode);
			} else {
				dailyCalendarList = objStatus.getNotProcessedCalendarForWPQY(
							_conn, _locationLevelID, _locationID, _calMode);
			}
			
			logger.debug("CalendarList Size: " + dailyCalendarList.size());

			// Loop for each calendar Id
			int processingCalendarId= 0;
			for (int dd = 0; dd < dailyCalendarList.size(); dd++) {
				PeriodCalendarDTO calendarDTO = dailyCalendarList.get(dd);

				if (processingCalendarId != calendarDTO.getPeriodCalendarId()){
					logger.debug("Process begin for Calendar Id:"	+ calendarDTO.getPeriodCalendarId());
					logger.info("Data aggregation between " + calendarDTO.getPeriodStartDate() + " and " + calendarDTO.getPeriodEndDate() );
					// Call Aggregation process for each calendar Id
					ComputeSummaryAggregation(objSA, calendarDTO);
					logger.debug("Process end for Calendar Id:"	+ calendarDTO.getPeriodCalendarId());
				}
				
				if (!_updateOnlyPLMetrics){				
					logger.debug("Update process status for (" +  calendarDTO.getDayCalendarId() + ") " +   calendarDTO.getProcessingDate());
					objStatus.updateSAStatus(_conn, calendarDTO.getDayCalendarId(), _locationLevelID, _locationID, _calMode);
				}

				processingCalendarId= calendarDTO.getPeriodCalendarId();
			}
			
			logger.info("---------------------------------------------");
			logger.info("Process Ends.");
			
		} catch (SQLException e) {
			throw new GeneralException(" Error In Processing ..."+e.getNextException(), e);
		} catch (GeneralException e) {
			throw new GeneralException(" Error In Processing ...", e);
		}
	}

	private void ComputeSummaryAggregation(SalesAggregationRollupDaoV3 objSA, 
			PeriodCalendarDTO calendarDto) throws SQLException,GeneralException 
	{
		try{
			if (!_updateOnlyPLMetrics){
				//Delete Existing Data
				objSA.deletePreviousAggregation(_conn, calendarDto, _tableName, _locationLevelID, _locationID) ;
			}
			
			//Insert data			
			objSA.processSalesAggregationRollUp(_conn, calendarDto, _tableName, 
								_locationLevelID, _locationID, _cascadeEnabled, 
										_quarterSource, _updateOnlyPLMetrics);

		}catch(Exception exe){
			logger.error(exe.getMessage());
			 PristineDBUtil.rollbackTransaction(_conn, "Rollback the Period Transation");
			 throw new GeneralException("Period Insertion Failed");
		}
	}

}
