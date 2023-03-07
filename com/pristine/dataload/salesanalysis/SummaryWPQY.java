package com.pristine.dataload.salesanalysis;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.salesanalysis.SalesAggregationDaoV3;
import com.pristine.dao.salesanalysis.SalesAggregationStatusDao;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.salesanalysis.PeriodCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryWPQY {

	static Logger logger = Logger.getLogger("SummaryWPQY");

	// global variables
	private Date _processDate       = null;     // hold the given input date
	private String _storeNumber 	= "";       // hold the input store number
	private String _districtNumber 	= "";       // hold the input district number
	private Connection _conn 		= null;     // hold the database connection
	private String _calMode		    = null;     // hold the mode (week,period,quarter,year)
	private String _tableName	    = null;     // hold the name of table
	private boolean _cascadeEnabled = false;    // by default the cascade mode is disabled, and all the tables depend on daily.
	private String _quarterSource	= "" ;
	boolean _updateOnlyPLMetrics = false;
	
	/*  PROCESS FLOW ALONG WITH LOCATION LEVELS(ONLY FOR CASCADE MODE)
	 * 
		Daily 5
		 |
		 V
		Weekly 5
		 |
		 V
		Period 5
		 |
		 V
		Quarter 5
		 |
		 V
		Year 5   
	 */

	/*
	 * ****************************************************************
	 * Class constructor 
	 * Argument 1 : ProcessDate 
	 * Argument 2 : Store Number
	 * Argument 3 : District Number
	 * @throws Exception
	 * ****************************************************************
	 */
	public SummaryWPQY(Date processDate, String storeNo, String districtNo, 
										String mode, String updatePLOnly) {
		PropertyManager.initialize("analysis.properties");
		// Validate the Input fields
		if (storeNo == "" && districtNo == "") {
			logger.error(" Store Numebr / District Id is missing in Input");
			System.exit(1);
		}

		if (districtNo != "") {
			storeNo = "";
		} else {
			districtNo = "0";
		}
		
		_calMode = mode;
		
		if(mode.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
			_tableName = "SALES_AGGR";
		else if(mode.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
			_tableName = "SALES_AGGR";
		else if(mode.equalsIgnoreCase(Constants.CALENDAR_YEAR))
			_tableName = "SALES_AGGR";
		else{
			_tableName = "SALES_AGGR_WEEKLY";
			_calMode = "W";
		}
		
		// Find the week start date for given input
		/*findWeekStartDate(processDate);*/
		_processDate = processDate;
		_storeNumber = storeNo;
		_districtNumber = districtNo;
		
		if (_storeNumber != "") {
			logger.info("Summary For Store:" + _storeNumber);
		} else {
			logger.info("Summary For District:" + _districtNumber);
		}
			
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
		boolean isValidInpDate			= true;
		Date processDate 				= null; // hold the input start date
		String storeNo 					= "";   // hold the input store number
		String districtNo 				= ""; // hold the input district number
		String mode		 				= ""; // hold the input district number
		DateFormat dateFormat 			= new SimpleDateFormat("MM/dd/yyyy"); // date format
		SummaryWPQY summaryWeekly 	= null;
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
				// Get the Store number from command line
				if (arg.startsWith("STORE")) {
					storeNo = arg.substring("STORE=".length());
				}

				// Get the District id from command line
				if (arg.startsWith("DISTRICT")) {
					districtNo = arg.substring("DISTRICT=".length());
				}
				
				// Get the Calendar Mode from command line
				if (arg.startsWith("CALMODE")) {
					mode = arg.substring("CALMODE=".length()).toUpperCase();
				}
				
				// Temporary property to support PL metrics
				if (arg.startsWith("UPDATEPLONLY")) {
					updatePLOnly = arg.substring("UPDATEPLONLY=".length());
				}		
			}
			
			// Initialize the summary log4j
			if(mode.equalsIgnoreCase("P"))
				PropertyConfigurator.configure("log4j-summary-period.properties");
			else if(mode.equalsIgnoreCase("Q"))
				PropertyConfigurator.configure("log4j-summary-quarter.properties");
			else if(mode.equalsIgnoreCase("Y"))
				PropertyConfigurator.configure("log4j-summary-year.properties");
			else
				PropertyConfigurator.configure("log4j-summary-weekly.properties");
			
			// Input Date validation
			if(!isValidInpDate){
				logger.error("Input error, the given process date is invalid");
				System.exit(1);
			}

			logger.info("*** Sales Aggregation Process begins ***");

			// call the Constructor
			summaryWeekly = new SummaryWPQY(processDate, storeNo, districtNo, mode, updatePLOnly);
			// call the process weekly data
			summaryWeekly.processWeeklyData();
			
		} catch (Exception exe) {
			
			logger.error("Process Error...... " + exe);
			throw new GeneralException("Process Error...... " + exe);
			
			
		} finally {
			logger.info("*** Sales Aggregation Process Ends ***");
			PristineDBUtil.close(summaryWeekly._conn);
		}
	}

	private void processWeeklyData() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		try {
			// hold the day calendar id based on week calendar id
			List<PeriodCalendarDTO> dailyCalendarList = null;

			logger.info("---------------------------------------------");
			logger.info("Process Starts for calendar mode " + _calMode);
			
			// Object for SalesAggregationBusiness
			SalesAggregationDaoV3 objSA = new SalesAggregationDaoV3();
			
			// Object for Sales Aggregation Status DAO
			SalesAggregationStatusDao objStatus = new SalesAggregationStatusDao();
			
			// Object for CompStoreDAO
			CompStoreDAO objStoreDao = new CompStoreDAO();
			
			//Get Location-id list for a give District / Store
			List<SummaryDataDTO> storeList = objStoreDao.getStoreNumebrs(_conn,
					_districtNumber, _storeNumber);
			
			logger.info("Total Stores count:" + storeList.size());
			
			//If processing date is specified the get the processing calendar Id
			if( _processDate != null ){
				dailyCalendarList = new ArrayList<PeriodCalendarDTO>();
				dailyCalendarList = objCalendarDao.getPeriodForDate(_conn, _processDate, _calMode);
			}
			
			//Iterate for each store
			for (int lo = 0; lo < storeList.size(); lo++) {
				SummaryDataDTO objStoreDto = storeList.get(lo);
				logger.info("Processing for store " + objStoreDto.getstoreNumber() + " begins");
				
				//If processing calendar id not specified the get Non-processed calendar Ids 
				if( _processDate == null ){
					dailyCalendarList = objStatus.getNotProcessedCalendarForWPQY(
						_conn, Constants.STORE_LEVEL_ID, objStoreDto.getLocationId(), _calMode);
				}
				logger.debug("CalendarList Size: " + dailyCalendarList.size());
				
				int proceeingCalendarId = 0;
				// Loop for each calendar Id
				for (int dd = 0; dd < dailyCalendarList.size(); dd++) {
					PeriodCalendarDTO calendarDTO = dailyCalendarList.get(dd);
					
					if (proceeingCalendarId != calendarDTO.getPeriodCalendarId()){
						logger.info("Process begins at Calendar level. Calendar Id:" + calendarDTO.getPeriodCalendarId());
						logger.info("Data aggregation between " + calendarDTO.getPeriodStartDate() + " and " + calendarDTO.getPeriodEndDate());

						// Call Aggregation process for each calendar Id
						ComputeSummaryAggregation(objSA, calendarDTO, objStoreDto, objStatus);
					}
					
					if (!_updateOnlyPLMetrics){
						//Update SA data processing status
						objStatus.updateSAStatus(_conn, calendarDTO.getDayCalendarId(), 
							Constants.STORE_LEVEL_ID, objStoreDto.getLocationId(), _calMode);
					}

					//Assign current processing period calendar Id to temp
					proceeingCalendarId = calendarDTO.getPeriodCalendarId();
					

					logger.debug("Process end for Calendar Id:"	+ calendarDTO.getPeriodCalendarId());
				}
			}
			
			logger.info("---------------------------------------------");
			logger.info("Process Ends.");
			
		} catch (SQLException e) {
			throw new GeneralException(" Error In Processing ...", e);
		} catch (GeneralException e) {
			throw new GeneralException(" Error In Processing ...", e);
		}
	}

	private void ComputeSummaryAggregation(SalesAggregationDaoV3 objSA, 
			PeriodCalendarDTO dailyCalendarDto, SummaryDataDTO objStoreDto, 
			SalesAggregationStatusDao objStatus) throws SQLException,GeneralException 
	{
		try{
			if (!_updateOnlyPLMetrics){
				//Delete existing data if any
				logger.debug("Delete existing data....");
				objSA.deletePreviousAggregation(_conn, dailyCalendarDto, 
									_tableName, objStoreDto.getstoreNumber(), "0");
			}
			
			//Process data
			objSA.processSalesAggregation(_conn, dailyCalendarDto, _tableName, 
				objStoreDto.getstoreNumber(), "0", _cascadeEnabled, _quarterSource, _updateOnlyPLMetrics);
			
		}catch(Exception exe){
			logger.error(exe.getMessage());
			 PristineDBUtil.rollbackTransaction(_conn, "Rollback the Period Transation");
			 throw new GeneralException("Period Insertion Failed");
		}
	}
}
