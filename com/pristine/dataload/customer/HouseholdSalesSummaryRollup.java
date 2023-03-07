/*
 * Title: TOPS Summary Daily Movement
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/03/2017	John Britto		Initial Version 
 **********************************************************************************
 */
package com.pristine.dataload.customer;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.customer.HouseholdSalesSummaryDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDailyDao;
import com.pristine.dao.salesanalysis.SalesAggregationStatusDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.customer.HouseholdSummaryDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HouseholdSalesSummaryRollup {
	// Instance for log 
	static Logger logger = Logger.getLogger("HouseholdSalesSummaryRollup");
	// DB connection
	private Connection _conn = null;
	// Processing store number
	private String _storeNo = null; 
	// Processing District number
	private String _districtNo = null;
	// Process start date
	private Date _startDate = null;
	// Process end date
	private Date _endDate = null;
	private String _calendarMode = null;

	/*
	 * ****************************************************************
	 * Class constructor
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public HouseholdSalesSummaryRollup(Date startDate, Date endDate, 
				String storeNo, String districtNo, String calendarMode) {

		if (storeNo == "" && districtNo == "") {
			logger.error(" Store No / District Id is missing in Input");
			System.exit(1);
		}

		if (districtNo != "") {
			storeNo = "";
		}

		if (calendarMode != "" && 
				(calendarMode.equalsIgnoreCase(Constants.CALENDAR_WEEK) || 
				calendarMode.equalsIgnoreCase(Constants.CALENDAR_PERIOD))) {
		}
		else{
			logger.error(" Invalid calendar mode or missing in Input");
			System.exit(1);
		}		
		
		// If there is no date input then set the default process date
		if (startDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			startDate = cal.getTime();
			endDate = null; // Set the end date as null
		}

		if (endDate != null) {
			logger.info("Process Start Date: "
					+ startDate.toString());
			logger.info("Process End Date: "
					+ endDate.toString());

		} else {
			logger.info("Process Date: "
					+ startDate.toString());
			endDate = startDate;
		}

		_storeNo = storeNo;
		_districtNo = districtNo;
		_startDate = startDate;
		_endDate = endDate;
		_calendarMode = calendarMode;

		PropertyManager.initialize("analysis.properties");

		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Household sales summay weekly ends unsucessfully");
			System.exit(1);
		}
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Store Number 
	 * Argument 2: District Number 
	 * Argument 3: Week end date
	 * ****************************************************************
	 */

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-household-sales-summary-rollup.properties");
		logger.info("*** Household sales summary rollup process begins ***");
		logCommand(args);
		Date startDate = null;
		Date endDate = null;
		String storeNo = "";
		String calendarMode = "";
		String districtNo = "";
		
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		HouseholdSalesSummaryRollup summaryObj = null;
		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				/* Get the Start date if process is for date range*/
				if (arg.startsWith("STARTDATE")) {
					String Inputdate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("Start Date Parse Error " + par);
					}
				}

				/* Get the End date if process is for date range This date is
				 * valid only if start date is specified in input */
				if (arg.startsWith("ENDDATE")) {
					String Inputdate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("End Date Parse Error " + par);
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

				// Get the Store number from command line
				if (arg.startsWith("CALMODE")) {
					calendarMode = arg.substring("CALMODE=".length());
				}
			
			}

			// Create object for summary daily
			summaryObj = new HouseholdSalesSummaryRollup(startDate, endDate, 
											storeNo, districtNo, calendarMode);

			// Call summary calculation process
			summaryObj.initiateSummaryDataProcess();

		} catch (GeneralException e) {
			logger.error(e.getMessage());

		} finally {
			logger.info("*** Summary Daily Process Ends ***");
			PristineDBUtil.close(summaryObj._conn);
		}

	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. Get the Group Data
	 * (Group and its child information) Call the aggregation for each calendar
	 * Id 
	 * Argument 1: Aggregation process Start Date 
	 * Argument 2: Aggregation process End Date
	 * ****************************************************************
	 */
	private void initiateSummaryDataProcess() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();

		// Object for SalesAggregationDailyDao
		SalesAggregationDailyDao objSADaily = new SalesAggregationDailyDao();
		
		// Object for SalesAggregationStatusDao
		SalesAggregationStatusDao objSAStat = new SalesAggregationStatusDao(); 
		
		HouseholdSalesSummaryDAO householdDAOObj = new HouseholdSalesSummaryDAO();

		try {
			logger.debug("Get Calendar Id for given date range");
			List<RetailCalendarDTO> calendarList = objCalendarDao.getProcessingPeriodList(_conn,
							_startDate, _endDate, _calendarMode);

			//Get store list for the given store/district
			List<SummaryDataDTO> storeList = objStoreDao.getStoreNumebrs(
												_conn, _districtNo, _storeNo);
			
			logger.info("Total stores count: " + storeList.size());

			// Iterate for each store
			for (int sLoop = 0; sLoop < storeList.size(); sLoop++) {

				SummaryDataDTO objStoreDto = storeList.get(sLoop);

				logger.info("Process begins at Store level. Store: "
											+ objStoreDto.getstoreNumber());

				logger.debug("Total Processing Calendar Id count: "
													+ calendarList.size());

				// Loop for each calendar Id
				for (int calLoop = 0; calLoop < calendarList.size(); calLoop++) {

					RetailCalendarDTO calendarDTO = calendarList.get(calLoop);

					logger.info("Process begins at Calendar level. Calendar Id:"
							+ calendarDTO.getCalendarId());

					// Call Aggregation process for each calendar Id
					processSummaryData(calendarDTO,	householdDAOObj, 
								calendarDTO, objStoreDto, objSADaily, 
								objSAStat, objCalendarDao);					

					logger.debug("Process end for Calendar Id:" + 
														calendarDTO.getCalendarId());
				}				
				
			}
		} 
		catch (Exception exe) {
			throw new GeneralException("processSummaryData", exe);
		}
	}



	/*
	 * ****************************************************************
	 * Steps 
	 * Step 1: Delete existing aggregation
	 * Step 2: Get aggregated data from daily
	 * Step 3: Insert into target table
	 * Function to Aggregate Data for Segment 
	 * Argument 1: RetailCalendarDTO (Calendar Id and Process Date) 
	 * Argument 2: StoreDTO (Store Number, Store Id and Visit Count) 
	 * Argument 3: SalesAggregationDailyDao 
	 * Argument 4: MovementDAO 
	 * Argument 5 :calendar_id 
	 * Argument 6 :SummaryDataDTO 
	 * Argument 7 :SalesAggregationDao 
	 * Argument 8 : CostDataLoadV2
	 * ****************************************************************
	 */
	private int processSummaryData(RetailCalendarDTO retailDTO, 
			HouseholdSalesSummaryDAO householdDAOObj,
			RetailCalendarDTO objCalendarDto, SummaryDataDTO objStoreDto,
			SalesAggregationDailyDao objSADaily, 
			SalesAggregationStatusDao objSAStat,
			RetailCalendarDAO objCalendarDao) 
					throws GeneralException {

		int processStatus = 0;
		
		try{
			// delete the previous aggregation for the store and calendar id
			logger.debug("Delete previous Aggregation Data...");
			householdDAOObj.deleteHouseholdSalesSummaryRollup(
									_conn, objCalendarDto.getCalendarId(), 
											objStoreDto.getLocationId(), _calendarMode);
			
			// Get aggregated household data
			HashMap<Integer, SummaryDataDTO> householdMap = 
				householdDAOObj.getHouseholdSummaryRollupData(_conn, 
				objStoreDto.getLocationId(), objCalendarDto, _calendarMode);
			
			HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> itemMap = 
					householdDAOObj.getItemSummaryRollupData(_conn, 
					objStoreDto.getLocationId(), objCalendarDto, _calendarMode);
			
			// Insert data into DB 
			if (householdMap != null && householdMap.size() > 0){
				householdDAOObj.insertHouseholdSalesSummaryRollup(_conn, 
						householdMap, objStoreDto.getLocationId(), 
						objCalendarDto.getCalendarId(), _calendarMode);

				householdDAOObj.insertItemSalesSummaryRollup(_conn, 
						itemMap, objStoreDto.getLocationId(), 
						objCalendarDto.getCalendarId(), _calendarMode);
			}
		}
		catch (Exception ex) {
			logger.error("Error in processSummaryData", ex);
			throw new GeneralException(" processSummaryData Error", ex);
		}
		return processStatus;
	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SummaryDailyV2 ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}