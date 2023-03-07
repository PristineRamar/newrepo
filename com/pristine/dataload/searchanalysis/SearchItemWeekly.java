/*
 * Title: Search Item Analysis Daily
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	08/02/2013	Stalin			Initial Version 
 * 				03/06/2013  Ganapathy 		Added competitor data processing
 *******************************************************************************
 */
package com.pristine.dataload.searchanalysis;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
//import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
//import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.searchanalysis.ItemMetricsSummaryDao;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SearchItemWeekly {

	static Logger logger = Logger.getLogger("SearchItemWeekly");
	
	private String _startdate = null; // Processing StartDate
	private String _storeNum = null; // Processing store number

	private Connection _conn = null; // DB connection

	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public SearchItemWeekly(String startDate, String storeno) {

		_storeNum = storeno;
		_startdate = startDate;

		PropertyManager.initialize("analysis.properties");

		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Search List Summary Daily Ends unsucessfully");
			System.exit(1);
		}
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument Argument
	 * 1: Store Number Argument 2: District Number Argument 3: Start Date
	 * Argument 4: End Date If the Date is not specified then process for
	 * yesterday If the District or Store Number is mandatory. If both are are
	 * specified then consider district alone
	 * ****************************************************************
	 */

	public static void main(String[] args) {
		
		PropertyConfigurator.configure("log4j-search-item-weekly.properties");
		logger.info("*** Search Item Analysis Weekly Process Begins ***");
		logCommand("SearchItemWeekly", args);
		
		Date startDate = null;
		String strStartDate ="";
		String storeno = "";

		// Variable added for repair process
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		SearchItemWeekly summaryWeekly = null;

		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];


				
				// Get the start/end date (for date range) from command line
				if (arg.startsWith("STARTDATE")) {
					strStartDate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(strStartDate);
					} catch (ParseException par) {
						logger.error("Start Date Parsing Error, check Input");
					}
				}

				// Get the Store number from command line
				if (arg.startsWith("STORE")) {
					storeno = arg.substring("STORE=".length());
				}


			}

			// Create object for summary daily
			summaryWeekly = new SearchItemWeekly(strStartDate,  storeno);

				// Call summary calculation process for Base Store
				summaryWeekly.processItemMetricsData(strStartDate);

		} catch (GeneralException e) {
			logger.error(e.getMessage());

		} finally {
			logger.info("*** Search List Summary Weekly Process Ends ***");
			PristineDBUtil.close(summaryWeekly._conn);
		}

	}

	
	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. Get the Group Data
	 * (Group and its child information) Call the aggregation for each calendar
	 * Id Argument 1: Aggregation process Start Date Argument 2: Aggregation
	 * process End Date
	 * ****************************************************************
	 */
	private void processItemMetricsData(String startDate)
			throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();
		
		ItemMetricsSummaryDao imsDao = new ItemMetricsSummaryDao(); 
		

		try {

			List<RetailCalendarDTO> calendarList = null;

			logger.debug("Get Calendar Id for given date range");

			RetailCalendarDTO calWeek = objCalendarDao.getCalendarId(_conn, startDate, Constants.CALENDAR_WEEK);
			RetailCalendarDTO calStart = objCalendarDao.getCalendarId(_conn, calWeek.getStartDate(), Constants.CALENDAR_DAY);
			RetailCalendarDTO calEnd = objCalendarDao.getCalendarId(_conn, calWeek.getEndDate(), Constants.CALENDAR_DAY);
			Integer calStartId = calStart.getCalendarId();
			Integer calEndId = calEnd.getCalendarId();
			
			List<Integer> storeList = new ArrayList<Integer>();

			// get Store list for day if store is not specified
			if(_storeNum.trim().length()==0){
				storeList =  imsDao.getDistinctStores(_conn, calStartId, calEndId);
			}else{
				storeList.add(objStoreDao.getStoreDetails(_conn, _storeNum).getLocationId());
			}

			logger.info("Total processing stores count:" + storeList.size());

			// iterate the storeList in loop
			for (int sL = 0; sL < storeList.size(); sL++) {

				Integer strId = storeList.get(sL);

				imsDao.deletePreviousItemMetricsWeekly(_conn, calWeek.getCalendarId(), strId);
				try{
				imsDao.insertWeeklyFromDailySummary(_conn,strId,calStartId, calEndId, calWeek.getCalendarId());
				}catch(Exception ie){
					logger.info("Error in StoreId:"+strId);
					ie.printStackTrace();
				}


			}

		} catch (Exception exe) {
			throw new GeneralException("processSummaryWeeklyData", exe);
		}
	}
	


	




	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	public static void logCommand(String pgm, String[] args) {
		StringBuffer sb = new StringBuffer("Command: ").append(pgm);
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}

