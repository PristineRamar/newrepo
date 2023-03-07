/*
 * Title: TOPS   Delete the salesanalysis aggregation records.
 *
 *************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 08-06-2012    Dinesh Kumar V	     Initial  Version 
 **************************************************************************************
 */

package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.DeleteAggregationDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class DeleteAggregation {

	static Logger logger = Logger.getLogger("DeleteAggregation");
	private String _locationLevel = null; 		// hold the input location level parameter
	private String _productLevel = null; 		// hold the input product level  parameter
	private String _calendarLevel = null; 		// hold the input calendar level parameter
	private String _tableName = null; 			// hold the process table name
	private int _locationLevelId = 0; 			 // hold the process location level id
	private int _productLevelId = 0; 			 // hold the process product level id
	private Connection _conn = null; // hold the spring jdbc template
	private String calendarMode = null; 		// hold the process calendar mode
	private Date _startDate = null; 			// hold the input start date
	private Date _endDate = null; 				// hold the input end date

	/*
	 * Class constructor for DeleteAggregation batch 
	 * Argument 1 : locationLevel
	 * Argument 2 : ProductLevel 
	 * Argument 3 : Calendar Level 
	 * Metod mainly used to get the Database connection
	 * @catch Exception
	 */
	public DeleteAggregation(Date startDate, Date endDate,
			String locationLevel, String productLevel, String calendarLevel) {

		// input validation
		if (locationLevel == null || productLevel == null
				|| calendarLevel == null) {
			logger.error(" Input is Missing : LOCATIONLEVEL / PRODUCTLEVEL /CALENDARLEVEL ");
			System.exit(1);
		} else {

			// assign the inputs to global varibles
			_startDate = startDate;
			_endDate = endDate;
			_locationLevel = locationLevel;
			_productLevel = productLevel;
			_calendarLevel = calendarLevel;
		}
		PropertyManager.initialize("analysis.properties");

		logger.info(" Get the Dbconnection");
		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Connection Failed");
			logger.error(exe);
		}

	}

	/*
	 * Main method for the batch 
	 * Argument 1 : Location Level ( STORE / DISTRICT/REGION/DIVISION/CHAIN ); 
	 * Argument 2 : Product Level ( SUBCATEGORY / CATEGORY / DEPARTMENT / FINANCE / MERCHANTISE / PORTFOLIO /SECTOR) 
	 * Argument 3 : Calendar Level ( DAY / WEEK /PERIOD/QUARTER/YEAR)
	 * Batch mainly used to delete the sales aggregation records in table based on input
	 * @catch Excepion
	 */
	public static void main(String[] args) throws GeneralException {

		PropertyConfigurator.configure("log4j-Delete-Aggregation.properties");
		logger.info(" Delete the sales aggregation records process starts");

		String locationLevel = null; // hold the input location level
		// parameter
		String productLevel = null; // hold the input product level
		// parameter
		String calendarLevel = null; // hold the input calendar level
		// parameter
		Date startDate = null; // hold the input start date
		Date endDate = null; // hold the input date
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		DeleteAggregation objDeleteAggr = null;
		logCommand(args);
		try {

			for (int iP = 0; iP < args.length; iP++) {

				String arg = args[iP];

				if (arg.startsWith("LOCATIONLEVEL")) {
					locationLevel = arg.substring("LOCATIONLEVEL=".length());
				}

				if (arg.startsWith("PRODUCTLEVEL")) {
					productLevel = arg.substring("PRODUCTLEVEL=".length());
				}

				if (arg.startsWith("CALENDARLEVEL")) {
					calendarLevel = arg.substring("CALENDARLEVEL=".length());
				}

				if (arg.startsWith("STARTDATE")) {
					String Inputdate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("Start Date Parsing Error, check Input");
						System.exit(1);
					}
				}

				if (arg.startsWith("ENDDATE")) {
					String Inputdate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("End Date Parsing Error, check Input");
						System.exit(1);
					}
				}

			}
			objDeleteAggr = new DeleteAggregation(startDate, endDate,
					locationLevel, productLevel, calendarLevel);
			// Pick defaults based on properties
			if(startDate == null || endDate == null){
				// Set start date and end date based on properties
				objDeleteAggr.setDefaults();				
			}
			objDeleteAggr.deleteAggregation();

			logger.info(" Delete Process Ends");
		} catch (Exception exe) {

			logger.error(" Delete Process Error...");

		} finally {

			PristineDBUtil.close(objDeleteAggr._conn);
		}

	}
	
	/**
	 * Sets start date and end date for processing based on properties defined
	 */
	private void setDefaults() {
		// Get number of weeks to retain data
		int weeksToRetain = Integer.parseInt(PropertyManager.getProperty("SALES_ANALYSIS.DELETE.AGGREGATION.WEEKSTOKEEP"));
		
		// Get number of weeks to delete data
		int weeksToDelete = Integer.parseInt(PropertyManager.getProperty("SALES_ANALYSIS.DELETE.AGGREGATION.WEEKSTODELETE"));
		
		String procStartDate = DateUtil.getWeekStartDate(weeksToDelete + weeksToRetain);
		
		try{
			_startDate = DateUtil.toDate(procStartDate, Constants.APP_DATE_FORMAT);
			
			String procEndDate = DateUtil.getWeekEndDate(DateUtil.toDate(DateUtil.getWeekStartDate(weeksToRetain + 1), Constants.APP_DATE_FORMAT));
			_endDate = DateUtil.toDate(procEndDate, Constants.APP_DATE_FORMAT);
		}catch(GeneralException exception){
			logger.error("Error in parsing start/ end date " + exception);
			System.exit(-1);
		}
		
		logger.info("Start date - " + _startDate + "\t" + "End date - " + _endDate);
	}

	/*
	 * Method Mainly used to delete the aggregation records based on the input
	 * Method Cal 1 : constructInput : find the table name and delete critria
	 * based on input Method cal 2 : deleteprocess : its used to delete the
	 * records
	 * 
	 * @catch Exception
	 * 
	 * @throws gendralException
	 */
	private void deleteAggregation() throws GeneralException {

		logger.info(" DeleteAggregation method starts");

		try {

			logger.info(" Construct the Input");

			// method used to find the table name and location level based on
			// input
			constructInput();

			// get the calendar id for given date range
			RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

			logger.info(" Table Name..." + _tableName + "....Location Level..."
					+ _locationLevelId + ".....Product Level...."
					+ _productLevelId);

			logger.info(" Get Calendar Id");
			DeleteAggregationDao objDeleteDao = new DeleteAggregationDao();
			// get the retail calendarList
			List<RetailCalendarDTO> calendarList = objCalendarDao
					.RowTypeBasedCalendarList(_conn, _startDate, _endDate,
							calendarMode);

			for (int ii = 0; ii < calendarList.size(); ii++) {

				RetailCalendarDTO dtoObj = calendarList.get(ii);

				logger.info(" Delete Process Calendar Id "
						+ dtoObj.getCalendarId());
				// logger.debug(" Processing Date...." +
				// dtoObj.getStartDate()+".... End Date" + dtoObj.getEndDate());

				// method used to move the records from main table to backup
				// table
				objDeleteDao.backupProcess(_conn, _tableName, _locationLevelId,
						_productLevelId, dtoObj.getCalendarId());

				PristineDBUtil.commitTransaction(_conn, "Commit Connection");

			}

			// hold the delete table name

			logger.info(" DeleteAggregation method Ends");
		} catch (GeneralException exe) {
			PristineDBUtil.rollbackTransaction(_conn, "Rollback Connection");
			throw new GeneralException("Error in processing.....", exe);
		}

	}

	private void constructInput() {

		_tableName = "SALES_AGGR";

		// for calendar level
		if (_calendarLevel.equalsIgnoreCase("DAY")) {
			calendarMode = Constants.CALENDAR_DAY;
			_tableName += "_DAILY";
			_locationLevelId = Constants.STORE_LEVEL_ID;
		} else if (_calendarLevel.equalsIgnoreCase("WEEK")) {
			_tableName += "_WEEKLY";
			calendarMode = Constants.CALENDAR_WEEK;
			_locationLevelId = Constants.STORE_LEVEL_ID;
		} else if (_calendarLevel.equalsIgnoreCase("PERIOD")) {
			calendarMode = Constants.CALENDAR_PERIOD;
			_locationLevelId = Constants.STORE_LEVEL_ID;
		} else if (_calendarLevel.equalsIgnoreCase("QUARTER")) {
			calendarMode = Constants.CALENDAR_QUARTER;
			_locationLevelId = Constants.STORE_LEVEL_ID;
		} else if (_calendarLevel.equalsIgnoreCase("YEAR")) {
			calendarMode = Constants.CALENDAR_YEAR;
			_locationLevelId = Constants.STORE_LEVEL_ID;
		}

		// for location level
		if (_locationLevel.equalsIgnoreCase("DISTRICT")) {
			_locationLevelId = Constants.DISTRICT_LEVEL_ID;
			_tableName += "_ROLLUP";
		} else if (_locationLevel.equalsIgnoreCase("DIVISION")) {
			_locationLevelId = Constants.DIVISION_LEVEL_ID;
			_tableName += "_ROLLUP";
		} else if (_locationLevel.equalsIgnoreCase("REGION")) {
			_locationLevelId = Constants.REGION_LEVEL_ID;
			_tableName += "_ROLLUP";
		} else if (_locationLevel.equalsIgnoreCase("CHAIN")) {
			_locationLevelId = Constants.CHAIN_LEVEL_ID;
			_tableName += "_ROLLUP";
		}
		 

		// for product level
		if (_productLevel.startsWith("SUBCATEGORY")) {
			_productLevelId = Constants.SUBCATEGORYLEVELID;
		} else if (_productLevel.startsWith("CATEGORY")) {
			_productLevelId = Constants.CATEGORYLEVELID;
		} else if (_productLevel.startsWith("DEPARTMENT")) {
			_productLevelId = Constants.DEPARTMENTLEVELID;
		} else if (_productLevel.startsWith("FINANCE")) {
			_productLevelId = Constants.FINANCEDEPARTMENT;
		} else if (_productLevel.startsWith("MERCHANTIS")) {
			_productLevelId = Constants.MERCHANTISEDEPARTMENT;
		} else if (_productLevel.startsWith("PORTFOLIO")) {
			_productLevelId = Constants.PORTFOLIO;
		} else if (_productLevel.startsWith("SECTOR")) {
			_productLevelId = Constants.SECTOR;
		}

	}

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: DeleteAggregation Process...");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
}
