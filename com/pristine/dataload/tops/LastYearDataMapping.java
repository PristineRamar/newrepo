/*
 * Title: TOPS   Tops Last year Data Mapping process
 ****
 *****************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 12-10-2016	    John Britto	     Initial  Version 
 *******************************************************************************************
 */
package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LastYearDataMapping {

	static Logger logger = Logger.getLogger("LastYearDataMapping");
	Connection _conn = null; // Connection Object

	public LastYearDataMapping(int processYear, String timeLine, 
										int locationLevel, int locationId) {
		
		PropertyManager.initialize("analysis.properties");
		
		if (processYear > 0) 
			{}
		else
		{
			logger.error(" Invalid year");
			System.exit(1);
		}

		if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY) || 
				timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK) || 
				timeLine.equalsIgnoreCase(Constants.CALENDAR_PERIOD) ||
				timeLine.equalsIgnoreCase(Constants.CALENDAR_QUARTER) ||
				timeLine.equalsIgnoreCase(Constants.CALENDAR_YEAR))
			{}
		else
		{
			logger.error(" Invalid Time line");
			System.exit(1);
		}
		
		if (locationLevel == Constants.STORE_LEVEL_ID || 
				locationLevel == Constants.DISTRICT_LEVEL_ID ||
				locationLevel == Constants.REGION_LEVEL_ID ||
				locationLevel == Constants.DIVISION_LEVEL_ID ||
				locationLevel == Constants.CHAIN_LEVEL_ID)
			{} 
		else
		{
			logger.error(" Invalid location level");
			System.exit(1);
		}			
		
		if (locationId > 0)
			{} 
		else
		{
			logger.error(" Invalid location Id");
			System.exit(1);
		}	

		// get the DB connection
		try {
			logger.info("get the db connection......");
			_conn = DBManager.getConnection();
		} catch (GeneralException e) {
			logger.error(" CONNECTION FAILED.....");
			System.exit(1);
		}

	}

	public static void main(String[] args) throws GeneralException {

		PropertyConfigurator.configure("log4j-lastyear-remapping.properties");
		logCommand(args);
		int processYear = 0; 
		String timeLine ="";
		int locationLevel =0;
		int locationId =0;
		
		LastYearDataMapping objLastYearMapping = null;
		try {
			// get the arguments
			for (int arg = 0; arg < args.length; arg++) {

				String argument = args[arg];

				if (argument.startsWith("PROCESSYEAR")) {

					try {
						processYear = Integer.parseInt(argument
								.substring("PROCESSYEAR=".length()));
						
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : PROCESSYEAR");
						System.exit(1);
					}
				}

				if (argument.startsWith("TIMELINE")) {
						timeLine = argument.substring("TIMELINE=".length());
						
				}

				if (argument.startsWith("LOCATIONLEVEL")) {

					try {
						locationLevel = Integer.parseInt(argument
								.substring("LOCATIONLEVEL=".length()));
						
					
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : LOCATIONLEVEL", exe);
						System.exit(1);
					}
				}

				if (argument.startsWith("LOCATIONID")) {

					try {
						locationId = Integer.parseInt(argument
								.substring("LOCATIONID=".length()));
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : LOCATIONID", exe );
						System.exit(1);
					}
				}				
			}

			// call the class constructor
			objLastYearMapping = new LastYearDataMapping(processYear, 
										timeLine, locationLevel, locationId);

			// Process the last year re-mapping
			objLastYearMapping.processDataMapping(processYear, locationLevel, locationId, timeLine);
			logger.debug("Last Year Re Mapping completed successfully");
			
		} catch (GeneralException gex) {
			PristineDBUtil.rollbackTransaction(objLastYearMapping._conn,
					"Last Year Re Mapping");
			logger.error(" MAIN METHOD ERROR..... " + gex);
			throw new GeneralException(" MAIN METHOD GENERAL EXCEPTION...... "
					+ gex);
		}

		finally {
			try {
				PristineDBUtil.close(objLastYearMapping._conn);
			} catch (Exception exe) {
				logger.error(" Finally Block.............");

			}
		}
	}

	private boolean processDataMapping(int processYear,	int locationLevel, 
			int locationId, String timeLine) throws GeneralException {
		SalesAggregationDao objSalesDaily = new SalesAggregationDao();
		boolean commit = false;

		try {
			// Get target calendar IDs to update mapping
			List<RetailCalendarDTO> calendarList = objSalesDaily.getProcessingCalendarId(
							_conn, processYear, locationLevel, locationId, timeLine);
			
			// proceed with update for each calendar Id
			if (calendarList != null && calendarList.size() > 0){
				logger.info("Total " + calendarList.size() + " calendar IDs to process");
				for (int i=0; i < calendarList.size(); i++){
					
					// Process @store level
					// Get target data
					logger.info("Processing calendar " + calendarList.get(i).getCalendarId() + " with last year calendar" + calendarList.get(i).getlstCalendarId());
					logger.debug("Processing at location level...");
					List<SummaryDataDTO> lastYearLocationDataList = objSalesDaily.getLastYearMappingData(
							_conn, calendarList.get(i).getlstCalendarId(), 
							locationLevel, locationId, timeLine, "STORE");
					
					logger.debug("Total last year mapping count: " + lastYearLocationDataList.size());
					// Update the mapping
					objSalesDaily.updateLastYearReference(_conn, lastYearLocationDataList, 
						calendarList.get(i).getCalendarId(), locationLevel, locationId, timeLine, "STORE");

				// Process @Product level
					logger.debug("Processing at product level...");				
					// Get target data
					List<SummaryDataDTO> lastYearProductDataList = objSalesDaily.getLastYearMappingData(
							_conn, calendarList.get(i).getlstCalendarId(), locationLevel, locationId, timeLine, "PRODUCT");
					
					logger.debug("Total last year mapping count: " + lastYearProductDataList.size());
					// Update the mapping
					objSalesDaily.updateLastYearReference(_conn, lastYearProductDataList, 
						calendarList.get(i).getCalendarId(), locationLevel, locationId, timeLine, "PRODUCT");
				}
			}

		} catch (Exception exe) {
			logger.error("Last Year Remapping Error..... ", exe);
			throw new GeneralException("Last Year Remapping Error..... ", exe);
		}

		return commit;

	}

	/*
	 * Static method to log the command line arguments
	 * ****************************************************************
	 */
	private static void logCommand(String[] args) {
		logger.info("***********************************************************************");
		StringBuffer sb = new StringBuffer(
				"COMMAND : TOPS LAST YEAR REMAPPING ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}
