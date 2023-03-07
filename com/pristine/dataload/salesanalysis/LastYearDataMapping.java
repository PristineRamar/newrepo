/*
 * Title: TOPS   Tops Lastyear Data Mapping process
 ****
 *****************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 26-06-2012	    Dinesh Kumar V	     Initial  Version 
 *******************************************************************************************
 */
package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LastYearDataMapping {

	static Logger logger = Logger.getLogger("LastYearDataMapping");
	Connection _conn = null; // Connection Object

	public LastYearDataMapping(int processYear, String timeLine, int timeLineNo, 
			int locationLevel, int locationId) {

		if (processYear == 0) {
			logger.error(" INPUT MISSING : PROCESSYEAR");
			System.exit(1);
		}

		if (timeLine.equalsIgnoreCase("D") || timeLine.equalsIgnoreCase("W") || 
				timeLine.equalsIgnoreCase("P") || timeLine.equalsIgnoreCase("Q") ||
				timeLine.equalsIgnoreCase("Y")) {}
		else{
			logger.error(" INPUT MISSING OR INVALID INPUT: TIMELINE");
			System.exit(1);
		}

		if ((timeLine.equalsIgnoreCase("D") && (timeLineNo < 0 || timeLineNo > 364)) ||
				(timeLine.equalsIgnoreCase("W") && (timeLineNo < 0 || timeLineNo > 52)) ||
				(timeLine.equalsIgnoreCase("P") && (timeLineNo < 0 || timeLineNo > 13)) ||
				(timeLine.equalsIgnoreCase("Q") && (timeLineNo < 0 || timeLineNo > 4)) ||
				(timeLine.equalsIgnoreCase("Y") && (timeLineNo < 0 || timeLineNo > 1))) {
			logger.error("INVALID INPUT: TIMELINENO");
			System.exit(1);
		}
		
		if (locationLevel == 0) {
			logger.error(" INPUT MISSING : LOCATIONLEVEL");
			System.exit(1);
		}		

		if (locationId == 0) {
			logger.error(" INPUT MISSING : LOCATION");
			System.exit(1);
		}		
		
		PropertyManager.initialize("analysis.properties");

		// get the database connection
		try {
			logger.info("get the db connection......");
			_conn = DBManager.getConnection();
		} catch (GeneralException e) {
			logger.error(" CONNECTION FAILED.....");
			System.exit(1);
		}
	}

	/*
	 * Main Method for the batch Batch used to re mapping the last year data to
	 * current date Argument 1 : Process Year
	 */

	public static void main(String[] args) throws GeneralException {

		PropertyConfigurator.configure("log4j-lastyear-remapping.properties");
		logCommand(args);
		int processYear = 0; // hold the input process year
		String timeLine ="";
		int locationLevel =0;
		int locationId =0;
		int timeLineNo =0;
		
		LastYearDataMapping objLastYearMapping = null;
		try {
			// get the arguments
			for (int arg = 0; arg < args.length; arg++) {

				String argument = args[arg];
				logger.debug("Argument " + arg + ": " + args[arg]);
				
				//Processing Presto calendar year 
				if (argument.startsWith("PROCESSYEAR")) {

					try {
						processYear = Integer.parseInt(argument
								.substring("PROCESSYEAR=".length()));
						logger.debug("Processing Year: " + processYear); 		
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : PROCESSYEAR");
						System.exit(1);
					}
				}

				//processing location level can be store/dist/region/division/chain
				if (argument.startsWith("LOCATIONTYPE")) {
					try {
						locationLevel = Integer.parseInt(argument.substring("LOCATIONTYPE=".length()));
						logger.debug("Location Level: " + locationLevel);
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : LOCATIONTYPE");
						System.exit(1);
					}
				}

				//processing district id /region id /division id /chain id
				if (argument.startsWith("LOCATIONID")) {
					try {
						locationId = Integer.parseInt(argument.substring("LOCATIONID=".length()));
						logger.debug("Location ID: " + locationId);
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : LOCATIONID");
						System.exit(1);
					}
				}
				
				//Processing calendar Type, it can be day, week, period, quarter and year
				if (argument.startsWith("TIMELINE")) {
					timeLine = argument.substring("TIMELINE=".length());
					logger.debug("Timeline: " + timeLine);
				}
				
				//This can be a day no, week no, period no, quarter no and year no
				if (argument.startsWith("TIMENO")) {
					try {
						timeLineNo = Integer.parseInt(argument.substring("TIMENO=".length()));
						logger.debug("Timeline No: " + timeLineNo);
					} catch (Exception exe) {
						logger.error(" INPUT ERROR : TIMELINENO");
						System.exit(1);
					}
				}				
			}

			// call the class constructor
			objLastYearMapping = new LastYearDataMapping(processYear, 
							timeLine, timeLineNo, locationLevel, locationId);

			// Process the last year re-mapping
			boolean commit = objLastYearMapping.processLastyearRemapping(
				processYear, timeLine, timeLineNo, locationLevel, locationId);

			if (commit) {
				PristineDBUtil.commitTransaction(objLastYearMapping._conn,
						"Last Year Re Mapping");
			} else {
				PristineDBUtil.rollbackTransaction(objLastYearMapping._conn,
						"Last Year Re Mapping");
			}

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

	/*
	 * Method used to process the Last year processed summary id and update into
	 * current year record
	 */

	private boolean processLastyearRemapping(int processYear, String timeLine,
		int timeLineNo, int locationLevel, int locationId) throws GeneralException {

		boolean methodStatus = false;
		try {
			// get the last year summary daily id
			SalesAggregationDao objSalesDaily = new SalesAggregationDao();

			if (timeLine.equalsIgnoreCase("D")){ 
			
				logger.info(" Process Last-Year day Mapping Begins.........");
				
				//Process the last Year mapping in store level
				logger.debug("Processing for Daily Store level data");
				methodStatus = dayLevel(objSalesDaily ,"STORE", processYear, 
							timeLine, timeLineNo, locationLevel, locationId);
				
				//Process the last Year mapping in product level
				logger.debug("Processing for Daily Product level data");
				methodStatus = dayLevel(objSalesDaily ,"PRODUCT", processYear, 
						timeLine, timeLineNo, locationLevel, locationId);
			}
			
			if (timeLine.equalsIgnoreCase("W")){ 
				logger.info(" Process Last-Year week Mapping Begins.........");
				
				//Process the last Year mapping in store level
				logger.debug("Processing for Week Store level data");
				methodStatus = weekLevel(objSalesDaily, "STORE", processYear, 
					timeLine, timeLineNo, locationLevel, locationId);
				
				//Process the last Year mapping in product level
				logger.debug("Processing for Week Product level data");
				methodStatus = weekLevel(objSalesDaily, "PRODUCT", processYear,	
					timeLine, timeLineNo, locationLevel, locationId);
			}
			
			if (timeLine.equalsIgnoreCase("P")){ 
				logger.info(" Process Last-Year period Mapping Begins.........");
				
				//Process the last Year mapping in store level	
				logger.debug("Processing for Period Store level data");
				methodStatus = periodLevel(objSalesDaily ,"STORE", processYear,	
						timeLine, timeLineNo, locationLevel, locationId);
				
				//Process the last Year mapping in product level
				logger.debug("Processing for Period Product level data");
				methodStatus = periodLevel(objSalesDaily, "PRODUCT", processYear,	
						timeLine, timeLineNo, locationLevel, locationId);
			}
			
			if (timeLine.equalsIgnoreCase("Q")){ 
				logger.info(" Process Last-Year quarter Mapping Begins.........");
	
				logger.debug("Processing for Quarter Store level data");
				methodStatus = quarterLevel(objSalesDaily,"STORE", processYear,	
						timeLine, timeLineNo, locationLevel, locationId);
				
				logger.debug("Processing for Quarter Product level data");
				methodStatus = quarterLevel(objSalesDaily,"PRODUCT", processYear,	
						timeLine, timeLineNo, locationLevel, locationId);
			}
			
			if (timeLine.equalsIgnoreCase("Y")){ 
				logger.info(" Process Last-Year year Mapping Begins.........");
				logger.debug("Processing for Year Store level data");
				methodStatus = yearRollup(objSalesDaily,"STORE", processYear,	
						timeLine, timeLineNo, locationLevel, locationId);
				
				logger.debug("Processing for Year Product level data");
				methodStatus = yearRollup(objSalesDaily,"PRODUCT", processYear,	
						timeLine, timeLineNo, locationLevel, locationId);
			}
			
		} catch (Exception exe) {
			logger.error(" Process LastyearRemapping Method Error..... " + exe);
			throw new GeneralException(
					" ProcessLastyearRemapping Method Error..... " + exe);
		}

		return methodStatus;

	}

	private boolean yearRollup(SalesAggregationDao objSalesDaily, 
			String byProcessLevel, int processYear, String timeLine, 
				int timeLineNo, int locationLevel, int locationId)
			throws GeneralException {

		boolean commit = false;
		try {

			if (locationLevel == Constants.STORE_LEVEL_ID){
				logger.debug("Get Last year Store Level data");			
				List<SummaryDataDTO> yearLevel = objSalesDaily.getLastYearMappingDetails(
						_conn, processYear, "SALES_AGGR", Constants.CALENDAR_YEAR, 
						byProcessLevel, timeLine, timeLineNo, locationLevel, locationId);
	
				logger.debug("Total Year Level data count:" + yearLevel.size());
	
				logger.debug("Update Last Year reference ID");			
				commit = objSalesDaily.updateRemapLastYear(_conn, yearLevel,
						processYear, "SALES_AGGR", Constants.CALENDAR_YEAR , byProcessLevel);
			}
			else{
				logger.debug("Get Last year above Store Level data");			
				List<SummaryDataDTO> yearRollupLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, 
					"SALES_AGGR_ROLLUP", Constants.CALENDAR_YEAR, byProcessLevel, 
								timeLine, timeLineNo, locationLevel, locationId);
	
				logger.debug("Total Year Level data count:"	+ yearRollupLevel.size());
	
				logger.debug("Update Last Year reference ID");
				commit = objSalesDaily.updateRemapLastYear(_conn, yearRollupLevel,
						processYear, "SALES_AGGR_ROLLUP", Constants.CALENDAR_YEAR ,byProcessLevel);
			}
		} catch (Exception exe) {

			logger.error(" Year Level Last Year Remapping error....", exe);
			throw new GeneralException(
					"Year Level Last Year Remapping error....", exe);

		}

		return commit;
	}

	private boolean quarterLevel(SalesAggregationDao objSalesDaily, 
			String byProcessLevel, int processYear, String timeLine, 
			int timeLineNo, int locationLevel, int locationId)
			throws GeneralException {

		boolean commit = false;

		try {
			if (locationLevel == Constants.STORE_LEVEL_ID){
				List<SummaryDataDTO> QuarterLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, "SALES_AGGR",
							Constants.CALENDAR_QUARTER , byProcessLevel, timeLine, 
											timeLineNo, locationLevel, locationId);
	
				logger.info(" Quarter Level List Size......" + QuarterLevel.size());
	
				commit = objSalesDaily.updateRemapLastYear(_conn, QuarterLevel,
						processYear, "SALES_AGGR", Constants.CALENDAR_QUARTER, byProcessLevel);
			}
			else
			{
				List<SummaryDataDTO> quarterRollupLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, 
					"SALES_AGGR_ROLLUP", Constants.CALENDAR_QUARTER , byProcessLevel, 
					timeLine, timeLineNo, locationLevel, locationId);
	
				logger.info(" quarter Level Rollup List Size......"
						+ quarterRollupLevel.size());
	
				commit = objSalesDaily.updateRemapLastYear(_conn,
						quarterRollupLevel, processYear, "SALES_AGGR_ROLLUP",
						Constants.CALENDAR_QUARTER , byProcessLevel);
			}
		} catch (Exception exe) {

			logger.error(" Quarter Level Last Year Remapping Error..... ", exe);
			throw new GeneralException(
					" Quarter Level Last Year Remapping Error..... ", exe);

		}

		return commit;
	}

	private boolean periodLevel(SalesAggregationDao objSalesDaily, 
					String byProcessLevel, int processYear,	String timeLine, 
						int timeLineNo, int locationLevel, int locationId)
			throws GeneralException {

		boolean commit = false;

		try {
			if (locationLevel == Constants.STORE_LEVEL_ID){
				List<SummaryDataDTO> periodLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, "SALES_AGGR",
							Constants.CALENDAR_PERIOD , byProcessLevel, timeLine, 
											timeLineNo, locationLevel, locationId);
	
				logger.info(" Period Level List Size......" + periodLevel.size());
	
				commit = objSalesDaily.updateRemapLastYear(_conn, periodLevel,
						processYear, "SALES_AGGR", Constants.CALENDAR_PERIOD ,byProcessLevel);
			}
			else
			{
				List<SummaryDataDTO> periodRollupLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, 
					"SALES_AGGR_ROLLUP", Constants.CALENDAR_PERIOD, byProcessLevel, 
								timeLine, timeLineNo, locationLevel, locationId);
	
				logger.info(" Period Level Rollup List Size......"
						+ periodRollupLevel.size());
	
				commit = objSalesDaily.updateRemapLastYear(_conn,
						periodRollupLevel, processYear, "SALES_AGGR_ROLLUP",
						Constants.CALENDAR_PERIOD , byProcessLevel);
			}
		} catch (Exception exe) {

			logger.error(" Period Level Last Year Remapping Error...", exe);
			throw new GeneralException(
					" Period Level Last Year Remapping Error...", exe);
		}

		return commit;
	}

	private boolean weekLevel(SalesAggregationDao objSalesDaily, 
		String byProcessLevel, int processYear,	String timeLine, 
		int timeLineNo, int locationLevel, int locationId)
			throws GeneralException {

		boolean commit = false;

		try {
			if (locationLevel == Constants.STORE_LEVEL_ID){
				List<SummaryDataDTO> weekLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, 
					"SALES_AGGR_WEEKLY", Constants.CALENDAR_WEEK , byProcessLevel, 
								timeLine, timeLineNo, locationLevel, locationId);
	
				logger.info(" Week Level List Size......" + weekLevel.size());
	
				commit = objSalesDaily.updateRemapLastYear(_conn, weekLevel,
					processYear, "SALES_AGGR_WEEKLY", Constants.CALENDAR_WEEK ,byProcessLevel);
			}
			else {
				List<SummaryDataDTO> weekRollupLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear, 
					"SALES_AGGR_WEEKLY_ROLLUP", Constants.CALENDAR_WEEK, 
					byProcessLevel, timeLine, timeLineNo, locationLevel, locationId);
	
				logger.info(" Week Level Rollup List Size......" + weekRollupLevel.size());
	
				commit = objSalesDaily.updateRemapLastYear(_conn, weekRollupLevel,
						processYear, "SALES_AGGR_WEEKLY_ROLLUP",
						Constants.CALENDAR_WEEK ,byProcessLevel);
			}
		} catch (Exception exe) {
			logger.error(" Week Level Last Remapping Error...... ", exe);
			throw new GeneralException(
					" Week Level Last Remapping Error...... ", exe);
		}

		return commit;
	}

	private boolean dayLevel(SalesAggregationDao objSalesDaily, 
		String byProcessLevel, int processYear,	String timeLine, 
		int timeLineNo, int locationLevel, int locationId)
			throws GeneralException {

		boolean commit = false;

		try {

			if (locationLevel == Constants.STORE_LEVEL_ID){
				List<SummaryDataDTO> dayLevel = objSalesDaily.getLastYearMappingDetails(
					_conn, processYear, "SALES_AGGR_DAILY", Constants.CALENDAR_DAY, 
					byProcessLevel, timeLine, timeLineNo, locationLevel, locationId);
	
				logger.info(" Day Level List Size ... " + dayLevel.size());
				
				// For Store Level Last Year Mapping
				commit = objSalesDaily.updateRemapLastYear(_conn, dayLevel,
						processYear, "SALES_AGGR_DAILY", Constants.CALENDAR_DAY , byProcessLevel);
				
				// Process Ends
			}
			else
			{
				List<SummaryDataDTO> dayRollupLevel = objSalesDaily
					.getLastYearMappingDetails(_conn, processYear,
					"SALES_AGGR_DAILY_ROLLUP", Constants.CALENDAR_DAY, byProcessLevel, 
								timeLine, timeLineNo, locationLevel, locationId);
	
				logger.info(" Day Level Rollup List size .... "	+ dayRollupLevel.size());
				
				// For Store Level Last Year Mapping
				commit = objSalesDaily.updateRemapLastYear(_conn, dayRollupLevel,
						processYear, "SALES_AGGR_DAILY_ROLLUP",
						Constants.CALENDAR_DAY , byProcessLevel);
			}
		} catch (Exception exe) {
			logger.error(" Day Level Last Year Remapping Error..... ", exe);
			throw new GeneralException(
					" Day Level Last Year Remapping Error..... ", exe);
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
