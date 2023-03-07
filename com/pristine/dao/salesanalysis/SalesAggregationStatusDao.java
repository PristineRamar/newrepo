package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.SummaryDailyDTO;
import com.pristine.dto.salesanalysis.PeriodCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationStatusDao {
	
	static Logger logger = Logger.getLogger("SalesAggregationStatusDao");
	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
	/*
	 * Method to insert daily data processed status
	 * Process added for repair process.
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id
	 * Argument 3 : Location Level Id
	 * Argument 4 : Location Id
	 * @throws GeneralException
	 */
	public void insertSAStatus(Connection _conn, int calendarId, int locationLevelId, int locationId)
			throws GeneralException {

		//For Insert SQL
		StringBuffer sql = new StringBuffer();
		sql.append(" INSERT INTO SALES_AGGR_STATUS" + 
			" (SALES_AGGR_STATUS_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID)" + 
			" VALUES (SALES_AGGR_STATUS_SEQ.nextval, " + calendarId + ", " +
			locationLevelId + ", " + locationId + ")");
		try {
			logger.debug("insertSAStatus: " + sql.toString());
			PristineDBUtil.executeUpdate(_conn, sql, "insertSAStatus");
			PristineDBUtil.commitTransaction(_conn,	"insertSAStatus");
		}
		catch (GeneralException e) {
			logger.error("Error While update Sales Aggr status:"	+ e.getMessage());
			throw new GeneralException("insertSAStatus", e);
		}
	}
	
	/*
	 * Method to insert daily data processed status
	 * Process added for repair process.
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id
	 * Argument 3 : Location Level Id
	 * Argument 4 : Location Id
	 * @throws GeneralException
	 */
	public void deleteSAStatus(Connection _conn, int calendarId, int locationLevelId, int locationId)
			throws GeneralException {

		//For Insert SQL
		StringBuffer sql = new StringBuffer();
		sql.append(" DELETE FROM SALES_AGGR_STATUS" + 
			" WHERE CALENDAR_ID =" + calendarId + 
			" AND LOCATION_LEVEL_ID = " + locationLevelId + 
			" AND LOCATION_ID = " + locationId);
		try {
			PristineDBUtil.executeUpdate(_conn, sql, "deleteSAStatus");
			PristineDBUtil.commitTransaction(_conn,	"deleteSAStatus");
		}
		catch (GeneralException e) {
			logger.error("Error While update Sales Aggr status:"	+ e.getMessage());
			throw new GeneralException("deleteSAStatus", e);
		}
	}
	
	/*
	 * Method to get non processed calendar Ids
	 * Argument 1 : Connection
	 * Argument 2 : Location Level Id
	 * Argument 3 : Location Id
	 * @throws GeneralException
	 */
	 public List<RetailCalendarDTO> getNotProcessedCalendar(Connection _conn, 
 		int locationLevelId, int locationId, String calType) throws GeneralException {
		
		 // Object for return calendar list
		 List<RetailCalendarDTO> returnList = new ArrayList<RetailCalendarDTO>();
			 
		 try {
			// Query
			 StringBuffer sql = new StringBuffer();
			 
			 sql.append(" SELECT DISTINCT SA.CALENDAR_ID, RC.START_DATE," +
				 " RCL.CALENDAR_ID AS LAST_CALENDAR_ID FROM SALES_AGGR_STATUS SA"); 
				 
			 if (locationLevelId == Constants.DISTRICT_LEVEL_ID)
				 sql.append(" JOIN COMPETITOR_STORE CS" + 
					 " ON CS.DISTRICT_ID = " + locationId +
					 " AND SA.LOCATION_ID = CS.COMP_STR_ID");
			 else if (locationLevelId == Constants.REGION_LEVEL_ID)	 
				 sql.append(" JOIN RETAIL_DISTRICT CS" + 
					 " ON CS.REGION_ID = " + locationId + 
					 " AND SA.LOCATION_ID = CS.ID");
			 else if (locationLevelId == Constants.DIVISION_LEVEL_ID)
				 sql.append(" JOIN RETAIL_REGION CS" + 
					 " ON CS.DIVISION_ID =  " + locationId + 
					 " AND SA.LOCATION_ID = CS.ID");				 
			 else if (locationLevelId == Constants.CHAIN_LEVEL_ID)
				 sql.append(" JOIN RETAIL_DIVISION CS" + 
					 " ON CS.CHAIN_ID = " + locationId + 
					 " AND SA.LOCATION_ID = CS.ID");
			 
			 //Get Details for Calendar IDs   
			 sql.append(" JOIN RETAIL_CALENDAR RC ON SA.CALENDAR_ID = RC.CALENDAR_ID");
			
			//Join to get last year calendar Id
			 if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
				sql.append(" JOIN RETAIL_CALENDAR RCL" +
				" ON RCL.ROW_TYPE = RC.ROW_TYPE " +
				" AND RC.START_DATE - 364 = RCL.START_DATE");
			 else
				 sql.append(" JOIN RETAIL_CALENDAR RCL" + 
				" ON RCL.CAL_YEAR = (RC.CAL_YEAR - 1)" + 
				" AND RCL.ROW_TYPE = RC.ROW_TYPE" + 
				" AND RCL.ACTUAL_NO = RC.ACTUAL_NO");			
			
			
			if (locationLevelId == Constants.DISTRICT_LEVEL_ID)
				sql.append(" WHERE SA.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID);
			else if (locationLevelId == Constants.REGION_LEVEL_ID)
				sql.append(" WHERE SA.LOCATION_LEVEL_ID = " + Constants.DISTRICT_LEVEL_ID);
			else if (locationLevelId == Constants.DIVISION_LEVEL_ID)
				sql.append(" WHERE SA.LOCATION_LEVEL_ID = " + Constants.REGION_LEVEL_ID);
			else if (locationLevelId == Constants.CHAIN_LEVEL_ID)
				sql.append(" WHERE SA.LOCATION_LEVEL_ID = " + Constants.DIVISION_LEVEL_ID);
				
			if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
				sql.append(" AND SA.DAILY_DATA = 'N'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				sql.append(" AND SA.WEEK_DATA = 'N'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
				sql.append(" AND SA.PERIOD_DATA = 'N'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
				sql.append(" AND SA.QUARTER_DATA = 'N'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR))
				sql.append(" AND SA.YEAR_DATA = 'N'");
			
			sql.append(" ORDER BY SA.CALENDAR_ID");
			 
			 logger.debug("GetNotProcessingCalendar SQL:" + sql.toString());
			
			 // Execute the query
			 CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getNotProcessingCalendar");

			 while(result.next()){
				 
				 RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
				 objCalendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
				 objCalendarDto.setStartDate(result.getString("START_DATE"));
				 objCalendarDto.setlstCalendarId(result.getInt("LAST_CALENDAR_ID"));
				 returnList.add(objCalendarDto);
			 }
			 
			 result.close();
		} catch (SQLException e) {
			logger.error(" Error While Fetching Not processing Calendar List: ", e);
			throw new GeneralException("Error in getNotProcessingCalendar " , e);
		} catch (GeneralException e) {
			logger.error(" Error While Fetching Not processing Calendar List: " , e);
			throw new GeneralException("Error in getNotProcessingCalendar " , e);
		}
		return returnList;
	}
	
		/*
		 * Method to get non processed calendar IDs for Week/Period/Quarter/Year
		 * Argument 1 : Connection
		 * Argument 2 : Location Level Id
		 * Argument 3 : Location Id
		 * @throws GeneralException
		 */
		 public List<PeriodCalendarDTO> getNotProcessedCalendarForWPQY(Connection _conn, 
	 		int locationLevelId, int locationId, String calType) throws GeneralException {
			
			 // Object for return calendar list
			 List<PeriodCalendarDTO> returnList = new ArrayList<PeriodCalendarDTO>();
				 
			 try {
				// Query
				 StringBuffer sql = new StringBuffer();
				 
				 sql.append("SELECT DISTINCT SA.CALENDAR_ID AS DAY_CALENDAR_ID," +
					 " TO_CHAR(RCD.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS PROCESSING_DATE," + 
					 " RCW.CALENDAR_ID AS WPQY_CALENDAR_ID," +
					 " RCW.ACTUAL_NO AS WPQY_NO," +
					 " TO_CHAR(RCW.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS WEEK_START_DATE," +
					 " TO_CHAR(RCW.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS WEEK_END_DATE," +
					 " RCWL.CALENDAR_ID AS LAST_WPQY_CALENDAR_ID" +
					 " FROM SALES_AGGR_STATUS SA");

	

				 // Join to get day level data based on calendar Id in SA Status
				 sql.append(" JOIN RETAIL_CALENDAR RCD ON" + 
						 " RCD.CALENDAR_ID = SA.CALENDAR_ID");
				 
				 // Get Week/Period/Quarter/Year in which day is belongs from
				 if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
					 sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'W'" +
						 " AND RCW.START_DATE <= RCD.START_DATE AND RCW.END_DATE >= RCD.START_DATE");				
				else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
					 sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'P'" +
							 " AND RCW.START_DATE <= RCD.START_DATE AND RCW.END_DATE >= RCD.START_DATE");				
				else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
					 sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'Q'" +
							 " AND RCW.START_DATE <= RCD.START_DATE AND RCW.END_DATE >= RCD.START_DATE");				
				else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR))
					 sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'Y'" +
							 " AND RCW.START_DATE <= RCD.START_DATE AND RCW.END_DATE >= RCD.START_DATE");				

				//Join to get Last Year Calendar Id for Week/Period/Quarter/Year
				 if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
					 sql.append(" JOIN RETAIL_CALENDAR RCWL");
					 sql.append(" ON RCW.ROW_TYPE = RCWL.ROW_TYPE");
					 sql.append(" AND RCW.START_DATE - 364 = RCWL.START_DATE ");
					 sql.append(" AND RCW.END_DATE - 364 = RCWL.END_DATE");
				 }
				 else{
					sql.append(" JOIN RETAIL_CALENDAR RCWL ON" + 
						" RCW.ROW_TYPE = RCWL.ROW_TYPE" + 
						" AND RCWL.CAL_YEAR = (RCW.CAL_YEAR - 1)" + 
						" AND RCW.ACTUAL_NO = RCWL.ACTUAL_NO");
				 }
				 
				//if (locationLevelId == Constants.STORE_LEVEL_ID)
					sql.append(" WHERE SA.LOCATION_LEVEL_ID = " + locationLevelId + 
						" AND SA.LOCATION_ID = " + locationId);
					
					if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
						 sql.append(" AND SA.WEEK_DATA = 'N'");				
					else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
						 sql.append(" AND SA.PERIOD_DATA = 'N'");				
					else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
						 sql.append(" AND SA.QUARTER_DATA = 'N'");				
					else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR))
						 sql.append(" AND SA.YEAR_DATA = 'N'");				
				 
					sql.append(" ORDER BY RCW.CALENDAR_ID, SA.CALENDAR_ID DESC");					
				 logger.debug("GetNotProcessingCalendar SQL:" + sql.toString());
				
				 // Execute the query
				 CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getNotProcessingCalendar");

				 while(result.next()){
					 PeriodCalendarDTO objCalendarDto = new PeriodCalendarDTO();
					 objCalendarDto.setPeriodCalendarId(result.getInt("WPQY_CALENDAR_ID"));
					 objCalendarDto.setPeriodStartDate(result.getString("WEEK_START_DATE"));
					 objCalendarDto.setPeriodEndDate(result.getString("WEEK_END_DATE"));
					 objCalendarDto.setActualNo(result.getInt("WPQY_NO"));
					 objCalendarDto.setLastPeriodCalendarId(result.getInt("LAST_WPQY_CALENDAR_ID"));
					 objCalendarDto.setDayCalendarId(result.getInt("DAY_CALENDAR_ID"));
					 objCalendarDto.setProcessingDate(result.getString("PROCESSING_DATE"));
					 objCalendarDto.setCalendarMode(calType);
					 returnList.add(objCalendarDto);
				 }
				 
				 result.close();
			} catch (SQLException e) {
				logger.error(" Error While Fetching Not processing Calendar List: ", e);
				throw new GeneralException("Error in getNotProcessingCalendar " , e);
			} catch (GeneralException e) {
				logger.error(" Error While Fetching Not processing Calendar List: " , e);
				throw new GeneralException("Error in getNotProcessingCalendar " , e);
			}
			return returnList;
		}	 
	 
	 

		/*
		 * Update SA Status
		 * Argument 1 : Connection
		 * Argument 2 : Location Level Id
		 * Argument 3 : Location Id
		 * Argument 4 : Calendar Id
		 * @throws GeneralException
		 */
		 public void updateSAStatus(Connection _conn, 
				 int calendarId, int locationLevelId, int locationId, String calType) throws GeneralException {
			
			// Query
			 StringBuffer sql = new StringBuffer();
				 
			 sql.append("UPDATE SALES_AGGR_STATUS SET");

			if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
				sql.append(" DAILY_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				sql.append(" WEEK_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
				sql.append(" PERIOD_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
				sql.append(" QUARTER_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR))
				sql.append(" YEAR_DATA = 'Y'");				 
				 
			sql.append(" WHERE CALENDAR_ID     = " + calendarId);

			if (locationLevelId == Constants.STORE_LEVEL_ID){
				sql.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID +
					" AND LOCATION_ID = " + locationId);
			}
			else if (locationLevelId == Constants.DISTRICT_LEVEL_ID){
				if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
					sql.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID +
						" AND LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE" + 
						" WHERE" + " DISTRICT_ID = " + locationId +")");
				else
					sql.append(" AND LOCATION_LEVEL_ID = " + locationLevelId +
						" AND LOCATION_ID = " + locationId);					
			}
			else if (locationLevelId == Constants.REGION_LEVEL_ID){
				if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
					sql.append(" AND LOCATION_LEVEL_ID = " + Constants.DISTRICT_LEVEL_ID +
						" AND LOCATION_ID IN (SELECT ID FROM RETAIL_DISTRICT" + 
						" WHERE" + " REGION_ID = " + locationId +")");
				else
					sql.append(" AND LOCATION_LEVEL_ID = " + locationLevelId +
						" AND LOCATION_ID = " + locationId);					
			}
			else if (locationLevelId == Constants.DIVISION_LEVEL_ID){
				if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
					sql.append(" AND LOCATION_LEVEL_ID = " + Constants.REGION_LEVEL_ID +
						" AND LOCATION_ID IN (SELECT ID FROM RETAIL_REGION" + 
						" WHERE" + " DIVISION_ID = " + locationId +")");
				else
					sql.append(" AND LOCATION_LEVEL_ID = " + locationLevelId +
						" AND LOCATION_ID = " + locationId);					
			}
			else if (locationLevelId == Constants.CHAIN_LEVEL_ID){
				if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
					sql.append(" AND LOCATION_LEVEL_ID = " + Constants.DIVISION_LEVEL_ID +
						" AND LOCATION_ID IN (SELECT ID FROM RETAIL_DIVISION" + 
						" WHERE" + " CHAIN_ID = " + locationId +")");
				else
					sql.append(" AND LOCATION_LEVEL_ID = " + locationLevelId +
						" AND LOCATION_ID = " + locationId);					
			}
				 
				 logger.debug("updateSAStatus SQL:" + sql.toString());
				
			try {
				PristineDBUtil.executeUpdate(_conn, sql, "updateSAStatus");
				PristineDBUtil.commitTransaction(_conn,	"updateSAStatus");
			}
			catch (GeneralException e) {
				logger.error("Error While update Sales Aggr status:"	+ e.getMessage());
				throw new GeneralException("updateSAStatus", e);
			}
		}
		
	 public List<RetailCalendarDTO> getHouseholdProcessingCalendarData(
			 	Connection _conn, int locationLevelId, int locationId, 
		 String calType, String processType, Date startDate, Date endDate) throws GeneralException {
					
		 // Object for return calendar list
		 List<RetailCalendarDTO> returnList = new ArrayList<RetailCalendarDTO>();
						 
		 try {
			 // Query
			 StringBuffer sql = new StringBuffer();
						 
			 sql.append("SELECT DISTINCT CALENDAR_ID, START_DATE,");
			 sql.append(" END_DATE FROM (");
			 
			 if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY)){
				 sql.append(" SELECT RCD.CALENDAR_ID,");
				 sql.append(" TO_CHAR(RCD.START_DATE,'").append(Constants.DB_DATE_FORMAT).append("') AS START_DATE,");
				 sql.append(" TO_CHAR(RCD.END_DATE,'").append(Constants.DB_DATE_FORMAT).append("') AS END_DATE");
			 }
			 else {
				 sql.append(" SELECT RCR.CALENDAR_ID,");
				 sql.append(" TO_CHAR(RCR.START_DATE,'").append(Constants.DB_DATE_FORMAT).append("') AS START_DATE,");
				 sql.append(" TO_CHAR(RCR.END_DATE,'").append(Constants.DB_DATE_FORMAT).append("') AS END_DATE");
			 }

			 sql.append(" FROM SALES_AGGR_STATUS SA");
			 sql.append(" JOIN RETAIL_CALENDAR RCD ON RCD.CALENDAR_ID = SA.CALENDAR_ID");
			 
			 if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_NORMAL)){
				 sql.append(" AND RCD.START_DATE >= to_date('").append(formatter.format(startDate)).append("','dd-MM-yyyy')");
				 sql.append(" AND RCD.START_DATE <= to_date('").append(formatter.format(endDate)).append("','dd-MM-yyyy')");
			 }
			 
			 if (!calType.equalsIgnoreCase(Constants.CALENDAR_DAY)){
				 sql.append(" JOIN RETAIL_CALENDAR RCR");
				 sql.append(" ON RCR.ROW_TYPE = '").append(calType).append("'");
				 sql.append(" AND RCR.START_DATE <= RCD.START_DATE");
				 sql.append(" AND RCR.END_DATE >= RCD.START_DATE");
			 }

			 sql.append(" WHERE SA.LOCATION_LEVEL_ID = ").append(locationLevelId);
			 sql.append(" AND SA.LOCATION_ID = ").append(locationId);

			 if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY)){
				 sql.append(" AND SA.DAILY_DATA = 'Y'");
				 
				 if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_FULL))
					 sql.append(" AND SA.DAILY_HOUSEHOLD_DATA = 'N'");
			 }
			 else if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
				 sql.append(" AND SA.WEEK_DATA = 'Y'");

				 if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_FULL))
					 sql.append(" AND SA.WEEK_HOUSEHOLD_DATA = 'N'");
			 }
			 else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD)){
				 sql.append(" AND SA.PERIOD_DATA = 'Y'");
				
				 if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_FULL))
					 sql.append(" AND SA.PERIOD_HOUSEHOLD_DATA = 'N'");
			 }
			 else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER)){
				 sql.append(" AND SA.QUARTER_DATA = 'Y'");
				
				 if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_FULL))
					 sql.append(" AND SA.QUARTER_HOUSEHOLD_DATA = 'N'");
			 }
			 else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR)){
				 sql.append(" AND SA.YEAR_DATA = 'Y'");				
				
				 if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_FULL))
					 sql.append(" AND SA.YEAR_HOUSEHOLD_DATA = 'N'");
			 }
				 
			 sql.append(" ) ORDER BY CALENDAR_ID");						 

			 logger.debug("getHouseholdProcessingCalendarData SQL:" + sql.toString());
						
			 // Execute the query
			 CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getHouseholdProcessingCalendarData");

			 while(result.next()){
				 RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
				 objCalendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
				 objCalendarDto.setStartDate(result.getString("START_DATE"));
				 if (result.getObject("END_DATE") != null)
					 objCalendarDto.setEndDate(result.getString("END_DATE"));
				 
				 returnList.add(objCalendarDto);
			 }
						 
			 result.close();
		} catch (SQLException e) {
			logger.error(" Error While Fetching household processing Calendar List: ", e);
			throw new GeneralException("Error in getHouseholdProcessingCalendarData " , e);
		} catch (GeneralException e) {
			logger.error(" Error While Fetching Not processing Calendar List: " , e);
			throw new GeneralException("Error in getHouseholdProcessingCalendarData " , e);
		}

		return returnList;
	}	 

		/*
		 * Update SA Status
		 * Argument 1 : Connection
		 * Argument 2 : Location Level Id
		 * Argument 3 : Location Id
		 * Argument 4 : Calendar Id
		 * @throws GeneralException
		 */
		 public void updateSAStatusForHouseholdData(Connection _conn, 
				 int calendarId, int locationLevelId, int locationId, String calType) throws GeneralException {
			
			// Query
			 StringBuffer sql = new StringBuffer();
				 
			 sql.append("UPDATE SALES_AGGR_STATUS SET");

			if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
				sql.append(" DAILY_HOUSEHOLD_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				sql.append(" WEEK_HOUSEHOLD_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
				sql.append(" PERIOD_HOUSEHOLD_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
				sql.append(" QUARTER_HOUSEHOLD_DATA = 'Y'");
			else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR))
				sql.append(" YEAR_HOUSEHOLD_DATA = 'Y'");				 
			
			if (calType.equalsIgnoreCase(Constants.CALENDAR_DAY))
				sql.append(" WHERE CALENDAR_ID = ").append(calendarId);
			else{
				sql.append(" WHERE CALENDAR_ID IN (");
				sql.append(" SELECT RCD.CALENDAR_ID FROM RETAIL_CALENDAR RCD");
				sql.append(" JOIN RETAIL_CALENDAR RCR ON");
				sql.append(" RCR.ROW_TYPE ='").append(calType).append("'"); 
				sql.append(" AND RCD.START_DATE >= RCR.START_DATE"); 
				sql.append(" AND RCD.START_DATE <= RCR.END_DATE )");
			}
			
			sql.append(" AND LOCATION_LEVEL_ID = ").append(locationLevelId);
			sql.append(" AND LOCATION_ID = ").append(locationId);
				 
			 logger.debug("updateSAStatusForHouseholdData SQL:" + sql.toString());
				
			try {
				PristineDBUtil.executeUpdate(_conn, sql, "updateSAStatus");
			}
			catch (GeneralException e) {
				logger.error("Error While update Sales Aggr household status:"	+ e.getMessage());
				throw new GeneralException("updateSAStatusForHouseholdData", e);
			}
		}

}
