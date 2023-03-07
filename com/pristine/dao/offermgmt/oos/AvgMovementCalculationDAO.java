package com.pristine.dao.offermgmt.oos;

import java.sql.Connection;
import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.sql.SQLException;
//import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.LocationKey;
//import com.pristine.dto.offermgmt.WeeklyAvgRevenue;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.service.offermgmt.oos.OOSService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class AvgMovementCalculationDAO implements IDAO {
	private static Logger logger = Logger.getLogger("AvgRevenueCalculationDAO");

//	private static final String INSERT_LOCATION_AVG_REV = "INSERT INTO LOCATION_AVERAGE_REVENUE (LOCATION_LEVEL_ID, LOCATION_ID, " +
//			" PRODUCT_LEVEL_ID, PRODUCT_ID, WEEKLY_AVG_REVENUE) "
//			+ " VALUES(?, ?, ?)";
//
//	private static final String INSERT_DAY_PART_AVG_REV = "INSERT INTO DAY_PART_AVERAGE_REVENUE (LOCATION_LEVEL_ID, LOCATION_ID, " +
//			" PRODUCT_LEVEL_ID, PRODUCT_ID, DAY_ID, DAY_PART_ID, DAY_PART_AVG_REVENUE) "
//			+ " VALUES(?, ?, ?, ?, ?)";

	private static final String DELETE_WEEKLY_AVERAGE_MOVEMENT = "DELETE FROM OOS_WEEKLY_AVERAGE_MOVEMENT WHERE " +
			" LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? ";

	private static final String DELETE_DAY_PART_AVERAGE_MOVEMENT = "DELETE FROM OOS_DAY_PART_AVERAGE_MOVEMENT WHERE " +
			" LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? ";
	
//	private static final String STORE_AVG_REV = 
//			"SELECT TL.ITEM_ID, ROUND(SUM(NET_AMT) / ?) AS AVG_REVENUE FROM TRANSACTION_LOG TL " +
//			//Join RETAIL_CALENDAR to get the date of transaction
//			" LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID " +
//			" WHERE TL.STORE_ID = ? AND " +
//			//Go X weeks back
//			" (RC.START_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) AND  " +
//			//Ignore current week and end with last week last date
//			" RC.START_DATE <= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1)) " +
//			//For debugging
//			" AND TL.ITEM_ID =91" +
//			" GROUP BY TL.ITEM_ID " ;
//	
//	private static final String DAY_PART_AVG_REV =
//			" SELECT DAY_ID, DAY_PART_ID, ITEM_ID, ROUND(ROUND(SUM(NET_AMT)) / ?) AS AVG_REVENUE FROM (SELECT " +
//			" TL.ITEM_ID, TL.NET_AMT, " + 
//			//Day Part case used to group by time slot
//			" %DAY_PART_CASE% ," +
//			//Day Case used to group by day 
//			" %DAY_CASE% " +
//			" FROM TRANSACTION_LOG TL " +
//			//Join RETAIL_CALENDAR to get the date of transaction
//			" LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID " +
//			" WHERE TL.STORE_ID = ?	AND " +
//			//Go X weeks back
//			" (RC.START_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) AND " +
//			//Ignore current week and end with last week last date
//			" RC.START_DATE <= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1))) " +
//			" GROUP BY DAY_ID, DAY_PART_ID, ITEM_ID " ;
	
	private static final String GET_AND_INSERT_STORE_AVG_MOV = 
			"INSERT INTO OOS_WEEKLY_AVERAGE_MOVEMENT  (LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, WEEKLY_AVG_MOV) " + 
			" SELECT ? AS LOCATION_LEVEL_ID, ? AS LOCATION_ID, ? AS PRODUCT_LEVEL_ID, ITEM_ID AS PRODUCT_ID, AVG_MOV FROM ( " +
			" SELECT ITEM_ID, ROUND((SUM(MOV) / ?), 2) AS AVG_MOV FROM " +
			" (SELECT TL.ITEM_ID, " +
			" CASE WHEN QUANTITY IS NOT NULL AND QUANTITY > 0 THEN QUANTITY " +
		    " WHEN WEIGHT IS NOT NULL AND WEIGHT > 0 THEN 1 ELSE 0 " +
		    " END AS MOV " +
			" FROM TRANSACTION_LOG_T1 TL " +
			//Join RETAIL_CALENDAR to get the date of transaction
			" LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID " +
			" WHERE TL.STORE_ID = ? AND " +
			//Go X weeks back
			" (RC.START_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) AND  " +
			//Ignore current week and end with last week last date
			" RC.START_DATE <= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1))) " +
			//For debugging
			//" AND TL.ITEM_ID =91" +
			" GROUP BY ITEM_ID )" ;
	
	private static final String GET_AND_INSERT_DAY_PART_AVG_MOV =
			" INSERT INTO OOS_DAY_PART_AVERAGE_MOVEMENT  (LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, " +
			" PRODUCT_ID, DAY_ID, DAY_PART_ID, DAY_PART_AVG_MOV) SELECT ? AS LOCATION_LEVEL_ID, ? AS LOCATION_ID, " +
			" ? AS PRODUCT_LEVEL_ID, ITEM_ID AS PRODUCT_ID,	DAY_ID, DAY_PART_ID, AVG_MOV FROM " +	
			" (SELECT DAY_ID, DAY_PART_ID, ITEM_ID,  ROUND((SUM(MOV) / ?), 2) AS AVG_MOV  FROM (SELECT " +
			" TO_CHAR(TO_DATE(NEW_TRX_DATE), 'D') AS DAY_ID, DAY_PART_ID, ITEM_ID, MOV FROM " +
			" (SELECT TL.ITEM_ID, " +
			" CASE WHEN QUANTITY IS NOT NULL AND QUANTITY > 0 THEN QUANTITY " +
		    " WHEN WEIGHT IS NOT NULL AND WEIGHT > 0 THEN 1 ELSE 0 " +
		    " END AS MOV, " +
			//Day Part case used to group by time slot
			" %DAY_PART_CASE% ," +
			//Day Case used to group by day 
			" %DAY_CASE% " +
			" FROM TRANSACTION_LOG_T1 TL " +
			//Join RETAIL_CALENDAR to get the date of transaction
			" LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID " +
			" WHERE TL.STORE_ID = ?	AND " +
			//Go X weeks back
			" (RC.START_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) AND " +
			//Ignore current week and end with last week last date
			" RC.START_DATE <= TRUNC((SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1) + 1))) " +
			//Condition added to ignore transaction happened on first week time slot which spans day
			" WHERE NEW_TRX_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) " +
			//Condition added to add transaction happened on last week next day time slot which spans day
			" AND NEW_TRX_DATE   <= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1)) " +
			" GROUP BY DAY_ID, DAY_PART_ID, ITEM_ID) " ;
	/**
	 * Batch Insert to WEEKLY_AVERAGE_REVENUE
	 * @param conn
	 * @param weeklyAvgRevenue
	 * @throws Exception
	 */
//	public void insertLocationLevelAvgRevenue(Connection conn, List<WeeklyAvgRevenue> weeklyAvgRevenue)
//			throws Exception {
//		PreparedStatement stmt = null;
//		try {
//			stmt = conn.prepareStatement(INSERT_LOCATION_AVG_REV);
//			int insertCount = 0;
//			for (WeeklyAvgRevenue weekAvgRev : weeklyAvgRevenue) {
//				int counter = 0;
//
//				stmt.setInt(++counter, weekAvgRev.getLocationLevelId());
//				stmt.setInt(++counter, weekAvgRev.getLocationId());
//				stmt.setInt(++counter, weekAvgRev.getProductLevelId());
//				stmt.setInt(++counter, weekAvgRev.getProductId());
//				stmt.setLong(++counter, weekAvgRev.getAverageRevenue());
//
//				stmt.addBatch();
//				insertCount++;
//
//				if (insertCount % Constants.BATCH_UPDATE_COUNT == 0) {
//					stmt.executeBatch();
//					stmt.clearBatch();
//					insertCount = 0;
//				}
//			}
//			if (insertCount > 0) {
//				stmt.executeBatch();
//				stmt.clearBatch();
//			}
//		} catch (SQLException ex) {
//			logger.error("Error in insertLocationLevelAvgRevenue() -- " + ex.toString(), ex);
//			throw new Exception();
//		} finally {
//			PristineDBUtil.close(stmt);
//		}
//	}

	/***
	 * Batch Insert to DAY_PART_AVERAGE_REVENUE table
	 * @param conn
	 * @param weeklyAvgRevenue
	 * @throws Exception
	 */
//	public void insertDayPartAvgRevenue(Connection conn, List<WeeklyAvgRevenue> weeklyAvgRevenue) throws Exception {
//		PreparedStatement stmt = null;
//		try {
//			stmt = conn.prepareStatement(INSERT_DAY_PART_AVG_REV);
//			int insertCount = 0;
//			for (WeeklyAvgRevenue weekAvgRev : weeklyAvgRevenue) {
//				int counter = 0;
//
//				stmt.setInt(++counter, weekAvgRev.getLocationLevelId());
//				stmt.setInt(++counter, weekAvgRev.getLocationId());
//				stmt.setInt(++counter, weekAvgRev.getProductLevelId());
//				stmt.setInt(++counter, weekAvgRev.getProductId());
//				stmt.setInt(++counter, weekAvgRev.getDayId());
//				stmt.setInt(++counter, weekAvgRev.getDayPartId());
//				stmt.setLong(++counter, weekAvgRev.getAverageRevenue());
//
//				stmt.addBatch();
//				insertCount++;
//
//				if (insertCount % Constants.BATCH_UPDATE_COUNT == 0) {
//					stmt.executeBatch();
//					stmt.clearBatch();
//					insertCount = 0;
//				}
//			}
//			if (insertCount > 0) {
//				stmt.executeBatch();
//				stmt.clearBatch();
//			}
//		} catch (SQLException ex) {
//			logger.error("Error in insertDayPartAvgRevenue() -- " + ex.toString(), ex);
//			throw new Exception();
//		} finally {
//			PristineDBUtil.close(stmt);
//		}
//	}

	/***
	 * Get and insert average movement of each item for each time slot in a day
	 * @param conn
	 * @param dayPartLookup
	 * @param noOfWeeksHistory
	 * @param locationId
	 * @throws Exception
	 */
	public void getAndInsertItemLevelDayPartAvgMovement(Connection conn, List<DayPartLookupDTO> dayPartLookup,
			int noOfWeeksHistory, int locationId) throws Exception {
		PreparedStatement stmt = null;
		try {
			OOSService oosService = new OOSService();
			String query = new String(GET_AND_INSERT_DAY_PART_AVG_MOV);	
			//Fill Day Part Case
			query = query.replaceAll("%DAY_PART_CASE%", oosService.fillDayPartCase(dayPartLookup));
			
			//Fill Day Case
			query = query.replaceAll("%DAY_CASE%", oosService.fillDayCase(dayPartLookup));
			
			stmt = conn.prepareStatement(query);
			int counter = 0;
			stmt.setInt(++counter, Constants.STORE_LEVEL_ID);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, Constants.ITEMLEVELID);
			stmt.setInt(++counter, noOfWeeksHistory);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, noOfWeeksHistory);
			stmt.setInt(++counter, noOfWeeksHistory);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			logger.error("Error in getAndInsertItemLevelDayPartAvgMovement() -- " + ex.toString(), ex);
			throw new Exception();
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void getAndInsertItemLevelStoreAvgMovement(Connection conn, int noOfWeeksHistory, int locationId)
			throws Exception {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(GET_AND_INSERT_STORE_AVG_MOV);
			int counter = 0;
			stmt.setInt(++counter, Constants.STORE_LEVEL_ID);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, Constants.ITEMLEVELID);
			stmt.setInt(++counter, noOfWeeksHistory);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, noOfWeeksHistory);
			stmt.executeUpdate();
		} catch (SQLException ex) {
			logger.error("Error in getAndInsertItemLevelStoreAvgMovement() -- " + ex.toString(), ex);
			throw new Exception();
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	/***
	 * Get weekly average revenue of all the items in a store
	 * @param conn
	 * @param noOfWeeksHistory
	 * @param storeId
	 * @return
	 * @throws Exception
	 */
//	public List<WeeklyAvgRevenue> getStoreAvgRevenue(Connection conn, int noOfWeeksHistory, int storeId) throws Exception{
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		List<WeeklyAvgRevenue> weeklyAvgRevenue =  new ArrayList<WeeklyAvgRevenue>();
//		
//		try{
//			stmt = conn.prepareStatement(STORE_AVG_REV);
//			stmt.setInt(1, noOfWeeksHistory);
//			stmt.setInt(2, storeId);
//			stmt.setInt(3, noOfWeeksHistory);
//			rs = stmt.executeQuery();
//			while(rs.next()){
//				WeeklyAvgRevenue war = new WeeklyAvgRevenue();
//				war.setLocationLevelId(Constants.STORE_LEVEL_ID);
//				war.setLocationId(storeId);
//				war.setProductLevelId(Constants.ITEMLEVELID);
//				war.setProductId(rs.getInt("ITEM_ID"));
//				war.setAverageRevenue(rs.getLong("AVG_REVENUE"));
//			}
//		}catch(SQLException ex){
//			logger.error("Error in getStoreAvgRevenue() -- " + ex.toString(), ex);
//			throw new Exception();
//		}finally{
//			PristineDBUtil.close(rs);
//			PristineDBUtil.close(stmt);
//		}
//		return weeklyAvgRevenue;
//	}
	
	/***
	 * Get average revenue for all the items in each time slot of each day for a store
	 * @param conn
	 * @param noOfWeeksHistory
	 * @param storeId
	 * @return
	 * @throws Exception
	 */
//	public List<WeeklyAvgRevenue> getStoreDayPartAvgRevenue(Connection conn, List<DayPartLookupDTO> dayPartLookup, 
//			int noOfWeeksHistory, int storeId) throws Exception{
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		List<WeeklyAvgRevenue> dayPartAvgRev = new ArrayList<WeeklyAvgRevenue>();
//		try{
//			String query = new String(DAY_PART_AVG_REV);	
//			//Fill Day Part Case
//			query = query.replaceAll("%DAY_PART_CASE%", fillDayPartCase(dayPartLookup));
//			
//			//Fill Day Case
//			query = query.replaceAll("%DAY_CASE%", fillDayCase(dayPartLookup));
//			
//			stmt = conn.prepareStatement(query);
//			stmt.setInt(1, noOfWeeksHistory);
//			stmt.setInt(2, storeId);
//			stmt.setInt(3, noOfWeeksHistory);
//			rs = stmt.executeQuery();
//			while(rs.next()){
//				WeeklyAvgRevenue weeklyAvgRev = new WeeklyAvgRevenue();
//				weeklyAvgRev.setLocationLevelId(Constants.STORE_LEVEL_ID);
//				weeklyAvgRev.setLocationId(storeId);
//				weeklyAvgRev.setProductLevelId(Constants.ITEMLEVELID);
//				weeklyAvgRev.setProductId(rs.getInt("ITEM_ID"));
//				weeklyAvgRev.setDayId(rs.getInt("DAY_ID"));
//				weeklyAvgRev.setDayPartId(rs.getInt("DAY_PART_ID"));
//				weeklyAvgRev.setAverageRevenue(rs.getLong("AVG_REVENUE"));
//				dayPartAvgRev.add(weeklyAvgRev);				
//			}
//		}catch(SQLException ex){
//			logger.error("Error in getStoreDayPartAvgRevenue() -- " + ex.toString(), ex);
//			throw new Exception();
//		}finally{
//			PristineDBUtil.close(rs);
//			PristineDBUtil.close(stmt);
//		}
//		return dayPartAvgRev;
//	}
	
	/***
	 * Delete records from LOCATION_AVERAGE_REVENUE table
	 * @param conn
	 * @param locations
	 * @return
	 * @throws Exception
	 */
	public int deleteWeeklyAverageMovement(Connection conn, List<LocationKey> locations) throws Exception{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		
		try{
			stmt = conn.prepareStatement(DELETE_WEEKLY_AVERAGE_MOVEMENT);
			for(LocationKey locationKey: locations){
				int counter = 0;
				stmt.setInt(++counter, locationKey.getLocationLevelId());
				stmt.setInt(++counter, locationKey.getLocationId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException exception){
			logger.error("Error in deleteWeeklyAverageMovement() - " + exception);
			throw new Exception();
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	/***
	 * Delete records from DELETE_DAY_PART_AVERAGE_REVENUE table
	 * @param conn
	 * @param locations
	 * @return
	 * @throws Exception
	 */
	public int deleteDayPartAverageMovement(Connection conn, List<LocationKey> locations) throws Exception{
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;
		
		try{
			stmt = conn.prepareStatement(DELETE_DAY_PART_AVERAGE_MOVEMENT);
			for(LocationKey locationKey: locations){
				int counter = 0;
				stmt.setInt(++counter, locationKey.getLocationLevelId());
				stmt.setInt(++counter, locationKey.getLocationId());
				stmt.addBatch();
			}
			count = stmt.executeBatch();
		}catch(SQLException exception){
			logger.error("Error in deleteDayPartAverageMovement() - " + exception);
			throw new Exception();
		}finally{
			PristineDBUtil.close(stmt);
		}
		if(count != null)
			deleteCount = count.length;
		
		return deleteCount;
	}
	
	
	
	/***
	 * From Day Case based on Day Part Lookup from the table
	 * @param dayPartLookup
	 * @return
	 */
//	private String fillDayCase(List<DayPartLookupDTO> dayPartLookup){
//		String dayCase = "";
//		boolean isThereTimeSlotSpanDays = false;
//		String endTimeOfSlotSpanDays = "";
//		//Check if there is time slot spans across day
//		for (DayPartLookupDTO dayPart : dayPartLookup) {
//			if (dayPart.isSlotSpanDays()) {
//				isThereTimeSlotSpanDays = true;
//				endTimeOfSlotSpanDays = dayPart.getEndTime();
//				break;
//			}
//		}
//		
//		//If a time spans across days, then consider the other days as day where the start time started
//		//e.g. if the time slot is 21:00 -- 07:00 and assume transaction happened on Monday 21:25 and another on 
//		//Tuesday 01:35, then the transaction happened on 01:35 will go under the day Monday
//		//While doing this shifting to previous day must be handled properly when the shift happens on Monday
//		//it should go to Sunday
//		if(isThereTimeSlotSpanDays){
//			dayCase = dayCase + " CASE ";
//			dayCase = dayCase + " WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '00:00:00' ";
//			dayCase = dayCase + " AND TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '" + endTimeOfSlotSpanDays + ":00') THEN ";
//			dayCase = dayCase + " CASE WHEN TO_CHAR(TRX_TIME, 'D') - 1 = 0 THEN 7 ELSE TO_CHAR(TRX_TIME, 'D') - 1 END ";
//			//If on week start date, then shift to last day of the week
//			dayCase = dayCase + " ELSE TO_CHAR(TRX_TIME, 'D') - 0 END AS DAY_ID ";
//		} else {
//			dayCase = dayCase + " TO_CHAR(TRX_TIME, 'D') - 0 AS DAY_ID";
//		}
//		return dayCase;			
//	}
	
}
