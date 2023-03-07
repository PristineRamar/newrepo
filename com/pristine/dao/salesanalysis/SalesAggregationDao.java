package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;

import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.HouseholdDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationDao {
	
	static Logger logger = Logger.getLogger("SalesAggregationDao");
	
	/*
	 * ****************************************************************
	 * Delete the Previous Aggregation  
	* Arguemnt 1  : Connection
	 * Argument 2  : Period Calendar Id
	 * Argument 3  : Location Id
	 * @catch GeneralException
	 * ****************************************************************
	 */


	public void deletePreviousSalesAggregation(Connection _conn,
			int calendarId, int locationId, int locationLevelId,
			String tableName) throws GeneralException {
		try{
		StringBuffer sql = new StringBuffer();
		sql.append(" Delete from " + tableName + " where");
		sql.append(" CALENDAR_ID='" + calendarId + "' and LOCATION_ID='"
				+ locationId + "'  ");
		sql.append(" and LOCATION_LEVEL_ID = '" + locationLevelId + "'");

		logger.debug("deletePreviousSalesAggregation SQ:" + sql.toString());
		PristineDBUtil.executeUpdate(_conn, sql, "deletePreviousAggregation");
		}catch( Exception exe){
			logger.error(" Error while deleting the records...." , exe);
			throw new GeneralException(" Error while deleting the records...." , exe);
		}

	}
	
	/*
	 * ****************************************************************
	 * Get Last Aggregation Date from Sales_Aggr_Weekly Records 
	 * Argument 1 :  _conn 
	 * Argument 2 : weekCalendarId 
	 * Records LastAggregated Calendar Id
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	public int getLastAggerCalendarId(Connection _conn, int weekCalendarId,
			int locationId, int locationLevelId, String tableName) throws GeneralException {

		int lastCalendarId = 0;
		try{
		StringBuffer sql = new StringBuffer();
		sql.append(" select distinct LAST_AGGR_CALENDARID from " + tableName+ " ");
		sql.append(" where CALENDAR_ID=" + weekCalendarId + " ");
		sql.append(" and LOCATION_ID='" + locationId + "'");
		sql.append(" and LOCATION_LEVEL_ID=" + locationLevelId + " ");
		sql.append(" and PRODUCT_LEVEL_ID IS NULL");

		logger.debug("getLastAggerCalendarId SQL:" + sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getLastAggerCalendarId");
			if (result.next()) {
				lastCalendarId = result.getInt("LAST_AGGR_CALENDARID");
			}
		}catch(Exception exe){
			logger.error(" Error while Fetching Last aggregation Calendar Id.... " + exe);
			throw new GeneralException(" Error while Fetching Last aggregation Calendar Id.... " + exe);
		}
		
		
		return lastCalendarId;
	}
	
	
	/* Method comnmon for week ,weeklyrollup, period ,quarter ,year .
	 * ****************************************************************
	 * get the previous aggregation records for given mode 
	 * Argument 1 : Connection
	 * Argument 2 : Week Calendar Id
	 * Argument 3 : Location Id
	 * Returns List<SummaryDataDTO> Weekly List
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */

	public HashMap<String, SummaryDataDTO> getSalesAggregation(
			Connection _conn, int weekCalendarId, int locationId,
			String tableName)
			throws GeneralException {

		HashMap<String, SummaryDataDTO> weekMap = new HashMap<String, SummaryDataDTO>();

		try {
			StringBuffer sql = new StringBuffer();
			
			sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID ,SA.LOCATION_ID");
			sql.append(" ,SA.TOT_VISIT_CNT as TOT_VISIT_CNT");
			sql.append(" ,SA.TOT_MOVEMENT as TOT_MOVEMENT");
			sql.append(" ,SA.REG_MOVEMENT as REG_MOVEMENT");
			sql.append(" ,SA.SALE_MOVEMENT as SALE_MOVEMENT");
			sql.append(" ,SA.REG_REVENUE as REG_REVENUE");
			sql.append(" ,SA.SALE_REVENUE as SALE_REVENUE");
			sql.append(" ,SA.TOT_MARGIN as TOT_MARGIN");
			sql.append(" ,SA.REG_MARGIN as REG_MARGIN");
			sql.append(" ,SA.SALE_MARGIN as SALE_MARGIN");
			sql.append(" ,SA.TOT_REVENUE AS TOT_REVENUE");

			// code added for movement by volume
			sql.append(" ,REG_MOVEMENT_VOL as REG_MOVEMENT_VOL");
			sql.append(" ,SALE_MOVEMENT_VOL as SALE_MOVEMENT_VOL");
			sql.append(" ,REG_IGVOL_REVENUE as REG_IGVOL_REVENUE");
			sql.append(" ,SALE_IGVOL_REVENUE as SALE_IGVOL_REVENUE");
			sql.append(" ,TOT_MOVEMENT_VOL as TOT_MOVEMENT_VOL");
			sql.append(" ,TOT_IGVOL_REVENUE as TOT_IGVOL_REVENUE");
			sql.append(" ,STORE_TYPE");
			
			if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY"))
				sql.append(" ,SUMMARY_CTD_ID");

			sql.append(" from");
			sql.append(" " + tableName + " SA");
			sql.append(" where SA.LOCATION_ID=").append(locationId);

			sql.append("  AND SA.CALENDAR_ID =" + weekCalendarId + "");
			
			sql.append(" order by SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID,SA.LOCATION_ID");

			logger.debug("getSalesAggregation SQL:" + sql.toString());

			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"getWeeklyAggregation");
			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				if (resultSet.getString("PRODUCT_LEVEL_ID") == null)
					summaryDto.setProductLevelId(0);
				else
					summaryDto.setProductLevelId(resultSet
							.getInt("PRODUCT_LEVEL_ID"));
				summaryDto.setProductId(resultSet.getString("PRODUCT_ID"));
				summaryDto.setTotalVisitCount(resultSet
						.getDouble("TOT_VISIT_CNT"));
				summaryDto
						.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				summaryDto.setRegularMovement(resultSet
						.getDouble("REG_MOVEMENT"));
				summaryDto
						.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				summaryDto
						.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
				summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
				summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
				summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
				summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));
				summaryDto.setLocationId(resultSet.getInt("LOCATION_ID"));

				// code added for Movement By Volume
				summaryDto.setregMovementVolume(resultSet
						.getDouble("REG_MOVEMENT_VOL"));
				summaryDto.setsaleMovementVolume(resultSet
						.getDouble("SALE_MOVEMENT_VOL"));
				summaryDto.setigRegVolumeRev(resultSet
						.getDouble("REG_IGVOL_REVENUE"));
				summaryDto.setigSaleVolumeRev(resultSet
						.getDouble("SALE_IGVOL_REVENUE"));
				summaryDto.settotMovementVolume(resultSet
						.getDouble("TOT_MOVEMENT_VOL"));
				summaryDto.setigtotVolumeRev(resultSet
						.getDouble("TOT_IGVOL_REVENUE"));
				summaryDto.setStoreStatus(resultSet.getString("STORE_TYPE"));
				summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));

				if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY")) {
					summaryDto.setsummaryCtdId(resultSet
							.getInt("SUMMARY_CTD_ID"));
				}

				weekMap.put(
						summaryDto.getProductLevelId() + "_"
								+ summaryDto.getProductId(), summaryDto);

			}
		} catch (Exception exe) {
			logger.error("....Error in sales Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales Aggregation Dao....",
					exe);
		}

		return weekMap;
	}
	
	/*
	 * ****************************************************************
	 * Method to  insert the Store level aggregation records
	 * Argument 1 : Product HashMap
	 * Argument 2 : Connection
	 * Argument 3 : calendarId
	 * Argument 4 : storeId
	 * call the execute batch method
	 * ****************************************************************
	 */

	public void insertSalesAggr(Connection _conn,
			HashMap<String, SummaryDataDTO> dailyMap, int _CalendarId,
			int _salesCalendarId, String tableName,
			HashMap<String, Long> lastSummaryList, String store_Status) throws GeneralException {

		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		try{
		PreparedStatement weekPsmt = _conn.prepareStatement(processSql(tableName, "INSERT"));
	

		Object[] outerLoop = dailyMap.values().toArray();
		for (int ii = 0; ii < outerLoop.length; ii++) {
			SummaryDataDTO summaryDto = (SummaryDataDTO) outerLoop[ii];
			summaryDto.setcalendarId(_salesCalendarId);
			summaryDto.setlastAggrCalendarId(_CalendarId);
			
			// get last aggregation id....
			if (summaryDto.getProductLevelId() == 0){
			logger.debug("Store level:" + summaryDto.getProductLevelId());
			if (lastSummaryList.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+null)) {
					summaryDto.setlastAggrSalesId(lastSummaryList
							.get(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+null));
				}
			}
			else {
				logger.debug("Product level:" + summaryDto.getProductLevelId());
				if (lastSummaryList.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+summaryDto.getProductId())) {
					summaryDto.setlastAggrSalesId(lastSummaryList
							.get(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+summaryDto.getProductId()));
				}
			}

			// calculate the other metrics info
			businessLogic.CalculateMetrixInformation(summaryDto, true);
			// add the values into psmt batch
			addSqlBatch(summaryDto, true, weekPsmt, "INSERT_SALES_AGGR" , store_Status);
		}
		// Prepare the Store Aggregation records
		int[] wcount = weekPsmt.executeBatch();
		logger.debug("No of records inserted in Aggregation:" + wcount.length);
		
		weekPsmt.close();
		
		}catch(Exception exe){
			 logger.error("....Error in sales Aggregation Dao...." , exe);
			 throw new GeneralException(" Error in sales Aggregation Dao...." , exe);
		}

	}
	
	/*
	 * ****************************************************************
	 * Update the weekly data
	 * Argument 1 : Connection
	 * Argument 2 : Week Calendar Id
	 * Argument 3 : productMap
	 * Returns List<SummaryDataDTO> Weekly List
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	public void updateSalesAggr(Connection _conn,
			HashMap<String, SummaryDataDTO> productMap, int calendarId,
			int _salesCalendarId, String tableName , String processMode, String store_Status) throws SQLException, GeneralException {

		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		try{
		PreparedStatement updatePsmt = _conn.prepareStatement(processSql(tableName, "UPDATE"));
		PreparedStatement updateSPsmt = _conn.prepareStatement(processSql(tableName, "UPDATESTORE"));
		

		Object[] outerLoop = productMap.values().toArray();
		for (int ii = 0; ii < outerLoop.length; ii++) {
			SummaryDataDTO summaryDto = (SummaryDataDTO) outerLoop[ii];
			summaryDto.setcalendarId(_salesCalendarId);
			
			if( calendarId !=0){
				summaryDto.setlastAggrCalendarId(calendarId);
			}
				
			if( processMode.equalsIgnoreCase("ACTUAL"))
			// calculate the other metrics info
			businessLogic.CalculateMetrixInformation(summaryDto, true);
			
			if( store_Status == null){
				store_Status = summaryDto.getStoreStatus();
			}
			
			if( ii ==0){
				logger.debug(" Summary store status:" + summaryDto.getStoreStatus());
				logger.debug(" Store Status:" +store_Status);
			}
			
			if( store_Status !=null && summaryDto.getStoreStatus() !=null){
				
				if( store_Status.equalsIgnoreCase("M")){
					store_Status = "M";
				}
				else if( ! store_Status .equalsIgnoreCase(summaryDto.getStoreStatus())){
					store_Status  = "M";
				}

			}
			
			
				if (summaryDto.getProductLevelId() == 0) {
					logger.debug(" Product Id:" + summaryDto.getProductId()
							+ "--Product Level Id... "
							+ summaryDto.getProductLevelId());
					addSqlBatch(summaryDto, false, updateSPsmt,
							"UPDATE_SALES_AGGR" , store_Status);
				} else {

					addSqlBatch(summaryDto, true, updatePsmt,
							"UPDATE_SALES_AGGR" , store_Status);

				}
			
		}
		// Prepare the Store Aggregation records


		int[] count1 = updatePsmt.executeBatch();
		count1 = updateSPsmt.executeBatch();
		
		updatePsmt.close();
		updateSPsmt.close();
		logger.debug("No of records updated:" + (count1.length));
		}catch(Exception exe){
			 logger.error("....Error in sales Aggregation Dao...." , exe);
			 throw new GeneralException(" Error in sales Aggregation Dao...." , exe);
		}
	}
	
	
	/*
	 * ****************************************************************
	 * Summary Weekly insert and Update Sql
	 * Argument 1 : sqlmode
	 * returns Query
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	
	private String processSql(String tableName, String sqlMode) {

		StringBuffer sql = new StringBuffer();

		if (sqlMode.equalsIgnoreCase("INSERT")) {

			sql.append(" insert into "+ tableName+ " ("+ tableName+"_ID, CALENDAR_ID, LOCATION_LEVEL_ID");
			
			sql.append(" ,LOCATION_ID,PRODUCT_LEVEL_ID");
			
			sql.append(" ,PRODUCT_ID , TOT_VISIT_CNT ,AVG_ORDER_SIZE,TOT_MOVEMENT, TOT_REVENUE , REG_REVENUE  ");

			sql.append(" , SALE_REVENUE ,TOT_MARGIN ,REG_MARGIN , SALE_MARGIN  ");

			sql	.append(", TOT_MARGIN_PCT, REG_MARGIN_PCT , SALE_MARGIN_PCT,LAST_AGGR_CALENDARID");
			
			sql.append(" ,SALE_MOVEMENT,REG_MOVEMENT,LST_"+tableName+"_ID ");
			
			// code updated for Movement by Volume
			// 6 fields added newly
			sql.append(" ,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_MOVEMENT_VOL,REG_IGVOL_REVENUE");
			
			sql.append(" ,SALE_IGVOL_REVENUE,TOT_IGVOL_REVENUE , STORE_TYPE) ");
			
			sql.append(" values (" + tableName+ "_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?");
			
			sql.append(" ,?,?,?,?,?,?,?)");

		} else if (sqlMode.equalsIgnoreCase("UPDATE")) {

			sql.append(" update  " + tableName + " SET  ");

			sql.append(" TOT_VISIT_CNT=? ,AVG_ORDER_SIZE=?,TOT_MOVEMENT=?, TOT_REVENUE=? , REG_REVENUE=?  ");

			sql.append(" , SALE_REVENUE=? ,TOT_MARGIN=? ,REG_MARGIN=? , SALE_MARGIN=?  ");

			sql.append(", TOT_MARGIN_PCT=?, REG_MARGIN_PCT=? , SALE_MARGIN_PCT=?");
			
			sql.append(" ,LAST_AGGR_CALENDARID=?,SALE_MOVEMENT=?,REG_MOVEMENT=?  ");
			
			// code update for movement by volume
			// 6 columns added newly 
			sql.append(" ,REG_MOVEMENT_VOL=?,SALE_MOVEMENT_VOL=?,TOT_MOVEMENT_VOL=?,REG_IGVOL_REVENUE=?");
			
			sql.append(" ,SALE_IGVOL_REVENUE=?,TOT_IGVOL_REVENUE=?, STORE_TYPE=? ");
			

			sql.append(" where CALENDAR_ID=? and LOCATION_ID=? and PRODUCT_LEVEL_ID=? and PRODUCT_ID=? ");
		} 
		
		else if(sqlMode.equalsIgnoreCase("DELETE")){
			
			 sql.append(" delete from SALES_AGGR_WEEKLY  ");
			 sql.append(" where CALENDAR_ID = ? and  LOCATION_ID =?");
			 sql.append(" and PRODUCT_ID=? and PRODUCT_LEVEL_ID= ? ");
			
		}
		else if(sqlMode.equalsIgnoreCase("UPDATESTORE")){
			sql.append(" update  " + tableName + " SET  ");

			sql.append(" TOT_VISIT_CNT=? ,AVG_ORDER_SIZE=?,TOT_MOVEMENT=?, TOT_REVENUE=? , REG_REVENUE=?  ");

			sql.append(" , SALE_REVENUE=? ,TOT_MARGIN=? ,REG_MARGIN=? , SALE_MARGIN=?  ");

			sql.append(", TOT_MARGIN_PCT=?, REG_MARGIN_PCT=? , SALE_MARGIN_PCT=?");
			
			sql.append(" ,LAST_AGGR_CALENDARID=?,SALE_MOVEMENT=?,REG_MOVEMENT=?  ");
			
			// code update for movement by volume
			// 6 columns added newly 
			sql.append(" ,REG_MOVEMENT_VOL=?,SALE_MOVEMENT_VOL=?,TOT_MOVEMENT_VOL=?,REG_IGVOL_REVENUE=?");
			
			sql.append(" ,SALE_IGVOL_REVENUE=?,TOT_IGVOL_REVENUE=? , STORE_TYPE =? ");
			

			sql.append(" where CALENDAR_ID=? and LOCATION_ID=? and PRODUCT_LEVEL_ID is null  and PRODUCT_ID is null ");
		}
		

		logger.debug("Summary Weekly Process SQL:" + sql.toString());
		return sql.toString();
	}
	
	
	/*
	 * ****************************************************************
	 * Add the values into PreparedStatement  
	 * Argument 1 : summaryDto
	 * Argument 2 : mode of sql
	 * Argument 3 : PreparedStatement
	 * Argument 4 : table name
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	private void addSqlBatch(SummaryDataDTO summaryDto, boolean insertMode,
			PreparedStatement psmt, String tableName, String store_Status) throws GeneralException {
		try {
			if (tableName.equalsIgnoreCase("INSERT_SALES_AGGR")) {
				psmt.setObject(1, summaryDto.getcalendarId());
				psmt.setObject(2, Constants.STORE_LEVEL_ID);
				psmt.setObject(3, summaryDto.getLocationId());
				if( summaryDto.getProductLevelId() == 0)
					psmt.setNull(4, java.sql.Types.INTEGER);
				else
				psmt.setInt(4, summaryDto.getProductLevelId());
				psmt.setString(5, summaryDto.getProductId());
				psmt.setDouble(6, GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
				psmt.setDouble(7,GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));

				psmt.setDouble(8, GenericUtil.Round(summaryDto.getTotalMovement(), 2));
				psmt.setDouble(9, GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
				psmt.setDouble(10, GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
				psmt.setDouble(11, GenericUtil.Round(summaryDto.getSaleRevenue(), 2));
				
				psmt.setDouble(12, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
				psmt.setDouble(13, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
				psmt.setDouble(14, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
				psmt.setDouble(15, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
				psmt.setDouble(16, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
				psmt.setDouble(17, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
				
				psmt.setDouble(18, summaryDto.getlastAggrCalendarId());
				psmt.setDouble(19, GenericUtil.Round(summaryDto.getSaleMovement(), 2));
				psmt.setDouble(20, GenericUtil.Round(summaryDto.getRegularMovement(), 2));
				psmt.setLong(21, summaryDto.getlastAggrSalesId());
				
				// code added for movement by volume process
				psmt.setDouble(22, summaryDto.getregMovementVolume());
				psmt.setDouble(23, summaryDto.getsaleMovementVolume());
				psmt.setDouble(24, summaryDto.gettotMovementVolume());
				psmt.setDouble(25, summaryDto.getigRegVolumeRev());
				psmt.setDouble(26, summaryDto.getigSaleVolumeRev());
				psmt.setDouble(27, summaryDto.getigtotVolumeRev());
				psmt.setObject(28, store_Status);

				psmt.addBatch();
			} else if (tableName.equalsIgnoreCase("UPDATE_SALES_AGGR")) {
				
				psmt.setDouble(1, GenericUtil.Round(summaryDto.getTotalVisitCount(),2));
				psmt.setDouble(2, GenericUtil.Round(summaryDto.getAverageOrderSize(),2));
				psmt.setDouble(3, GenericUtil.Round(summaryDto.getTotalMovement(), 2));
				psmt.setDouble(4, GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
				psmt.setDouble(5, GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
				psmt.setDouble(6, GenericUtil.Round(summaryDto.getSaleRevenue(), 2));
				
				psmt.setDouble(7, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
				psmt.setDouble(8, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
				psmt.setDouble(9, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
				psmt.setDouble(10, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
				psmt.setDouble(11, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
				psmt.setDouble(12, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
				
				psmt.setObject(13, summaryDto.getlastAggrCalendarId());
				psmt.setDouble(14, GenericUtil.Round(summaryDto.getSaleMovement(), 2));
				psmt.setDouble(15, GenericUtil.Round(summaryDto.getRegularMovement(), 2));
				
				// code added for movement by volume process
				psmt.setDouble(16, GenericUtil.Round(summaryDto.getregMovementVolume(),2));
				psmt.setDouble(17, GenericUtil.Round(summaryDto.getsaleMovementVolume(),2));
				psmt.setDouble(18, GenericUtil.Round(summaryDto.gettotMovementVolume(),2));
				psmt.setDouble(19, GenericUtil.Round(summaryDto.getigRegVolumeRev(),2));
				psmt.setDouble(20, GenericUtil.Round(summaryDto.getigSaleVolumeRev(),2));
				psmt.setDouble(21, GenericUtil.Round(summaryDto.getigtotVolumeRev(),2));
				psmt.setString(22, store_Status);
				psmt.setInt(23, summaryDto.getcalendarId());
				psmt.setInt(24, summaryDto.getLocationId());
				
				if( insertMode){
				psmt.setInt(25,summaryDto.getProductLevelId());
				psmt.setString(26,summaryDto.getProductId());
				}
				
				psmt.addBatch();
				
			}
		} catch (SQLException sql) {
			 logger.error(" Error while adding the valus into batch ...." , sql);
			 throw new GeneralException(" Error while adding the valus into batch ...." , sql);
		}

	}
	
	 
	
	
	/*
	 * ****************************************************************
	 * Get the Last Year Sales_Aggr_Id
	 * Argument 1 : Connection
	 * Argument 2 : LastYear Calendar Id
	 * Argument 3 : LocationId
	 * @catch Exception , Gendral Exception
	 * ****************************************************************
	 */


	public HashMap<String, Long> getLastYearSalesAggrList(Connection conn,
			int lstCalendarId, int locationId, int locationLevelId,
			String tableName, String column) throws GeneralException {
		
		// Map contains last year aggregation records
		// key : location_level_id and location_id 
		// value Summary_Daily_Id
		HashMap<String, Long> lastSummaryList = new HashMap<String, Long>();
		 
		StringBuffer sql = new StringBuffer();
		sql.append(" select LOCATION_LEVEL_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,"+column+" ");
		sql.append(" from "+tableName+"");
		sql.append(" where LOCATION_ID="+locationId+"");
		sql.append(" and CALENDAR_ID="+lstCalendarId+"");
		sql.append(" and LOCATION_LEVEL_Id="+locationLevelId+"");
		
		logger.debug("getLastYearSalesAggrList SQL:" + sql.toString());
		try{
		   
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getLastYearSalesAggrList");
			while(result.next()){
					lastSummaryList.put(result.getInt("LOCATION_LEVEL_ID")
							+ "_" + result.getInt("PRODUCT_LEVEL_ID") + "_"
							+ result.getString("PRODUCT_ID"), result.getLong(4));
			}
			
			result.close();
			
		}catch(Exception exe){
			logger.error(exe);
			throw new GeneralException("getLastYearSalesAggrList Method Error");
		} 
		 
		return lastSummaryList;
	}
	
	
	
	/*
	 * Method used to get the summarydaily-id with retail calendar actual number...
	 * Argument 1 : Connection 
	 * Argument 2 : Process Year 
	 * @catch SqlException ,Exception
	 * @throws General exception
	 *  
	 */

	public List<SummaryDataDTO> getLastYearMappingDetails(Connection _conn,
			int processYear, String tableName, String calendarConstant, 
			String byProcessLevel, String timeLine,	int timeLineNo, 
			int locationLevel, int locationId) throws GeneralException {
	
		logger.debug(" Get Last Year "+ tableName +" Id");
		int lastYear = processYear - 1;
		
		List<SummaryDataDTO> returnList = new ArrayList<SummaryDataDTO>();
		
		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" SELECT RA.ACTUAL_NO,SA.LOCATION_ID ,SA.LOCATION_LEVEL_ID" +
				" ,SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		
			if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY"))
				sql.append(" ,SA.SUMMARY_DAILY_ID as LASTYEARID");
			else
				sql.append(" ,SA.").append(tableName).append("_ID as LASTYEARID");
		
			sql.append(" FROM "+tableName+"  SA");
		
			sql.append(" INNER JOIN RETAIL_CALENDAR RA" + 
				" ON RA.CAL_YEAR=" + lastYear +
				" AND RA.ROW_TYPE='" + calendarConstant + "'");
		
			if (timeLineNo > 0)
				sql.append(" AND RA.ACTUAL_NO=" + timeLineNo);
		
			sql.append(" AND SA.CALENDAR_ID= RA.CALENDAR_ID");
		
			sql.append(" WHERE SA.LOCATION_LEVEL_ID=" + locationLevel);
			
			if (locationLevel == Constants.STORE_LEVEL_ID)
				sql.append(" AND SA.LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DISTRICT_ID =" + locationId + ")");
			else
				sql.append(" AND SA.LOCATION_ID=" + locationId);				
		
			// check the process level store or not
			if( byProcessLevel.equalsIgnoreCase("STORE"))
				sql.append(" AND SA.PRODUCT_LEVEL_ID IS NULL" + 
					" AND SA.PRODUCT_ID IS NULL");
			else
				sql.append(" AND SA.PRODUCT_LEVEL_ID IS NOT NULL" + 
					" AND SA.PRODUCT_ID IS NOT NULL");

			logger.debug("getLastYearMappingDetails SQL:" + sql.toString());
		
			// execute the query....
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, 
														"getLastYearDayLevel");
		
			while( result.next()){
				
				SummaryDataDTO objDto = new SummaryDataDTO();
				objDto.setactualNo(result.getInt("ACTUAL_NO"));
				objDto.setLocationId(result.getInt("LOCATION_ID"));
				objDto.setLocationLevelId(result.getInt("LOCATION_LEVEL_ID"));
				objDto.setlastAggrSalesId(result.getInt("LASTYEARID"));
				
				// Product and product Level last Mapping
				objDto.setProductLevelId(result.getInt("PRODUCT_LEVEL_ID"));
				objDto.setProductId(result.getString("PRODUCT_ID"));
				returnList.add(objDto);			
			}
				
		}catch(Exception exe){
			logger.error(" getLastYearDayLevel error.... " + exe);
			throw new GeneralException(" getLastYearDayLevel error.... " + exe);
		}
		return returnList;
	}
	
	
	/*
	 * Method use to remap the last year records to current year records
	 * Argument 1 : Connection
	 * Argument 2 : DayLevel Summary Daily Map
	 * Argument 3 : Process Year
	 * @catch Exception
	 * @throws Genral exception
	 * 
	 */

	public boolean updateRemapLastYear(Connection _conn,
			List<SummaryDataDTO> dayLevel, int _processYear, String tableName,
			String calendarConstant, String byProcessLevel) throws GeneralException {

		boolean commit = false;
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();
		try {
			logger.debug(" " + tableName + " Update Process begins......");
			PreparedStatement psmt = null;
			try {
							
				psmt = _conn.prepareStatement(updateLastYearRemapping(tableName ,byProcessLevel));
				
			} catch (SQLException e) {
				logger.error(" Intialize the Prepared statment error....... Method Name : updateRemapLastYear "
						+ e);
				throw new GeneralException(
						"Intialize the Prepared statment error... Method Name : updateRemapLastYear"
								+ e);
			}

			HashMap<Integer, Integer> currentCalendarMap = new HashMap<Integer, Integer>();

			// itreate the the previous year data
			try {
				for (int ii = 0; ii < dayLevel.size(); ii++) {

					SummaryDataDTO objDto = dayLevel.get(ii);
					psmt.setObject(1, objDto.getlastAggrSalesId());

					if (currentCalendarMap.containsKey(objDto.getactualNo())) {
						psmt.setObject(2,
								currentCalendarMap.get(objDto.getactualNo()));
					} else {
						int calendarId = objCalendarDao
								.calendarIdBasedActualNumber(_conn,
										_processYear, objDto.getactualNo(),
										calendarConstant, 0);
						psmt.setObject(2, calendarId);
						currentCalendarMap
								.put(objDto.getactualNo(), calendarId);
					}
					psmt.setObject(3, objDto.getLocationId());
					psmt.setObject(4, objDto.getLocationLevelId());
					
					// check process level product then add the product details
					// into batch
					if(byProcessLevel.equalsIgnoreCase("PRODUCT")){
					psmt.setObject(5, objDto.getProductLevelId());
					psmt.setObject(6, objDto.getProductId());	
					}
					
					psmt.addBatch();
				}
			} catch (SQLException sql) {
				logger.error(" Adding Batch Error .... Method Name : updateRemapLastYear "
						+ sql);
				throw new GeneralException(
						"Adding Batch Error .... Method Name : updateRemapLastYear "
								+ sql);
			}

			try {

				int[] count = psmt.executeBatch();
				logger.debug(" " + tableName + " Updated Count..... "
						+ count.length);
				if (count.length > 0)
					commit = true;

			} catch (SQLException sql) {

				logger.error(" Error in excute the batch........ Method Name : updateRemapLastYear "
						+ sql);
				throw new GeneralException(
						" Error in excute the batch........ Method Name : updateRemapLastYear "
								+ sql);

			}

		} catch (Exception exe) {
			logger.error(" Common Exception .... Method Name : updateRemapLastYear "
					+ exe);
			throw new GeneralException(
					"Common Exception .... Method Name : updateRemapLastYear "
							+ exe);
		}
		return commit;
	}

	public boolean updateLastYearReference(Connection _conn, 
			List<SummaryDataDTO> lastYearDataList, int calendarId, int locationLevel, 
				int locationId, String timeLine, String processLevel) throws GeneralException {
		boolean commit = false;
		
		// Logic to decide target table
		String tableName;
		if (locationLevel == Constants.STORE_LEVEL_ID){
			if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY))
				tableName = "SALES_AGGR_DAILY";
			else if (timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				tableName = "SALES_AGGR_WEEKLY";
			else
				tableName = "SALES_AGGR";
		}
		else{
			if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY))
				tableName = "SALES_AGGR_DAILY_ROLLUP";
			else if (timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				tableName = "SALES_AGGR_WEEKLY_ROLLUP";
			else
				tableName = "FROM SALES_AGGR_ROLLUP";
		}
		
		PreparedStatement psmt = null;

		try {
			psmt = _conn.prepareStatement(updateLastYearMapping(tableName, processLevel));
			
		} catch (SQLException e) {
			logger.error("UpdateLastYearReference - Prepared statment error:" + e);
			throw new GeneralException("UpdateLastYearReference - Prepared statment error" + e);
		}

			// Iterate the the previous year data
		try 
		{
			for (int ii = 0; ii < lastYearDataList.size(); ii++) {

				SummaryDataDTO objDto = lastYearDataList.get(ii);
				psmt.setObject(1, objDto.getlastAggrSalesId());
				psmt.setObject(2, calendarId);
				psmt.setObject(3, objDto.getLocationId());
				psmt.setObject(4, objDto.getLocationLevelId());
				
				if (!processLevel.equalsIgnoreCase("STORE")){
					psmt.setObject(5, objDto.getProductLevelId());
					psmt.setObject(6, objDto.getProductId());	
				}
				
				psmt.addBatch();
			}
				
			int[] count = psmt.executeBatch();
			logger.debug(" " + tableName + " Updated Count.." + count.length);
				
			// If everything goes fine, commit the transaction
			PristineDBUtil.commitTransaction(_conn, "Commit the update last year reference");
			
		} 
		catch (SQLException sql) 
		{
			logger.error("updateRemapLastYear - Error while adding data into batch..." + sql);
			throw new GeneralException("updateRemapLastYear - Error while adding data into batch..." + sql);
		}

		return commit;
	}
	
	
	private String updateLastYearRemapping(String tableName, String byProcessLevel) {
		 
		StringBuffer sql = new StringBuffer();
		
		sql.append(" update "+tableName+" set");
		
		if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY"))
		sql.append(" LST_SUMMARY_DAILY_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
		sql.append(" LST_SALES_AGGR_ROLLUP_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY"))
		sql.append(" LST_SALES_AGGR_WEEKLY_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP"))
		sql.append(" LST_SALES_AGGR_ROLLUP_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR"))
		sql.append(" LST_SALES_AGGR_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP"))
		sql.append(" LST_SALES_AGGR_ROLLUP_ID=? ");
		
		sql.append(" where CALENDAR_ID=?  and LOCATION_ID=? and LOCATION_LEVEL_ID=?");
		
		// code added for supporting last year data mapping for product
		if(byProcessLevel.equalsIgnoreCase("PRODUCT"))
		sql.append(" and PRODUCT_LEVEL_ID =? and PRODUCT_ID=?");
		else if(byProcessLevel.equalsIgnoreCase("STORE"))
		sql.append(" and PRODUCT_LEVEL_ID is null and PRODUCT_ID is null");
		
		
		return sql.toString();
	}
	
	private String updateLastYearMapping(String tableName, String processLevel) {
		 
		StringBuffer sql = new StringBuffer();
		
		sql.append(" UPDATE ").append(tableName).append(" SET");
		
		if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY"))
		sql.append(" LST_SUMMARY_DAILY_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
		sql.append(" LST_SALES_AGGR_ROLLUP_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY"))
		sql.append(" LST_SALES_AGGR_WEEKLY_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP"))
		sql.append(" LST_SALES_AGGR_ROLLUP_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR"))
		sql.append(" LST_SALES_AGGR_ID=? ");
		else if( tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP"))
		sql.append(" LST_SALES_AGGR_ROLLUP_ID=? ");
		
		sql.append(" WHERE CALENDAR_ID=? AND LOCATION_ID=? AND LOCATION_LEVEL_ID=?");
		
		if (processLevel.equalsIgnoreCase("STORE") )
			sql.append(" AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL");
		else
			sql.append(" AND PRODUCT_LEVEL_ID =? AND PRODUCT_ID=?");
		
		return sql.toString();
	}
	
	/**
	 * Method used to get the summary details based on calendar id and location id
	 * @param _conn
	 * @param calendarId
	 * @param locationId
	 * @param locationLevelId
	 * @param tableName
	 * @return HashMap<String, SummaryDataDTO> returnMap 
	 * @throws GeneralException
	 */
	public HashMap<String, SummaryDataDTO> getSummaryDetails(Connection _conn,
			int calendarId, int locationId, int locationLevelId, String tableName)
			throws GeneralException {

		HashMap<String, SummaryDataDTO> returnMap = new HashMap<String, SummaryDataDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID");
		sql.append(" ,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT,TOT_MOVEMENT,REG_MOVEMENT");
		sql.append(" ,SALE_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE");
		sql.append(" ,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT,REG_MARGIN_PCT");
		sql.append(" ,SALE_MARGIN_PCT,LOYALTY_CARD_SAVING");
		
		// Code added for Movement By Volume
		sql.append(" ,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,REG_IGVOL_REVENUE"); 
		sql.append(" ,SALE_IGVOL_REVENUE,TOT_MOVEMENT_VOL,TOT_IGVOL_REVENUE");
		
		if(tableName.equalsIgnoreCase("SALES_AGGR_DAILY")){
			sql.append(" ,STORE_TYPE");
		}
		
		if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP")
				|| tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")
				|| tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")) {
			sql.append(" ,ID_TOT_VISIT_CNT,ID_TOT_MOVEMENT,ID_TOT_REVENUE,ID_REG_REVENUE");
			sql.append(" ,ID_SALE_REVENUE,ID_AVG_ORDER_SIZE,ID_REG_MOEVEMNT,ID_SALE_MOVEMENT");
			sql.append(" ,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL,ID_REG_IGVOL_REVENUE"); 
			sql.append(" ,ID_SALE_IGVOL_REVENUE,ID_TOT_MOVEMENT_VOL,ID_TOT_IGVOL_REVENUE"); 
		}
		
		if( tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY")){
			sql.append(" ,LAST_AGGR_CALENDARID,STORE_TYPE");
		}
		
		if( tableName.equalsIgnoreCase("SALES_AGGR")){
			sql.append(" ,ADJ_TOT_REVENUE,LAST_AGGR_CALENDARID,STORE_TYPE");
		}
		
		if( tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")){
			sql.append(" ,ADJ_TOT_REVENUE,ADJ_ID_TOT_REVENUE ");
		}
		
		if (tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")
				|| tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")) {
			sql.append(" ,LAST_AGGR_CALENDARID");
			
		}
		
		sql.append(" from ").append(tableName);
		sql.append(" where CALENDAR_ID=").append(calendarId);
		sql.append(" and LOCATION_ID=").append(locationId);
		sql.append(" and LOCATION_LEVEL_ID=").append(locationLevelId);

		logger.debug(" Sql......" +  sql.toString());
				
		CachedRowSet result = null;

		try {
			result = PristineDBUtil.executeQuery(_conn, sql,
					"getSummaryDetails");

		} catch (GeneralException e) {

			throw new GeneralException(
					" Error While fetching the results...... ", e);

		}

		try {
			while (result.next()) {

				SummaryDataDTO objDto = new SummaryDataDTO();

				objDto.setcalendarId(result.getInt("CALENDAR_ID"));
				objDto.setLocationLevelId(result.getInt("LOCATION_LEVEL_ID"));
				objDto.setLocationId(result.getInt("LOCATION_ID"));
				objDto.setProductLevelId(result.getInt("PRODUCT_LEVEL_ID"));
				objDto.setProductId(result.getString("PRODUCT_ID"));
				objDto.setTotalVisitCount(result.getDouble("TOT_VISIT_CNT"));
				objDto.setTotalMovement(result.getDouble("TOT_MOVEMENT"));
				objDto.setRegularMovement(result.getDouble("REG_MOVEMENT"));
				objDto.setSaleMovement(result.getDouble("SALE_MOVEMENT"));
				objDto.setTotalRevenue(result.getDouble("TOT_REVENUE"));
				objDto.setRegularRevenue(result.getDouble("REG_REVENUE"));
				objDto.setSaleRevenue(result.getDouble("SALE_REVENUE"));
				// objDto.setAverageOrderSize(result.getDouble("AVG_ORDER_SIZE"));
				objDto.setTotalMargin(result.getDouble("TOT_MARGIN"));
				objDto.setRegularMargin(result.getDouble("REG_MARGIN"));
				objDto.setSaleMargin(result.getDouble("SALE_MARGIN"));
				// objDto.setsummaryCtdId(result.getInt("SUMMARY_CTD_ID"));

				if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP") || tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")
									|| tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")) {

					objDto.setiregularMovement(result.getDouble("ID_REG_MOEVEMNT"));
					objDto.setisaleMovement(result.getDouble("ID_SALE_MOVEMENT"));
					objDto.setitotalRevenue(result.getDouble("ID_TOT_REVENUE"));
					objDto.setitotalMovement(result.getDouble("ID_TOT_MOVEMENT"));
					objDto.setitotalVisitCount(result.getDouble("ID_TOT_VISIT_CNT"));
					objDto.setiregularRevenue(result.getDouble("ID_REG_REVENUE"));
					objDto.setisaleRevenue(result.getDouble("ID_SALE_REVENUE"));
					objDto.setiaverageOrderSize(result.getDouble("ID_AVG_ORDER_SIZE"));
					
					// code newly added for identical movement by volume
					objDto.setidRegMovementVolume(result.getDouble("ID_REG_MOVEMENT_VOL"));
					objDto.setidSaleMovementVolume(result.getDouble("ID_SALE_MOVEMENT_VOL"));
					objDto.setidIgRegVolumeRev(result.getDouble("ID_REG_IGVOL_REVENUE"));
					objDto.setidIgSaleVolumeRev(result.getDouble("ID_SALE_IGVOL_REVENUE"));
					objDto.setidTotMovementVolume(result.getDouble("ID_TOT_MOVEMENT_VOL"));
					objDto.setIdIgtotVolumeRev(result.getDouble("ID_TOT_IGVOL_REVENUE"));
				}

				if (tableName.equalsIgnoreCase("SALES_AGGR")) {
					objDto.setAdjTotRevenue(result.getDouble("ADJ_TOT_REVENUE"));
					objDto.setlastAggrCalendarId(result.getInt("LAST_AGGR_CALENDARID"));
					objDto.setStoreStatus(result.getString("STORE_TYPE"));
				}

				if (tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")) {
					objDto.setAdjTotRevenue(result.getDouble("ADJ_TOT_REVENUE"));
					objDto.setAdjIdTotRevenue(result.getDouble("ADJ_ID_TOT_REVENUE"));
					objDto.setlastAggrCalendarId(result.getInt("LAST_AGGR_CALENDARID"));
				}

				if (tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY")) {
					objDto.setlastAggrCalendarId(result.getInt("LAST_AGGR_CALENDARID"));
					objDto.setStoreStatus(result.getString("STORE_TYPE"));
				}
				
				if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY")) {
					objDto.setStoreStatus(result.getString("STORE_TYPE"));
				}
				
				if(tableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")){
					objDto.setlastAggrCalendarId(result.getInt("LAST_AGGR_CALENDARID"));
				}
				
				objDto.setregMovementVolume(result.getDouble("REG_MOVEMENT_VOL"));
				objDto.setsaleMovementVolume(result.getDouble("SALE_MOVEMENT_VOL"));
				objDto.setigRegVolumeRev(result.getDouble("REG_IGVOL_REVENUE"));
				objDto.setigSaleVolumeRev(result.getDouble("SALE_IGVOL_REVENUE"));
				objDto.settotMovementVolume(result.getDouble("TOT_MOVEMENT_VOL"));
				objDto.setigtotVolumeRev(result.getDouble("TOT_IGVOL_REVENUE"));
				
				returnMap.put(objDto.getLocationLevelId() + "_"+ objDto.getLocationId() + "_"+ objDto.getProductLevelId() + "_"+ objDto.getProductId(), objDto);

			}

		} catch (Exception e) {
			throw new GeneralException(
					" Error While fetching the results...... ", e);
		}

		return returnMap;
	}

	
 
	
	public HashMap<String, SummaryDataDTO> getTempSalesAggregation(
			Connection _conn, int locationId, int locationlevelId,
			String sourceTable, String dayTable,
			List<RetailCalendarDTO> dailyCalendarList, int _weekCalendarId)
			throws GeneralException { 

		HashMap<String, SummaryDataDTO> weekMap = new HashMap<String, SummaryDataDTO>();

		try {
			StringBuffer sql = new StringBuffer();
			
			sql.append(" SELECT APRODUCT_LEVEL_ID , APRODUCT_ID, ALOCATION_ID,ALOCATION_LEVEL_ID,");
			sql.append(" (ATOT_VISIT_CNT - BTOT_VISIT_CNT) AS TOT_VISIT_CNT,");
			sql.append(" (AREG_MOVEMENT - BREG_MOVEMENT)  AS REG_MOVEMENT,");
			sql.append(" (ASALE_MOVEMENT - BSALE_MOVEMENT) AS SALE_MOVEMENT,");
			sql.append(" (AREG_REVENUE - BREG_REVENUE) AS REG_REVENUE,");
			sql.append(" (ASALE_REVENUE - BSALE_REVENUE) AS SALE_REVENUE,");
			sql.append(" (ATOT_MARGIN - BTOT_MARGIN) AS TOT_MARGIN,");
			sql.append(" (AREG_MARGIN - BREG_MARGIN)  AS REG_MARGIN,");
			sql.append(" (ASALE_MARGIN -BSALE_MARGIN)  AS SALE_MARGIN,");
			sql.append(" (AREG_MOVEMENT_VOL - BREG_MOVEMENT_VOL)  AS REG_MOVEMENT_VOL,");
			sql.append(" (ASALE_MOVEMENT_VOL - BSALE_MOVEMENT_VOL) AS SALE_MOVEMENT_VOL,");
			sql.append(" (AREG_IGVOL_REVENUE -BREG_IGVOL_REVENUE) AS REG_IGVOL_REVENUE,");
			sql.append(" (ASALE_IGVOL_REVENUE-BSALE_IGVOL_REVENUE) AS SALE_IGVOL_REVENUE,");
			sql.append(" (ATOT_MOVEMENT_VOL  -BTOT_MOVEMENT_VOL) AS TOT_MOVEMENT_VOL,");
			sql.append(" (ATOT_IGVOL_REVENUE - BTOT_IGVOL_REVENUE) AS TOT_IGVOL_REVENUE,");
			sql.append(" (ATOT_REVENUE - BTOT_REVENUE) AS TOT_REVENUE,");
			sql.append(" (ATOT_MOVEMENT - BTOT_MOVEMENT) AS TOT_MOVEMENT");
			
			// for daily rollup 
			if( dayTable.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP_TEMP")){
			  sql.append(" ,(AID_TOT_VISIT_CNT - BID_TOT_VISIT_CNT) AS ID_TOT_VISIT_CNT,");                     
			  sql.append("  (AID_TOT_MOVEMENT - BID_TOT_MOVEMENT) AS ID_TOT_MOVEMENT,");                      
			  sql.append("  (AID_TOT_REVENUE - BID_TOT_REVENUE) AS ID_TOT_REVENUE,");                      
			  sql.append("  (AID_REG_REVENUE - BID_REG_REVENUE) AS ID_REG_REVENUE,");                      
			  sql.append("  (AID_SALE_REVENUE - BID_SALE_REVENUE) AS ID_SALE_REVENUE,");                      
			  sql.append("  (AID_TOT_MARGIN - BID_TOT_MARGIN) AS ID_TOT_MARGIN,");                        
			  sql.append("  (AID_REG_MARGIN - BID_REG_MARGIN) AS ID_REG_MARGIN,");                        
			  sql.append("  (AID_SALE_MARGIN - BID_SALE_MARGIN) AS ID_SALE_MARGIN,");                       
			  sql.append("  (AID_REG_MOEVEMNT - BID_REG_MOEVEMNT) AS ID_REG_MOEVEMNT,");                     
			  sql.append("  (AID_SALE_MOVEMENT - BID_SALE_MOVEMENT) AS ID_SALE_MOVEMENT,");    
			  
			  sql.append("  (AID_REG_MOVEMENT_VOL -BID_REG_MOVEMENT_VOL) AS ID_REG_MOVEMENT_VOL,");                 
			  sql.append("  (AID_SALE_MOVEMENT_VOL - BID_SALE_MOVEMENT_VOL) AS ID_SALE_MOVEMENT_VOL,");                
			  sql.append("  (AID_REG_IGVOL_REVENUE - BID_REG_IGVOL_REVENUE) AS ID_REG_IGVOL_REVENUE,");                 
			  sql.append("  (AID_SALE_IGVOL_REVENUE - BID_SALE_IGVOL_REVENUE) AS ID_SALE_IGVOL_REVENUE,");            
			  sql.append("  (AID_TOT_MOVEMENT_VOL - BID_TOT_MOVEMENT_VOL) AS ID_TOT_MOVEMENT_VOL,");                 
			  sql.append("  (AID_TOT_IGVOL_REVENUE - BID_TOT_IGVOL_REVENUE) AS ID_TOT_IGVOL_REVENUE");
			}
			else{
				 sql.append(" ,ASTORETYPE");          
			}
			
			
			sql.append(" FROM");
			sql.append(" (SELECT CASE WHEN SA.PRODUCT_LEVEL_ID is null  THEN 0  ELSE  SA.PRODUCT_LEVEL_ID  END  AS APRODUCT_LEVEL_ID,");
			sql.append(" case when SA.PRODUCT_ID is null then 0  else  SA.PRODUCT_ID  end  AS APRODUCT_ID,");
			sql.append(" SA.LOCATION_ID AS ALOCATION_ID,LOCATION_LEVEL_ID AS ALOCATION_LEVEL_ID");
			sql.append(" ,SUM(SA.TOT_VISIT_CNT) AS ATOT_VISIT_CNT,");
			sql.append(" SUM(SA.TOT_MOVEMENT) AS ATOT_MOVEMENT,");
			sql.append(" SUM(SA.REG_MOVEMENT) AS AREG_MOVEMENT,");
			sql.append(" SUM(SA.SALE_MOVEMENT) AS ASALE_MOVEMENT,");
			sql.append(" SUM(SA.REG_REVENUE) AS AREG_REVENUE,");
			sql.append(" SUM(SA.SALE_REVENUE) AS ASALE_REVENUE,");
			sql.append(" SUM(SA.TOT_MARGIN) AS ATOT_MARGIN,");
			sql.append(" SUM(SA.REG_MARGIN) AS AREG_MARGIN,");
			sql.append(" SUM(SA.SALE_MARGIN) AS ASALE_MARGIN,");
			sql.append(" SUM(SA.REG_MOVEMENT_VOL) AS AREG_MOVEMENT_VOL,");
			sql.append(" SUM(SA.SALE_MOVEMENT_VOL) AS ASALE_MOVEMENT_VOL,");
			sql.append(" SUM(SA.REG_IGVOL_REVENUE) AS AREG_IGVOL_REVENUE,");
			sql.append(" SUM(SA.SALE_IGVOL_REVENUE) AS ASALE_IGVOL_REVENUE,");
			sql.append(" SUM(SA.TOT_MOVEMENT_VOL) AS ATOT_MOVEMENT_VOL,");
			sql.append(" SUM(SA.TOT_IGVOL_REVENUE) AS ATOT_IGVOL_REVENUE,");
			sql.append(" SUM(SA.TOT_REVENUE ) AS ATOT_REVENUE");
			
			//  for daily rollup
			if( dayTable.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP_TEMP")){
				  sql.append(" ,SUM(ID_TOT_VISIT_CNT) AS AID_TOT_VISIT_CNT,");                     
				  sql.append("  SUM(ID_TOT_MOVEMENT) AS AID_TOT_MOVEMENT,");                      
				  sql.append("  SUM(ID_TOT_REVENUE) AS AID_TOT_REVENUE,");                      
				  sql.append("  SUM(ID_REG_REVENUE) AS AID_REG_REVENUE,");                      
				  sql.append("  SUM(ID_SALE_REVENUE) AS AID_SALE_REVENUE,");                      
				  sql.append("  SUM(ID_TOT_MARGIN) AS AID_TOT_MARGIN,");                        
				  sql.append("  SUM(ID_REG_MARGIN) AS AID_REG_MARGIN,");                        
				  sql.append("  SUM(ID_SALE_MARGIN) AS AID_SALE_MARGIN,");                       
				  sql.append("  SUM(ID_REG_MOEVEMNT) AS AID_REG_MOEVEMNT,");                     
				  sql.append("  SUM(ID_SALE_MOVEMENT) AS AID_SALE_MOVEMENT,");                     
				  sql.append("  SUM(ID_REG_MOVEMENT_VOL) AS AID_REG_MOVEMENT_VOL,");                 
				  sql.append("  SUM(ID_SALE_MOVEMENT_VOL) AS AID_SALE_MOVEMENT_VOL,");                
				  sql.append("  SUM(ID_REG_IGVOL_REVENUE) AS AID_REG_IGVOL_REVENUE,");                 
				  sql.append("  SUM(ID_SALE_IGVOL_REVENUE) AS AID_SALE_IGVOL_REVENUE,");            
				  sql.append("  SUM(ID_TOT_MOVEMENT_VOL) AS AID_TOT_MOVEMENT_VOL,");                 
				  sql.append("  SUM(ID_TOT_IGVOL_REVENUE) AS AID_TOT_IGVOL_REVENUE");
				}
			else{
				sql.append(" ,SA.STORE_TYPE AS ASTORETYPE");
			}
						
			sql.append(" FROM "+sourceTable+" SA");
			sql.append(" WHERE SA.LOCATION_ID=").append(locationId);
			sql.append(" AND SA.LOCATION_LEVEL_ID=").append(locationlevelId);
			sql.append(" AND SA.CALENDAR_ID  =").append(_weekCalendarId);
			sql.append(" GROUP BY SA.PRODUCT_LEVEL_ID, SA.PRODUCT_ID,  SA.LOCATION_ID,SA.LOCATION_LEVEL_ID");
			if( dayTable.equalsIgnoreCase("SALES_AGGR_DAILY_TEMP"))
			sql.append(" ,SA.STORE_TYPE ");
			sql.append("  ) A,");
			sql.append(" (SELECT CASE WHEN SB.PRODUCT_LEVEL_ID is null THEN 0  ELSE  SB.PRODUCT_LEVEL_ID  END  AS BPRODUCT_LEVEL_ID ,");
			sql.append(" case when SB.PRODUCT_ID is null then 0  else  SB.PRODUCT_ID  end  AS BPRODUCT_ID,");
			sql.append(" SB.LOCATION_ID AS BLOCATION_ID,LOCATION_LEVEL_ID AS BLOCATION_LEVEL_ID,");
			sql.append(" SUM(SB.TOT_VISIT_CNT) AS BTOT_VISIT_CNT ,");
			sql.append(" SUM(SB.TOT_MOVEMENT)  AS BTOT_MOVEMENT ,");
			sql.append(" SUM(SB.REG_MOVEMENT)  AS BREG_MOVEMENT ,");
			sql.append(" SUM(SB.SALE_MOVEMENT) AS BSALE_MOVEMENT ,");
			sql.append(" SUM(SB.REG_REVENUE)   AS BREG_REVENUE ,");
			sql.append(" SUM(SB.SALE_REVENUE)  AS BSALE_REVENUE ,");
			sql.append(" SUM(SB.TOT_MARGIN)    AS BTOT_MARGIN ,");
			sql.append(" SUM(SB.REG_MARGIN)    AS BREG_MARGIN ,");
			sql.append(" SUM(SB.SALE_MARGIN)   AS BSALE_MARGIN ,");
			sql.append(" SUM(SB.REG_MOVEMENT_VOL) AS BREG_MOVEMENT_VOL ,");
			sql.append(" SUM(SB.SALE_MOVEMENT_VOL) AS BSALE_MOVEMENT_VOL ,");
			sql.append(" SUM(SB.REG_IGVOL_REVENUE) AS BREG_IGVOL_REVENUE ,");
			sql.append(" SUM(SB.SALE_IGVOL_REVENUE) AS BSALE_IGVOL_REVENUE ,");
			sql.append(" SUM(SB.TOT_MOVEMENT_VOL)   AS BTOT_MOVEMENT_VOL ,");
			sql.append(" SUM(SB.TOT_IGVOL_REVENUE)  AS BTOT_IGVOL_REVENUE ,");
			sql.append(" SUM(SB.TOT_REVENUE ) AS BTOT_REVENUE");
			
			// for daily rollup 
			if( dayTable.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP_TEMP")){
				  sql.append(" ,SUM(ID_TOT_VISIT_CNT) AS BID_TOT_VISIT_CNT,");                     
				  sql.append("  SUM(ID_TOT_MOVEMENT) AS BID_TOT_MOVEMENT,");                      
				  sql.append("  SUM(ID_TOT_REVENUE) AS BID_TOT_REVENUE,");                      
				  sql.append("  SUM(ID_REG_REVENUE) AS BID_REG_REVENUE,");                      
				  sql.append("  SUM(ID_SALE_REVENUE) AS BID_SALE_REVENUE,");                      
				  sql.append("  SUM(ID_TOT_MARGIN) AS BID_TOT_MARGIN,");                        
				  sql.append("  SUM(ID_REG_MARGIN) AS BID_REG_MARGIN,");                        
				  sql.append("  SUM(ID_SALE_MARGIN) AS BID_SALE_MARGIN,");                       
				  sql.append("  SUM(ID_REG_MOEVEMNT) AS BID_REG_MOEVEMNT,");                     
				  sql.append("  SUM(ID_SALE_MOVEMENT) AS BID_SALE_MOVEMENT,");                     
				  sql.append("  SUM(ID_REG_MOVEMENT_VOL) AS BID_REG_MOVEMENT_VOL,");                 
				  sql.append("  SUM(ID_SALE_MOVEMENT_VOL) AS BID_SALE_MOVEMENT_VOL,");                
				  sql.append("  SUM(ID_REG_IGVOL_REVENUE) AS BID_REG_IGVOL_REVENUE,");                 
				  sql.append("  SUM(ID_SALE_IGVOL_REVENUE) AS BID_SALE_IGVOL_REVENUE,");            
				  sql.append("  SUM(ID_TOT_MOVEMENT_VOL) AS BID_TOT_MOVEMENT_VOL,");                 
				  sql.append("  SUM(ID_TOT_IGVOL_REVENUE) AS BID_TOT_IGVOL_REVENUE");
				}
			
			sql.append(" FROM "+dayTable+" SB");
			sql.append(" WHERE Sb.LOCATION_ID=").append(locationId);
			sql.append(" and SB.LOCATION_LEVEL_ID=").append(locationlevelId);
			sql.append(" AND SB.CALENDAR_ID IN (");
			
			
			for (int ii = 0; ii < dailyCalendarList.size(); ii++) {

				RetailCalendarDTO objDto = dailyCalendarList.get(ii);
				if (ii > 0)
					sql.append(" ,");
				sql.append(objDto.getCalendarId());
			}
			sql.append(" )");
			sql.append(" GROUP BY Sb.PRODUCT_LEVEL_ID, Sb.PRODUCT_ID, Sb.LOCATION_ID,SB.LOCATION_LEVEL_ID ) b ");
			sql.append(" WHERE A.APRODUCT_LEVEL_ID =B.BPRODUCT_LEVEL_ID ");
			sql.append(" and ALOCATION_LEVEL_ID = BLOCATION_LEVEL_ID");
			sql.append(" AND A.APRODUCT_ID  = B.BPRODUCT_ID");
							
			
			logger.debug("getTempSalesAggregation SQL:" + sql.toString());

			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"getWeeklyAggregation");
			while (resultSet.next()) {
				
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				if (resultSet.getString("APRODUCT_LEVEL_ID") == null)
					summaryDto.setProductLevelId(0);
				else
				summaryDto.setProductLevelId(resultSet.getInt("APRODUCT_LEVEL_ID"));
				summaryDto.setProductId(resultSet.getString("APRODUCT_ID"));
				if( !(resultSet.getDouble("TOT_VISIT_CNT") < 0) )
				summaryDto.setTotalVisitCount(resultSet.getDouble("TOT_VISIT_CNT"));
				if( !(resultSet.getDouble("TOT_MOVEMENT") < 0) )
				summaryDto.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				if( !(resultSet.getDouble("REG_MOVEMENT") < 0) )
				summaryDto.setRegularMovement(resultSet.getDouble("REG_MOVEMENT"));
				if( !(resultSet.getDouble("SALE_MOVEMENT") < 0) )
				summaryDto.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				if( !(resultSet.getDouble("REG_REVENUE") < 0) )
				summaryDto.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
				if( !(resultSet.getDouble("SALE_REVENUE") < 0) )
				summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
				if( !(resultSet.getDouble("TOT_MARGIN") < 0) )
				summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
				if( !(resultSet.getDouble("REG_MARGIN") < 0) )
				summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
				if( !(resultSet.getDouble("SALE_MARGIN") < 0) )
				summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));
				summaryDto.setLocationId(resultSet.getInt("ALOCATION_ID"));
				if( !(resultSet.getDouble("TOT_REVENUE") < 0) )
				summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));

				// code added for Movement By Volume
				if( !(resultSet.getDouble("REG_MOVEMENT_VOL") < 0) )
				summaryDto.setregMovementVolume(resultSet.getDouble("REG_MOVEMENT_VOL"));
				if( !(resultSet.getDouble("SALE_MOVEMENT_VOL") < 0) )
				summaryDto.setsaleMovementVolume(resultSet.getDouble("SALE_MOVEMENT_VOL"));
				if( !(resultSet.getDouble("REG_IGVOL_REVENUE") < 0) )
				summaryDto.setigRegVolumeRev(resultSet.getDouble("REG_IGVOL_REVENUE"));
				if( !(resultSet.getDouble("SALE_IGVOL_REVENUE") < 0) )
				summaryDto.setigSaleVolumeRev(resultSet.getDouble("SALE_IGVOL_REVENUE"));
				if( !(resultSet.getDouble("TOT_MOVEMENT_VOL") < 0) )
				summaryDto.settotMovementVolume(resultSet.getDouble("TOT_MOVEMENT_VOL"));
				if( !(resultSet.getDouble("TOT_IGVOL_REVENUE") < 0) )
				summaryDto.setigtotVolumeRev(resultSet.getDouble("TOT_IGVOL_REVENUE"));
				summaryDto.setLocationLevelId(locationlevelId);
				
				// for rollup details
				if( dayTable.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP_TEMP")){
				
				  if( !(resultSet.getDouble("TOT_IGVOL_REVENUE") < 0) )
				  summaryDto.setidRegMovementVolume(resultSet.getDouble("ID_REG_MOVEMENT_VOL"));
				  summaryDto.setidSaleMovementVolume(resultSet.getDouble("ID_SALE_MOVEMENT_VOL"));
				  summaryDto.setidIgRegVolumeRev(resultSet.getDouble("ID_REG_IGVOL_REVENUE"));
				  summaryDto.setidIgSaleVolumeRev(resultSet.getDouble("ID_SALE_IGVOL_REVENUE"));
				  summaryDto.setidTotMovementVolume(resultSet.getDouble("ID_TOT_MOVEMENT_VOL"));
				  summaryDto.setIdIgtotVolumeRev(resultSet.getDouble("ID_TOT_IGVOL_REVENUE"));
					
				  summaryDto.setitotalVisitCount(resultSet.getDouble("ID_TOT_VISIT_CNT"));
				  summaryDto.setitotalMovement(resultSet.getDouble("ID_TOT_MOVEMENT")); 
				  summaryDto.setiregularMovement(resultSet.getDouble("ID_REG_MOEVEMNT"));
				  summaryDto.setisaleMovement(resultSet.getDouble("ID_SALE_MOVEMENT"));
				  summaryDto.setiregularRevenue(resultSet.getDouble("ID_REG_REVENUE"));
				  summaryDto.setisaleRevenue(resultSet.getDouble("ID_SALE_REVENUE"));
				  summaryDto.setitotalMargin(resultSet.getDouble("ID_TOT_MARGIN"));
				  summaryDto.setiregularMargin(resultSet.getDouble("ID_REG_MARGIN"));
				  summaryDto.setisaleMargin(resultSet.getDouble("ID_SALE_MARGIN"));
				  summaryDto.setitotalRevenue(resultSet.getDouble("ID_TOT_REVENUE"));
				}
				
				if( dayTable.equalsIgnoreCase("SALES_AGGR_DAILY_TEMP"))
				summaryDto.setStoreStatus(resultSet.getString("ASTORETYPE"));
										 
				weekMap.put(summaryDto.getProductLevelId() + "_"+ summaryDto.getProductId(), summaryDto);

			}
		} catch (Exception exe) {
			logger.error("....Error in sales Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales Aggregation Dao....",
					exe);
		}

		return weekMap;
	}

	
	/*
	 * Method used to find and update the given store is identical or new
	 * Update the store status into daily table  
	 * Argument 1 : Connection
	 * Argument 2 : ProcessCalendarId
	 * Argument 3 : input Process Date
	 * Argument 4 : SummaryDataDTO storeDataDto
	 * Argument 5 : calendarRowType
	 * @throws GeneralException
	 */
	
	
	public String getStoreStatus(Connection _conn, String inputDate,
			SummaryDataDTO storeDataDto, String calendarRowType)
			throws ParseException, GeneralException {
	
		// Find given store is Identical or New
		String retVal = "";
		// format the the given date string
		DateFormat sformatter = new SimpleDateFormat("yyyy-mm-dd");
		Date currentProcessDate = (Date) sformatter.parse(inputDate);
		Date storeOpenDate = (Date) sformatter.parse(storeDataDto.getStoreOpenDate());
		
		try {
			// query for getting Identical
			StringBuffer sql = new StringBuffer();
			sql.append(" select");
			sql.append(" case");
			
			// for Identical
			sql.append(" when A.START_DATE  > to_date('").append(sformatter.format(storeOpenDate))
													  .append("','yyyy-mm-dd')")	 	
													  .append(" then 'I'");
			// for new
			sql.append(" when A.START_DATE  < to_date('").append(sformatter.format(storeOpenDate))
			  											.append("','yyyy-mm-dd')")	 	
			  											.append(" then 'N'");
			sql.append(" END as status");
			sql.append(" from (");
			sql.append(" select START_DATE - 364 AS START_DATE from  RETAIL_CALENDAR");
			sql.append(" where to_date('").append(sformatter.format(currentProcessDate))
										   .append("' ,'yyyy-mm-dd')");			
			sql.append(" between START_DATE and END_DATE and ROW_TYPE='").append(calendarRowType)
																		  .append("') A");
			 
			logger.debug("GetStoreStatus SQL " +sql.toString());
			  
			String singleColumnVal = PristineDBUtil.getSingleColumnVal(_conn, sql, "updateIdenticalDetails");
			
			
			if( singleColumnVal !=null){
				retVal =  singleColumnVal;
			}
		} catch (GeneralException e) {
			logger.error(" Error while getting Identical status " ,e);
			throw new GeneralException(" Error in getStoreStatus " ,e);
			
		}

		return retVal;
	}

	public void deleteTempAggragation(Connection _conn, int locationId,
			int calendarId, int storeLevelId, String tableName) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		
		sql.append(" delete from ").append( tableName);
		sql.append(" where calendar_id = ").append(calendarId);
		sql.append(" and location_id =").append(locationId);
		sql.append(" and location_level_id=").append(storeLevelId);
		
		PristineDBUtil.execute(_conn, sql, "deleteTempAggragation");
		
		

		
	}
		
	public void updateHouseholdMetrics(Connection _Conn, int calendarId,
		String calendarType, int locationLevelId, int locationId, 
		HashMap<Integer, HashMap<Integer, HouseholdDTO>> householdCountMap) 
												throws GeneralException {
		try
		  {
	        PreparedStatement psmt=_Conn.prepareStatement(updateHouseHoldSql(calendarType, locationLevelId));
	        
	        Object[] outerHouseholdLoop=householdCountMap.values().toArray();
	        for (int ii = 0; ii < outerHouseholdLoop.length; ii++) {
	        	@SuppressWarnings("unchecked")
				HashMap<Integer, HouseholdDTO> productHouseholdMap = (HashMap<Integer, HouseholdDTO>) outerHouseholdLoop[ii];
	        	
	        	Object[] productLoop = productHouseholdMap.values().toArray();
	        	for (int jj = 0; jj < productLoop.length; jj++) {
	        		HouseholdDTO householdDto = (HouseholdDTO) productLoop[jj];
					addupdateHouseholdBatch(psmt, householdDto, calendarId, locationLevelId, locationId);
	        	}
	        }

			logger.debug("Execue Update");
			
			int[] count = psmt.executeBatch();
			logger.debug(" updated record count:" + count.length);
			
			psmt.close();
			 
			}
		catch(Exception exe)
		{
			logger.error("Summary Daily Insert Error" , exe);
			throw new GeneralException("Summary Daily Insert Error" , exe);
		}
	}

	private void addupdateHouseholdBatch(PreparedStatement psmt, 
		HouseholdDTO householdDto, int calendarId, int locationLevelId,
			 				int locationId) throws GeneralException {
		try {
			/*logger.debug("C: " + calendarId + " LL: " + locationLevelId + " L: " + locationId + " PL: " + 
				householdDto.getProductLevelId()  + " P: " + householdDto.getProductId() + " HC: " + 
				householdDto.getHouseholdCount() + " TR: " + householdDto.getTotalRevenue() + " TM: " + 
				householdDto.getTotalMovement() + " AS: " + householdDto.getAverageSpend() + " AS: " + 
				householdDto.getAverageMovement());*/
			
			psmt.setDouble(1, householdDto.getHouseholdCount());
			psmt.setDouble(2, householdDto.getTotalRevenue());
			psmt.setDouble(3, householdDto.getTotalMovement());
			psmt.setDouble(4, householdDto.getAverageSpend());
			psmt.setDouble(5, householdDto.getAverageMovement());
			
			psmt.setInt(6, calendarId);
			psmt.setInt(7, locationLevelId);
			psmt.setInt(8, locationId);
			
			if( householdDto.getProductLevelId() == 0)
				psmt.setInt(9, -1);
			else
				psmt.setInt(9, householdDto.getProductLevelId());
			
			if( householdDto.getProductId() == 0)
				psmt.setInt(10, -1);
			else
				psmt.setInt(10, householdDto.getProductId());

			psmt.addBatch();
				
		} catch (SQLException sql) {
			 logger.error(" Error while adding the valus into batch ...." , sql);
			 throw new GeneralException(" Error while adding the valus into batch ...." , sql);
		}
	}

	private String updateHouseHoldSql(String calendarType, int locationlevel) {

		StringBuffer sql = new StringBuffer();
		
		if (calendarType.equalsIgnoreCase(Constants.CALENDAR_DAY))
		{
			if (locationlevel == Constants.STORE_LEVEL_ID)
				sql.append(" UPDATE SALES_AGGR_DAILY");
			else
				sql.append(" UPDATE SALES_AGGR_DAILY_ROLLUP");
		}
		else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
		{
			if (locationlevel == Constants.STORE_LEVEL_ID)
				sql.append(" UPDATE SALES_AGGR_WEEKLY");
			else
				sql.append(" UPDATE SALES_AGGR_WEEKLY_ROLLUP");
		}		
		else
		{
			if (locationlevel == Constants.STORE_LEVEL_ID)
				sql.append(" UPDATE SALES_AGGR");
			else
				sql.append(" UPDATE SALES_AGGR_ROLLUP");
		}
		
		sql.append(" SET HOUSEHOLD_COUNT = ?,");
		sql.append(" HOUSEHOLD_REVENUE = ?,");
		sql.append(" HOUSEHOLD_MOVEMENT = ?,");
		sql.append(" HOUSEHOLD_AVERAGE_SPEND = ?,");
		sql.append(" HOUSEHOLD_AVERAGE_UNITS = ?");
		sql.append(" WHERE CALENDAR_ID = ?");
		sql.append(" AND LOCATION_LEVEL_ID = ?");
		sql.append(" AND LOCATION_ID = ? ");
		sql.append(" AND NVL(PRODUCT_LEVEL_ID, -1) = ?");
		
		sql.append(" AND NVL(PRODUCT_ID, -1) = ?");
		logger.debug("UpdateHouseHoldSql: " + sql.toString());
		return sql.toString();
	}

	/*
	 * Method used to get the summarydaily-id with retail calendar actual number...
	 * Argument 1 : Connection 
	 * Argument 2 : Process Year 
	 * @catch SqlException ,Exception
	 * @throws General exception
	 *  
	 */

	public List<SummaryDataDTO> getLastYearMappingDetails(Connection _conn,
			int _processYear, String tableName, String calendarConstant, String byProcessLevel) throws GeneralException {
	
		logger.debug(" Get Last Year "+ tableName +" Id");
		
		List<SummaryDataDTO> returnList = new ArrayList<SummaryDataDTO>();
		
		try{
			
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT RA.ACTUAL_NO,SA.LOCATION_ID");
		sql.append(" ,SA.LOCATION_LEVEL_ID");
		sql.append(" ,SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY"))
		sql.append(" ,SA.SUMMARY_DAILY_ID as LASTYEARID");
		else
		sql.append(" , SA.").append(tableName).append("_ID as LASTYEARID");
		sql.append(" from  "+tableName+"  SA"); 
		sql.append(" inner join RETAIL_CALENDAR RA ON SA.CALENDAR_ID= RA.CALENDAR_ID");
		sql.append(" where RA.CAL_YEAR=").append(_processYear);
		sql.append(" AND RA.row_type='"+calendarConstant+"'");
		
		// check the process level store or not
		if( byProcessLevel.equalsIgnoreCase("STORE"))
		sql.append(" and PRODUCT_LEVEL_ID is null and PRODUCT_ID is null");
		
		logger.debug("getLastYearMappingDetails SQL:" + sql.toString());
		
		// execute the query....
		CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getLastYearDayLevel");
		
		while( result.next()){
			
			SummaryDataDTO objDto = new SummaryDataDTO();
			objDto.setactualNo(result.getInt("ACTUAL_NO"));
			objDto.setLocationId(result.getInt("LOCATION_ID"));
			objDto.setLocationLevelId(result.getInt("LOCATION_LEVEL_ID"));
			objDto.setlastAggrSalesId(result.getInt("LASTYEARID"));
			
			// Product and product Level last Mapping
			objDto.setProductLevelId(result.getInt("PRODUCT_LEVEL_ID"));
			objDto.setProductId(result.getString("PRODUCT_ID"));
			returnList.add(objDto);			
		}
				
		}catch(Exception exe){
			logger.error(" getLastYearDayLevel error.... " + exe);
			throw new GeneralException(" getLastYearDayLevel error.... " + exe);
		}
		return returnList;
	}

	
	public List<SummaryDataDTO> getLastYearMappingData(Connection _conn,
		int calendarId, int locationLevel, int locationId, String timeLine, String processingLevel) 
												throws GeneralException {
	
		StringBuffer sql = new StringBuffer();
		String tableName;
		
		List<SummaryDataDTO> returnList = new ArrayList<SummaryDataDTO>();
		
		try{
			if (locationLevel == Constants.STORE_LEVEL_ID)
			{
				if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY))	
					tableName = "SALES_AGGR_DAILY";
				else if (timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK))	
					tableName = "SALES_AGGR_WEEKLY";
				else	
					tableName = "SALES_AGGR";
			}
			else
			{
				if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY))	
					tableName = "SALES_AGGR_DAILY_ROLLUP";
				else if (timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK))	
					tableName = "SALES_AGGR_WEEKLY_ROLLUP";
				else	
					tableName = "SALES_AGGR_ROLLUP";
			}				
			
			sql.append("SELECT SA.LOCATION_LEVEL_ID,");
			sql.append(" SA.LOCATION_ID, SA.PRODUCT_LEVEL_ID, SA.PRODUCT_ID,");
			
			if( tableName.equalsIgnoreCase("SALES_AGGR_DAILY"))
				sql.append(" SA.SUMMARY_DAILY_ID as LAST_YEAR_ID");
			else
				sql.append(" SA.").append(tableName).append("_ID as LAST_YEAR_ID");
		
			sql.append(" FROM  ").append(tableName).append(" SA"); 
			
			if (locationLevel == Constants.STORE_LEVEL_ID)
				sql.append(" JOIN COMPETITOR_STORE CS ON SA.LOCATION_ID = CS.COMP_STR_ID");
			
			sql.append(" WHERE SA.CALENDAR_ID=").append(calendarId);
			
			if (locationLevel == Constants.STORE_LEVEL_ID)
				sql.append(" AND CS.DISTRICT_ID = ").append(locationId);
			else
				sql.append(" AND LOCATION_LEVEL_ID=").append(locationLevel).append(" AND LOCATION_ID=").append(locationId);
			
			if (processingLevel.equalsIgnoreCase("STORE"))
				sql.append(" AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL");
			else
				sql.append(" AND PRODUCT_LEVEL_ID IS NOT NULL AND PRODUCT_ID IS NOT NULL");
			
		logger.debug("getLastYearMappingData SQL:" + sql.toString());
		
		// execute the query....
		CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getLastYearMappingData");
		
		while( result.next()){
			SummaryDataDTO objDto = new SummaryDataDTO();
			objDto.setLocationId(result.getInt("LOCATION_ID"));
			objDto.setLocationLevelId(result.getInt("LOCATION_LEVEL_ID"));
			objDto.setProductLevelId(result.getInt("PRODUCT_LEVEL_ID"));
			objDto.setProductId(result.getString("PRODUCT_ID"));			
			objDto.setlastAggrSalesId(result.getInt("LAST_YEAR_ID"));
			returnList.add(objDto);			
		}
				
		}catch(Exception exe){
			logger.error(" getLastYearMappingData error.... " + exe);
			throw new GeneralException(" getLastYearMappingData error.... " + exe);
		}
		return returnList;
	}
	
	
	
	public List<RetailCalendarDTO> getProcessingCalendarId(Connection _conn,
						int _processYear, int locationLevel, int locationId, 
									String timeLine) throws GeneralException {
	
		List<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
		
		try{
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT SA.CALENDAR_ID,");
			sb.append(" LC.CALENDAR_ID AS LAST_CALENDAR_ID");
			
			
			// Logic to decide target table
			if (locationLevel == Constants.STORE_LEVEL_ID){
				if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY))
					sb.append(" FROM SALES_AGGR_DAILY SA");
				else if (timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK))
					sb.append(" FROM SALES_AGGR_WEEKLY SA");
				else
					sb.append(" FROM SALES_AGGR SA");
			}
			else{
				if (timeLine.equalsIgnoreCase(Constants.CALENDAR_DAY))
					sb.append(" FROM SALES_AGGR_DAILY_ROLLUP SA");
				else if (timeLine.equalsIgnoreCase(Constants.CALENDAR_WEEK))
					sb.append(" FROM SALES_AGGR_WEEKLY_ROLLUP SA");
				else
					sb.append(" FROM SALES_AGGR_ROLLUP SA");
			}
			
			// Condition to get calendar id only fro target stores
			if (locationLevel == Constants.STORE_LEVEL_ID)
				sb.append(" JOIN COMPETITOR_STORE CS ON SA.LOCATION_ID = CS.COMP_STR_ID");
			
			// Join get calendar Id for processing year
			sb.append(" JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = SA.CALENDAR_ID");
			sb.append(" AND RC.ROW_TYPE = '" + timeLine + "'");
			
			// Join for last year
			sb.append(" JOIN RETAIL_CALENDAR LC");
			sb.append(" ON LC.START_DATE = RC.START_DATE - 364");
			sb.append(" AND LC.ROW_TYPE = RC.ROW_TYPE");
			
			sb.append(" WHERE RC.CAL_YEAR = " + _processYear);
			
			if (locationLevel == Constants.STORE_LEVEL_ID)
				sb.append(" AND SA.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + " AND CS.DISTRICT_ID = " + locationId);
			else
				sb.append(" AND SA.LOCATION_LEVEL_ID = " + locationLevel + " AND SA.LOCATION_ID = " + locationId);
			
			sb.append(" AND SA.PRODUCT_LEVEL_ID IS NULL");
			
		logger.debug("getProcessingCalendarId SQL:" + sb.toString());
		
		// execute the query....
		CachedRowSet result = PristineDBUtil.executeQuery(_conn, sb, "getProcessingCalendarId");
		
		RetailCalendarDTO calDto = new RetailCalendarDTO();
		
		while( result.next()){
			calDto = new RetailCalendarDTO();
			calDto.setCalendarId(result.getInt("CALENDAR_ID"));
			calDto.setlstCalendarId(result.getInt("LAST_CALENDAR_ID"));
			calendarList.add(calDto);
		}
				
		}catch(Exception exe){
			logger.error(" getProcessingCalendarId error.... " + exe);
			throw new GeneralException(" getProcessingCalendarId error.... " + exe);
		}
		return calendarList;
	}	
}
