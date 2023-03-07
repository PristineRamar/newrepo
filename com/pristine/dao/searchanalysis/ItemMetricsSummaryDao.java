package com.pristine.dao.searchanalysis;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.ProductMetricsDataDTO;

import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
@SuppressWarnings("unused")
public class ItemMetricsSummaryDao {
	
	static Logger logger = Logger.getLogger("SearchItemDaily");
	static String competitiveDataScheduleSQL = "";
	static String competitiveDataStoreDateSQL = "";
	static String getSchedulesForCalendarSQL = "";
	static String insertItemMetricSummaryWeeklyComp = "";
	private  final int commitCount = Constants.LIMIT_COUNT;
	
	
	private static final String DELETE_IMS_BY_ITEM = "DELETE FROM ITEM_METRIC_SUMMARY_WEEKLY WHERE CALENDAR_ID = ? AND "
			+ " LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?"; 
	
	public ItemMetricsSummaryDao(){
		String sqlFileName = "sql/SQLDefinitions.xml";
		 Properties properties = new Properties();
		 InputStream is = this.getClass().getClassLoader().getResourceAsStream(sqlFileName);
		 try {
			properties.loadFromXML(is);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 competitiveDataScheduleSQL = properties.getProperty("competitiveDataScheduleSQL");
		 competitiveDataStoreDateSQL = properties.getProperty("competitiveDataStoreDateSQL");
		 getSchedulesForCalendarSQL = properties.getProperty("getSchedulesForCalendarSQL");
		 insertItemMetricSummaryWeeklyComp = properties.getProperty("insertItemMetricSummaryWeeklyComp");
	}

	/*
	 * ****************************************************************
	 * Method used to delete the previous item summary data
	 * Argument 1 : _Conn
	 * Argument 2 : calendarId
	 * Argument 3 : locationId
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */
	public void deletePreviousItemMetrics(Connection _Conn, int calendarId, 
														int locationId) throws GeneralException {
		
		// check if records exist, if it exists then delete, else do not execute delete statement
		
		StringBuffer sql = new StringBuffer();
		sql.append("Select count(*) from item_metric_summary where calendar_id = '"+ calendarId +"'");
	    sql.append(" and LOCATION_ID = '" + locationId + "'");
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,"getItemDaily");
			if(result !=null){
				result.next();
				int count = result.getInt(1);
				if(count==0){
					return;
				}
			}
		
		sql = new StringBuffer();
	    sql.append(" Delete from ITEM_METRIC_SUMMARY WHERE CALENDAR_ID = '" + calendarId +"'");
	    sql.append(" and LOCATION_ID = '" + locationId + "'");
		logger.debug("deletePreviousItemDaily SQL:" +sql.toString());
		
			// execute the delete query
			PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousItemDaily");
		} catch (GeneralException e) {
			 logger.error("Error while deleting previous item summary" + e);
			throw new GeneralException("deletePreviousItemDaily", e);		
		}
		catch (SQLException e) {
			 logger.error("Error while deleting previous item summary" + e);
			throw new GeneralException("deletePreviousItemDaily", e);		
		}
	}

	
	public void deletePreviousItemMetricsComp(Connection _Conn, int calendarId, 
			int locationId) throws GeneralException {

		// TODO check if records exist before executing the delete query
		
		StringBuffer sql = new StringBuffer();
		sql.append("Select count(*) from item_metric_summary_comp where calendar_id = '"+ calendarId +"'");
	    sql.append(" and LOCATION_ID = '" + locationId + "'");
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,"getItemDailyComp");
			if(result !=null){
				result.next();
				int count = result.getInt(1);
				if(count==0){
					return;
				}
			}
		
		sql = new StringBuffer();

		sql.append("delete from item_metric_summary_comp  " +
				"where location_id = "+locationId +
						" and calendar_id = "+calendarId+
		"");
		logger.debug("deletePreviousItemDailyCompSQL:" +sql.toString());

			// 	execute the delete query
			PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousItemDailyComp");
		} catch (GeneralException e) {
			logger.error("Error while deleting previous item summary" + e);
			throw new GeneralException("deletePreviousItemDailyComp", e);		
		}
		catch (SQLException e) {
			 logger.error("Error while deleting previous item summary" + e);
			throw new GeneralException("deletePreviousItemDaily", e);		
		}
	}
	
	public void deletePreviousItemMetricsComp(Connection _Conn,  
			int scheduleId) throws GeneralException {

		StringBuffer sql = new StringBuffer();
		sql.append("  select count(*) from item_metric_summary_comp  " +
				"where location_id = (select comp_str_id from schedule where schedule_id = "+scheduleId+")  " +
				"and calendar_id =   (select retail_calendar.calendar_id from schedule,retail_calendar " +
					"where schedule_id = "+scheduleId+" and retail_calendar.start_date = schedule.start_date " +
					"and row_type ='W')");
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,"getItemDailyComp");
			if(result !=null){
				result.next();
				int count = result.getInt(1);
				if(count==0){
					return;
				}
			}
		
		sql = new StringBuffer();

		sql.append("  delete from item_metric_summary_comp  " +
				"where location_id = (select comp_str_id from schedule where schedule_id = "+scheduleId+")  " +
				"and calendar_id =   (select retail_calendar.calendar_id from schedule,retail_calendar " +
					"where schedule_id = "+scheduleId+" and retail_calendar.start_date = schedule.start_date " +
					"and row_type ='W')");
		logger.debug("deletePreviousItemDailyCompSQL:" +sql.toString());

			// 	execute the delete query
			PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousItemDailyComp");
		} catch (GeneralException e) {
			logger.error("Error while deleting previous item summary" + e);
			throw new GeneralException("deletePreviousItemDailyComp", e);		
		}
		catch (SQLException e) {
			 logger.error("Error while deleting previous item summary" + e);
			throw new GeneralException("deletePreviousItemDaily", e);		
		}
	}


	
	/*
	 * Method to extract from Competitor data, into Daily Item metric summary
	 * 
	 *   Input Date, store no
	 * 
	 *   Uses competitive_data, schedule, retail_calendar tables and directly
	 *   populates item_metric_summary using the following query.
	 *   
	 *   location level 5
	 *   product level 1 
	 *   
	 *   select ITEM_METRIC_SUMMARY_COMP_SEQ.NextVal,calendar_id,
  				5 location_level_id,
    			competitor_store.comp_str_id,
  				item_code as product_id,
  				1 product_level_id,
   				(case when sale_price > 0 then 'Y' else 'N' end) sale_flag,
  				reg_price, 
  				reg_m_pack reg_multiple_pack,
  				reg_m_price reg_multiple_price,
  				sale_price , 
  				sale_m_pack sale_multiple_pack, 
  				sale_m_price sale_multiple_price,
				(case when sale_price < reg_price then sale_price else reg_price) final_price
  		from competitive_data, schedule, competitor_store  , retail_calendar
  		where competitive_data.schedule_id = schedule.schedule_id 
  				and schedule.start_date  <= to_date(<DATE>,'MM/DD/YYYY') and to_date(<DATE>,'MM/DD/YYYY') <= schedule.end_date
  				and competitor_store.comp_str_no = <COMP STORE NO>
  				and schedule.comp_str_id = competitor_store.comp_str_id
  				and schedule.start_date = retail_calendar.start_date 
  				and retail_calendar.row_type = 'W'
	 * 
	 */
	public boolean InsertItemMetricsDataComp (Connection conn, int calendarId, int locationId) 
			throws GeneralException {						
			
					PreparedStatement psmt= null;
					logger.debug("InsertItemMetricsDataComp : Begin insert into db");
					try{				    	   
/*
 *   OLD stff needs purging
						String sql = new String();
						sql="INSERT into item_metric_summary_comp(" +
								"item_metric_summary_comp_id, calendar_id,location_level_id," +
								"location_id,product_id,product_level_id,sale_flag,reg_price," +
								"reg_multiple_pack,reg_multiple_price,sale_price," +
								"sale_multiple_pack,sale_multiple_price,final_price)" +
								"" +
							"SELECT ITEM_METRIC_SUMMARY_COMP_SEQ.NextVal," +
								"calendar_id, 5 location_level_id, schedule.comp_str_id," +
								" item_code as product_id,  1 product_level_id,   " +
								"(case when sale_price > 0 then 'Y' else 'N' end) sale_flag,  " +
								"reg_price,  reg_m_pack reg_multiple_pack,  reg_m_price reg_multiple_price,  " +
								"sale_price ,  sale_m_pack sale_multiple_pack,  sale_m_price sale_multiple_price,  " +
								"(case when sale_price < reg_price then sale_price else reg_price end) final_price  " +
								"from competitive_data, schedule, competitor_store  , retail_calendar  " +
							"WHERE competitive_data.schedule_id = schedule.schedule_id  " +
							"and schedule.comp_str_id = "+locationId+"  " +
								"and schedule.start_date = retail_calendar.start_date  " +
								"and retail_calendar.calendar_id = "+calendarId;
						
							psmt=conn.prepareStatement(sql);
							*/ 
						psmt=conn.prepareStatement(competitiveDataStoreDateSQL);
						psmt.setInt(1, locationId);
						psmt.setInt(2, calendarId);
						psmt.setInt(3, locationId);
						psmt.setInt(4, calendarId);
							
				        	int count = psmt.executeUpdate();
				        	PristineDBUtil.commitTransaction(conn, "InsertItemMetricsDataComp");
							logger.debug("InsertItemMetricsDataComp:End Execute insert into db");
			        		logger.debug("InsertItemMetricsDataComp:The number of records inserted: "+count);
					}catch (SQLException e)
					{
						logger.error("Error in InsertItemMetricsDataComp - " + e);
						throw new GeneralException("Error", e);
					}finally{
						PristineDBUtil.close(psmt);
					}
						return true;
			}


	/*
	 * Method to extract from Competitor data, into Daily Item metric summary 
	 * 
	 *   input schedule Id
	 * 
	 *   Uses competitive_data, schedule, retail_calendar tables and directly
	 *   populates item_metric_summary using the following query.
	 *   
	 *   location level 5
	 *   product level 1 
	 *   
	 *   select ITEM_METRIC_SUMMARY_COMP_SEQ.NextVal,calendar_id,
  			5 localtion_level_id,
    		competitor_store.comp_str_id,
  			item_code as product_id,
  			1 product_level_id,
   			(case when sale_price > 0 then 'Y' else 'N' end) sale_flag,
  			reg_price, 
  			reg_m_pack reg_multiple_pack,
  			reg_m_price reg_multiple_price,
  			sale_price , 
  			sale_m_pack sale_multiple_pack, 
  			sale_m_price sale_multiple_price,
  			(case when sale_price < reg_price then sale_price else reg_price) final_price
  		from competitive_data, schedule, competitor_store  , retail_calendar
  		where competitive_data.schedule_id = schedule.schedule_id 
  			and schedule.schedule_id = <ScheduleId> 
  			and schedule.comp_str_id = competitor_store.comp_str_id
  			and schedule.start_date = retail_calendar.start_date 
  			and retail_calendar.row_type = 'W'
	 * 
	 */
	public boolean InsertItemMetricsDataComp (Connection conn, int scheduleId) 
			throws GeneralException {						
			
					PreparedStatement psmt= null;
					logger.debug("InsertItemMetricsDataComp : Begin insert into db");
					try{				    	   
						
						/*  OLD
						  
						  String sql = new String();
						 
						sql="INSERT into item_metric_summary_comp(" +
								"item_metric_summary_comp_id, calendar_id,location_level_id," +
								"location_id,product_id,product_level_id,sale_flag,reg_price," +
								"reg_multiple_pack,reg_multiple_price,sale_price," +
								"sale_multiple_pack,sale_multiple_price,final_price)" +
								"" +
							"SELECT ITEM_METRIC_SUMMARY_COMP_SEQ.NextVal," +
								"calendar_id,  5 localtion_level_id,    competitor_store.comp_str_id,  " +
								"item_code as product_id,  1 product_level_id,   " +
								"(case when sale_price > 0 then 'Y' else 'N' end) sale_flag,  " +
								"reg_price,  reg_m_pack reg_multiple_pack,  reg_m_price reg_multiple_price,  " +
								"sale_price ,  sale_m_pack sale_multiple_pack,  sale_m_price sale_multiple_price,  " +
								"(case when sale_price < reg_price then sale_price else reg_price end) final_price  " +
							"FROM competitive_data, schedule, competitor_store  , retail_calendar  " +
							"WHERE competitive_data.schedule_id = schedule.schedule_id  " +
								"and schedule.schedule_id = "+scheduleId +
								"and schedule.comp_str_id = competitor_store.comp_str_id  " +
								"and schedule.start_date = retail_calendar.start_date  " +
								"and retail_calendar.row_type = 'W'";
						
							psmt=conn.prepareStatement(sql);
							
							*/
							psmt=conn.prepareStatement(competitiveDataScheduleSQL);
							psmt.setInt(1, scheduleId);

							
							
				        	int count = psmt.executeUpdate();
				        	PristineDBUtil.commitTransaction(conn, "InsertItemMetricsDataComp");
							logger.debug("InsertItemMetricsDataComp:End Execute insert into db");
			        		logger.debug("InsertItemMetricsDataComp:The number of records inserted: "+count);
					}catch (SQLException e)
					{
						logger.error("Error in InsertItemMetricsDataComp - " + e);
						throw new GeneralException("Error", e);
					}finally{
						PristineDBUtil.close(psmt);
					}
						return true;
			}
	
	
	
	public boolean InsertItemMetricsData (Connection conn, 
		List<ProductMetricsDataDTO> movementDataList, int calendar_id, 
							int locationId) throws GeneralException {						
				
				PreparedStatement psmt= null;
				logger.debug("Begin insert into db");
				try{				    	    
			        psmt=conn.prepareStatement(InsertSql());
					
					for(ProductMetricsDataDTO summaryDto: movementDataList){
						int counter = 0;

//						logger.debug("Processing item code:" + summaryDto.getProductId());

						psmt.setInt(++counter, Constants.STORE_LEVEL_ID);
						psmt.setInt(++counter, locationId);
						psmt.setInt(++counter, Constants.ITEMLEVELID);
						psmt.setDouble(++counter, summaryDto.getProductId());
						psmt.setInt(++counter, calendar_id);
						psmt.setString(++counter, summaryDto.isSaleFlag());
						psmt.setDouble(++counter, summaryDto.getPromoId());
						psmt.setDouble(++counter, summaryDto.getRegularMPack());
						psmt.setDouble(++counter, summaryDto.getRegularMPrice());
						psmt.setDouble(++counter, summaryDto.getRegularPrice());
						psmt.setDouble(++counter, summaryDto.getListPrice());
						psmt.setDouble(++counter, summaryDto.getRegularRevenue());
						psmt.setDouble(++counter, summaryDto.getRegularMarginPct());
						psmt.setDouble(++counter, summaryDto.getRegularMovement());
						psmt.setDouble(++counter, summaryDto.getSaleMPack());
						psmt.setDouble(++counter, summaryDto.getSaleMPrice());
						psmt.setDouble(++counter, summaryDto.getSalePrice());
						psmt.setDouble(++counter, summaryDto.getDealPrice());
						psmt.setDouble(++counter, summaryDto.getSaleRevenue());
						psmt.setDouble(++counter, summaryDto.getNetMargin());
						psmt.setDouble(++counter, summaryDto.getNetMarginPct());
						psmt.setDouble(++counter, summaryDto.getSaleMovement());
						psmt.setDouble(++counter, summaryDto.get13WeekMovementAvg());
						psmt.setDouble(++counter, summaryDto.getTotalVisits());
						psmt.setDouble(++counter, summaryDto.getAvgOrderSize());
						psmt.setDouble(++counter, summaryDto.getFinalPrice());
						psmt.setDouble(++counter, summaryDto.getFinalCost());
						
						psmt.addBatch();
			        	
			        }
					logger.debug("Begin Execute insert into db");
			        	int[] count = psmt.executeBatch();
			        	PristineDBUtil.commitTransaction(conn, "InsertItemMetricsData");
			        	psmt.clearBatch();
						logger.debug("End Execute insert into db");
		        		logger.debug("The number of records inserted: "+count.length);
				}catch (SQLException e)
				{
					logger.error("Error in InsertItemMetricsData - " + e);
					throw new GeneralException("Error", e);
				}finally{
					PristineDBUtil.close(psmt);
				}
					return true;
		}

	/*
	 * ****************************************************************
	 * Method returns the Summary Search Daily Insert Query
	 * ****************************************************************
	 */

	private String InsertSql() {

		StringBuffer sql = new StringBuffer();
		sql.append("insert into ITEM_METRIC_SUMMARY (ITEM_METRIC_SUMMARY_ID, ");
		sql.append("LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, ");
		sql.append("PRODUCT_ID, CALENDAR_ID, SALE_FLAG, PROMOTION_ID, ");
		sql.append("REG_MULTIPLE_PACK, REG_MULTIPLE_PRICE, REG_PRICE, LIST_COST, ");
		sql.append("REG_REVENUE, REG_MARGIN_PCT, REG_MOVEMENT,");
		sql.append("SALE_MULTIPLE_PACK, SALE_MULTIPLE_PRICE, SALE_PRICE, DEAL_COST, ");
		sql.append("SALE_REVENUE, NET_MARGIN, NET_MARGIN_PCT, SALE_MOVEMENT,");
		sql.append(" AVG_13_WEEK_MOVEMENT, TOT_VISIT_CNT, AVG_ORDER_SIZE, FINAL_PRICE,FINAL_COST )");
		sql.append(" values (ITEM_METRIC_SUMMARY_SEQ.NEXTVAL,");
		sql.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		return sql.toString();
	}
	
	public void insertItemMetricsSummaryWeekly(Connection conn, List<ProductMetricsDataDTO> metricsDtoList, int batchUpdateCount) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(InsertWeeklySql());
			int itemNoInBatch = 0;
			for(ProductMetricsDataDTO metricsDto : metricsDtoList){
				int counter = 0;
				statement.setInt(++counter, metricsDto.getCalendarId());
				statement.setInt(++counter, metricsDto.getLocationLevelId());
				statement.setInt(++counter, metricsDto.getLocationId());
				statement.setInt(++counter, metricsDto.getProductLevelId());
				statement.setInt(++counter, metricsDto.getProductId());
				statement.setString(++counter, metricsDto.isSaleFlag());
				statement.setDouble(++counter, metricsDto.getRegularPrice());
				statement.setDouble(++counter, metricsDto.getRegularQuantity());
				statement.setDouble(++counter, metricsDto.getSalePrice());
				statement.setDouble(++counter, metricsDto.getSaleQuantity());
				statement.setDouble(++counter, metricsDto.getFinalPrice());
				statement.setDouble(++counter, metricsDto.getListPrice());
				statement.setDouble(++counter, metricsDto.getDealPrice());
				statement.setDouble(++counter, metricsDto.getFinalCost());
				statement.setDouble(++counter, metricsDto.getRegularRevenue());
				statement.setDouble(++counter, metricsDto.getSaleRevenue());
				statement.setDouble(++counter, metricsDto.getTotalRevenue());
				statement.setDouble(++counter, metricsDto.getRegularMovement());
				statement.setDouble(++counter, metricsDto.getSaleMovement());
				statement.setDouble(++counter, metricsDto.getTotalMovement());
				statement.setDouble(++counter, metricsDto.getRegularMargin());
				statement.setDouble(++counter, metricsDto.getRegularMarginPct());
				statement.setDouble(++counter, metricsDto.getNetMargin());
				statement.setDouble(++counter, metricsDto.getNetMarginPct());
				logger.debug(metricsDto.getCalendarId() + " " + metricsDto.getLocationLevelId() + " " + metricsDto.getLocationId() + " " + metricsDto.getProductLevelId() + " " + metricsDto.getProductId() + " " + metricsDto.isSaleFlag() + " " + 
						metricsDto.getRegularPrice() + " " + metricsDto.getRegularQuantity() + " " + metricsDto.getSalePrice() + " " + metricsDto.getSaleQuantity() + " " + metricsDto.getFinalPrice() + " " + metricsDto.getListPrice() + " " + metricsDto.getDealPrice() + " " +
						metricsDto.getFinalCost() + " " + metricsDto.getRegularRevenue() + " " + metricsDto.getSaleRevenue() + " " + metricsDto.getRegularMovement() + " " + metricsDto.getSaleMovement() + " " + metricsDto.getRegularMargin() + " " +
						metricsDto.getRegularMarginPct() + " " + metricsDto.getSaleMargin() + " " + metricsDto.getSaleMarginPct());
				//statement.executeUpdate();
				statement.addBatch();
				itemNoInBatch++;
				
				if(itemNoInBatch % batchUpdateCount == 0){
					int count[] = statement.executeBatch();
					itemNoInBatch = 0;
					statement.clearBatch();
				}
			}
			
			if(itemNoInBatch > 0){
				int count[] = statement.executeBatch();
				itemNoInBatch = 0;
				statement.clearBatch();
			}
		}catch(SQLException ex){
			logger.error("Error when insert in item metrics summary weekly - " + ex.getMessage());
			throw new GeneralException("Exception in getBaseItemDetails() " + ex);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	public void deleteItemMetricsSummaryWeeklyByItem(Connection conn, List<ProductMetricsDataDTO> metricsDtoList,
			int batchUpdateCount) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_IMS_BY_ITEM);
			int itemNoInBatch = 0;
			for (ProductMetricsDataDTO metricsDto : metricsDtoList) {
				int counter = 0;
				statement.setInt(++counter, metricsDto.getCalendarId());
				statement.setInt(++counter, metricsDto.getLocationLevelId());
				statement.setInt(++counter, metricsDto.getLocationId());
				statement.setInt(++counter, metricsDto.getProductLevelId());
				statement.setInt(++counter, metricsDto.getProductId());
				// statement.executeUpdate();
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % batchUpdateCount == 0) {
					int count[] = statement.executeBatch();
					itemNoInBatch = 0;
					statement.clearBatch();
				}
			}

			if (itemNoInBatch > 0) {
				int count[] = statement.executeBatch();
				itemNoInBatch = 0;
				statement.clearBatch();
			}
		} catch (SQLException ex) {
			throw new GeneralException("deleteItemMetricsSummaryWeeklyByItem() - Error while deleting IMS data", ex);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	

	public void insertItemMetricsSummaryWeeklyV2(Connection conn, List<ProductMetricsDataDTO> metricsDtoList, int batchUpdateCount){
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(InsertWeeklySqlV2());
			int itemNoInBatch = 0;
			for(ProductMetricsDataDTO metricsDto : metricsDtoList){
				int counter = 0;
				statement.setInt(++counter, metricsDto.getCalendarId());
				statement.setInt(++counter, metricsDto.getLocationLevelId());
				statement.setInt(++counter, metricsDto.getLocationId());
				statement.setInt(++counter, metricsDto.getProductLevelId());
				statement.setInt(++counter, metricsDto.getProductId());
				statement.setString(++counter, metricsDto.isSaleFlag());
				statement.setDouble(++counter, metricsDto.getRegularPrice());
				statement.setDouble(++counter, metricsDto.getRegularMPack());
				statement.setDouble(++counter, metricsDto.getRegularMPrice());
				statement.setDouble(++counter, metricsDto.getSalePrice());
				statement.setDouble(++counter, metricsDto.getSaleMPack());
				statement.setDouble(++counter, metricsDto.getSaleMPrice());
				statement.setDouble(++counter, metricsDto.getFinalPrice());
				statement.setDouble(++counter, metricsDto.getListPrice());
				statement.setDouble(++counter, metricsDto.getDealPrice());
				statement.setDouble(++counter, metricsDto.getFinalCost());
				statement.setDouble(++counter, metricsDto.getRegularRevenue());
				statement.setDouble(++counter, metricsDto.getSaleRevenue());
				statement.setDouble(++counter, metricsDto.getTotalRevenue());
				statement.setDouble(++counter, metricsDto.getRegularMovement());
				statement.setDouble(++counter, metricsDto.getSaleMovement());
				statement.setDouble(++counter, metricsDto.getTotalMovement());
				statement.setDouble(++counter, metricsDto.getRegularMargin());
				statement.setDouble(++counter, metricsDto.getRegularMarginPct());
				statement.setDouble(++counter, metricsDto.getNetMargin());
				statement.setDouble(++counter, metricsDto.getNetMarginPct());
				statement.setDouble(++counter, metricsDto.getAvgOrderSize());
				statement.setDouble(++counter, metricsDto.getTotalVisits());
				statement.setDouble(++counter, metricsDto.getSaleMargin());
				logger.debug(metricsDto.getCalendarId() + " " + metricsDto.getLocationLevelId() 
				+ " " + metricsDto.getLocationId() + " " + metricsDto.getProductLevelId() 
				+ " " + metricsDto.getProductId() + " " + metricsDto.isSaleFlag() + " " + 
						metricsDto.getRegularPrice() + " " + metricsDto.getRegularMPack() 
						+ " " + metricsDto.getRegularMPrice() + " " + metricsDto.getSalePrice() 
						+ " " + metricsDto.getSaleMPack() + " " + metricsDto.getSaleMPrice() 
						+ " " + metricsDto.getFinalPrice() + " " + metricsDto.getListPrice() 
						+ " " + metricsDto.getDealPrice() + " " +
						metricsDto.getFinalCost() + " " + metricsDto.getRegularRevenue() 
						+ " " + metricsDto.getSaleRevenue() + " " + metricsDto.getTotalRevenue() 
						+ " " + metricsDto.getRegularMovement() + " " + metricsDto.getSaleMovement() 
						+ " " + metricsDto.getTotalMovement() + " " + metricsDto.getRegularMargin() + " " +
						metricsDto.getRegularMarginPct() + " " + metricsDto.getNetMargin() 
						+ " " + metricsDto.getNetMarginPct() + " " + metricsDto.getAvgOrderSize() 
						+ " " + metricsDto.getTotalVisits() + " " + metricsDto.getSaleMargin());
			
				
				
				
				
				//statement.executeUpdate();
				statement.addBatch();
				itemNoInBatch++;
				
				if(itemNoInBatch % batchUpdateCount == 0){
					logger.debug("Executing batch... Count: " + itemNoInBatch);
					int count[] = statement.executeBatch();
					itemNoInBatch = 0;
					statement.clearBatch();
				}
			}
			
			if(itemNoInBatch > 0){
				logger.debug("Executing batch... Count: " + itemNoInBatch);
				int count[] = statement.executeBatch();
				itemNoInBatch = 0;
				statement.clearBatch();
			}
		}catch(SQLException ex){
			logger.error("Error when insert in item metrics summary weekly - " + ex.getMessage());
			ex.printStackTrace();
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	public void insertItemMetricsSummaryWeeklyZone(Connection conn, List<ProductMetricsDataDTO> metricsDtoList, int batchUpdateCount){
		PreparedStatement statement = null;
		try{
			logger.info("insertItemMetricsSummaryWeeklyZone() -# items to insert: " + metricsDtoList.size() );
			statement = conn.prepareStatement(InsertWeeklySqlZone());
			int itemNoInBatch = 0;
			for(ProductMetricsDataDTO metricsDto : metricsDtoList){
				int counter = 0;
				statement.setInt(++counter, metricsDto.getCalendarId());
				statement.setInt(++counter, metricsDto.getLocationLevelId());
				statement.setInt(++counter, metricsDto.getLocationId());
				statement.setInt(++counter, metricsDto.getProductLevelId());
				statement.setInt(++counter, metricsDto.getProductId());
				statement.setString(++counter, metricsDto.isSaleFlag());
				statement.setDouble(++counter, metricsDto.getRegularPrice());
				statement.setDouble(++counter, metricsDto.getRegularMPack());
				statement.setDouble(++counter, metricsDto.getRegularMPrice());
				statement.setDouble(++counter, metricsDto.getSalePrice());
				statement.setDouble(++counter, metricsDto.getSaleMPack());
				statement.setDouble(++counter, metricsDto.getSaleMPrice());
				statement.setDouble(++counter, metricsDto.getFinalPrice());
				statement.setDouble(++counter, metricsDto.getListPrice());
				statement.setDouble(++counter, metricsDto.getDealPrice());
				statement.setDouble(++counter, metricsDto.getFinalCost());
				statement.setDouble(++counter, metricsDto.getRegularRevenue());
				statement.setDouble(++counter, metricsDto.getSaleRevenue());
				statement.setDouble(++counter, metricsDto.getTotalRevenue());
				statement.setDouble(++counter, metricsDto.getRegularMovement());
				statement.setDouble(++counter, metricsDto.getSaleMovement());
				statement.setDouble(++counter, metricsDto.getTotalMovement());
				statement.setDouble(++counter, metricsDto.getRegularMargin());
				statement.setDouble(++counter, metricsDto.getRegularMarginPct());
				statement.setDouble(++counter, metricsDto.getNetMargin());
				statement.setDouble(++counter, metricsDto.getNetMarginPct());
				statement.setDouble(++counter, metricsDto.getAvgOrderSize());
				statement.setDouble(++counter, metricsDto.getTotalVisits());
				statement.setDouble(++counter, metricsDto.getSaleMargin());
				statement.setDouble(++counter, metricsDto.getGrossUnits());
				statement.setDouble(++counter, metricsDto.getGrossSales());
//				logger.debug(metricsDto.getCalendarId() + " " + metricsDto.getLocationLevelId() 
//				+ " " + metricsDto.getLocationId() + " " + metricsDto.getProductLevelId() 
//				+ " " + metricsDto.getProductId() + " " + metricsDto.isSaleFlag() + " " + 
//						metricsDto.getRegularPrice() + " " + metricsDto.getRegularMPack() 
//						+ " " + metricsDto.getRegularMPrice() + " " + metricsDto.getSalePrice() 
//						+ " " + metricsDto.getSaleMPack() + " " + metricsDto.getSaleMPrice() 
//						+ " " + metricsDto.getFinalPrice() + " " + metricsDto.getListPrice() 
//						+ " " + metricsDto.getDealPrice() + " " +
//						metricsDto.getFinalCost() + " " + metricsDto.getRegularRevenue() 
//						+ " " + metricsDto.getSaleRevenue() + " " + metricsDto.getTotalRevenue() 
//						+ " " + metricsDto.getRegularMovement() + " " + metricsDto.getSaleMovement() 
//						+ " " + metricsDto.getTotalMovement() + " " + metricsDto.getRegularMargin() + " " +
//						metricsDto.getRegularMarginPct() + " " + metricsDto.getNetMargin() 
//						+ " " + metricsDto.getNetMarginPct() + " " + metricsDto.getAvgOrderSize() 
//						+ " " + metricsDto.getTotalVisits() + " " + metricsDto.getSaleMargin());
			
				
				
				
				
				//statement.executeUpdate();
				statement.addBatch();
				itemNoInBatch++;
				
				if(itemNoInBatch % batchUpdateCount == 0){
					logger.debug("Executing batch... Count: " + itemNoInBatch);
					int count[] = statement.executeBatch();
					itemNoInBatch = 0;
					statement.clearBatch();
				}
			}
			
			if(itemNoInBatch > 0){
				logger.debug("Executing batch... Count: " + itemNoInBatch);
				int count[] = statement.executeBatch();
				itemNoInBatch = 0;
				statement.clearBatch();
			}
		}catch(SQLException ex){
			logger.error("Error when insert in item metrics summary weekly - " + ex.getMessage());
			ex.printStackTrace();
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	private String InsertWeeklySql() {
		StringBuffer sql = new StringBuffer();
		sql.append("insert into ITEM_METRIC_SUMMARY_WEEKLY (ITEM_METRIC_SUMMARY_WEEKLY_ID, ");
		sql.append("CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, ");
		sql.append("SALE_FLAG, REG_PRICE, REG_M_PACK, SALE_PRICE, SALE_M_PACK, FINAL_PRICE, LIST_COST, DEAL_COST, FINAL_COST, ");
		sql.append("REG_REVENUE, SALE_REVENUE, TOT_REVENUE, REG_MOVEMENT, SALE_MOVEMENT, TOT_MOVEMENT, ");
		sql.append("REG_MARGIN, REG_MARGIN_PCT, NET_MARGIN, NET_MARGIN_PCT) ");
		sql.append(" values (ITEM_METRIC_SUMMARY_WEEKLY_SEQ.NEXTVAL,");
		sql.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		return sql.toString();
	}
	
	
	private String InsertWeeklySqlV2() {
		StringBuffer sql = new StringBuffer();
		sql.append("insert into ITEM_METRIC_SUMMARY_WEEKLY (ITEM_METRIC_SUMMARY_WEEKLY_ID, ");
		sql.append("CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, ");
		sql.append("SALE_FLAG, REG_PRICE, REG_M_PACK, REG_M_PRICE, SALE_PRICE, SALE_M_PACK, ");
		sql.append("SALE_M_PRICE, FINAL_PRICE, LIST_COST, DEAL_COST, FINAL_COST, ");
		sql.append("REG_REVENUE, SALE_REVENUE, TOT_REVENUE, REG_MOVEMENT, SALE_MOVEMENT, TOT_MOVEMENT, ");
		sql.append("REG_MARGIN, REG_MARGIN_PCT, NET_MARGIN, NET_MARGIN_PCT, AVG_ORDER_SIZE, TOT_VISIT_CNT, SALE_MARGIN) ");
		sql.append(" values (ITEM_METRIC_SUMMARY_WEEKLY_SEQ.NEXTVAL,");
		sql.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		return sql.toString();
	}
	
	private String InsertWeeklySqlZone() {
		StringBuffer sql = new StringBuffer();
		sql.append("insert into IMS_WEEKLY_ZONE (ITEM_METRIC_SUMMARY_WEEKLY_ID, ");
		sql.append("CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, ");
		sql.append("SALE_FLAG, REG_PRICE, REG_M_PACK, REG_M_PRICE, SALE_PRICE, SALE_M_PACK, ");
		sql.append("SALE_M_PRICE, FINAL_PRICE, LIST_COST, DEAL_COST, FINAL_COST, ");
		sql.append("REG_REVENUE, SALE_REVENUE, TOT_REVENUE, REG_MOVEMENT, SALE_MOVEMENT, TOT_MOVEMENT, ");
		sql.append("REG_MARGIN, REG_MARGIN_PCT, NET_MARGIN, NET_MARGIN_PCT, AVG_ORDER_SIZE, ");
		sql.append("TOT_VISIT_CNT, SALE_MARGIN, GROSS_UNITS, GROSS_REVENUE) ");
		sql.append(" values (IMS_WEEKLY_TEMP_SEQ.NEXTVAL,");
		sql.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		return sql.toString();
	}
	
	public List<Integer> getDistinctStores(Connection _conn,
			int calStartId, int calEndId) throws GeneralException {

		// Return List
		List<Integer> returnList = new ArrayList<Integer>();

		try {
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct LOCATION_ID from ITEM_METRIC_SUMMARY");
			sql.append(" where CALENDAR_ID >= ").append(calStartId);
			sql.append(" AND CALENDAR_ID <= ").append(calEndId);
			logger.debug("getDistinctStoresFromSummary SQL:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getDistinctStores");

			while (result.next()) {

				returnList.add(result.getInt("LOCATION_ID"));
			}

			result.close();

			logger.debug("Store List size:" + returnList.size());
		} catch (Exception e) {
			logger.error(
					" Error while fetching the distinct Store list from Item metric summary ",
					e);
			throw new GeneralException(
					" Error while fetching the distinct Store list from Item metric summary ",
					e);
		} 

		return returnList;
	}

	public void insertWeeklyFromDailySummary(Connection conn, int locationId,
			Integer calStartId, Integer calEndId, Integer calWeekId) throws GeneralException {
		PreparedStatement psmt= null;
		logger.debug("InsertItemMetricsWeekly : Begin insert into db");
		  String sql = new String();
				try{				    	   
			
			  
			 sql = "insert into item_metric_summary_weekly (item_metric_summary_weekly_id,location_level_id,location_id,product_level_id,product_id, calendar_id," +
			 		"sale_flag, promotion_id,  reg_m_pack,reg_m_price,reg_price,list_cost, reg_revenue, reg_margin_pct,reg_movement, " +
			 		"sale_m_pack,sale_m_price,sale_price,deal_cost," +
			 		"sale_revenue,net_margin,   net_margin_pct,sale_movement, tot_visit_cnt, avg_order_size,final_price, final_cost,tot_movement, tot_revenue) " +
			 		"select item_metric_summary_weekly_seq.nextval,location_level_id,location_id,product_level_id,product_id, calendar_id," +
			 		"case when round(sale_flag) = 1 then 'Y' else 'N' end sale_flag, promotion_id,  reg_m_pack,reg_m_price,reg_price,list_cost, " +
			 		"reg_revenue, reg_margin_pct,reg_movement," +
			 		"sale_m_pack,sale_m_price,sale_price,deal_cost," +
			 		"sale_revenue,net_margin,   net_margin_pct,sale_movement, tot_visit_cnt, avg_order_size, " + 
			 		"find_final_price(reg_price, reg_m_price, reg_m_pack, sale_price, sale_m_price, sale_m_pack) final_price, " +
			 		//Commented by Pradeep
			 		//Wrong calculation
			 		/*"(case " +
			 		"when (sale_m_pack > 0 or reg_m_pack > 1 ) and (sale_m_price = 0 ) and (reg_m_pack <> 0) then (reg_price + reg_m_price)/reg_m_pack " +
			 		"when (sale_m_pack > 0 or reg_m_pack > 1) and (reg_price + reg_m_price = 0 ) and (sale_m_pack <> 0)  then sale_m_price/sale_m_pack " +
			 		"when (sale_m_pack > 0 or reg_m_pack > 1) and ((reg_price + reg_m_price > 0 and sale_m_price > 0))  and (reg_m_pack <> 0 and sale_m_pack <> 0) and sale_m_price/sale_m_pack <= (reg_price + reg_m_price)/reg_m_pack then sale_m_price/sale_m_pack " +
			 		"when (sale_m_pack > 0 or reg_m_pack > 1) and ((reg_price + reg_m_price > 0 and sale_m_price > 0)) and (reg_m_pack <> 0 and sale_m_pack <> 0) and sale_m_price/sale_m_pack > (reg_price + reg_m_price)/reg_m_pack then (reg_price + reg_m_price)/reg_m_pack " +
			 		"when (sale_m_pack = 0 and reg_m_pack <2 ) and (sale_price = 0) then reg_price " +
			 		"when (sale_m_pack = 0 and reg_m_pack <2 ) and (reg_price = 0) then sale_price " +
			 		"when (sale_m_pack = 0 and reg_m_pack <2 ) and (reg_price > 0 and sale_price > 0) and sale_price < reg_price then sale_price " +
			 		"else reg_price end) fin_price," +*/
			 		" (case " +
			 		"when list_cost = 0 then deal_cost " +
			 		"when deal_cost =0 then list_cost " +
			 		"when (deal_cost > 0 and list_cost > 0) and list_cost <= deal_cost then list_cost else deal_cost end) fin_cost, " +
			 		" (reg_movement+sale_movement) tot_movement, (reg_revenue + sale_revenue) tot_revenue from " +
			 		"(select  5 location_level_id, location_id, 1 product_level_id, product_id, ? calendar_id, avg(case when sale_flag = 'Y' then 1 else 0 end) sale_flag,0 promotion_id," +
			 		"avg(reg_multiple_pack) reg_m_pack, avg(reg_multiple_price) reg_m_price," +
			 		"avg(reg_price) reg_price, avg(list_cost) list_cost,sum(reg_revenue) reg_revenue, avg(reg_margin_pct) reg_margin_pct," +
			 		"sum(reg_movement) reg_movement, " +
			 		"avg(sale_multiple_pack) sale_m_pack,avg(sale_multiple_price) sale_m_price, avg(sale_price) sale_price, avg(deal_cost) deal_cost, " +
			 		"sum(sale_revenue) sale_revenue,sum(net_margin) net_margin, avg(net_margin_pct) net_margin_pct, sum(sale_movement) sale_movement, " +
			 		"sum(tot_visit_cnt) tot_visit_cnt ,avg(avg_order_size) avg_order_size, avg(final_price) final_price, avg(final_cost) final_cost " +
			 		"from item_metric_summary where location_id = ? and calendar_id >= ? and calendar_id <= ? " +
			 		"group by location_id, product_id )";
			  
				psmt=conn.prepareStatement(sql);
				
				psmt.setInt(1, calWeekId);
				psmt.setInt(2, locationId);
				psmt.setInt(3, calStartId);
				psmt.setInt(4, calEndId);

				
				
	        	int count = psmt.executeUpdate();
	        	PristineDBUtil.commitTransaction(conn, "InsertItemMetricsWeekly");
        		logger.debug("InsertItemMetricsWeekly:The number of records inserted: "+count);
		}catch (SQLException e)
		{
			logger.error("Error in InsertItemMetricsWeekly - " + e);
			logger.error("SQL used :"+sql);
			throw new GeneralException("Error", e);
		}finally{
			PristineDBUtil.close(psmt);
		}
			return;
		
	}

	public void deletePreviousItemMetricsWeekly(Connection _Conn, int calendarId, 
			int locationId) throws GeneralException {

		StringBuffer sql = new StringBuffer();
		sql.append("  select count(*) from item_metric_summary_Weekly  " +
				"where location_id = "+locationId+"  " +
				"and calendar_id =   "+calendarId);
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,"getItemDailyComp");
			if(result !=null){
				result.next();
				int count = result.getInt(1);
				if(count==0){
					return;
				}
			}
		
		sql = new StringBuffer();
sql.append(" Delete from ITEM_METRIC_SUMMARY_WEEKLY WHERE CALENDAR_ID = '" + calendarId +"'");
sql.append(" and LOCATION_ID = '" + locationId + "'");
logger.debug("deletePreviousItemWeekly SQL:" +sql.toString());

// execute the delete query
PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousItemWeekly");
} catch (GeneralException e) {
logger.error("Error while deleting previous item summary Weekly" + e);
throw new GeneralException("deletePreviousItemWeekly", e);		
}catch (Exception e) {
	logger.error("Error while deleting previous item summary Weekly" + e);
	throw new GeneralException("deletePreviousItemWeekly", e);		
	}
}
	
	
	
	public void deletePreviousItemMetricsWeeklyV2(Connection _Conn, int calendarId, 
			int locationId, int locationLevelId) throws GeneralException {

		StringBuffer sql = new StringBuffer();
		sql.append("  select count(*) from IMS_WEEKLY_ZONE  " +
				"where location_id = "+locationId+"  " +
				"and location_level_id = " + locationLevelId +
				"and calendar_id =   "+calendarId);
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,"getItemDailyComp");
			if(result !=null){
				result.next();
				int count = result.getInt(1);
				if(count==0){
					return;
				}
			}
		
		sql = new StringBuffer();
sql.append(" Delete from IMS_WEEKLY_ZONE WHERE CALENDAR_ID = '" + calendarId +"'");
sql.append(" and LOCATION_ID = '" + locationId + "' and location_level_id = " + locationLevelId);
logger.debug("deletePreviousItemWeekly SQL:" +sql.toString());

// execute the delete query
PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousItemWeekly");
} catch (GeneralException e) {
logger.error("Error while deleting previous item summary Weekly" + e);
throw new GeneralException("deletePreviousItemWeekly", e);		
}catch (Exception e) {
	logger.error("Error while deleting previous item summary Weekly" + e);
	throw new GeneralException("deletePreviousItemWeekly", e);		
	}
}


	/**
	 * This method returns competitor schedules for the given calendar week
	 * @param conn			Connection
	 * @param calendarId	Calendar Id for the week
	 * @return
	 * @throws GeneralException
	 */
	public List<Integer> getSchedulesForCalendar(Connection conn, int calendarId) throws GeneralException{
		List<Integer> scheduleList = new ArrayList<Integer>();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try{
			String chainId = new RetailPriceDAO().getChainId(conn);
			psmt=conn.prepareStatement(getSchedulesForCalendarSQL);
			psmt.setInt(1, calendarId);
			psmt.setString(2, chainId);
			rs = psmt.executeQuery();
			while(rs.next()){
				scheduleList.add(rs.getInt("SCHEDULE_ID"));
			}
		}catch (SQLException e){
			logger.error("Error in getSchedulesForCalendar - " + e);
			throw new GeneralException("Error", e);
		}finally{
			PristineDBUtil.close(psmt);
		}	
		return scheduleList;
	}
	
	/**
	 * Inserts records from competitive_Data for given schedule into item_metric_summary_wk_comp
	 * @param conn			Connection
	 * @param scheduleId	Schedule Id
	 * @return
	 * @throws GeneralException
	 */
	public int insertItemMetricsSummaryWeekly(Connection conn, int scheduleId) throws GeneralException {						
		PreparedStatement psmt= null;
		int count = 0;
		logger.debug("InsertItemMetricsSummaryWeeklyComp : Begin insert into db");
		try{				    	   
			psmt=conn.prepareStatement(insertItemMetricSummaryWeeklyComp);
			psmt.setInt(1, scheduleId);
							
			count = psmt.executeUpdate();
			PristineDBUtil.commitTransaction(conn, "InsertItemMetricsDataComp");
			logger.debug("InsertItemMetricsSummaryWeeklyComp Execute insert into db");
		}catch (SQLException e){
			logger.error("Error in InsertItemMetricsDataComp - " + e);
			throw new GeneralException("Error", e);
		}finally{
			PristineDBUtil.close(psmt);
		}
		return count;
	}
	
	
	public String getIncludeStoreList ( int locationLevel, int locationID) {
		String sqlStr ="";
		
		if( locationLevel == Constants.CHAIN_LEVEL_ID){
			sqlStr = "SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = " + locationID;  
		}else if( locationLevel == Constants.DIVISION_LEVEL_ID){
			sqlStr = "SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = " + locationID;
		}else if( locationLevel == Constants.ZONE_LEVEL_ID){
			sqlStr = "SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = " + locationID;
		} else {
			sqlStr = "1717,1718, 1719,1723";
		}
		return sqlStr;
	}
	
	public String getIncludeProductList ( int productLevel, int productID) {
	
		StringBuffer sb = new StringBuffer (); 
		sb.append( "SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR START WITH PRODUCT_LEVEL_ID = "); 
		sb.append( productLevel );
		sb.append( " AND PRODUCT_ID = " );
		sb.append( productID ); 
		sb.append( " CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID  AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID)" );
		sb.append( " WHERE CHILD_PRODUCT_LEVEL_ID = 1 ");
		return sb.toString();
	}
	
	public List<ProductMetricsDataDTO> getMovementInfo( Connection conn, int calendarId, String locationSql, 
			String productSql, int locationLevelId, int locationId) throws GeneralException { 
		List<ProductMetricsDataDTO>  movementList = new ArrayList<ProductMetricsDataDTO> ();
		
		
		StringBuffer sql = new StringBuffer( " SELECT PRODUCT_ID, A.CALENDAR_ID, " );
		sql.append( " SUM(TOT_MOVEMENT) AS TOT_MOVEMENT, " );
		sql.append( " SUM(TOT_VISIT_CNT)AS TOT_VISIT_CNT, ");
		sql.append( " SUM(REG_REVENUE) AS REG_REVENUE, " );
		sql.append( " SUM(SALE_REVENUE) AS SALE_REVENUE," );
		sql.append( " SUM(TOT_REVENUE) AS TOT_REVENUE, " );
		//sql.append( " SUM(TOT_MOVEMENT * UNITPRICE( REG_PRICE,REG_M_PRICE, REG_M_PACK )) AS GROSS_REVENUE, " );
		sql.append( "( CASE WHEN SUM(SALE_REVENUE) > 0 THEN 'Y' ELSE 'N'END) SALE_FLAG, " );
		sql.append( "sum(NET_MARGIN) as NET_MARGIN,");
		sql.append( "ROUND(avg(NET_MARGIN_PCT),2) as NET_MARGIN_PCT,");
		sql.append( " sum( LIST_COST * TOTALMOVEMENT) as TOTAL_LIST_COST "); 
        sql.append( " FROM ITEM_METRIC_SUMMARY_WEEKLY_T2 A " );
		sql.append( " WHERE A.CALENDAR_ID = " + calendarId );
		sql.append( " AND A.LOCATION_ID  IN (" + locationSql + ")" );
		sql.append( " AND PRODUCT_ID      IN (" + productSql + ")" );
		sql.append( " GROUP BY PRODUCT_ID, A.CALENDAR_ID" );
		logger.debug(sql);
		
		CachedRowSet crs = PristineDBUtil. executeQuery( conn, sql, "GetWeeklySummarizedMovement");
		try {
			while(crs.next()){
				ProductMetricsDataDTO movement = new ProductMetricsDataDTO ();
				movement.setProductId(crs.getInt("PRODUCT_ID"));
				movement.setProductLevelId(Constants.ITEMLEVELID);
				movement.setCalendarId(crs.getInt("CALENDAR_ID"));
				movement.setLocationId(locationId);
				movement.setLocationLevelId(locationLevelId);
				movement.setTotalMovement(crs.getDouble("TOT_MOVEMENT"));
				movement.setTotalVisits(crs.getDouble("TOT_VISIT_CNT"));
				movement.setRegularRevenue(crs.getDouble("REG_REVENUE"));
				movement.setSaleRevenue(crs.getDouble("SALE_REVENUE"));
				movement.setTotalRevenue(crs.getDouble("TOT_REVENUE"));
				movement.setSaleFlag(crs.getString("SALE_FLAG"));
				movement.setNetMargin(crs.getDouble("NET_MARGIN"));
				movement.setNetMarginPct(crs.getDouble("NET_MARGIN_PCT"));
				double totalMovement = movement.getTotalMovement();
				if( totalMovement > 0 ){
					movement.setListPrice(crs.getDouble("TOTAL_LIST_COST")/totalMovement);
					movement.setDealPrice(( movement.getTotalRevenue() - movement.getNetMargin())/movement.getTotalRevenue() );
				}
			}
			
		}catch(SQLException e){
			throw new GeneralException( "Cached Row Exception ", e);
		}

		
		return movementList;
	}
	
	public void deleteItemMetricsInItemLevel(Connection conn, 
			List<ProductMetricsDataDTO> movementDataList, int calendarId, 
			int locationId) throws GeneralException{
		logger.debug("Inside deleteFutureRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		String DELETE_ITEM_METRIC_IN_ITEMLEVEL ="DELETE FROM ITEM_METRIC_SUMMARY WHERE CALENDAR_ID =? AND LOCATION_LEVEL_ID =? AND LOCATION_ID =? "
				+ " AND PRODUCT_LEVEL_ID=? AND PRODUCT_ID=?";
		try{
			String query = new String(DELETE_ITEM_METRIC_IN_ITEMLEVEL);
			logger.debug(query);
			statement = conn.prepareStatement(query);
			int itemNoInBatch = 0;
			for(ProductMetricsDataDTO summaryDto: movementDataList){
				int counter = 0;

				statement.setInt(++counter, calendarId);
				statement.setInt(++counter, Constants.STORE_LEVEL_ID);
				statement.setInt(++counter, locationId);
				statement.setInt(++counter, Constants.ITEMLEVELID);
				statement.setDouble(++counter, summaryDto.getProductId());
				statement.addBatch();
				itemNoInBatch++;
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		logger.debug("# of rows deleted: "+count);
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
				logger.debug("# of rows deleted: "+count);
			}
	        	 
		}
		catch (SQLException e)
		{
			logger.error("Error while executing deleteItemMetricsInItemLevel");
			throw new GeneralException("Error while executing deleteItemMetricsInItemLevel", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}

	public void deletePreviousItemMetricsforTestZones(Connection _Conn, int calId, int priceZoneId, int zoneLevelId,
			List<Integer> itemList) throws GeneralException, SQLException {

		List<Integer> itemsList = new ArrayList<Integer>();
		int itemslimiCount = 0;
		for (Integer item : itemList) {
			itemsList.add(item);
			itemslimiCount++;
			if (itemslimiCount > 0 && (itemslimiCount % this.commitCount == 0)) {
				Object[] itemValues = itemsList.toArray();
				deletePreviousItemMetricsTestZone(_Conn, calId, itemValues, priceZoneId, zoneLevelId);
				itemsList.clear();
			}

		}
		if (itemsList.size() > 0) {
			Object[] itemValues = itemsList.toArray();
			deletePreviousItemMetricsTestZone(_Conn, calId, itemValues, priceZoneId, zoneLevelId);
			itemsList.clear();
		}

	}

	private void deletePreviousItemMetricsTestZone(Connection _Conn, int calendarId, Object[] itemValues,
			int locationId, int locationLevelId) throws GeneralException, SQLException {

		StringBuffer sql = new StringBuffer();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int existingcount = 0;
		sql.append("select count(*) as existingCount from IMS_WEEKLY_ZONE  " + "where location_id = " + locationId
				+ " and location_level_id = " + locationLevelId + " and calendar_id =   " + calendarId
				+ " and product_id in(");

		for (int i = 0; i < itemValues.length; i++) {
			if (i != itemValues.length - 1)
				sql.append(itemValues[i] + ",");
			else
				sql.append(itemValues[i] + ")");
		}
		logger.info("deletePreviousItemMetricsTestZone()- getExisting Items qry :" + sql.toString());
		try {
			statement = _Conn.prepareStatement(sql.toString());
			resultSet = statement.executeQuery();

			if (resultSet.next()) {
				existingcount = resultSet.getInt("existingCount");
			}
			
			if (existingcount > 0) {
				logger.info("# items to delete : for calendarId:  " + calendarId + " : " + existingcount);

				sql = new StringBuffer();
				sql.append(" Delete from IMS_WEEKLY_ZONE WHERE CALENDAR_ID = '" + calendarId + "'");
				sql.append(" and LOCATION_ID = " + locationId + " and location_level_id = " + locationLevelId
						+ " and product_id in(");

				for (int i = 0; i < itemValues.length; i++) {
					if (i != itemValues.length - 1)
						sql.append(itemValues[i] + ",");
					else
						sql.append(itemValues[i] + ")");
				}

				logger.debug("deletePreviousItemWeekly SQL:" + sql.toString());

				// execute the delete query
				PristineDBUtil.executeUpdate(_Conn, sql, "deletePreviousItemWeekly");
			}
		} catch (GeneralException e) {
			logger.error("deletePreviousItemMetricsTestZone()- Error while deleting previous item summary Weekly" + e);
			throw new GeneralException("deletePreviousItemWeekly", e);
		} catch (Exception e) {
			logger.error("deletePreviousItemMetricsTestZone()-Error while deleting previous item summary Weekly" + e);
			throw new GeneralException("deletePreviousItemWeekly", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

	}
	
}

