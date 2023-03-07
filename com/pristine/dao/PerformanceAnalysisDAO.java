
package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.PerformanceGoalDTO;
import com.pristine.dto.PriceCheckStatsDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PerformanceAnalysisDAO implements IDAO
{

	static Logger	logger	= Logger.getLogger("com.pristine.dao.PerformanceAnalysisDAO");
	Integer				goalCheckingBeforeCurrentDays	= Integer.valueOf(PropertyManager.getProperty("PERFORMANCE.GOALCHECKING_BEFORECURRENTDAYS", "15"));
	
	public static final String GET_PL_ITEM_COUNT_FOR_SCHEDULES = "SELECT PL_ITEMS_CT FROM PERFORMANCE_STAT WHERE SCHEDULE_ID = ?";
	
	/**
	 * method to insert data into performance_goal table
	 * 
	 * @param conn
	 * @param chainId
	 * @param startDate
	 * @param endDate
	 * @param isDefaultValues
	 * @return
	 * @throws GeneralException
	 */

	public int InsertPerformanceGoal(Connection connection, int chainId, String startDate, String endDate, boolean isDefaultValues) throws GeneralException
	{
		int recCount = 0;

		StringBuffer query = new StringBuffer();
		logger.debug("inserting goal data for ..." + chainId);
		// Delete any goal for that chain during that which is less than 6 days
		// old
		query.append("DELETE FROM PERFORMANCE_GOAL WHERE AS_OF_DATE > SYSDATE - 6 AND ");
		query.append(" CHAIN_ID = ");
		query.append(chainId);
		logger.debug(query);
		recCount = PristineDBUtil.executeUpdate(connection, query, "PerformanceAnalysisDAO-InsertPerformanceGoal - 1");
		query = new StringBuffer();
		query.append("SELECT ROUND(PERFORMANCE_GOAL_SEQ.NEXTVAL) GOAL_ID FROM DUAL");
		logger.debug(query);
		CachedRowSet crs = PristineDBUtil.executeQuery(connection, query, "PerformanceAnalysisDAO-InsertPerformanceGoal - 2");
		int performance_goal_id = -1;
		try
		{
			if (crs.next())
				performance_goal_id = crs.getInt("GOAL_ID");
		}
		catch (SQLException sqle)
		{
			throw new GeneralException("Cached Rowset access - goal id sequence", sqle);
		}
		if (performance_goal_id == -1)
			throw new GeneralException("Invalid Goal ID");

		query = new StringBuffer();
		query.append("INSERT INTO PERFORMANCE_GOAL");
		query.append("(");
		query.append(" PERFORMANCE_GOAL_ID, CHAIN_ID, ITEMS_NOT_CHECKED_GOAL_PCT, ITEMS_NOT_FOUND_GOAL_PCT, AVG_ITEMS_PER_HR_GOAL_PCT, ");
		query.append(" ITEMS_ON_SALE_GOAL_PCT, RANGE_CHECK_GOAL_PCT, REASONABILITY_CHECK_GOAL_PCT, ITEMS_FIXED_LATER_GOAL_PCT, NUM_OF_CHECKS, AS_OF_DATE");
		query.append(")");

		if (!isDefaultValues)
		{

			query.append(" SELECT ");
			query.append(performance_goal_id).append(", ");
			query.append(" COMP_CHAIN_ID,");
			query.append(" 0,"); // Hardcoded value for Items_Not_Checked_Goal_PCT as it should be 0
			//query.append(" DECODE(SUM(ITEM_CT), 0, 0, ROUND((SUM(ITEMS_NOT_CHECKED_CT)*100)/SUM(ITEM_CT))) AS ITEMS_NOT_CHECKED_GOAL_PCT,");
			query.append(" DECODE(SUM(ITEMS_CHECKED_CT), 0, 0, ROUND((SUM(ITEM_NOT_FOUND_CT)*100)/SUM(ITEMS_CHECKED_CT))) AS ITEMS_NOT_FOUND_GOAL_PCT,");
			query.append(" DECODE(SUM(DURATION), 0, 0, ROUND((SUM(ITEMS_CHECKED_CT)*60)/SUM(DURATION))) AS AVG_ITEMS_PER_HR_GOAL_PCT, ");
			query.append(" DECODE(SUM(ITEMS_CHECKED_CT - ITEM_NOT_FOUND_CT), 0, 0, ROUND((SUM(ON_SALE_CT)*100)/SUM(ITEMS_CHECKED_CT - ITEM_NOT_FOUND_CT))) AS ITEMS_ON_SALE_GOAL_PCT, ");
			query.append(" DECODE(SUM(ITEMS_CHECKED_CT - ITEM_NOT_FOUND_CT), 0, 0, ROUND((SUM(OUT_OF_RANGE_CT)*100)/SUM(ITEMS_CHECKED_CT - ITEM_NOT_FOUND_CT))) AS RANGE_CHECK_GOAL_PCT,");
			query.append(" DECODE(SUM(ITEMS_CHECKED_CT - ITEM_NOT_FOUND_CT), 0, 0, ROUND((SUM(OUT_OF_REASONABILITY_CT)*100)/SUM(ITEMS_CHECKED_CT - ITEM_NOT_FOUND_CT))) AS REASONABILITY_CHECK_GOAL_PCT ,");
			query.append(" 0, COUNT(*), SYSDATE");
			query.append(" FROM PERFORMANCE_STAT_VIEW  where COMP_CHAIN_ID = ");

			query.append(chainId);
			query.append(" AND").append(" START_DATE BETWEEN");
			query.append(" TO_DATE('").append(startDate).append("', '").append(Constants.DB_DATE_FORMAT).append("')");
			query.append(" AND ").append("TO_DATE('").append(endDate).append("', '").append(Constants.DB_DATE_FORMAT).append("')");
			query.append(" group by COMP_CHAIN_ID");
		} else
		{
			query.append(" VALUES");
			query.append("(").append(chainId).append(',');
			query.append(" 0, 0, 0, ");
			query.append(" 0, 0, 0, 0, ").append(" SYSDATE");
			query.append(")");
		}

		logger.debug(query);

		recCount = PristineDBUtil.executeUpdate(connection, query, "PerformanceAnalysisDAO-InsertPerformanceGoal - 3");
		logger.debug("Goal calculation done, recs inserted = " + recCount);
		return recCount;
	}

	public PriceCheckStatsDTO getPriceCheckStats(Connection conn, int scheduleId) throws GeneralException
	{

		PriceCheckStatsDTO priceCheckStatsDto = null;
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		// Fill in the query
		sb.append(" SELECT schedule_id, comp_chain_id, TO_CHAR(start_date, 'MM/DD/YYYY') START_DATE, ");
		sb.append(" TO_CHAR(end_date, 'MM/DD/YYYY') END_DATE, nvl( WEEK_THRESHOLD, -1) WEEK_THRESHOLD, comp_str_id, comp_chain_name, ");
		sb.append(" price_checker_id,price_checker_name,comp_str_no,addr_line1,addr_line2,");
		sb.append(" city,state,zip,item_ct,items_checked_ct,items_checked_pct,items_not_checked_ct,");
		sb.append(" items_not_checked_pct,item_not_found_ct,item_not_found_pct,price_not_found_ct,");
		sb.append(" price_not_found_pct,out_of_range_ct,out_of_range_pct,out_of_reasonability_ct,");
		sb.append(" out_of_reasonability_pct,duration,avg_items_per_hr,on_sale_ct,on_sale_pct,");
		sb.append(" went_up_ct,went_up_pct,went_down_ct,went_down_pct,not_found_x_times_ct,");
		sb.append(" not_found_x_pct,no_change_x_times_ct,no_change_x_pct,no_change_ct,no_change_pct,");
		sb.append(" to_char(start_time,'MM/DD/YYYY HH24:MI:SS') START_TIME,to_char(end_time,'MM/DD/YYYY HH24:MI:SS') END_TIME,");
		sb.append(" gps_lat,gps_long,gps_violation,to_char(status_chg_date,'MM/DD/YYYY') STATUS_CHG_DATE,");
		sb.append(" current_status,performance_goal_id,items_not_checked_pass,");
		sb.append(" items_not_found_pass,average_items_per_hr_pass,item_on_sale_pass,range_check_pass,");
		sb.append(" reasonability_check_pass,no_of_items_fixed_later,items_fixed_later_pass");
		sb.append(" From performance_stat_view  WHERE schedule_id = ");
		sb.append(scheduleId);
		logger.info(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PerformanceAnalysisDAO - getPriceCheckStats");
		try
		{
			if (crs.next())
			{
				priceCheckStatsDto = new PriceCheckStatsDTO();
				priceCheckStatsDto.setScheduleId(scheduleId);
				priceCheckStatsDto.setCompChainId(crs.getInt("comp_chain_id"));
				priceCheckStatsDto.setCompStrId(crs.getInt("comp_str_id"));
				priceCheckStatsDto.setWeekThreshold(crs.getInt("WEEK_THRESHOLD"));
				priceCheckStatsDto.setStartDate(crs.getString("START_DATE"));
				priceCheckStatsDto.setEndDate(crs.getString("END_DATE"));
				priceCheckStatsDto.setCompChainName(crs.getString("comp_chain_name"));
				priceCheckStatsDto.setPriceCheckerId(crs.getString("price_checker_id"));
				priceCheckStatsDto.setPriceCheckerName(crs.getString("price_checker_name"));
				priceCheckStatsDto.setCompStrNo(crs.getString("comp_str_no"));
				priceCheckStatsDto.setAddress1(crs.getString("addr_line1"));
				priceCheckStatsDto.setAddress2(crs.getString("addr_line2"));
				priceCheckStatsDto.setCity(crs.getString("city"));
				priceCheckStatsDto.setState(crs.getString("state"));
				priceCheckStatsDto.setZIP(crs.getString("zip"));
				priceCheckStatsDto.setItemCT(crs.getInt("item_ct"));
				priceCheckStatsDto.setItemsCheckedCT(crs.getInt("items_checked_ct"));
				priceCheckStatsDto.setItemsCheckedPCT(crs.getFloat("items_checked_pct"));
				priceCheckStatsDto.setItemsNotCheckedCT(crs.getInt("items_not_checked_ct"));
				priceCheckStatsDto.setItemsNotCheckedPCT(crs.getFloat("items_not_checked_pct"));
				priceCheckStatsDto.setItemsNotFoundCT(crs.getInt("item_not_found_ct"));
				priceCheckStatsDto.setItemsNotFoundPCT(crs.getFloat("item_not_found_pct"));
				priceCheckStatsDto.setPriceNotFoundCT(crs.getInt("price_not_found_ct"));
				priceCheckStatsDto.setPriceNotFoundPCT(crs.getFloat("price_not_found_pct"));
				priceCheckStatsDto.setOutOfRangeCT(crs.getInt("out_of_range_ct"));
				priceCheckStatsDto.setOutOfRangePCT(crs.getFloat("out_of_range_pct"));
				priceCheckStatsDto.setOutOfReasonabilityCT(crs.getInt("out_of_reasonability_ct"));
				priceCheckStatsDto.setOutOfReasonabilityPCT(crs.getFloat("out_of_reasonability_pct"));
				priceCheckStatsDto.setDuration(crs.getFloat("duration"));
				priceCheckStatsDto.setAvgItemsPerHr(crs.getFloat("avg_items_per_hr"));
				priceCheckStatsDto.setOnSaleCT(crs.getInt("on_sale_ct"));
				priceCheckStatsDto.setOnSalePCT(crs.getFloat("on_sale_pct"));
				priceCheckStatsDto.setWentUpCT(crs.getInt("went_up_ct"));
				priceCheckStatsDto.setWentUpPCT(crs.getFloat("went_up_pct"));
				priceCheckStatsDto.setWentDownCT(crs.getInt("went_down_ct"));
				priceCheckStatsDto.setWentDownPCT(crs.getFloat("went_down_pct"));
				priceCheckStatsDto.setNoChangeXTimesCT(crs.getInt("not_found_x_times_ct"));
				priceCheckStatsDto.setNoChangeXPCT(crs.getFloat("not_found_x_pct"));
				priceCheckStatsDto.setNoChangeXTimesCT(crs.getInt("no_change_x_times_ct"));
				priceCheckStatsDto.setNoChangeXPCT(crs.getFloat("no_change_x_pct"));
				priceCheckStatsDto.setNoChangeCT(crs.getInt("no_change_ct"));
				priceCheckStatsDto.setNoChangePCT(crs.getFloat("no_change_pct"));
				priceCheckStatsDto.setStartTime(crs.getString("START_TIME"));
				priceCheckStatsDto.setEndTime(crs.getString("END_TIME"));
				priceCheckStatsDto.setGpsLat(crs.getString("gps_lat"));
				priceCheckStatsDto.setGpsLong(crs.getString("gps_long"));
				priceCheckStatsDto.setGpsViolation(crs.getString("gps_violation"));
				priceCheckStatsDto.setStatusChgDate(crs.getString("STATUS_CHG_DATE"));
				priceCheckStatsDto.setCurrentStatus(crs.getString("current_status"));
				priceCheckStatsDto.setPerformanceGoalId(crs.getInt("performance_goal_id"));
				priceCheckStatsDto.setItemsNotCheckedPASS(crs.getString("items_not_checked_pass"));
				priceCheckStatsDto.setItemsNotFoundPASS(crs.getString("items_not_found_pass"));
				priceCheckStatsDto.setAvgItemsPerHrPASS(crs.getString("average_items_per_hr_pass"));
				priceCheckStatsDto.setItemsOnSalePASS(crs.getString("item_on_sale_pass"));
				priceCheckStatsDto.setRangeCheckPASS(crs.getString("range_check_pass"));
				priceCheckStatsDto.setReasonabilityCheckPASS(crs.getString("reasonability_check_pass"));
				priceCheckStatsDto.setNoOfItemsFixedLater(crs.getInt("no_of_items_fixed_later"));
				priceCheckStatsDto.setItemsFixedLaterPASS(crs.getString("items_fixed_later_pass"));

				if (priceCheckStatsDto.getWeekThreshold() < 0)
					priceCheckStatsDto.setWeekThreshold(Integer.parseInt(PropertyManager.getProperty("DEFAULT_WEEK_THRESHOLD", "0")));

			}
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}
		return priceCheckStatsDto;

	}

	public PerformanceGoalDTO getPerformanceGoal(Connection conn, int chainId) throws GeneralException
	{

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		PerformanceGoalDTO performGoalDto = null;
		// Fill in the query
		sb.append(" SELECT chain_id, items_not_checked_goal_pct,items_not_found_goal_pct,");
		sb.append(" avg_items_per_hr_goal_pct,range_check_goal_pct,reasonability_check_goal_pct, ");
		sb.append(" to_char(as_of_date,'MM/DD/YYYY') AS_OF_DATE,performance_goal_id,");
		sb.append(" num_of_checks,items_fixed_later_goal_pct,items_on_sale_goal_pct");
		sb.append(" From performance_goal  WHERE chain_id = ");
		sb.append(chainId);
		sb.append(" and AS_OF_DATE > sysdate -"+goalCheckingBeforeCurrentDays);
		sb.append(" ORDER BY as_of_date DESC");

		logger.info(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PerformanceAnalysisDAO - getPerformanceGoal");
		try
		{
			if (crs.next())
			{
				performGoalDto = new PerformanceGoalDTO();

				performGoalDto.setChainId(crs.getInt("chain_id"));
				performGoalDto.setItemsNotCheckedGoalPCT(crs.getFloat("items_not_checked_goal_pct"));
				performGoalDto.setItemsNotFoundGoalPCT(crs.getFloat("items_not_found_goal_pct"));
				performGoalDto.setAvgItemsPerHrGoalPCT(crs.getFloat("avg_items_per_hr_goal_pct"));
				performGoalDto.setRangeCheckGoalPCT(crs.getFloat("range_check_goal_pct"));
				performGoalDto.setReasonabilityCheckGoalPCT(crs.getFloat("reasonability_check_goal_pct"));
				performGoalDto.setAsOfDate(crs.getString("AS_OF_DATE"));
				performGoalDto.setPerformanceGoalId(crs.getInt("performance_goal_id"));
				performGoalDto.setNoOfChecks(crs.getInt("num_of_checks"));
				performGoalDto.setItemsFixedLaterGoalPCT(crs.getFloat("items_fixed_later_goal_pct"));
				performGoalDto.setItemsOnSaleGoalPCT(crs.getFloat("items_on_sale_goal_pct"));
				logger.debug("getPerformanceGoal>>ChainID>>" + performGoalDto.getChainId());

			} else
			{
				logger.debug("getPerformanceGoal-->NO RECORDS FOUND");
				performGoalDto = null;
			}
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}

		return performGoalDto;

	}

	public boolean updatePerformanceAnalysis(Connection conn, PriceCheckStatsDTO priceCheckStats) throws GeneralException
	{

		String LATE_FLAG = checkScheduleLateFlag(conn, priceCheckStats);
		logger.info("LATE_FLAG>>" +LATE_FLAG);
		String NOT_DONE_FLAG = checkScheduleNotDoneFlag(conn, priceCheckStats);
		logger.info("NOT_DONE_FLAG>>" +NOT_DONE_FLAG);
		int executeStatus = 0;
		// update pass flags
		// Update
		StringBuffer query = new StringBuffer();
		query.append(" UPDATE PERFORMANCE_STAT ");
		query.append(" SET PERFORMANCE_GOAL_ID = ");
		query.append(priceCheckStats.getGoalId());
		query.append(",ITEMS_NOT_CHECKED_PASS = '");
		query.append(priceCheckStats.getItemsNotCheckedPASS());
		query.append("',ITEMS_NOT_FOUND_PASS = '");
		query.append(priceCheckStats.getItemsNotFoundPASS());
		query.append("',AVERAGE_ITEMS_PER_HR_PASS = '");
		query.append(priceCheckStats.getAvgItemsPerHrPASS());
		query.append("',ITEM_ON_SALE_PASS = '");
		query.append(priceCheckStats.getItemsOnSalePASS());
		query.append("',RANGE_CHECK_PASS = '");
		query.append(priceCheckStats.getRangeCheckPASS());
		query.append("',REASONABILITY_CHECK_PASS ='");
		query.append(priceCheckStats.getReasonabilityCheckPASS());
		query.append("',ITEMS_FIXED_LATER_PASS ='");
		query.append(priceCheckStats.getReasonabilityCheckPASS() + "'");
		query.append(",LATE_FLAG ='");
		query.append(LATE_FLAG.trim() + "'");
		query.append(",NOT_DONE_FLAG ='");
		query.append(NOT_DONE_FLAG.trim() + "'");
		query.append("  WHERE SCHEDULE_ID = ");
		query.append(priceCheckStats.getScheduleId());
		logger.info(query.toString());
		executeStatus = PristineDBUtil.executeUpdate(conn, query, "PerformanceAnalysisDAO-updatePerformanceAnalysis");

		return executeStatus > 0;
	}
	
	/**
	 * This function checks the LATE_FLAG status by comparing STATUS_CHG_DATE
	 * AND END_DATE, added by Vaibhav
	 * @param conn
	 * @param priceCheckStats
	 * @return
	 * @throws GeneralException
	 */
	
	public String checkScheduleLateFlag(Connection conn, PriceCheckStatsDTO priceCheckStats) throws GeneralException
	{
	 
	 String LATE_FLAG = null;
	 CachedRowSet crs = null;
	 try
		{
	 StringBuffer query = new StringBuffer();
	 
	 query.append(" SELECT SCHEDULE_ID FROM SCHEDULE WHERE ");
	 query.append(" STATUS_CHG_DATE >= END_DATE + 1 ");
	 query.append(" AND SCHEDULE_ID="+priceCheckStats.getScheduleId());
	 
	 logger.info(query.toString());
	 
	 crs = PristineDBUtil.executeQuery(conn, query, "PerformanceAnalysisDAO - getPerformanceGoal");
	
	 if(crs.next())
	 {
		 LATE_FLAG ="Y"; 
	 }
	 else
	 {
		 LATE_FLAG ="N";  
	 }
	}catch(SQLException se)
	{
		se.printStackTrace();
	}
		
	 return LATE_FLAG;
		
	}
	
	
	/**
	 * This function checks the NOT_DONE_FLAG status by checked CURRENT_STATUS
	 * is 2(Completed) or not, added by Vaibhav
	 * @param conn
	 * @param priceCheckStats
	 * @return
	 * @throws GeneralException
	 */
	
	public String checkScheduleNotDoneFlag(Connection conn, PriceCheckStatsDTO priceCheckStats) throws GeneralException
	{
	 
	 String NOT_DONE_FLAG = null;
	 CachedRowSet crs = null;
	 try
		{
	 StringBuffer query = new StringBuffer();
	 
	 query.append(" SELECT SCHEDULE_ID FROM SCHEDULE WHERE ");
	 query.append(" CURRENT_STATUS = 2 ");
	 query.append(" AND SCHEDULE_ID="+priceCheckStats.getScheduleId());
	 
	 logger.info(query.toString());
	 
	 crs = PristineDBUtil.executeQuery(conn, query, "PerformanceAnalysisDAO - getPerformanceGoal");
	
	 if(crs.next())
	 {
		 NOT_DONE_FLAG ="N"; 
	 }
	 else
	 {
		 NOT_DONE_FLAG ="Y";  
	 }
	}catch(SQLException se)
	{
		se.printStackTrace();
	}
		
	 return NOT_DONE_FLAG;
		
	}
	
	public int getSummaryCompData ( Connection conn, int scheduleId, String additionalCondition ) throws GeneralException {

		StringBuffer query = new StringBuffer();
		 
		query.append(" SELECT COUNT(CHECK_DATA_ID) FROM COMPETITIVE_DATA ");
		query.append(" WHERE SCHEDULE_ID="+ scheduleId);
		 
		if ( additionalCondition != null)
			query.append(additionalCondition);
		 
		//logger.debug(query.toString());
		 
		String result = PristineDBUtil.getSingleColumnVal(conn, query, "getPerformanceGoal");
		int retVal = 0;
		if (result != null)
			retVal = Integer.parseInt(result);
		return retVal;
	
	}
	
	public int getPLCount ( Connection conn, int scheduleId) throws GeneralException {

		StringBuffer query = new StringBuffer();
		 
		query.append(" SELECT PL_ITEMS_CT FROM PERFORMANCE_STAT ");
		query.append(" WHERE SCHEDULE_ID="+ scheduleId);
		 
		String result = PristineDBUtil.getSingleColumnVal(conn, query, "getPerformanceGoal");
		int retVal = 0;
		if (result != null)
			retVal = Integer.parseInt(result);
		return retVal;
	
	}
	
	public void updatePriceCheckStat(Connection conn, PriceCheckStatsDTO priceCheckStatsDto)
		throws GeneralException{
		
		StringBuffer query = new StringBuffer();
		// Delete any stats for that schedule
		query.append("DELETE FROM PERFORMANCE_STAT WHERE ");
		query.append("SCHEDULE_ID = " + priceCheckStatsDto.getScheduleId());
		logger.debug(query);
		int recCount = PristineDBUtil.executeUpdate(conn, query, "PerformanceAnalysisDAO-InsertPerformanceStats - 1");
		
		query = new StringBuffer();
		query.append(" INSERT INTO PERFORMANCE_STAT ( ");
		query.append(" SCHEDULE_ID, ITEM_CT, ITEMS_CHECKED_CT,");
		query.append(" ITEM_NOT_FOUND_CT, PRICE_NOT_FOUND_CT, ");
		query.append(" ON_SALE_CT, WENT_UP_CT, WENT_DOWN_CT, NO_CHANGE_CT,");
		query.append(" PRICE_FOUND_CT, DURATION, PL_ITEMS_CT )");
		query.append(" VALUES (");
		query.append(priceCheckStatsDto.getScheduleId()).append(",");
		query.append(priceCheckStatsDto.getItemCT()).append(",");
		query.append(priceCheckStatsDto.getItemsCheckedCT()).append(",");
		query.append(priceCheckStatsDto.getItemsNotFoundCT()).append(",");
		query.append(priceCheckStatsDto.getPriceNotFoundCT()).append(",");
		query.append(priceCheckStatsDto.getOnSaleCT()).append(",");
		query.append(priceCheckStatsDto.getWentUpCT()).append(",");
		query.append(priceCheckStatsDto.getWentDownCT()).append(",");
		query.append(priceCheckStatsDto.getNoChangeCT()).append(",");
		query.append(priceCheckStatsDto.getItemCT() - priceCheckStatsDto.getItemsNotFoundCT()).append(", 0,");
		query.append(priceCheckStatsDto.getPlItemsCount()).append(")");
		
		logger.debug(query);
		PristineDBUtil.execute(conn, query, "PerformanceAnalysisDAO-InsertPerformanceStats - 2");		
	}
	

	/**
	 * 
	 * @param conn
	 * @param scheduleId
	 * @return count of PL items
	 * @throws GeneralException
	 */
	public int getPlItemsCountForSchedule(Connection conn, 
			int scheduleId) throws GeneralException{
		int plItemsCount = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_PL_ITEM_COUNT_FOR_SCHEDULES);
			stmt.setInt(1, scheduleId);
			rs = stmt.executeQuery();
			if(rs.next()){
				plItemsCount = rs.getInt("PL_ITEMS_CT");
			}
		}
		catch(SQLException sqlE){
			throw new GeneralException("Error -- getPlItemsCountForSchedule()", sqlE);
		}finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
		}
		return plItemsCount;
	}
	
}
