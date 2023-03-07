package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;


import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.SummaryDailyDTO;
import com.pristine.dto.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;


public class SummaryDailyDAO {
	static Logger logger = Logger.getLogger("SummaryDailyDAO");
	
	
	public List<SummaryDailyDTO> getVisitSummary(Connection conn,
			String storeNum, Date fromTime, Date toTime,
			List<Integer> excludeDeptList, List<String> includeItemList, boolean ignoreTransactionNumber)
			throws GeneralException {
		CachedRowSet result = new SummaryWeeklyDAO()
				.getVisitSummaryWeeklyRowSet(conn, storeNum, fromTime, toTime,
						excludeDeptList, includeItemList, ignoreTransactionNumber);
		List<SummaryDailyDTO> list = new ArrayList<SummaryDailyDTO>();
		try {
			while (result.next()) {
				SummaryDailyDTO dto = new SummaryDailyDTO();
				dto.setCompStoreNum(result.getString("COMP_STR_NO"));
				dto.setVisitCount(result.getInt("VISIT_COUNT"));
				list.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("Error", ex);
		}

		return list;
	}

	public List<SummaryDailyDTO> getStoreRevenueCostSummary(Connection conn,
			int[] schedules, int[] stores) throws GeneralException {
		return getRevenueCostSummary(conn, schedules, stores, false, false,
				false, false);
	}

	public List<SummaryDailyDTO> getDepartmentRevenueCostSummary(
			Connection conn, int[] schedules, int[] stores)
			throws GeneralException {
		return getRevenueCostSummary(conn, schedules, stores, true, false,
				false, false);
	}

	// getRevenueCostSummaryWeekly
	// getCategoryRevenueCostSummaryWeekly

	public List<SummaryDailyDTO> getCategoryRevenueCostSummary(Connection conn,
			int[] schedules, int[] stores) throws GeneralException {
		return getRevenueCostSummary(conn, schedules, stores, true, true,
				false, false);
	}

	public List<SummaryDailyDTO> getSubCategoryRevenueCostSummary(
			Connection conn, int[] schedules, int[] stores)
			throws GeneralException {
		return getRevenueCostSummary(conn, schedules, stores, true, true, true,
				false);
	}

	public List<SummaryDailyDTO> getSegmentRevenueCostSummary(Connection conn,
			int[] schedules, int[] stores) throws GeneralException {
		return getRevenueCostSummary(conn, schedules, stores, true, true, true,
				true);
	}

	private List<SummaryDailyDTO> getRevenueCostSummary(Connection conn,
			int[] schedules, int[] stores, boolean includeDept,
			boolean includeCat, boolean includeSubCat, boolean includeSegment)
			throws GeneralException {
		SummaryWeeklyDAO dao = new SummaryWeeklyDAO();

		CachedRowSet result = dao.getRevenueCostSummaryWeeklyRowSet(conn,
				schedules, stores, includeDept, includeCat, false, false);
		List<SummaryDailyDTO> list = new ArrayList<SummaryDailyDTO>();
		if (result == null)
			return list;

		try {
			while (result.next()) {
				SummaryDailyDTO dto = new SummaryDailyDTO();
				dao.setupSummaryDTO(result, dto);

				if (includeDept) {
					dto.setDeptId(result.getInt("DEPT_ID"));
				}
				if (includeCat) {
					dto.setCatId(result.getInt("CATEGORY_ID"));
				}
				if (includeSubCat) {
					dto.setSubCatId(result.getInt("SUB_CATEGORY_ID"));
				}
				if (includeSegment) {
					dto.setSegmentId(result.getInt("SEGMENT_ID"));
				}

				list.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("getRevenueCostSummary", ex);
		}

		return list;
	}

	public void insertStoreSummaryDaily(Connection conn, SummaryDailyDTO dto)
			throws GeneralException {
		insertSummaryDaily(conn, dto, false, false, false, false);
	}

	public void insertDepartmentSummaryDaily(Connection conn,
			SummaryDailyDTO dto) throws GeneralException {
		insertSummaryDaily(conn, dto, true, false, false, false);
	}

	public void insertCategorySummaryDaily(Connection conn, SummaryDailyDTO dto)
			throws GeneralException {
		 insertSummaryDaily (conn, dto, true, true, false, false);
	}

	public void insertSubCategorySummaryDaily(Connection conn,
			SummaryDailyDTO dto) throws GeneralException {
		// insertSummaryDaily (conn, dto, true, true, false, false);
	}

	public void insertSegmentSummaryDaily(Connection conn, SummaryDailyDTO dto)
			throws GeneralException {
		// insertSummaryDaily (conn, dto, true, true, false, false);
	}

	private void insertSummaryDaily(Connection conn, SummaryDailyDTO dto,
			boolean includeDept, boolean includeCat, boolean includeSubCat,
			boolean includeSegment) throws GeneralException {
		StringBuffer sb = new StringBuffer(
				" insert into SUMMARY_DAILY (SCHEDULE_ID, COMP_STR_ID, DOW_ID, TOD_ID");
		if (includeDept) {
			sb.append(", DEPT_ID");
		}
		if (includeCat) {
			sb.append(", CATEGORY_ID");
		}
		if (includeSubCat) {
			sb.append(", SUB_CATEGORY_ID");
		}
		int segId = dto.getSegmentId();
		if (includeSegment && segId != -1) {
			sb.append(", SEGMENT_ID");
		}
		// sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE");
		sb.append(", REVENUE_REGULAR, REVENUE_SALE");
		sb.append(", FINAL_COST, MARGIN, VISIT_COUNT, VISIT_REVENUE_AVG");
		sb.append(", REVENUE_TOTAL, MARGIN_PCT)");
		sb.append(" values (");
		sb.append(dto.getScheduleId());
		sb.append(",").append(dto.getCompStoreId());
		sb.append(",").append(dto.getDayOfWeekId());
		sb.append(",").append(dto.getTimeOfDayId());
		if (includeDept) {
			sb.append(",").append(dto.getDeptId());
		}
		if (includeCat) {
			sb.append(",").append(dto.getCatId());
		}
		if (includeSubCat) {
			sb.append(",").append(dto.getSubCatId());
		}
		if (includeSegment && segId != -1) {
			sb.append(",").append(dto.getSegmentId());
		}
		sb.append(",").append(dto.getRegularRevenue());
		sb.append(",").append(dto.getSaleRevenue());
		sb.append(",").append(dto.getFinalCost());
		sb.append(",").append(dto.getMargin());
		sb.append(",").append(dto.getVisitCount());
		sb.append(",").append(dto.getVisitCostAverage());
		//sb.append(",").append(dto.getTotalRevenue());
		sb.append(",").append(dto.getMarginPercent());
		sb.append(")");

		try {
			String sql = sb.toString();
			// logger.debug ("insertSummaryDaily SQL: " + sql);
			PristineDBUtil.execute(conn, sb, "insertSummaryDaily");
		} catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
			throw ex;
		}
	}

	public List<MovementDailyAggregateDTO> getQuantityMovementsForPeriod(
			Connection conn, String storeNum, Date start, Date end, boolean sale)
			throws GeneralException {
		CachedRowSet result = new MovementDAO().getQuantityMovementsForPeriodRowSet(conn, storeNum, start,
						end, sale, Constants.STORE_LEVEL_TYPE_ID,false);
		return populateList(result, sale);
	}

	private CachedRowSet getQuantityMovementsForPeriodRowSet(Connection conn,
			String storeNum, Date start, Date end, boolean flag) {

		StringBuffer sb = new StringBuffer();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		sb.append(" select m.COMP_STR_NO, m.UPC,i.item_code , sum(m.PRICE) as PRICE, sum(m.QUANTITY) as QUANTITY from MOVEMENT_DAILY m, ");
		sb.append(" item_lookup i where  0+i.upc = m.upc and  ");
		sb.append("  m.POS_TIMESTAMP >=  To_DATE('" + formatter.format(start)
				+ "', 'YYYYMMDDHH24MI') and m.POS_TIMESTAMP ");
		sb.append("  <= To_DATE('" + formatter.format(end)
				+ "', 'YYYYMMDDHH24MI') ");
		if (flag) {
			sb.append(" and m.SALE_FLAG = 'Y' ");
		} else {
			sb.append(" and m.SALE_FLAG = 'N' ");
		}
		sb.append(" and m.QUANTITY <> 0  ");
		if (storeNum != null) {
			sb.append(" and m.COMP_STR_NO = '" + storeNum + "'  ");
		}
		sb.append(" group by m.COMP_STR_NO, m.UPC,i.item_code order by m.COMP_STR_NO,m. UPC ");

		System.out.println("Q M Query" + sb.toString());
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb,
					"getQuantityMovementsForPeriod");
		} catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return result;

	}

	public List<MovementDailyAggregateDTO> getWeightMovementsForPeriod(
			Connection conn, String storeNum, Date start, Date end, boolean sale)
			throws GeneralException {
		CachedRowSet result = new MovementDAO()
				.getWeightMovementsForPeriodRowSet(conn, storeNum, start, end,
						sale, Constants.STORE_LEVEL_TYPE_ID,false);
		return populateList(result, sale);
	}

	private List<MovementDailyAggregateDTO> populateList(CachedRowSet result,
			boolean sale) {
		List<MovementDailyAggregateDTO> list = new ArrayList<MovementDailyAggregateDTO>();
		try {
			while (result.next()) {
				MovementDailyAggregateDTO dto = new MovementDailyAggregateDTO();
				new MovementDAO().loadWeeklyDTO(dto, result);
				dto.setSaleFlag(sale);
				list.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("Error:", ex);
		}

		return list;
	}

	public void getCostInfo(Connection conn, MovementDailyAggregateDTO movement)
			throws GeneralException {
		StringBuffer sb = new StringBuffer("select LIST_COST, EFF_LIST_COST_DATE");
		sb.append(" ,(case when DEAL_COST is null then LIST_COST");
		sb.append(" when DEAL_COST <= 0 then LIST_COST");
		sb.append(" else DEAL_COST");
		sb.append(" end) DEAL_COST");
		sb.append(" ,DEAL_START_DATE, DEAL_END_DATE, COST_CHG_DIRECTION from MOVEMENT_WEEKLY where");
		sb.append(" COMP_STR_ID = ").append(movement.getCompStoreId());
		sb.append(" and ITEM_CODE = ").append(movement.getItemCode());
		sb.append(" and CHECK_DATA_ID = ").append(movement.getCheckDataId());

		String sql = sb.toString();
		// logger.debug ("getCostInfo SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,"getCostInfo");

		try {
			while (result.next()) {
				movement.setListCost(result.getDouble("LIST_COST"));
				movement.setEffListCostDate(result.getDate("EFF_LIST_COST_DATE"));

				movement.setDealCost(result.getDouble("DEAL_COST"));
				Object obj1 = result.getObject("DEAL_START_DATE");
				if (obj1 != null) {
					movement.setDealEndDate(result.getDate("DEAL_START_DATE"));
				}
				Object obj2 = result.getObject("DEAL_END_DATE");
				if (obj2 != null) {
					movement.setDealEndDate(result.getDate("DEAL_END_DATE"));
				}

				movement.setCostChangeDirection(result.getInt("COST_CHG_DIRECTION"));
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public boolean insertPeriodMovement(Connection conn,
			MovementDailyAggregateDTO movement, boolean saleFlag, boolean update)
			throws GeneralException {
		StringBuffer sb;
		int count = 0;
		if (update) {
			sb = new StringBuffer("update MOVEMENT_DAILY_TEMP set");
			if (saleFlag) {
				sb.append(" REVENUE_SALE = ").append(movement.getTotalPrice());
				sb.append(", QUANTITY_SALE = ").append(movement.getExtnQty());
			} else {
				sb.append(" REVENUE_REGULAR = ").append(
						movement.getTotalPrice());
				sb.append(", QUANTITY_REGULAR = ")
						.append(movement.getExtnQty());
			}
			sb.append(", DOW_ID = ").append(movement.getDayOfWeekId());
			sb.append(", TOD_ID = ").append(movement.getTimeOfDayId());
			sb.append(" where CHECK_DATA_ID = ").append(
					movement.getCheckDataId());
			/*
			 * sb.append(" and DOW_ID = ").append(movement.getDayOfWeekId());
			 * sb.append(" and TOD_ID = ").append(movement.getTimeOfDayId());
			 */
			try {
				String sql = sb.toString();
				// logger.debug ("insertPeriodMovement update SQL: " + sql);
				count = PristineDBUtil.executeUpdate(conn, sb,
						"insertPeriodMovement - update");
			} catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}

		if (count == 0) {
			// Nothing was updated, insert record...
			sb = new StringBuffer("insert into MOVEMENT_DAILY_TEMP (");
			sb.append("COMP_STR_ID, DOW_ID, TOD_ID, ITEM_CODE, CHECK_DATA_ID");
			if (saleFlag) {
				sb.append(", REVENUE_SALE, QUANTITY_SALE");
			} else {
				sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR");
			}
			sb.append(", LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE");
			sb.append(", COST_CHG_DIRECTION");

			sb.append(") values (");

			sb.append(movement.getCompStoreId());
			sb.append(", ").append(movement.getDayOfWeekId());
			sb.append(", ").append(movement.getTimeOfDayId());
			sb.append(", ").append(movement.getItemCode());
			sb.append(", ").append(movement.getCheckDataId());
			sb.append(", ").append(movement.getTotalPrice());
			sb.append(", ").append(movement.getExtnQty());

			sb.append(", ").append(movement.getListCost());
			String dateStr;
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			if (movement.getEffListCostDate() != null) {
				dateStr = formatter.format(movement.getEffListCostDate());
				sb.append(", To_DATE('").append(dateStr)
						.append("', 'YYYYMMDD')");
			} else {
				sb.append(", NULL");
			}

			sb.append(", ").append(movement.getDealCost());
			if (movement.getDealStartDate() != null) {
				dateStr = formatter.format(movement.getDealStartDate());
				sb.append(", To_DATE('").append(dateStr)
						.append("', 'YYYYMMDD')");
			} else {
				sb.append(", NULL");
			}
			if (movement.getDealEndDate() != null) {
				dateStr = formatter.format(movement.getDealEndDate());
				sb.append(", To_DATE('").append(dateStr)
						.append("', 'YYYYMMDD')");
			} else {
				sb.append(", NULL");
			}
			sb.append(", ").append(movement.getCostChangeDirection());

			sb.append(")"); // close

			try {
				String sql = sb.toString();
				// logger.debug ("insertPeriodMovement insert SQL: " + sql);
				PristineDBUtil.execute(conn, sb,
						"insertPeriodMovement - insert");
			} catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
			}
		}

		return true;
	}

	public boolean deletePeriodMovement(Connection conn)
			throws GeneralException {
		StringBuffer sb = new StringBuffer("delete from MOVEMENT_DAILY_TEMP");
		try {
			String sql = sb.toString();
			// logger.debug ("deletePeriodMovement SQL: " + sql);
			PristineDBUtil.execute(conn, sb, "deletePeriodMovement");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
		}

		return true;
	}

	
	/*
	 * ****************************************************************
	 * Method used to get the Com_store_Id Argument 1: Connection Argument 2:
	 * compStoreNum return : Comp_Store_Id
	 * 
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */
	public int GetCompetitiveStoreId(Connection _conn, String compStoreNum)
			throws GeneralException {
		StringBuffer sb = new StringBuffer();
		int compStrIdVal = 0;
		try {

			sb.append("SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE ");
			sb.append(" COMP_STR_NO ='" + compStoreNum + "' OR ");
			sb.append(" GLN ='" + compStoreNum + "'");
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sb,
					"getCompetitiveStoreId");
			while (result.next()) {
				compStrIdVal = result.getInt("COMP_STR_ID");

			}
		} catch (Exception exe) {
			logger.error(exe);
		}
		return compStrIdVal;
	}

		
	public static double Round(double value, int Rpl) {
		double p = (double) Math.pow(10, Rpl);
		value = value * p;
		double tmp = Math.round(value);
		return (double) tmp / p;
	}

	/*
	 * ****************************************************************
	 * Update the status log Argument 1: Com_str_id Argument 2: Calendar Id
	 * 
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */

	public void UpdateStatusLogInfo(Connection _Conn, int com_Str_id,
			int calendarId) {

		logger.info("UpdateStatusLogInfo Method Begins");
		StringBuffer sb = new StringBuffer();
		sb.append(" INSERT INTO STATUS_LOG (LOG_ID,COM_STR_ID,CALENDAR_ID,STATUS) ");
		sb.append(" VALUES  ");
		sb.append(" (STATUS_LOG_SEQ.NEXTVAL,'" + com_Str_id + "','"
				+ calendarId + "','Y') ");

		logger.debug("StatusLogInfo Update Query" + sb.toString());

		try {
			String sql = sb.toString();

			PristineDBUtil.execute(_Conn, sb, "insertSummaryDaily");
		} catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);

		}

		logger.info("UpdateStatusLogInfo Method Exist");

	}

		

	/*
	 * ****************************************************************
	 * Method to  insert the Store level aggregation records
	 * Argument 1 : Product HashMap
	 * Argument 2 : Connection
	 * Argument 3 : calendarId
	 * Arguemnt 4 : storeId
	 * call the execute batch method
	 * ****************************************************************
	 */

	

	public void insertSummaryDailyBatch(Connection _Conn,int calendarId, int storeId, SalesaggregationbusinessV2 businessLogic,
										double totalVisit, HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap, SummaryDataDTO storeDto) {
		
		
		logger.info(" Process Enter into Execute Batch Method  ");
		try
		{
        PreparedStatement psmt=_Conn.prepareStatement(InsertSql());
        
        logger.info(" Product HashMap Count  " + productMap.size());
                        
        Object[] outerLoop=productMap.values().toArray();
        for(int ii=0 ; ii < outerLoop.length ;ii++)
        {
        	@SuppressWarnings("unchecked")
			HashMap<String, SummaryDailyDTO> subProductMap= (HashMap<String, SummaryDailyDTO>) outerLoop[ii];
        	 
        	 Object[] innerLoop=subProductMap.values().toArray();
        	             
             for ( int jj = 0; jj < innerLoop.length; jj++ )
     		{
            	SummaryDataDTO summaryDto=(SummaryDataDTO) innerLoop[jj];
            	summaryDto.setLocationId(storeId);
             	summaryDto.setcalendarId(calendarId);
             	addSqlBatch(summaryDto, false, psmt);
     		}
        }
        
     
                             
  		int[] count=psmt.executeBatch();
		logger.info(" Insert Count   " +count.length);
		
		psmt=_Conn.prepareStatement(InsertSql());
		   // Prepare the Store Aggregation records
		storeDto.setLocationId(storeId);
        storeDto.setcalendarId(calendarId);
        addSqlBatch(storeDto,true, psmt);
        psmt.executeBatch();
		        
		}
		catch(Exception exe)
		{
			logger.info(exe);
		}
		  

	}
	
	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */

	private String InsertSql() {
	 
		StringBuffer Sql = new StringBuffer();
		
		Sql.append(" insert into SALES_AGGR_DAILY (SUMMARY_DAILY_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID,PRODUCT_LEVEL_ID ");

		Sql.append(", PRODUCT_ID , TOT_VISIT_CNT ,AVG_ORDER_SIZE,TOT_MOVEMENT, TOT_REVENUE , REG_REVENUE  ");

		Sql.append(" , SALE_REVENUE ,TOT_MARGIN ,REG_MARGIN , SALE_MARGIN  ");

		Sql.append(", TOT_MARGIN_PCT, REG_MARGIN_PCT , SALE_MARGIN_PCT ,REG_MOVEMENT,SALE_MOVEMENT,SUMMARY_CTD_ID) ");

		Sql.append(" values (SALES_AGGR_DAILY_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CTD_ID_SEQ.NEXTVAL)");
		
		logger.info(" SQL >>> " +Sql.toString()) ;
		
		return Sql.toString();
	}
	
	/*
	 * ****************************************************************
	 * Method to Create the insert script and add the script into batch List
	 * Argument 1 : SummaryDailyDto
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */

	public void addSqlBatch(SummaryDataDTO summaryDto,boolean insertMode, PreparedStatement psmt)  {
		
		try
		{
		psmt.setObject(1,summaryDto.getcalendarId());
		psmt.setObject(2,5);
		psmt.setObject(3,summaryDto.getLocationId());
		if(insertMode)
		{
			psmt.setObject(6,Round(summaryDto.getTotalVisitCount(),2));
			psmt.setObject(7,Round(summaryDto.getAverageOrderSize(),2));
			psmt.setObject(4,"");
			psmt.setObject(5,"");
			}
		else
		{
			psmt.setObject(4,summaryDto.getProductLevelId());
			psmt.setObject(5,summaryDto.getProductId());
			psmt.setObject(6, "");
			psmt.setObject(7, "");
		}
		 
		psmt.setObject(8,Round(summaryDto.getTotalMovement(), 2));
		psmt.setObject(9,Round(summaryDto.getTotalRevenue(), 2));
		psmt.setObject(10,Round(summaryDto.getRegularRevenue(), 2));
		psmt.setObject(11,Round(summaryDto.getSaleRevenue(), 2));
		psmt.setObject(12,Round(summaryDto.getTotalMargin(), 2));
		psmt.setObject(13,Round(summaryDto.getRegularMargin(), 2));
		psmt.setObject(14,Round(summaryDto.getSaleMargin(), 2));
		psmt.setObject(15,Round(summaryDto.getTotalMarginPer(), 2));
		psmt.setObject(16, Round(summaryDto.getRegularMarginPer(), 2));
		psmt.setObject(17, Round(summaryDto.getSaleMarginPer(), 2));
		psmt.setObject(18, Round(summaryDto.getRegularMovement(),2));
		psmt.setObject(19,Round(summaryDto.getSaleMovementId(),2));
		
		psmt.addBatch(); 
		}
		catch(Exception sql)
		{
			logger.debug(sql);
		}
				
	}
	
	/*
	 * ****************************************************************
	 * Method used to delete the previous aggregation for store
	 * Argument 1 : _Conn
	 * Argument 2 : calendarId
	 * Argument 3 : locationId
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */
	
	public void deletePreviousAggregation(Connection _Conn, int calendarId, int locationId) {
		
		logger.debug("Delete Previous Aggregation Starts");
		
		StringBuffer sql = new StringBuffer();
		  
		    sql.append(" Delete from SALES_AGGR_DAILY WHERE CALENDAR_ID='"+calendarId+"' and LOCATION_ID='"+locationId+"' ");
		 
		logger.debug(" Sql --- " +sql.toString());
		
		try {
			
			// execute the delete query
			PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousAggregation");
		} catch (GeneralException e) {
			 logger.error(e.getMessage());
		}
	}

	
}
