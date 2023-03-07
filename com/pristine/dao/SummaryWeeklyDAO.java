package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.SummaryWeeklyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class SummaryWeeklyDAO implements IDAO
{
	static Logger logger = Logger.getLogger("SummaryWeeklyDAO");

	public int getScheduleId (Connection conn, int storeId, Date start, Date end)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String startStr = formatter.format(start);

		StringBuffer sb = new StringBuffer("select schedule_id from schedule where start_date = ").append("To_DATE('").append(startStr).append("', 'YYYYMMDD')");
		sb.append(" and comp_str_id = ").append(storeId);

		String sql = sb.toString();
		logger.debug ("getScheduleId SQL: " + sql);
		CachedRowSet result = null;
		int schId = -1;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getScheduleId");
			while (result.next()) {
				schId = result.getInt("schedule_id");
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		
		return schId;
	}

	public List<SummaryWeeklyDTO> getVisitSummaryWeekly (Connection conn, String storeNum, Date start, Date end, boolean ignoreTransactionNumber)
	throws GeneralException
	{
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		return getVisitSummaryWeekly (conn, storeNum, start, end, null, null,ignoreTransactionNumber);
	}
	
	public List<SummaryWeeklyDTO> getVisitSummaryWeekly (Connection conn, String storeNum, Date start, Date end,
										List<Integer> excludeDeptList, List<String> includeItemList, boolean ignoreTransactionNumber)
	throws GeneralException
	{
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		CachedRowSet result = getVisitSummaryWeeklyRowSet(conn, storeNum, start, end, excludeDeptList, includeItemList, ignoreTransactionNumber);
		List<SummaryWeeklyDTO> list = new ArrayList<SummaryWeeklyDTO>();
		try
		{
			while (result.next())
			{
				SummaryWeeklyDTO dto = new SummaryWeeklyDTO();
				dto.setCompStoreNum(result.getString("COMP_STR_NO"));
				dto.setVisitCount(result.getInt("VISIT_COUNT"));
				list.add (dto); 
			}
		}
		catch (SQLException ex) {
			logger.error("Error", ex);
		}
	
		return list;
	}

	public CachedRowSet getVisitSummaryWeeklyRowSet (Connection conn, String storeNum, Date start, Date end, boolean ignoreTransactionNumber)
	throws GeneralException
	{
		return getVisitSummaryWeeklyRowSet (conn, storeNum, start, end, null, null,ignoreTransactionNumber);
	}
	
	public CachedRowSet getVisitSummaryWeeklyRowSet (Connection conn, String storeNum, Date start, Date end,
								List<Integer> excludeDeptList, List<String> includeItemList, boolean ignoreTransactionNumber)
	throws GeneralException
	{
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		
		StringBuffer sb = new StringBuffer("select comp_str_no, sum(visit_count) as visit_count from");
		sb.append(" (");
		if (!ignoreTransactionNumber)
			sb.append(" select m.comp_str_no, m.weekday, count(distinct m.transaction_no) as visit_count from MOVEMENT_DAILY m, ITEM_LOOKUP i");
		else
			sb.append(" select m.comp_str_no, m.weekday, 0 as visit_count from MOVEMENT_DAILY m, ITEM_LOOKUP i");
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		sb.append("  where pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		String endStr = formatter.format(end);
		sb.append("  and pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		if (!ignoreTransactionNumber)
			sb.append("  and transaction_no > 0");
		if ( storeNum != null ) {
			sb.append("  and comp_str_no = '").append(storeNum).append("'");
		}
		//sb.append(" and comp_str_no in ('0363', '0108')");
		//sb.append("  and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc(+)");
		sb.append(" and m.item_code = i.item_code(+)");
		if ( excludeDeptList != null && excludeDeptList.size() > 0 )
		{
			sb.append("  and (");
			sb.append("   i.dept_id not in (");
			for (int ii = 0; ii < excludeDeptList.size(); ii++) {
				if ( ii > 0 ) sb.append(",");
				sb.append(excludeDeptList.get(ii));
			}
			sb.append("   )");
			if ( includeItemList != null && includeItemList.size() > 0 )
			{
				sb.append("  or m.upc in (");
				for (int ii = 0; ii < includeItemList.size(); ii++) {
					if ( ii > 0 ) sb.append(",");
					sb.append("'").append(includeItemList.get(ii)).append("'");
				}
				sb.append("   )");
			}
			sb.append("  )");
		}
		sb.append("  group by m.comp_str_no, m.weekday");
		sb.append("  )");
		sb.append(" group by comp_str_no");

		String sql = sb.toString();
		logger.debug ("getVisitSummaryWeeklyRowSet SQL: " + sql);
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getVisitSummaryWeeklyRowSet");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		
		return result;
	}

	public List<SummaryWeeklyDTO> getStoreRevenueCostSummaryWeekly (Connection conn, int[] schedules, int[] stores)
		throws GeneralException
	{
		return getRevenueCostSummaryWeekly (conn, schedules, stores, false, false, false, false);
	}
	
	public List<SummaryWeeklyDTO> getDepartmentRevenueCostSummaryWeekly (Connection conn, int[] schedules, int[] stores)
		throws GeneralException
	{
		return getRevenueCostSummaryWeekly (conn, schedules, stores, true, false, false, false);
	}
	
	public List<SummaryWeeklyDTO> getCategoryRevenueCostSummaryWeekly (Connection conn, int[] schedules, int[] stores)
	throws GeneralException
	{
		return getRevenueCostSummaryWeekly (conn, schedules, stores, true, true, false, false);
	}

	public List<SummaryWeeklyDTO> getSubCategoryRevenueCostSummaryWeekly (Connection conn, int[] schedules, int[] stores)
	throws GeneralException
	{
		return getRevenueCostSummaryWeekly (conn, schedules, stores, true, true, true, false);
	}

	public List<SummaryWeeklyDTO> getSegmentRevenueCostSummaryWeekly (Connection conn, int[] schedules, int[] stores)
	throws GeneralException
	{
		return getRevenueCostSummaryWeekly (conn, schedules, stores, true, true, true, true);
	}

	private List<SummaryWeeklyDTO> getRevenueCostSummaryWeekly (Connection conn, int[] schedules, int[] stores,
										boolean includeDept, boolean includeCat,
										boolean includeSubCat, boolean includeSegment)
	throws GeneralException
	{
		CachedRowSet result = getRevenueCostSummaryWeeklyRowSet(conn, schedules, stores,
				includeDept, includeCat, includeSubCat, includeSegment);
		List<SummaryWeeklyDTO> list = new ArrayList<SummaryWeeklyDTO>();
		if ( result == null )
			return list;
		
		try
		{
			while (result.next())
			{
				SummaryWeeklyDTO dto = new SummaryWeeklyDTO();
				setupSummaryDTO(result, dto);
				
				if ( includeDept ) {
					dto.setDeptId(result.getInt("DEPT_ID"));
				}
				if ( includeCat ) {
					dto.setCatId(result.getInt("CATEGORY_ID"));
				}
				if ( includeSubCat ) {
					dto.setSubCatId(result.getInt("SUB_CATEGORY_ID"));
				}
				if ( includeSegment ) {
					dto.setSegmentId(result.getInt("SEGMENT_ID"));
				}
				
				list.add (dto);
			}
		}
		catch (SQLException ex) {
			logger.error("getRevenueCostSummaryWeekly", ex);
		}
	
		return list;
	}

	public CachedRowSet getRevenueCostSummaryWeeklyRowSet (Connection conn, int[] schedules, int[] stores,
										boolean includeDept, boolean includeCat,
										boolean includeSubCat, boolean includeSegment)
	{
		StringBuffer sb = new StringBuffer("select COMP_STR_ID");
		if ( includeDept ) {
			sb.append(", i.DEPT_ID");
		}
		if ( includeCat ) {
			sb.append(", i.CATEGORY_ID");
		}
		if ( includeSubCat ) {
			sb.append(", i.SUB_CATEGORY_ID");
		}
		if ( includeSegment ) {
			sb.append(", i.SEGMENT_ID");
		}
		sb.append(", sum(REVENUE_REGULAR) as REVENUE_REGULAR");
		sb.append(", sum(QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(REVENUE_SALE) as REVENUE_SALE");
		sb.append(", sum(QUANTITY_SALE) as QUANTITY_SALE, round(sum(THE_COST),2) as FINAL_COST from");
		sb.append(" (");
		sb.append(" select COMP_STR_ID, CHECK_DATA_ID, ITEM_CODE, REVENUE_REGULAR, QUANTITY_REGULAR");
		sb.append(" , REVENUE_SALE, QUANTITY_SALE, (QUANTITY_REGULAR * THE_COST + QUANTITY_SALE * THE_COST) as THE_COST");
		sb.append(" from (");
		sb.append("  select m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR");
		sb.append("  , m.REVENUE_SALE, m.QUANTITY_SALE, m.LIST_COST");
		sb.append("  , (case when m.DEAL_COST is null then m.LIST_COST");
		sb.append("    when m.DEAL_COST <= 0 then m.LIST_COST");
		sb.append("    else m.DEAL_COST");
		sb.append("   end) THE_COST");
		sb.append("  from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d");
		sb.append("  where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.SCHEDULE_ID in (");
		for (int ii = 0; ii < schedules.length; ii++ ) {
			if ( ii > 0 ) sb.append(",");
			sb.append(schedules[ii]);
		}
		sb.append("  )");
		sb.append("  and m.COMP_STR_ID in (");
		for (int ii = 0; ii < stores.length; ii++ ) {
			if ( ii > 0 ) sb.append(",");
			sb.append(stores[ii]);
		}
		sb.append("  )");
		
		sb.append(" )");
		sb.append(")");
		
		String extraStr = " m, ITEM_LOOKUP i";
		String extraStr2 = " where m.ITEM_CODE = i.ITEM_CODE";
		if ( includeDept ) {
			extraStr += ", DEPARTMENT d";
			extraStr2 += " and i.DEPT_ID = d.ID";
		}
		if ( includeCat ) {
			extraStr += ", CATEGORY c";
			extraStr2 += " and i.CATEGORY_ID = c.ID";
		}
		if ( includeSubCat ) {
			extraStr += ", SUB_CATEGORY sc";
			extraStr2 += " and i.SUB_CATEGORY_ID = sc.ID";
		}
		if ( includeSegment ) {
			extraStr += ", ITEM_SEGMENT s";
			extraStr2 += " and i.SEGMENT_ID = s.ID";
		}
		sb.append(extraStr).append(extraStr2);
		
		sb.append(" group by COMP_STR_ID");
		extraStr = "";
		if ( includeDept ) {
			extraStr += ", i.DEPT_ID";
		}
		if ( includeCat ) {
			extraStr += ", i.CATEGORY_ID";
		}
		if ( includeSubCat ) {
			extraStr += ", i.SUB_CATEGORY_ID";
		}
		if ( includeSegment ) {
			extraStr += ", i.SEGMENT_ID";
		}
		sb.append(extraStr);

		String sql = sb.toString();
		logger.debug("SQL: " + sql);
		
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getRevenueCostSummaryWeekly");
		}
		catch (GeneralException ex) {
			logger.error("getRevenueCostSummaryWeeklyRowSet: " + sb.toString(), ex);
		}
		
		return result;
	}

	public void setupSummaryDTO (CachedRowSet result, SummaryWeeklyDTO dto)
	throws SQLException
	{
		dto.setCompStoreId(result.getInt("COMP_STR_ID"));
		dto.setRegularRevenue(result.getDouble("REVENUE_REGULAR"));
		dto.setRegularQuantity(result.getDouble("QUANTITY_REGULAR"));
		dto.setSaleRevenue(result.getDouble("REVENUE_SALE"));
		dto.setSaleQuantity(result.getDouble("QUANTITY_SALE"));
		dto.setFinalCost(result.getDouble("FINAL_COST"));
	}
	
	public void insertStoreSummaryWeekly (Connection conn, SummaryWeeklyDTO dto)
	throws GeneralException
	{
		insertSummaryWeekly (conn, dto, false, false, false, false);
	}

	public void insertDepartmentSummaryWeekly (Connection conn, SummaryWeeklyDTO dto)
	throws GeneralException
	{
		insertSummaryWeekly (conn, dto, true, false, false, false);
	}

	public void insertCategorySummaryWeekly (Connection conn, SummaryWeeklyDTO dto)
	throws GeneralException
	{
		insertSummaryWeekly (conn, dto, true, true, false, false);
	}

	public void insertSubCategorySummaryWeekly (Connection conn, SummaryWeeklyDTO dto)
	throws GeneralException
	{
		insertSummaryWeekly (conn, dto, true, true, true, false);
	}

	public void insertSegmentSummaryWeekly (Connection conn, SummaryWeeklyDTO dto)
	throws GeneralException
	{
		insertSummaryWeekly (conn, dto, true, true, true, true);
	}

	private void insertSummaryWeekly (Connection conn, SummaryWeeklyDTO dto,
		boolean includeDept, boolean includeCat, boolean includeSubCat, boolean includeSegment)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer("insert into SUMMARY_WEEKLY (SCHEDULE_ID, COMP_STR_ID");
		if ( includeDept ) {
			sb.append(", DEPT_ID");
		}
		if ( includeCat ) {
			sb.append(", CATEGORY_ID");
		}
		if ( includeSubCat ) {
			sb.append(", SUB_CATEGORY_ID");
		}
		int segId = dto.getSegmentId();
		if ( includeSegment && segId != -1 ) {
			sb.append(", SEGMENT_ID");
		}
		//sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE");
		sb.append(", REVENUE_REGULAR, REVENUE_SALE");
		sb.append(", FINAL_COST, MARGIN, VISIT_COUNT, VISIT_REVENUE_AVG");
		sb.append(", REVENUE_TOTAL, MARGIN_PCT)");
		sb.append(" values (");
		sb.append(dto.getScheduleId());
		sb.append(",").append(dto.getCompStoreId());
		if ( includeDept ) {
			sb.append(",").append(dto.getDeptId());
		}
		if ( includeCat ) {
			sb.append(",").append(dto.getCatId());
		}
		if ( includeSubCat ) {
			sb.append(",").append(dto.getSubCatId());
		}
		if ( includeSegment && segId != -1 ) {
			sb.append(",").append(dto.getSegmentId());
		}
		sb.append(",").append(dto.getRegularRevenue());
		sb.append(",").append(dto.getSaleRevenue());
		sb.append(",").append(dto.getFinalCost());
		sb.append(",").append(dto.getMargin());
		
		sb.append(",").append(dto.getVisitCount());
		sb.append(",").append(dto.getVisitCostAverage());
		sb.append(",").append(dto.getTotalRevenue());
		
		/* Margin percent greater than or equal 1000% (on the positive and negative side) 
		to be stored as 999.99 with appropriate sign */ 
		double marginPct = dto.getMarginPercent();
		if (marginPct > 999.99) 
			marginPct = 999.99;
		else if (marginPct < -999.99)
			marginPct = -999.99;
		
		sb.append(",").append(marginPct);
		sb.append(")");
	
		try {
			PristineDBUtil.execute(conn, sb, "SummaryWeekly - Insert");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
			throw ex;
		}
	}

	public void deleteSummaryWeekly (Connection conn, Date start, Date end, int storeId,
									 boolean noStoreDelete, boolean onlyStoreDelete)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer("delete from summary_weekly where schedule_id in (");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String startStr = formatter.format(start);
		sb.append(" select schedule_id from schedule where start_date = ").append("To_DATE('").append(startStr).append("', 'YYYYMMDD')");
		if ( storeId != -1 ) {
			sb.append(" and comp_str_id = ").append(storeId);
		}
		sb.append(")");
		if ( noStoreDelete ) {
			sb.append(" and not (dept_id is null and category_id is null)");
		}
		if ( onlyStoreDelete ) {
			sb.append(" and (dept_id is null and category_id is null)");
		}
		
		logger.debug("deleteSummaryWeekly SQL: " + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "SummaryWeekly - Delete");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
			throw ex;
		}
	}

	/*
delete from summary_weekly where schedule_id in
(select schedule_id from schedule where start_date = '7-aug-11' and comp_str_id = 5704)
order by comp_str_id, dept_id, category_id
	 */
/*
	private void updateVisit (Connection conn, SummaryWeeklyDTO dto)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer("update SUMMARY_WEEKLY set");
		sb.append(" VISIT_COUNT = ").append(dto.getVisitCount());
		sb.append(", VISIT_REVENUE_AVG = ").append(dto.getVisitCostAverage());
		sb.append(" where SCHEDULE_ID = ").append(dto.getScheduleId());
		sb.append(" and COMP_STR_ID = ").append(dto.getCompStoreId());
		sb.append(" and DEPT_ID is null and CATEGORY_ID is null");
	
		try {
			PristineDBUtil.execute(conn, sb, "SummaryWeekly - update");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
			throw ex;
		}
	}
*/
}

/*
CREATE TABLE SUMMARY_WEEKLY
(
  SCHEDULE_ID		NUMBER(8,0) NOT NULL,
  COMP_STR_ID		NUMBER(5,0) NOT NULL,
  DEPT_ID			NUMBER(3,0) NULL,
  CATEGORY_ID		NUMBER(4,0) NULL,
  REVENUE_REGULAR	NUMBER(10,2) DEFAULT 0,
--  QUANTITY_REGULAR  NUMBER(10,4) DEFAULT 0,
  REVENUE_SALE    	NUMBER(10,2) DEFAULT 0,
--  QUANTITY_SALE  	NUMBER(10,4) DEFAULT 0,
  FINAL_COST  		NUMBER(10,2) DEFAULT 0,
  MARGIN  			NUMBER(10,2) DEFAULT 0,
  VISIT_COUNT		NUMBER(6,0) DEFAULT 0,
  VISIT_REVENUE_AVG	NUMBER(7,2) DEFAULT 0	
)
TABLESPACE "USERS";

  CREATE INDEX Summary_Weekly_StrID ON SUMMARY_WEEKLY (COMP_STR_ID)
  	  TABLESPACE "USERS";
  ALTER TABLE SUMMARY_WEEKLY ADD (
      CONSTRAINT FK_SUMMARY_WEEKLY_COMP_STORE
      FOREIGN KEY (COMP_STR_ID)
      REFERENCES COMPETITOR_STORE(COMP_STR_ID));
  ALTER TABLE SUMMARY_WEEKLY ADD (
 	  CONSTRAINT FK_SUMMARY_WEEKLY_SCHEDULE
 	  FOREIGN KEY (SCHEDULE_ID) 
 	  REFERENCES SCHEDULE (SCHEDULE_ID));
  ALTER TABLE SUMMARY_WEEKLY ADD (
 	  CONSTRAINT FK_SUMMARY_WEEKLY_DEPT
 	  FOREIGN KEY (DEPT_ID) 
 	  REFERENCES DEPARTMENT (ID));
  ALTER TABLE SUMMARY_WEEKLY ADD (
 	  CONSTRAINT FK_SUMMARY_WEEKLY_CAT
 	  FOREIGN KEY (CATEGORY_ID) 
 	  REFERENCES CATEGORY (ID));

*
select m.COMP_STR_NO, DISTINCT i.DEPT_ID from MOVEMENT_DAILY m, ITEM_LOOKUP i, DEPARTMENT d 
where i.STANDARD_UPC like '''' || m.UPC || '%''' and i.DEPT_ID = d.ID   
and pos_timestamp < '9-mar-11' and terminal_no > 1
group by m.comp_str_no, il.dept_id, il.category_id, m.transaction_no
order by m.comp_str_no, m.transaction_no

select comp_str_no, count(transaction_no) as visit_count, sum(revenue) as revenue
, round(sum(revenue)/count(transaction_no), 2) as visit_revenue_avg from
(
select comp_str_no, weekday, transaction_no, sum(price) revenue from movement_daily
where pos_timestamp > '16-1pr-11' and pos_timestamp <= '23-apr-11'  and transaction_no > 0
group by comp_str_no, weekday, transaction_no
)
group by comp_str_no
order by comp_str_no

select m.COMP_STR_ID, sum(REVENUE_REGULAR) as REVENUE_REGULAR, sum(QUANTITY_REGULAR) as QUANTITY_REGULAR
, sum(REVENUE_SALE) as REVENUE_SALE, sum(QUANTITY_SALE) as QUANTITY_SALE
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d
where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.SCHEDULE_ID in (282) and m.COMP_STR_ID in (5754)
group by m.COMP_STR_ID

select COMP_STR_ID, sum(REVENUE_REGULAR) as REVENUE_REGULAR, sum(REVENUE_SALE) as REVENUE_SALE, round(sum(THE_COST),2) as FINAL_COST
from
(
select COMP_STR_ID, CHECK_DATA_ID, ITEM_CODE, REVENUE_REGULAR, REVENUE_SALE, (QUANTITY_REGULAR * THE_COST + QUANTITY_SALE * THE_COST) as THE_COST 
from
(
select m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE,
	(case when m.DEAL_COST is null then m.LIST_COST
		when m.DEAL_COST = 0 then m.LIST_COST
		else m.DEAL_COST
	end) THE_COST
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d 
where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.SCHEDULE_ID in (282) and m.COMP_STR_ID in (5754)
)
)
group by COMP_STR_ID

select COMP_STR_ID, DEPT_ID, sum(REVENUE_REGULAR) as REVENUE_REGULAR, sum(REVENUE_SALE) as REVENUE_SALE, round(sum(THE_COST),2) as FINAL_COST
from
(
select COMP_STR_ID, CHECK_DATA_ID, ITEM_CODE, REVENUE_REGULAR, REVENUE_SALE, (QUANTITY_REGULAR * THE_COST + QUANTITY_SALE * THE_COST) as THE_COST 
from
(
select m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE,
	(case when m.DEAL_COST is null then m.LIST_COST
		when m.DEAL_COST = 0 then m.LIST_COST
		else m.DEAL_COST
	end) THE_COST
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d 
where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.SCHEDULE_ID in (282) and m.COMP_STR_ID in (5754)
)
) m, ITEM_LOOKUP i, DEPARTMENT d where m.ITEM_CODE = i.ITEM_CODE and i.DEPT_ID = d.ID    
group by COMP_STR_ID, DEPT_ID

-- sch 282 store 5754 dept 8 cat 272 item code 73040, 76337, 76338, 66565, 66567
-- sch 283 store 5756 dept 9 cat 335
item 66565 841058050193, item 66567 841058022015, item 73040 675690500170, item 76337 309973985032
item 76338	071249054680
select m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE,
	(case when m.DEAL_COST is null then m.LIST_COST
		when m.DEAL_COST = 0 then m.LIST_COST
		else m.DEAL_COST
	end) THE_COST
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d, item_lookup i
where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.SCHEDULE_ID in (282) and m.COMP_STR_ID in (5754)
and i.item_code = m.item_code and i.category_id = 272

select * from movement_daily m, competitor_store s where m.comp_str_no = s.comp_str_no
and s.comp_str_id = 5754 and pos_timestamp > '17-apr-11' and upc like '%10580501%'
and m.price = 0
select item_code, standard_upc from item_lookup where item_code in (73040, 76337, 76338, 66565, 66567)

----------
item 76338	071249054680 cat
item 41888 024100440856, item 42079 071921003395

select * from movement_daily m, competitor_store s where m.comp_str_no = s.comp_str_no
and s.comp_str_id = 5754 and upc like '%8137005635%'

select m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE,
	(case when m.DEAL_COST is null then m.LIST_COST
		when m.DEAL_COST = 0 then m.LIST_COST
		else m.DEAL_COST
	end) THE_COST
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA_VIEW d, item_lookup i
where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.COMP_STR_ID = m.COMP_STR_ID and d.start_date = '17-apr-11'
and m.item_code in (41888, 42079)
and i.item_code = m.item_code and i.category_id = 271 and m.item_code in (71116)

select s.comp_str_id, upc, sum(price) as revenue from movement_daily m, competitor_store s where m.comp_str_no = s.comp_str_no
and s.comp_str_id = 5754 and (upc like '%2410044085%' or upc like '%7192100339%') and pos_timestamp > '17-apr-11'
group by s.comp_str_id, upc

select m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE,
	(case when m.DEAL_COST is null then m.LIST_COST
		when m.DEAL_COST = 0 then m.LIST_COST
		else m.DEAL_COST
	end) THE_COST
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA_VIEW d
where m.CHECK_DATA_ID = d.CHECK_DATA_ID and d.COMP_STR_ID = m.COMP_STR_ID and d.start_date = '17-apr-11' and m.COMP_STR_ID = 5662
and m.item_code in (41888, 42079)

update movement_weekly set REVENUE_REGULAR = 0, QUANTITY_REGULAR = 0, REVENUE_SALE = 0, QUANTITY_SALE = 0

select count(*) from summary_weekly where schedule_id in (select schedule_id from schedule where start_date = '31-jul-11') and category_id is null
order by schedule_id desc

// TOPS Revenue from daily movement
select i.dept_id, sum(m.price) as revenue from MOVEMENT_DAILY m, ITEM_LOOKUP i
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and m.sale_flag = 'N'
and m.pos_timestamp >= To_DATE('201108210000', 'YYYYMMDDHH24MI') and m.pos_timestamp <= To_DATE('201108272359', 'YYYYMMDDHH24MI')
and i.dept_id not in (21,34) and m.comp_str_no = '0363'
group by i.dept_id order by i.dept_id

select m.comp_str_no, i.dept_id, i.item_code, m.price as revenue, m.sale_flag,
(case when m.quantity <> 0 then m.quantity else m.weight end) quantity from MOVEMENT_DAILY m, ITEM_LOOKUP i
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc
and m.pos_timestamp >= To_DATE('201108210000', 'YYYYMMDDHH24MI') and m.pos_timestamp <= To_DATE('201108272359', 'YYYYMMDDHH24MI')
and i.dept_id not in (21,34) and m.comp_str_no = '0363'
group by i.dept_id order by i.dept_id

//////////////////////////
select comp_str_no, dept_id, item_code, sum(reg_revenue) as reg_revenue, sum(sale_revenue) as sale_revenue,
sum(reg_quantity) as reg_quantity, sum(sale_quantity) as sale_quantity from (
select comp_str_no, dept_id, item_code, (case when sale_flag = 'N' then revenue end) reg_revenue,
(case when sale_flag = 'Y' then revenue end) sale_revenue,
(case when sale_flag = 'N' then quantity end) reg_quantity,
(case when sale_flag = 'Y' then quantity end) sale_quantity
from (
select m.comp_str_no, i.dept_id, i.item_code, m.price as revenue, m.sale_flag,
(case when m.quantity <> 0 then m.quantity else m.weight end) quantity from MOVEMENT_DAILY m, ITEM_LOOKUP i
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc
and m.pos_timestamp >= To_DATE('201108210000', 'YYYYMMDDHH24MI') and m.pos_timestamp <= To_DATE('201108272359', 'YYYYMMDDHH24MI')
and i.dept_id not in (21,34) and m.comp_str_no = '0363'
order by m.comp_str_no, i.dept_id, i.item_code
)
)
group by comp_str_no, dept_id, item_code
order by comp_str_no, dept_id, item_code

// Weekly visit
select comp_str_no, sum(visit_count) as visit_count from (
	select m.comp_str_no, m.weekday, count(distinct m.transaction_no) as visit_count from MOVEMENT_DAILY m, ITEM_LOOKUP i
	where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc(+) and m.comp_str_no = '0363'
	and m.pos_timestamp >= To_DATE('201109180000', 'YYYYMMDDHH24MI') and m.pos_timestamp <= To_DATE('201109242359', 'YYYYMMDDHH24MI')
	and (i.dept_id not in (21,34,37) or m.upc in ('00000000101','00000000102')) 
	group by m.comp_str_no, m.weekday
)
group by comp_str_no

select revenue_total, visit_count * visit_revenue_avg as rev_visit from summary_weekly where comp_str_id = 5704 and dept_id is null order by schedule_id desc


*/
