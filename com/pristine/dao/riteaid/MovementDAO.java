package com.pristine.dao.riteaid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.dao.IDAO;
import com.pristine.dto.Movement13WeeklyDTO;
import com.pristine.dto.MovementDTO;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.MovementWeeklyDTO;


public class MovementDAO implements IDAO
{
	static Logger logger = Logger.getLogger("MovementDAO");
	
	private static final String GET_WEEKLY_MOVEMENT = "SELECT m.ITEM_CODE, sum(NVL(m.QUANTITY_REGULAR, 0)) as QUANTITY_REGULAR, sum(NVL(m.QUANTITY_SALE,0)) as QUANTITY_SALE " +
			   " FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m " +
			   " WHERE c.SCHEDULE_ID in (%SCHEDULE_IDS%) AND c.CHECK_DATA_ID = m.CHECK_DATA_ID " +
			   " AND m.ITEM_CODE in (%s) group by m.ITEM_CODE";

	private static final String UPDATE_12WEEK_MOVEMENTS = "UPDATE %TARGET_TABLE% SET QTY_REGULAR_12WK = ?, QTY_SALE_12WK = ? where CHECK_DATA_ID = ?";

	private static final String UPDATE_13WEEK_MOVEMENTS = "UPDATE %TARGET_TABLE% SET QTY_REGULAR_13WK = ?, QTY_SALE_13WK = ? where CHECK_DATA_ID = ?";

	public ArrayList<String> getTopsStores (Connection conn, Date start, Date end)
	throws GeneralException
	{
		ArrayList<String> stores = new ArrayList<String>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT cs.comp_str_no FROM schedule s, competitor_store cs");
		sb.append(" WHERE s.comp_str_id = cs.comp_str_id");
		sb.append(" AND TO_CHAR(s.start_date, 'MM/DD/YYYY') = '").append(startStr).append("'");
		sb.append(" AND cs.comp_chain_id = 50");		// TOPS chain id is 50

		try 
		{
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getTopsStores");
			while ( result.next() )
			{
				String storeNo = result.getString("comp_str_no");
				stores.add(storeNo);
			}
		}
		catch (Exception ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
			
		return stores;
	}
	
	public ArrayList<String> getTopsMovementStores (Connection conn, Date start, Date end)
	throws GeneralException
	{
		ArrayList<String> stores = new ArrayList<String>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);

		StringBuffer sb = new StringBuffer("select distinct COMP_STR_NO from MOVEMENT_DAILY where");
		sb.append(" POS_TIMESTAMP >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and POS_TIMESTAMP <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");

		try 
		{
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getTopsMovementStores");
			while ( result.next() )
			{
				String storeNum = result.getString("COMP_STR_NO");
				stores.add(storeNum);
			}
		}
		catch (Exception ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
			
		return stores;
	}
	
	public boolean insertDailyMovement (Connection conn, MovementDTO movement, boolean deleteIfPresent)
		throws GeneralException
	{
		StringBuffer sb = new StringBuffer("insert into MOVEMENT_DAILY (");
		sb.append(" COMP_STR_NO, UPC, POS_TIMESTAMP, WEEKDAY, UNIT_PRICE, PRICE, SALE_FLAG, QUANTITY, WEIGHT");
		sb.append(", TRANSACTION_NO, CUSTOMER_CARD_NO) values (");
		sb.append("'").append(movement.getItemStore()).append("'");	
		String upc = PrestoUtil.castUPC( movement.getItemUPC(), true);
		sb.append(", '").append(upc).append("'");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String dateStr = formatter.format(movement.getItemDateTime());
		sb.append(", To_DATE('").append(dateStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(", '").append(movement.getDayOfWeek()).append("'");
		
		double unitPrice = movement.getItemNetPrice();
		double price = movement.getExtendedNetPrice();
		char saleFlag = ( 0 <= price && price < movement.getExtendedGrossPrice() ) ? 'Y' : 'N';
		sb.append(", ").append(unitPrice);
		sb.append(", ").append(price);
		sb.append(", '").append(saleFlag).append("'");
		
		sb.append(", ").append(movement.getExtnQty());
		sb.append(", ").append(movement.getExtnWeight());
		sb.append(", ").append(movement.getTransactionNo());
		sb.append(", '").append(movement.getCustomerId()).append("'");
		
		sb.append(")");		// close

		//String sql = sb.toString();
		try {
			PristineDBUtil.execute(conn, sb, "MovementDAO - Insert");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}
		
		return true;
	}

	public boolean updateDailyMovementTransactions (Connection conn, MovementDTO movement, boolean logError)
		throws GeneralException
	{
		StringBuffer sb = new StringBuffer("update MOVEMENT_DAILY set");
		sb.append(" TRANSACTION_NO = ").append(movement.getTransactionNo());	
		sb.append(" where COMP_STR_NO = '").append(movement.getItemStore()).append("'");	
		String upc = PrestoUtil.castUPC( movement.getItemUPC(), true);
		sb.append(" and UPC = '").append(upc).append("'");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		String dateStr = formatter.format(movement.getItemDateTime());
		sb.append(" and POS_TIMESTAMP = To_DATE('").append(dateStr).append("', 'YYYYMMDDHH24MISS')"); 

		/*
		char saleFlag;
		double unitPrice = movement.getItemNetPrice();
		double price;
		if ( 0 < unitPrice && unitPrice < movement.getItemGrossPrice() ) {
			saleFlag = 'Y';
			price = movement.getExtendedNetPrice();
		}
		else {
			saleFlag = 'N';
			unitPrice = movement.getItemGrossPrice();
			price = movement.getExtendedGrossPrice();
		}
		sb.append(" and UNIT_PRICE = ").append(unitPrice);
		sb.append(" and SALE_FLAG = '").append(saleFlag).append("'");
		
		sb.append(" and QUANTITY = ").append(movement.getExtnQty());
		sb.append(" and WEIGHT = ").append(movement.getExtnWeight());
		sb.append(" and PRICE = ").append(price);
		*/

		if ( movement.getCustomerId().length() > 0 )
			sb.append(" and CUSTOMER_CARD_NO = '").append(movement.getCustomerId()).append("'");
		
		//String sql = sb.toString();
		int count = 0;
		try
		{
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - UpdateDailyTransactions");
			if ( count > 1 ) {
				String msg = "Updated " + count + " records for store=" + movement.getItemStore()
							 + " time=" + dateStr + " ups=" + upc;     
				logger.info(msg);
			}
		}
		catch (GeneralException gex)
		{
			if ( logError ) {
				logger.error("SQL: " + sb.toString(), gex);
			}
			throw gex;
		}
	
		return count > 0;
	}

	public boolean deleteDailyMovement (Connection conn, MovementDTO movement, boolean logError)
		throws GeneralException
	{
		StringBuffer sb = new StringBuffer("delete from MOVEMENT_DAILY where");
		sb.append(" COMP_STR_NO = '").append(movement.getItemStore()).append("'");	
		String upc = PrestoUtil.castUPC( movement.getItemUPC(), true);
		sb.append(" and UPC = '").append(upc).append("'");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String dateStr = formatter.format(movement.getItemDateTime());
		sb.append(" and POS_TIMESTAMP = To_DATE('").append(dateStr).append("', 'YYYYMMDDHH24MI')"); 
		
		char saleFlag;
		double unitPrice = movement.getItemNetPrice();
		double price;
		if ( 0 < unitPrice && unitPrice < movement.getItemGrossPrice() ) {
			saleFlag = 'Y';
			price = movement.getExtendedNetPrice();
		}
		else {
			saleFlag = 'N';
			unitPrice = movement.getItemGrossPrice();
			price = movement.getExtendedGrossPrice();
		}
		sb.append(" and UNIT_PRICE = ").append(unitPrice);
		sb.append(" and SALE_FLAG = '").append(saleFlag).append("'");
		
		sb.append(" and QUANTITY = ").append(movement.getExtnQty());
		sb.append(" and WEIGHT = ").append(movement.getExtnWeight());
		sb.append(" and PRICE = ").append(price);
		sb.append(" and CUSTOMER_CARD_NO = '").append(movement.getCustomerId()).append("'");
		
		try {
			PristineDBUtil.execute(conn, sb, "MovementDAO - Delete");
		}
		catch (GeneralException gex)
		{
			if ( logError ) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}
	
		return true;
	}

	public List<MovementWeeklyDTO> getQuantityMovementsForPeriod (Connection conn, String storeNum,
			Date start, Date end, boolean sale)
	throws GeneralException
	{
		CachedRowSet result = getQuantityMovementsForPeriodRowSet (conn, storeNum, start, end, sale);
		return populateList (result, sale);
	}
	
	public CachedRowSet getQuantityMovementsForPeriodRowSet (Connection conn, String storeNum,
			Date start, Date end, boolean sale)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);

		StringBuffer sb = new StringBuffer("select COMP_STR_NO, UPC, sum(PRICE) as PRICE, sum(QUANTITY) as QUANTITY from MOVEMENT_DAILY where");
		sb.append(" POS_TIMESTAMP >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and POS_TIMESTAMP <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		char saleFlag = sale ? 'Y' : 'N';
		sb.append(" and SALE_FLAG = '").append(saleFlag).append("'");	
		sb.append(" and QUANTITY <> 0");	// need to include returns
		if ( storeNum != null ) {
			sb.append(" and COMP_STR_NO = '").append(storeNum).append("'");
		}
		//sb.append(" and UPC in ('07078424030', '07078427352', '07078428240', '03010001610', '03000031018', '04400000028')");
		sb.append(" group by COMP_STR_NO, UPC");
		sb.append(" order by COMP_STR_NO, UPC");
	
		String sql = sb.toString();
		logger.debug ("getQuantityMovementsForPeriod SQL: " + sql);
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getQuantityMovementsForPeriod");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
			
		return result;
	}

	public List<MovementWeeklyDTO> getWeightMovementsForPeriod (Connection conn, String storeNum,
			Date start, Date end, boolean sale)
	throws GeneralException
	{
		CachedRowSet result = getWeightMovementsForPeriodRowSet (conn, storeNum, start, end, sale);
		return populateList (result, sale);
	}

	public CachedRowSet getWeightMovementsForPeriodRowSet (Connection conn, String storeNum,
			Date start, Date end, boolean sale)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer("select COMP_STR_NO, UPC, sum(PRICE) as PRICE, sum(WEIGHT) as QUANTITY from MOVEMENT_DAILY where");
		sb.append(" POS_TIMESTAMP >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and POS_TIMESTAMP <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		char saleFlag = sale ? 'Y' : 'N';
		sb.append(" and SALE_FLAG = '").append(saleFlag).append("'");	
		sb.append(" and WEIGHT <> 0");	// need to include returns
		if ( storeNum != null ) {
			sb.append(" and COMP_STR_NO = '").append(storeNum).append("'");
		}
		//sb.append(" and UPC in ('20809900000', '20567900000', '20173600000', '20209700000')");
		sb.append(" group by COMP_STR_NO, UPC");
		sb.append(" order by COMP_STR_NO, UPC");
	
		String sql = sb.toString();
		logger.debug ("getWeightMovementsForPeriod SQL: " + sql);
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getWeightMovementsForPeriod");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
			
		return result;
	}

	private List<MovementWeeklyDTO> populateList (CachedRowSet result, boolean sale)
	{
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			while (result.next())
			{
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				loadWeeklyDTO (dto, result);
				dto.setSaleFlag(sale);
				list.add (dto); 
			}
		}
		catch (SQLException ex) {
			logger.error("Error:", ex);
		}

		return list;
	}
	
	public List<MovementWeeklyDTO> getVisitWeekly (Connection conn, String storeNum, Date start, Date end)
	throws GeneralException
	{
		CachedRowSet result = getVisitWeeklyRowSet(conn, storeNum, start, end);
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			while (result.next())
			{
				try
				{
					MovementWeeklyDTO dto = new MovementWeeklyDTO();
					dto.setItemStore(result.getString("COMP_STR_NO"));
					dto.setItemUPC(result.getString("UPC"));
					dto.setItemCode(result.getInt("ITEM_CODE"));
					dto.setVisitCount(result.getInt("VISIT_COUNT"));
					list.add (dto); 
				}
				catch (SQLException ex) {
					logger.error("Error", ex);
				}
			}
		}
		catch (SQLException ex) {
			logger.error("Error", ex);
		}
	
		return list;
	}

	public CachedRowSet getVisitWeeklyRowSet (Connection conn, String storeNum, Date start, Date end)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select COMP_STR_NO, UPC, ITEM_CODE, sum(VISIT_COUNT) as VISIT_COUNT from");
		sb.append(" (");
		sb.append("  select COMP_STR_NO, WEEKDAY, UPC, ITEM_CODE, count(distinct transaction_no) as VISIT_COUNT from MOVEMENT_DAILY");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		sb.append("   where pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		String endStr = formatter.format(end);
		sb.append("   and pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append("   and transaction_no > 0");
		if ( storeNum != null ) {
			sb.append("   and COMP_STR_NO ='").append(storeNum).append("'");
		}
		sb.append("   group by COMP_STR_NO, WEEKDAY, UPC, ITEM_CODE");
		sb.append(" )");
		sb.append(" group by COMP_STR_NO, UPC, ITEM_CODE");

		String sql = sb.toString();
		logger.debug ("getVisitWeekly SQL: " + sql);
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getVisitWeekly");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		
		return result;
	}
	
	public List<MovementWeeklyDTO> getLIGVisitWeekly (Connection conn, String storeNum, Date start, Date end)
	throws GeneralException
	{
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		
		StringBuffer sb = new StringBuffer();
		sb.append("select COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE, sum(VISIT_COUNT) as VISIT_COUNT from");
		sb.append(" (");
		sb.append("  select m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY, count(distinct m.transaction_no) as VISIT_COUNT");
		sb.append("   from MOVEMENT_DAILY m, ITEM_LOOKUP i, retailer_like_item_group l");
		// sb.append("   where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.ret_lir_id = l.ret_lir_id");
		sb.append("   where m.item_code = i.item_code and i.ret_lir_id = l.ret_lir_id");
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		sb.append("   and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		String endStr = formatter.format(end);
		sb.append("   and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append("   and m.transaction_no > 0");
		if ( storeNum != null ) {
			sb.append("   and m.COMP_STR_NO ='").append(storeNum).append("'");
		}
		sb.append("   group by m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY");
		sb.append(" )");
		sb.append(" group by COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE");
		sb.append(" order by COMP_STR_NO, RET_LIR_ID");

		String sql = sb.toString();
		logger.debug ("getLIGVisitWeekly SQL: " + sql);
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getLIGVisitWeekly");
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		
		try
		{
			while (result.next())
			{
				try
				{
					MovementWeeklyDTO dto = new MovementWeeklyDTO();
					dto.setItemStore(result.getString("COMP_STR_NO"));
					dto.setLirId(result.getInt("RET_LIR_ID"));
					dto.setItemCode(result.getInt("RET_LIR_ITEM_CODE"));
					dto.setVisitCount(result.getInt("VISIT_COUNT"));
					list.add (dto); 
				}
				catch (SQLException ex) {
					logger.error("Error", ex);
				}
			}
		}
		catch (SQLException ex) {
			logger.error("Error", ex);
		}
	
		return list;
	}

	public void getCostWeekly (Connection conn, MovementWeeklyDTO dto)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select m.LIST_COST");
		sb.append(",  (case when m.DEAL_COST is null then m.LIST_COST");
		sb.append("   when m.DEAL_COST <= 0 then m.LIST_COST");
		sb.append("   else m.DEAL_COST");
		sb.append("   end) COST");
		sb.append(" from MOVEMENT_WEEKLY m");
		sb.append(" where m.COMP_STR_ID = ").append(dto.getCompStoreId());
		sb.append(" and m.CHECK_DATA_ID = ").append(dto.getCheckDataId());
		sb.append(" and m.ITEM_CODE = ").append(dto.getItemCode());
		
		//String sql = sb.toString();
		//logger.debug ("getVisitWeekly SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getRevenueCostWeekly");
		try
		{
			while (result.next()) {
				dto.setListCost(result.getDouble("LIST_COST"));
				dto.setDealCost(result.getDouble("COST"));
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public void get13WeekData (Connection conn, MovementWeeklyDTO dto, String schCSV)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE");
		sb.append(" from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d");
		sb.append(" where d.SCHEDULE_ID in (").append(schCSV).append(")");
		sb.append(" and m.ITEM_CODE = ").append(dto.getItemCode());
		sb.append(" and m.CHECK_DATA_ID = d.CHECK_DATA_ID");
		//logger.debug ("get13WeekData SQL: " + sb.toString());

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "get13WeekData");
		try
		{
			while (result.next())
			{
				double d = dto.getQuantityRegular() + result.getDouble("QUANTITY_REGULAR");
				dto.setQuantityRegular13Week(d);
				d = dto.getQuantitySale() + result.getDouble("QUANTITY_SALE");
				dto.setQuantitySale13Week(d);
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public void get13WeekData (Connection conn, Date lastStartDate, MovementWeeklyDTO dto)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE");
		sb.append(" from MOVEMENT_WEEKLY m, COMPETITIVE_DATA_VIEW d");
		sb.append(" where m.CHECK_DATA_ID = d.CHECK_DATA_ID");
		sb.append(" and m.COMP_STR_ID = ").append(dto.getCompStoreId());
		sb.append(" and m.ITEM_CODE = ").append(dto.getItemCode());
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
		Date firstStartDate = get13WeekFirstStartDate(lastStartDate);
		String startStr = formatter.format(firstStartDate);
		Date lastWeekStartDate = getPreviousWeekStartDate(lastStartDate);
		String endStr = formatter.format(lastWeekStartDate);
		
		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') >= '").append(startStr).append("'");
		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') <= '").append(endStr).append("'");
		
		//logger.debug ("get13WeekData SQL: " + sb.toString());

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "get13WeekData");
		try
		{
			while (result.next())
			{
				double d = dto.getQuantityRegular() + result.getDouble("QUANTITY_REGULAR");
				dto.setQuantityRegular13Week(d);
				d = dto.getQuantitySale() + result.getDouble("QUANTITY_SALE");
				dto.setQuantitySale13Week(d);
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public void get13WeekLIGData (Connection conn, Date lastStartDate, MovementWeeklyDTO dto)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE");
		sb.append(" from MOVEMENT_WEEKLY_LIG m, COMPETITIVE_DATA_VIEW d, item_lookup i, retailer_like_item_group l");
		sb.append(" where m.COMP_STR_ID = d.COMP_STR_ID and m.check_data_id = d.check_data_id");
		sb.append(" and m.ITEM_CODE = i.ITEM_CODE and i.ret_lir_id = l.ret_lir_id");
		sb.append(" and m.ITEM_CODE = l.RET_LIR_ITEM_CODE and i.ret_lir_id is not null");
		sb.append(" and m.COMP_STR_ID = ").append(dto.getCompStoreId());
		sb.append(" and m.ITEM_CODE = ").append(dto.getItemCode());
		sb.append(" and i.ret_lir_id = ").append(dto.getLirId());

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
		Date firstStartDate = get13WeekFirstStartDate(lastStartDate);
		String startStr = formatter.format(firstStartDate);
		Date lastWeekStartDate = getPreviousWeekStartDate(lastStartDate);
		String endStr = formatter.format(lastWeekStartDate);
		
		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') >= '").append(startStr).append("'");
		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') <= '").append(endStr).append("'");
		
		//sb.append(" group by i.ret_lir_id");
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "get13WeekLIGData");
		try
		{
			while (result.next())
			{
				double d = dto.getQuantityRegular() + result.getDouble("QUANTITY_REGULAR");
				dto.setQuantityRegular13Week(d);
				d = dto.getQuantitySale() + result.getDouble("QUANTITY_SALE");
				dto.setQuantitySale13Week(d);
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public List<MovementWeeklyDTO> getWeeklyMovements (Connection conn, int storeId, Date start, Date end)
	throws GeneralException
	{
		Date date;
		
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("select m.*, l.ret_lir_id, l.ret_lir_item_code");
		
		//Added as got exception when directly assigning the DEAL_START_DATE
		//DEAL_END_DATE,EFF_LIST_COST_DATE to date variables
		sb.append(",to_char(m.eff_list_cost_date, 'MM/DD/YYYY') as date_eff_list_cost"); 
		sb.append(",to_char(m.deal_start_date, 'MM/DD/YYYY') as date_deal_start");
		sb.append(",to_char(m.deal_end_date, 'MM/DD/YYYY')as date_deal_end");
		
		sb.append(" from movement_weekly m, item_lookup i, retailer_like_item_group l, competitive_data_view d");
		sb.append(" where m.check_data_id = d.check_data_id");
		sb.append(" and to_char(d.start_date, 'MM/DD/YYYY') = '").append(startStr).append("'");
		sb.append(" and m.item_code = i.item_code and i.ret_lir_id = l.ret_lir_id and i.ret_lir_id is not null");
		sb.append(" and m.comp_str_id = ").append(storeId);
		//sb.append(" and m.comp_str_id = 5704 and l.ret_lir_id in (1, 2, 16, 3, 4, 5, 10, 11, 13, 14, 273, 274)");
		//sb.append(" and m.comp_str_id = 5720 and l.ret_lir_id in (381, 1)");
		//sb.append(" and l.ret_lir_id in (1, 2, 273, 274, 381)");
		sb.append(" order by m.comp_str_id, l.ret_lir_id");

		String sql = sb.toString();
		logger.debug("getWeeklyMovements SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyMovements");
		
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			while (result.next())
			{
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				dto.setCompStoreId(result.getInt("COMP_STR_ID"));
				dto.setItemCode(result.getInt("ITEM_CODE"));
				dto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				
				dto.setRevenueRegular(result.getDouble("REVENUE_REGULAR"));
				dto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				dto.setRevenueSale(result.getDouble("REVENUE_SALE"));
				dto.setQuantitySale(result.getDouble("QUANTITY_SALE"));
				
				dto.setListCost(result.getDouble("LIST_COST"));
				if(result.getObject("date_eff_list_cost") != null)
				{
					date  = (Date) formatter.parse(result.getString("date_eff_list_cost"));
					dto.setEffListCostDate(date);
				}
				//dto.setEffListCostDate(result.getDate("EFF_LIST_COST_DATE"));
				 
				
				dto.setDealCost(result.getDouble("DEAL_COST"));
				
				Object obj1 = result.getObject("date_deal_start");
				//Object obj1 = result.getObject("DEAL_START_DATE");
				if ( obj1 != null ) {				 
					date  = (Date) formatter.parse(result.getString("date_deal_start"));
					dto.setDealStartDate(date);					  
					//dto.setDealEndDate(result.getDate("DEAL_START_DATE"));					 
				}
				
				Object obj2 = result.getObject("date_deal_end");
				//Object obj2 = result.getObject("DEAL_END_DATE");
				if ( obj2 != null ) {
					 	date  = (Date) formatter.parse(result.getString("date_deal_end"));
						dto.setDealEndDate(date);
						//dto.setDealEndDate(result.getDate("DEAL_END_DATE"));					 
				}
				
				dto.setCostChangeDirection(result.getInt("COST_CHG_DIRECTION"));
				Object obj3 = result.getObject("MARGIN_CHG_DIRECTION");
				if ( obj3 != null ) {
					dto.setMarginChangeDirection(result.getInt("MARGIN_CHG_DIRECTION"));
				}
				Object obj4 = result.getObject("TOTAL_MARGIN");
				if ( obj4 != null ) {
					dto.setMargin(result.getDouble("TOTAL_MARGIN"));
				}
				Object obj5 = result.getObject("MARGIN_PCT");
				if ( obj5 != null ) {
					dto.setMarginPercent(result.getDouble("MARGIN_PCT"));
				}
				
				dto.setLirId(result.getInt("RET_LIR_ID"));
				dto.setLirItemCode(result.getInt("RET_LIR_ITEM_CODE"));

				list.add (dto);
				int size = list.size();
				if ( (size % 25000) == 0 ) {
					logger.debug("getWeeklyMovements: processed " + String.valueOf(size));
				}
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	
		return list;
	}

	public List<MovementWeeklyDTO> getWeeklyMovementsFor13Week (Connection conn, int storeId, Date start, Date end)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("select m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE");
		sb.append(" from movement_weekly m, competitive_data_view d");
		sb.append(" where m.check_data_id = d.check_data_id");
		sb.append(" and to_char(d.start_date, 'MM/DD/YYYY') = '").append(startStr).append("'");
		sb.append(" and m.comp_str_id = ").append(storeId);
		//sb.append(" and m.comp_str_id = 5662 and l.ret_lir_id in (1, 2, 16, 3, 4, 5, 10, 11, 13, 14, 273, 274)");
		//sb.append(" and m.comp_str_id = 5704 and m.ITEM_CODE in (41898,35963,49690,35969,35980,50446,55418,55422,42000)");
		//sb.append(" and l.ret_lir_id in (381, 1)");

		String sql = sb.toString();
		logger.debug("getWeeklyMovementsFor13Week SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyMovementsFor13Week");
		
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			while (result.next())
			{
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				dto.setCompStoreId(storeId);
				dto.setItemCode(result.getInt("ITEM_CODE"));
				dto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				
				dto.setRevenueRegular(result.getDouble("REVENUE_REGULAR"));
				dto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				dto.setRevenueSale(result.getDouble("REVENUE_SALE"));
				dto.setQuantitySale(result.getDouble("QUANTITY_SALE"));
				
				list.add (dto);
				int size = list.size();
				if ( (size % 25000) == 0 ) {
					logger.debug("getWeeklyMovementsFor13Week: processed " + String.valueOf(size));
				}
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	
		return list;
	}

	public List<MovementWeeklyDTO> getWeeklyLIGMovementsFor13Week (Connection conn, int storeId, Date start, Date end)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("select m.CHECK_DATA_ID, m.ITEM_CODE, i.RET_LIR_ID, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE");
		sb.append(" from movement_weekly_lig m, competitive_data_view_lig d, item_lookup i");
		sb.append(" where m.check_data_id = d.check_data_id");
		sb.append(" and m.item_code = i.item_code");
		sb.append(" and to_char(d.start_date, 'MM/DD/YYYY') = '").append(startStr).append("'");
		sb.append(" and m.comp_str_id = ").append(storeId);
		//sb.append(" and m.comp_str_id = 5662 and l.ret_lir_id in (1, 2, 16, 3, 4, 5, 10, 11, 13, 14, 273, 274)");
		//sb.append(" and m.comp_str_id = 5704 and m.ITEM_CODE in (41898,35963,49690,35969,35980,50446,55418,55422,42000)");
		//sb.append(" and l.ret_lir_id in (381, 1)");

		String sql = sb.toString();
		logger.debug("getWeeklyLIGMovementsFor13Week SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyLIGMovementsFor13Week");
		
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			while (result.next())
			{
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				dto.setCompStoreId(storeId);
				dto.setItemCode(result.getInt("ITEM_CODE"));
				dto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				dto.setLirId(result.getInt("RET_LIR_ID"));
				
				dto.setRevenueRegular(result.getDouble("REVENUE_REGULAR"));
				dto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				dto.setRevenueSale(result.getDouble("REVENUE_SALE"));
				dto.setQuantitySale(result.getDouble("QUANTITY_SALE"));
				
				list.add (dto);
				int size = list.size();
				if ( (size % 25000) == 0 ) {
					logger.debug("getWeeklyMovementsFor13Week: processed " + String.valueOf(size));
				}
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	
		return list;
	}

	public boolean updateWeeklyMovement (Connection conn, MovementWeeklyDTO movement, boolean update)
	throws GeneralException
	{
		StringBuffer sb;
		int count = 0;
		if ( update )
		{
			sb = new StringBuffer("update MOVEMENT_WEEKLY set");
			
			sb.append(" REVENUE_REGULAR = ").append(movement.getRevenueRegular());
			sb.append(", QUANTITY_REGULAR = ").append(movement.getQuantityRegular());
			sb.append(", REVENUE_SALE = ").append(movement.getRevenueSale());
			sb.append(", QUANTITY_SALE = ").append(movement.getQuantitySale());
			sb.append(", TOTAL_MARGIN = ").append(movement.getMargin());
			
			/* Margin percent greater than or equal 1000% (on the positive and negative side) 
			to be stored as 999.99 with appropriate sign */ 
			double marginPct = movement.getMarginPercent();
			if (marginPct > 999.99) 
				marginPct = 999.99;
			else if (marginPct < -999.99)
				marginPct = -999.99;			
			
			sb.append(", MARGIN_PCT = ").append(marginPct);
			sb.append(", VISIT_COUNT = ").append(movement.getVisitCount());
			//sb.append(", QTY_REGULAR_13WK = ").append(movement.getQuantityRegular13Week());
			//sb.append(", QTY_SALE_13WK = ").append(movement.getQuantitySale13Week());

			sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());
			
			try {
				count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - UpdateWeekly");
			}
			catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}
		
		if ( count == 0 )
		{
			// Nothing was updated, insert record...
			sb = new StringBuffer("insert into MOVEMENT_WEEKLY (");
			
			sb.append("COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID");
			sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE");
			sb.append(", TOTAL_MARGIN, MARGIN_PCT, VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK");
			
			sb.append(") values (");
			
			sb.append(movement.getCompStoreId());
			sb.append(", ").append(movement.getItemCode());
			sb.append(", ").append(movement.getCheckDataId());
			
			sb.append(", ").append(movement.getRevenueRegular());
			sb.append(", ").append(movement.getQuantityRegular());
			sb.append(", ").append(movement.getRevenueSale());
			sb.append(", ").append(movement.getQuantitySale());
			
			sb.append(", ").append(movement.getMargin());
			
			/* Margin percent greater than or equal 1000% (on the positive and negative side) 
			to be stored as 999.99 with appropriate sign */ 
			double marginPct = movement.getMarginPercent();
			if (marginPct > 999.99) 
				marginPct = 999.99;
			else if (marginPct < -999.99)
				marginPct = -999.99;	
			
			sb.append(", ").append(marginPct);
			sb.append(", ").append(movement.getVisitCount());
			sb.append(", ").append(movement.getQuantityRegular13Week());
			sb.append(", ").append(movement.getQuantitySale13Week());
			
			sb.append(")");		// close
			
			//String sql = sb.toString();
			try {
				PristineDBUtil.execute(conn, sb, "MovementDAO - InsertWeekly");
			}
			catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}

		return true;
	}

	public boolean updateWeeklyMovement13WeekData (Connection conn, MovementWeeklyDTO movement)
	throws GeneralException
	{
		int count = 0;
		
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY set");
		sb.append(" QTY_REGULAR_13WK = ").append(movement.getQuantityRegular13Week());
		sb.append(", QTY_SALE_13WK = ").append(movement.getQuantitySale13Week());
		sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());
		
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - UpdateWeekly13WeekData");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}
		
		return count > 0;
	}

	public boolean updateWeeklyLIGMovement13WeekData (Connection conn, MovementWeeklyDTO movement)
	throws GeneralException
	{
		int count = 0;
		
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set");
		sb.append(" QTY_REGULAR_13WK = ").append(movement.getQuantityRegular13Week());
		sb.append(", QTY_SALE_13WK = ").append(movement.getQuantitySale13Week());
		sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());
		
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - UpdateWeeklyLIG13WeekData");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}
		
		return count > 0;
	}
/*
	public boolean updateWeeklyMovementVisitMargin13WeekData (Connection conn, MovementWeeklyDTO movement)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY set");
		sb.append(" TOTAL_MARGIN = ").append(movement.getMargin());
		sb.append(", MARGIN_PCT = ").append(movement.getMarginPercent());
		sb.append(", VISIT_COUNT = ").append(movement.getVisitCount());
		sb.append(", QTY_REGULAR_13WK = ").append(movement.getVisitCount());
		sb.append(", QTY_SALE_13WK = ").append(movement.getVisitCount());
		sb.append(" where COMP_STR_ID = ").append(movement.getCompStoreId());
		sb.append(" and CHECK_DATA_ID = ").append(movement.getCheckDataId());
		sb.append(" and ITEM_CODE = ").append(movement.getItemCode());

		int count = 0;
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - updateWeeklyMovementVisitMargin13WeekData");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}

		return count > 0;
	}
*/
	
	public int getCheckDataID (Connection conn, int storeId, int itemCode, Date start, Date end)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT check_data_id FROM competitive_data_view_lig");
		sb.append(" WHERE comp_str_id = ").append(storeId);
		//sb.append(" WHERE comp_str_id = 5720");
		sb.append(" AND item_code = ").append(itemCode);
		sb.append(" AND to_char(start_date, 'MM/DD/YYYY') = '").append(startStr).append("'");

		//String sql = sb.toString();
		//logger.debug("getCheckDataID SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCheckDataID");

		int ret = -1;
		try
		{
			while (result.next()) {
				ret = result.getInt("check_data_id");
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return ret;
	}

	public boolean insertWeeklyLIGMovement(Connection conn, MovementWeeklyDTO movement)
	throws GeneralException
	{
		boolean ret = true;
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		
		// Try to update LIG entry
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set");
		sb.append(" REVENUE_REGULAR = ").append(movement.getRevenueRegular());
		sb.append(", QUANTITY_REGULAR = ").append(movement.getQuantityRegular());
		sb.append(", REVENUE_SALE = ").append(movement.getRevenueSale());
		sb.append(", QUANTITY_SALE = ").append(movement.getQuantitySale());
		
		sb.append(", LIST_COST = ").append(movement.getListCost());
		sb.append(", EFF_LIST_COST_DATE = ");
		String dateStr;
		if ( movement.getEffListCostDate() != null ) {
			dateStr = formatter.format(movement.getEffListCostDate());
			sb.append("To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
		}
		else {
			sb.append("NULL");
		}
		
		sb.append(", DEAL_COST = ").append(movement.getDealCost());
		sb.append(", DEAL_START_DATE = ");
		if ( movement.getDealStartDate() != null ) {
			dateStr = formatter.format(movement.getDealStartDate());
			sb.append("To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
		}
		else {
			sb.append("NULL");
		}
		sb.append(", DEAL_END_DATE = ");
		if ( movement.getDealEndDate() != null ) {
			dateStr = formatter.format(movement.getDealEndDate());
			sb.append("To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
		}
		else {
			sb.append("NULL");
		}

		sb.append(", COST_CHG_DIRECTION = ").append(movement.getCostChangeDirection());
		sb.append(", MARGIN_CHG_DIRECTION = ").append(movement.getMarginChangeDirection());
		
		/* Margin percent greater than or equal 1000% (on the positive and negative side) 
		to be stored as 999.99 with appropriate sign */ 
		double marginPct = movement.getMarginPercent();
		if (marginPct > 999.99) 
			marginPct = 999.99;
		else if (marginPct < -999.99)
			marginPct = -999.99;
		
		sb.append(", MARGIN_PCT = ").append(marginPct);
		sb.append(", TOTAL_MARGIN = ").append(movement.getMargin());
		sb.append(", VISIT_COUNT = ").append(movement.getVisitCount());
		
		sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());
		
		//String sql = sb.toString();
		int count = 0;
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - insertWeeklyLIGMovement: update");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			ret = false;
		}
		
		int storeId = movement.getCompStoreId();
		int itemCode = movement.getItemCode();
		int checkDataId = movement.getCheckDataId();
		//String logstr = "storeId = " + storeId + ", itemCode = " + itemCode + ", checkDataId = " + checkDataId;  
		if ( count == 0 )
		{
			// Nothing was updated, insert record...
			sb = new StringBuffer("insert into MOVEMENT_WEEKLY_LIG (COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID");
			sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE");
			sb.append(", LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE");
			sb.append(", COST_CHG_DIRECTION, MARGIN_CHG_DIRECTION, MARGIN_PCT, TOTAL_MARGIN");
			sb.append(", VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK");
			
			sb.append(") values (");
			
			sb.append(storeId);
			sb.append(", ").append(itemCode);
			sb.append(", ").append(checkDataId);
			
			sb.append(", ").append(movement.getRevenueRegular());
			sb.append(", ").append(movement.getQuantityRegular());
			sb.append(", ").append(movement.getRevenueSale());
			sb.append(", ").append(movement.getQuantitySale());
			
			sb.append(", ").append(movement.getListCost());
			if ( movement.getEffListCostDate() != null ) {
				dateStr = formatter.format(movement.getEffListCostDate());
				sb.append(", To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
			}
			else {
				sb.append(", NULL");
			}
			
			sb.append(", ").append(movement.getDealCost());
			if ( movement.getDealStartDate() != null ) {
				dateStr = formatter.format(movement.getDealStartDate());
				sb.append(", To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
			}
			else {
				sb.append(", NULL");
			}
			if ( movement.getDealEndDate() != null ) {
				dateStr = formatter.format(movement.getDealEndDate());
				sb.append(", To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
			}
			else {
				sb.append(", NULL");
			}
			sb.append(", ").append(movement.getCostChangeDirection());
			sb.append(", ").append(movement.getMarginChangeDirection());
			
			/* Margin percent greater than or equal 1000% (on the positive and negative side) 
			to be stored as 999.99 with appropriate sign */ 
			double marginPercent = movement.getMarginPercent();
			if (marginPercent > 999.99) 
				marginPercent = 999.99;
			else if (marginPercent < -999.99)
				marginPercent = -999.99;
			
			sb.append(", ").append(marginPercent);
			sb.append(", ").append(movement.getMargin());
			
			sb.append(", ").append(movement.getVisitCount());
			sb.append(", ").append(movement.getQuantityRegular13Week());
			sb.append(", ").append(movement.getQuantitySale13Week());
	
			sb.append(")");		// close
			
			//String sql = sb.toString();
			try {
				PristineDBUtil.execute(conn, sb, "MovementDAO - InsertWeeklyLIG");
			}
			catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				ret = false;
				//throw gex;
			}
			
			//logger.debug("Inserted: " + logstr);
		}
		else {
			//logger.debug("Updated: " + logstr);
		}

		return ret;
	}
	
	public boolean updateWeeklyLIGMovement (Connection conn, MovementWeeklyDTO movement)
	throws GeneralException
	{
		boolean ret = true;
		
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set");
		sb.append(" VISIT_COUNT = ").append(movement.getVisitCount());
		sb.append(" where COMP_STR_ID = ").append(movement.getCompStoreId());
		sb.append(" and ITEM_CODE = ").append(movement.getItemCode());
		sb.append(" and CHECK_DATA_ID = ").append(movement.getCheckDataId());
		
		//String sql = sb.toString();
		try {
			PristineDBUtil.execute(conn, sb, "MovementDAO - updateWeeklyLIGMovement");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			ret = false;
		}

		return ret;
	}
	
	public MovementWeeklyDTO loadWeeklyDTO (MovementWeeklyDTO dto, CachedRowSet row) throws SQLException
	{
		dto.setItemStore(row.getString("COMP_STR_NO"));
		dto.setItemUPC(row.getString("UPC"));
		dto.setTotalPrice(row.getDouble("PRICE"));
		dto.setExtnQty(row.getDouble("QUANTITY"));
		//dto.setExtnWeight(row.getDouble("WEIGHT"));
		return dto; 
	}
	
	private static Date get13WeekFirstStartDate (Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, - 7 * 12);		// 13 weeks
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}

	private static Date getPreviousWeekStartDate (Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, - 7);		// previous week
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}

	/*
	 ***************************************************************** 
	 * Function to get the weekly movement data for given schedule
	 * Argument 1: DB connection
	 * Argument 2: Schedule ID 
	 * Return : ArrayList
     * @throws GeneralException, SQLException
     *****************************************************************	
	 */
	public ArrayList<Movement13WeeklyDTO> getWeeklyMovementDataList (Connection conn, String schCSV, String departmentCode, String categoryid)
	throws GeneralException
	{
		ArrayList<Movement13WeeklyDTO> listMoment = new ArrayList<Movement13WeeklyDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, NVL(m.QTY_REGULAR_13WK, -99) as QTY_REGULAR_13WK,");
		sb.append(" NVL(m.QTY_SALE_13WK, -99) as QTY_SALE_13WK, m.QUANTITY_REGULAR, m.QUANTITY_SALE");
		sb.append(" FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m");
		
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , item_lookup i ");
		}
		
		sb.append(" WHERE c.SCHEDULE_ID =").append(schCSV).append("");
		sb.append(" AND c.CHECK_DATA_ID = m.CHECK_DATA_ID");
		
		if (!departmentCode.isEmpty()) {
			sb.append(" AND i.DEPT_ID NOT IN (") .append(departmentCode).append(")");
		}
		
		if (!categoryid.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (") .append(categoryid).append(")");
		}
		
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" AND i.item_code = c.item_code "); 
		}
		
		logger.info("getWeeklyMovementDataList - SQL : " + sb.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyMovements");
		
		try
		{
			while (result.next())
			{
				Movement13WeeklyDTO momentDto = new Movement13WeeklyDTO();
				momentDto.setCompStoreId(result.getInt("COMP_STR_ID"));
				momentDto.setItemCode(result.getInt("ITEM_CODE"));
				momentDto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				momentDto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				momentDto.setQuantitySale(result.getDouble("QUANTITY_SALE"));
				momentDto.setQuantityRegular13Week(result.getDouble("QTY_REGULAR_13WK"));
				momentDto.setQuantitySale13Week(result.getDouble("QTY_SALE_13WK"));				
				listMoment.add (momentDto);

				int size = listMoment.size();
				if ( (size % 25000) == 0 ) {
					logger.debug("getWeeklyMovements: processed " + String.valueOf(size));
				}
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	
		return listMoment;
	}
	
	
	/*
	 ***************************************************************** 
	 * Function to get the weekly movement data for given schedule
	 * Argument 1: DB connection
	 * Argument 2: Schedule ID 
	 * Return : HashMap
     * @throws GeneralException, SQLException
     *****************************************************************	
	 */
	public HashMap<Integer, Movement13WeeklyDTO> getWeeklyMovementDataMap (Connection conn, String schCSV, String departmentCode, String categoryid)
	throws GeneralException
	{
		HashMap<Integer, Movement13WeeklyDTO> hashMap = new HashMap<Integer, Movement13WeeklyDTO>();
		StringBuffer sb = new StringBuffer();
		
		sb.append("SELECT m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, NVL(m.QTY_REGULAR_13WK, -99) as QTY_REGULAR_13WK,");
		sb.append(" NVL(m.QTY_SALE_13WK, -99) as QTY_SALE_13WK, m.QUANTITY_REGULAR, m.QUANTITY_SALE");
		sb.append(" FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m");
		
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , item_lookup i ");
		}
		
		sb.append(" WHERE c.SCHEDULE_ID =").append(schCSV).append("");
		sb.append(" AND c.CHECK_DATA_ID = m.CHECK_DATA_ID");
		
		if (!departmentCode.isEmpty()) {
			sb.append(" AND i.DEPT_ID NOT IN (") .append(departmentCode).append(")");
		}
		
		if (!categoryid.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (") .append(categoryid).append(")");
		}
		
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" AND i.item_code = c.item_code "); 
		}
		
		logger.info("getWeeklyMovementDataMap - SQL : " + sb.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyMovements");
		
		try
		{
			while (result.next())
			{
				Movement13WeeklyDTO momentDto = new Movement13WeeklyDTO();
				momentDto.setCompStoreId(result.getInt("COMP_STR_ID"));
				momentDto.setItemCode(result.getInt("ITEM_CODE"));
				momentDto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				momentDto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				momentDto.setQuantitySale(result.getDouble("QUANTITY_SALE"));
				momentDto.setQuantityRegular13Week(result.getDouble("QTY_REGULAR_13WK"));
				momentDto.setQuantitySale13Week(result.getDouble("QTY_SALE_13WK"));
				hashMap.put(result.getInt("ITEM_CODE"), momentDto);

				int size = hashMap.size();
				if ( (size % 25000) == 0 ) {
					logger.debug("getWeeklyMovements: processed " + String.valueOf(size));
				}
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	
		return hashMap;
	}
	

	/*
	 ***************************************************************** 
	 * Function to get the 13 week movement data for an item
	 * Argument 1: DB connection
	 * Argument 2: Schedule IDs
	 * Argument 3: Item Code 
	 * Return : Movement13WeeklyDTO
     * @throws GeneralException, SQLException
     *****************************************************************	
	 */
	public Movement13WeeklyDTO getWeeklyMovements13WeekData (Connection conn, String scheduleIDs, int itemCode)
	throws GeneralException
	{

		Movement13WeeklyDTO movementWeeklyDTO = new Movement13WeeklyDTO();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT sum(NVL(m.QUANTITY_REGULAR, 0)) as QUANTITY_REGULAR,");
		sb.append(" sum(NVL(m.QUANTITY_SALE,0)) as QUANTITY_SALE,");
		sb.append(" count(m.CHECK_DATA_ID) as REC_COUNT");		
		sb.append(" FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m");
		sb.append(" WHERE c.SCHEDULE_ID in (").append(scheduleIDs);
		sb.append(" ) AND c.CHECK_DATA_ID = m.CHECK_DATA_ID");
		sb.append(" AND m.ITEM_CODE = ").append(itemCode);

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyMovementsFor13Week");
		
		try
		{
			movementWeeklyDTO.setItemCode(itemCode);
			while (result.next())
			{
				movementWeeklyDTO.setQuantityRegular13Week(result.getDouble("QUANTITY_REGULAR"));
				movementWeeklyDTO.setQuantitySale13Week(result.getDouble("QUANTITY_SALE"));
				movementWeeklyDTO.setWeek13RecordCount(result.getDouble("REC_COUNT"));
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	
		return movementWeeklyDTO;
	}
	
	
	/*
	 ***************************************************************** 
	 * Function to get the weekly movement data for LIG
	 * Argument 1: DB connection
	 * Argument 2: Schedule ID 
	 * Return : ArrayList
     * @throws GeneralException, SQLException
     *****************************************************************	
	 */
	public ArrayList<Movement13WeeklyDTO> getMovement13WeeklyDataForLIG (Connection conn, String schCSV, String departmentCode, String categoryId)
	throws GeneralException
	{
		ArrayList<Movement13WeeklyDTO> listMovement = new ArrayList<Movement13WeeklyDTO>();
		StringBuffer sb = new StringBuffer();
		
		sb.append("SELECT");
		sb.append(" m.COMP_STR_ID, m.CHECK_DATA_ID, i.ITEM_CODE, g.RET_LIR_ID,");
		sb.append(" g.RET_LIR_ITEM_CODE, m.QTY_REGULAR_13WK, m.QTY_SALE_13WK");
		sb.append(" FROM");		
		sb.append(" COMPETITIVE_DATA c, MOVEMENT_WEEKLY m ,");
		sb.append(" ITEM_LOOKUP i, RETAILER_LIKE_ITEM_GROUP g");
		sb.append(" WHERE");
		sb.append(" c.SCHEDULE_ID = ").append(schCSV);
		sb.append(" AND c.CHECK_DATA_ID = m.CHECK_DATA_ID AND c.ITEM_CODE = m.ITEM_CODE");
		sb.append(" AND i.item_code = c.item_code");
		sb.append(" AND i.RET_LIR_ID IS NOT NULL");
		sb.append(" AND i.RET_LIR_ID = g.RET_LIR_ID");

		if (!departmentCode.isEmpty()) {
			sb.append(" AND i.DEPT_ID NOT IN (") .append(departmentCode).append(")");
		}
		
		if (!categoryId.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (") .append(categoryId).append(")");
		}
		
		sb.append(" ORDER BY i.RET_LIR_ID");

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getWeeklyMovements");
		
		try
		{
			while (result.next())
			{
				Movement13WeeklyDTO movementDto = new Movement13WeeklyDTO();
				movementDto.setCompStoreId(result.getInt("COMP_STR_ID"));
				movementDto.setItemCode(result.getInt("ITEM_CODE"));
				movementDto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				movementDto.setRetLIRId(result.getInt("RET_LIR_ID"));
				movementDto.setRetLIRItemCode(result.getInt("RET_LIR_ITEM_CODE"));
				movementDto.setQuantityRegular13Week(result.getDouble("QTY_REGULAR_13WK"));
				movementDto.setQuantitySale13Week(result.getDouble("QTY_SALE_13WK"));				
				listMovement.add (movementDto);
				//logger.debug("Item Code for Week 1 " + result.getInt("ITEM_CODE"));
				int size = listMovement.size();
				if ( (size % 25000) == 0 ) {
					logger.debug("getWeeklyMovements: processed " + String.valueOf(size));
				}
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	
		return listMovement;
	}

	/*
	 ***************************************************************** 
	 * Function to set movement 13 week data to 0 for the current week
	 * Argument 1: DB connection
	 * Argument 2: Schedule ID 
	 * Return : boolean
     * @throws GeneralException, SQLException
     *****************************************************************
	 */
	public boolean updateWeeklyMovement13WeekInitialData (Connection conn, String scheduleID, String targetTableName, String departmentCode, String categoryid)
	throws GeneralException
	{
		int count = 0;
		
		StringBuffer sb = new StringBuffer("UPDATE ").append(targetTableName);
		sb.append(" SET");		
		sb.append(" QTY_REGULAR_13WK = 0, QTY_SALE_13WK = 0");
		sb.append(" WHERE CHECK_DATA_ID IN");
		sb.append(" (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA c");
		
		//This is application only if Department code or category id are not empty
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , ITEM_LOOKUP i ");
		}
		
		sb.append(" WHERE c.SCHEDULE_ID = ").append(scheduleID);

		//If department code has some value, value would be multiple separated with comma
		if (!departmentCode.isEmpty()) {		
			sb.append(" AND i.DEPT_ID NOT IN (").append(departmentCode).append(")");
		}

		//If category id has some value, value would be multiple separated with comma
		if (!categoryid.isEmpty()) {		
			sb.append(" AND i.CATEGORY_ID NOT IN (").append(categoryid).append(")");
		}
		
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" AND c.ITEM_CODE = i.ITEM_CODE ");
		}

		sb.append(" )");
		
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - UpdateWeekly13WeekData");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}
		
		return count > 0;
	}
	
	/*
	 *****************************************************************
	 * Function update the movement 13 week data
	 * Update the can be 13 week movement count or null 
 	 * Argument 1: DB connection
 	 * Argument 2: movement 13 week data (DTO)
 	 * Return: boolean
 	 * @throws GeneralException, SQLException
	 ***************************************************************** 	 
 	 */
	public boolean updateMovement13WeekData (Connection conn, Movement13WeeklyDTO movement13Dto, String targetTableName)
	throws GeneralException
	{
		int count = 0;
		
		StringBuffer sb = new StringBuffer("UPDATE ").append(targetTableName).append (" SET");
		
		//Check for the update type
		if (movement13Dto.getQuantityRegular13Week()==-99) {
			sb.append(" QTY_REGULAR_13WK = null");			
		}
		else {
			sb.append(" QTY_REGULAR_13WK = ").append(movement13Dto.getQuantityRegular13Week());
		}

		//Check for the update type
		if (movement13Dto.getQuantitySale13Week() == -99) {
			sb.append(", QTY_SALE_13WK = null");
		}
		else
		{
			sb.append(", QTY_SALE_13WK = ").append(movement13Dto.getQuantitySale13Week());
		}
		
		
		sb.append(" where CHECK_DATA_ID = ").append(movement13Dto.getCheckDataId());
		
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "MovementDAO - UpdateWeekly13WeekData");
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}
		
		return count > 0;
	}

	/*
	 * ****************************************************************
	 * Method used to get the Com_store_no,Visitor count 
	 * Argument 1: Connection
	 * Argument 2: Store Number
	 * Argument 3: Calendar Id
	 * Argument 4: specialDTO
	 * return :List<SummaryDailyDTO>
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */

	public HashMap<String, Integer> VisitorSummary(Connection _Conn, String storeNumber,
			int calendar_id, int productLevelId)
			throws GeneralException {

		//logger.info("Visitorsummary begin");
		
		HashMap<String, Integer> returnMap = new HashMap<String, Integer>();
		
		String product = "";
		// chekc the product id for visit count
		if( productLevelId == Constants.SUBCATEGORYLEVELID)
			product = "I.SUB_CATEGORY_ID";
		else if( productLevelId == Constants.CATEGORYLEVELID)
			product = "I.CATEGORY_ID";
		else if( productLevelId == Constants.DEPARTMENTLEVELID)
			product = "I.DEPT_ID";
		else if( productLevelId == Constants.POSDEPARTMENT)
			product = "M.POS_DEPARTMENT";
		else if( productLevelId == Constants.LOCATIONLEVELID)		
			product = "M.COMP_STR_NO";
		else 
			product = "PGR.PRODUCT_ID";
		
		try {
			StringBuffer sql = new StringBuffer();
				
				sql.append(" select ").append(product);
				sql.append(" ,COUNT(DISTINCT M.TRANSACTION_NO) AS VISIT_COUNT");
				sql.append(" from MOVEMENT_DAILY M");
				
				// get the finance department Visit Count
				if( productLevelId == Constants.FINANCEDEPARTMENT){
				sql.append(" Inner join PRODUCT_GROUP_RELATION_T PGR on PGR.CHILD_PRODUCT_ID = M.POS_DEPARTMENT");	 
				}
				else{
					sql.append(" Inner Join ITEM_LOOKUP I on  M.ITEM_CODE= I.ITEM_CODE");	
				}
								
				// get the portfolio Visit Count
				if( productLevelId == Constants.PORTFOLIO){
					sql.append(" inner join PRODUCT_GROUP_RELATION_T PGR on PGR.CHILD_PRODUCT_ID = I.CATEGORY_ID");	 
				}
				
				sql.append(" where  CALENDAR_ID=").append(calendar_id); 
				sql.append(" and  M.COMP_STR_NO='").append(storeNumber).append("'"); 
				
				if( productLevelId == Constants.FINANCEDEPARTMENT ){
					/*sql.append(" and PGR.PRODUCT_LEVEL_ID=").append(productLevelId);*/
					sql.append(" and M.POS_DEPARTMENT <= 37");
				}
				else if(productLevelId == Constants.PORTFOLIO){
			/*		sql.append(" and PGR.PRODUCT_LEVEL_ID=").append(productLevelId);*/
					sql.append(" and M.POS_DEPARTMENT < 37");
				}
				else{
					sql.append(" and M.POS_DEPARTMENT < 37");
				}
									
				sql.append("  and TRANSACTION_NO > 0");
			/*	
			if (specialDTO.getExcludeDepartments() != null) {
				sql.append("  and ( I.DEPT_ID not in (");
				sql.append(specialDTO.getExcludeDepartments());
				sql.append("   )");
				if (specialDTO.getIncludeItems() != null) {
					sql.append(" or I.UPC in (");
					sql.append(specialDTO.getIncludeItems());
					sql.append(")");
				}
				sql.append("   )");
			}*/
				
				
			sql.append(" group by ").append(product);

			logger.debug("Visitor Sql -" + sql.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,	"GetVisitorSummary");

			//logger.info("Total Store count " + result.size());
			
			while (result.next()) {
				
				returnMap.put(result.getString(1), result.getInt("VISIT_COUNT"));
			}
			
			result.close();

		} catch (SQLException sql) {
			logger.error("Error While Fetching VisitSummary " , sql);
			throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching VisitSummary " , e);
			throw new GeneralException("Error While Fetching VisitSummary" ,e);
		}

		return returnMap;
	}
	
	
	/*
	 * Method used to get the Movement data for daily aggregation.
	 * Argument 1 : _Conn
	 * Argument 2 : storeNumber
	 * Argument 3 : calendar_id
	 * Argument 4 : specialDTO
	 * @throws GeneralException
	 */
	
	
	public List<MovementDailyAggregateDTO> getMovementDailyData(
			Connection _Conn, String storeNumber, int calendar_id) throws GeneralException {

		//logger.info(" Daily Summary Process Dao Begins  ");
		
		List<MovementDailyAggregateDTO> movementDataList = 
				new ArrayList<MovementDailyAggregateDTO>();

		StringBuffer sql = new StringBuffer();
		
		sql.append(" select I.SUB_CATEGORY_ID, I.CATEGORY_ID, I.DEPT_ID,");
		sql.append(" MV.ITEM_CODE, MV.SALE_FLAG as FLAG, I.UOM_ID,");
		sql.append(" sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME,");
		sql.append(" sum(MV.QUANTITY) as QUANTITY,");
		sql.append(" count(case when MV.WEIGHT=0 then null else 1 end) as WEIGHT,");
		sql.append(" sum( MV.PRICE) as REVENUE,");
		//Actual weight to Margin calculation - Britto 10/03/2012 
		sql.append(" sum(MV.WEIGHT) as WEIGHT_ACTUAL");
		sql.append(" from MOVEMENT_DAILY MV Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE");
		sql.append(" where  MV.CALENDAR_ID =").append(calendar_id); 
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");
		sql.append(" group by DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID,");
		sql.append(" MV.ITEM_CODE, MV.SALE_FLAG, I.UOM_ID");
		sql.append(" order by DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID, MV.ITEM_CODE, MV.SALE_FLAG");
				
		logger.debug("getMovementDailyData SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
				"getMovementDailyData");

			logger.debug("Total Result Records........" + rst.size());
		 
			while (rst.next()) {
				
				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
				  objMovementDto.set_subcategoryid(rst.getString("SUB_CATEGORY_ID"));
				  objMovementDto.set_categoryid(rst.getString("CATEGORY_ID"));
				  objMovementDto.set_departmentid(rst.getString("DEPT_ID"));
					
				  objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				  /*movement.setupc(rst.getString("UPC"));*/
				  objMovementDto.setFlag(rst.getString("FLAG"));
				  objMovementDto.setuomId(rst.getString("UOM_ID"));
								
				  if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
				   objMovementDto.set_regularQuantity(rst.getDouble("QUANTITY")+ rst.getDouble("WEIGHT"));
				   objMovementDto.setActualWeight(rst.getDouble("WEIGHT_ACTUAL"));
				   objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
				   objMovementDto.setregMovementVolume(rst.getDouble("VOLUME")+ rst.getDouble("WEIGHT"));
				   objMovementDto.setRevenueRegular(rst.getDouble("REVENUE"));
				  }
				  if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					  objMovementDto.set_saleQuantity(rst.getDouble("QUANTITY")+ rst.getDouble("WEIGHT"));
					  objMovementDto.setActualWeight(rst.getDouble("WEIGHT_ACTUAL"));
					  objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					  objMovementDto.setsaleMovementVolume(rst.getDouble("VOLUME")+ rst.getDouble("WEIGHT"));
					  objMovementDto.setRevenueSale(rst.getDouble("REVENUE"));
					}
					movementDataList.add(objMovementDto);
				 
				
			}
			
			
			
			/*logger.info(" Daily Summary Process Count " + movementDataList.size());*/
			
			rst.close();
					

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ",exe);
			throw  new GeneralException(" Error while fetching the results.... ",exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ",exe);
			throw  new GeneralException(" Error while fetching the results.... ",exe);
		}
		
		return movementDataList;
	}
	

	public void insertMovementDaily(Connection _Conn, ArrayList<MovementDTO> movementList) throws GeneralException
	{
		try {
			//logger.debug("Start Batch insert process");

			PreparedStatement psmt=_Conn.prepareStatement(InsertMovementSql());
			
				MovementDTO objMDto = new MovementDTO();
			
				for(int recordCnt=0 ; recordCnt < movementList.size(); recordCnt++){
				
					objMDto = movementList.get(recordCnt);
			    	addMovementData(objMDto, psmt);
				}
				psmt.executeBatch();
				psmt.close();
				_Conn.commit();
				movementList = null;

		} catch (Exception se) {
			logger.debug(se);
			throw new GeneralException("addMovementData",se);
		}
		
	}
	
		/*
		 * ****************************************************************
		 * Method to Create the insert script and add the script into batch List
		 * Argument 1 : movementDto
		 * @throws GeneralException , SQLException
		 * ****************************************************************
		 */

		public void addMovementData(MovementDTO movementDto, PreparedStatement psmt) throws GeneralException  {
			
			try
			{
				//COMP_STR_NO
				psmt.setObject(1, movementDto.getItemStore());
				
				//UPC
				String upc = PrestoUtil.castUPC( movementDto.getItemUPC(), true);
				psmt.setObject(2, upc);
				
				//POS_TIMESTAMP
				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
				String dateStr = formatter.format(movementDto.getItemDateTime());
				psmt.setObject(3, dateStr);
				
				//WEEKDAY
				psmt.setObject(4, movementDto.getDayOfWeek());

				//UNIT_PRICE
				double unitPrice = movementDto.getItemNetPrice();
				psmt.setObject(5, unitPrice);

				
				//PRICE
				double price = movementDto.getExtendedNetPrice();
				psmt.setObject(6, price);
				
				//SALE_FLAG
				if ( (0 <= price) && (price < movementDto.getExtendedGrossPrice()) ) {
					//logger.debug("SALE FLAG...............Y");
					psmt.setObject(7, "Y");
				}
				else {
					//logger.debug("SALE FLAG...............N");
					psmt.setObject(7, "N");
				}
				
				//QUANTITY
				psmt.setObject(8, movementDto.getExtnQty());
				
				//WEIGHT
				psmt.setObject(9, movementDto.getExtnWeight());
				
				//TRANSACTION_NO
				psmt.setObject(10, movementDto.getTransactionNo()); 
				
				//CUSTOMER_CARD_NO
				psmt.setObject(11, movementDto.getCustomerId());

				//ITEM_CODE
				psmt.setObject(12, movementDto.getItemCode());

				//CALENDAR_ID
				psmt.setObject(13, movementDto.getCalendarId());			
				
				// pos Department
				psmt.setObject(14, movementDto.getPosDepartment());
				
				// Extended Gross Price
				psmt.setObject(15, movementDto.getExtendedGrossPrice());

				// Store coupon used
				psmt.setObject(16, movementDto.getStoreCpnUsed());

				// mfr coupon used
				psmt.setObject(17, movementDto.getMfrCpnUsed());
				
				psmt.addBatch(); 

				
			}
			catch(Exception sql)
			{
				logger.debug(sql);
				throw new GeneralException("addMovementData",sql);
				
			}
		}


	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */

	private String InsertMovementSql() {

		StringBuffer Sql = new StringBuffer();
		Sql.append("insert into MOVEMENT_DAILY (");
		Sql.append(" COMP_STR_NO, UPC, POS_TIMESTAMP, WEEKDAY, UNIT_PRICE, ");
		Sql.append(" PRICE, SALE_FLAG, QUANTITY, WEIGHT, TRANSACTION_NO, ");
		Sql.append(" CUSTOMER_CARD_NO, ITEM_CODE, CALENDAR_ID,POS_DEPARTMENT, EXTENDED_GROSS_PRICE, STORE_COUPON_USED, MFR_COUPON_USED) values (");
		Sql.append(" ?, ?, To_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		return Sql.toString();
	}

/*	
	 *  Method used to get the movement data for finance department aggregation
	 *  Argument 1 : Connection
	 *  Argument 2 : storeNumber
	 *  Argument 3 : calendarId 
	 *  Argument 4 : specialDTO
	 *  Argument 5 : productLevelId
	 *  Argument 6 : Salesaggregationbusiness
	 *  Argument 7 : Repair Flag
	 *  @throws GeneralException
	 *  @catch Exception
	 

	public List<MovementDailyAggregateDTO> financeAggregation(Connection conn,
			String storeNumber, int calendarId, SpecialCriteriaDTO specialDTO,
			int productLevelId, Salesaggregationbusiness summaryBusiness,
			String repairFlag)
			throws GeneralException {
		
		List<MovementDailyAggregateDTO> financeAggergation = new ArrayList<MovementDailyAggregateDTO>();
 		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT  MV.UPC,PR.PRODUCT_ID as PRODUCT_ID, MV.SALE_FLAG AS FLAG,");
		sql.append(" SUM(MV.QUANTITY * I.ITEM_SIZE) AS VOLUME");
		sql.append(" ,SUM(MV.QUANTITY) AS QUANTITY");
		sql.append(" ,COUNT(CASE WHEN MV.WEIGHT=0  THEN NULL ELSE 1  END)AS WEIGHT ,");
		sql.append(" SUM( MV.PRICE) AS REVENUE,I.UOM_ID");
		sql.append(" FROM MOVEMENT_DAILY MV");
		sql.append(" inner JOIN PRODUCT_GROUP_RELATION PR");
		sql.append(" ON MV.ITEM_CODE=PR.CHILD_PRODUCT_ID");
		sql.append(" inner Join item_lookup I  ");
		sql.append(" ON MV.ITEM_CODE=I.ITEM_CODE");
		sql.append(" WHERE MV.CALENDAR_ID = "+calendarId+"");
		sql.append(" AND COMP_STR_NO      ='"+storeNumber+"'");
		sql.append(" AND PR.PRODUCT_LEVEL_ID="+productLevelId+"");
		
		// code added for partial repair flag
		sql.append(" and MV.PROCESS_FLAG='N'");
		
		// code blocked for Movement by volume 
		  if (specialDTO.getExcludeDepartments() != null) {
				sql.append("  and ( I.DEPT_ID not in (");
				sql.append(specialDTO.getExcludeDepartments());
				sql.append("   )");
				if (specialDTO.getIncludeItems() != null) {
					sql.append("  or MV.UPC in (");
					sql.append(specialDTO.getIncludeItems()+",");
					sql.append(specialDTO.getIncludeGas());
					sql.append("   )");
				}
				sql.append(" )");
			}
		
		// add the upc in group by process
		sql.append("  GROUP BY MV.UPC,PRODUCT_ID, MV.SALE_FLAG,I.UOM_ID");
		sql.append("  ORDER BY PRODUCT_ID,MV.UPC,  MV.SALE_FLAG");
		logger.info("Finance Department Aggregation SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(conn, sql,
					"Finance Department Aggregation");

			while (rst.next()) {
				MovementDailyAggregateDTO movement = new MovementDailyAggregateDTO();
				movement.setProductId(rst.getString("PRODUCT_ID"));
				movement.setProductLevelId(productLevelId);
				movement.setupc(rst.getString("UPC"));
				movement.setuomId(rst.getString("UOM_ID"));
				if(rst.getString("FLAG").equalsIgnoreCase("Y")){
					movement.set_saleQuantity(rst.getDouble("QUANTITY")+ rst.getDouble("WEIGHT"));
					movement.setsaleMovementVolume(rst.getDouble("VOLUME")+ rst.getDouble("WEIGHT"));
					movement.setRevenueSale(rst.getDouble("REVENUE"));
				}
				else{
					movement.set_regularQuantity(rst.getDouble("QUANTITY")+ rst.getDouble("WEIGHT"));
					movement.setregMovementVolume(rst.getDouble("VOLUME")+ rst.getDouble("WEIGHT"));
					movement.setRevenueRegular(rst.getDouble("REVENUE"));
				}
				
				financeAggergation.add(movement);
		     }
			logger.info(" Daily Summary Process Count "
					+ movementDataList.size());

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Fetch Finance Departemnt Aggregation Proceess", exe);
		} 
		
		return financeAggergation;
	}*/

	
	public int[] batchUpdateToMovementWeekly(Connection conn,
			List<MovementWeeklyDTO> movWeeklyDtoCollection) throws SQLException {

		int[] count = null;
		PreparedStatement psmt = conn
				.prepareStatement(updateSqlForMovementWeekly());

		try {

			for (MovementWeeklyDTO movWeeklyDto : movWeeklyDtoCollection) {

				psmt.setObject(1, movWeeklyDto.getRevenueRegular());
				psmt.setObject(2, movWeeklyDto.getQuantityRegular());
				psmt.setObject(3, movWeeklyDto.getRevenueSale());
				psmt.setObject(4, movWeeklyDto.getQuantitySale());
				psmt.setObject(5, movWeeklyDto.getMargin());

				double marginPct = movWeeklyDto.getMarginPercent();
				if (marginPct > 999.99)
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;

				psmt.setObject(6, marginPct);
				psmt.setObject(7, movWeeklyDto.getVisitCount());
				psmt.setObject(8, movWeeklyDto.getCheckDataId());

				psmt.addBatch();
			}

			count = psmt.executeBatch();
			psmt.clearBatch();
			// conn.commit();

		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			PristineDBUtil.close(psmt);
		}
		return count;

	}
	
	private String updateSqlForMovementWeekly() {
		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE MOVEMENT_WEEKLY SET ");
		sb.append(" REVENUE_REGULAR = ?, QUANTITY_REGULAR = ?, REVENUE_SALE = ?, QUANTITY_SALE = ?, TOTAL_MARGIN = ?, ");
		sb.append(" MARGIN_PCT = ?, VISIT_COUNT = ? ");
		sb.append(" WHERE CHECK_DATA_ID = ? ");
		return sb.toString();
	}
	
	/**
	 * This method returns distinct item codes from movements specified between given dates.
	 * @param conn				Connection
	 * @param weekStartDate		Week Start Date
	 * @param weekEndDate		Week End Date
	 * @return
	 * @throws GeneralException
	 */
	public Set<String> getItemsFromMovements(Connection conn,
			Date weekStartDate, Date weekEndDate) throws GeneralException {
		logger.info("Inside getItemsFromMovements() of MovementDAO");
		Set<String> itemCodeSet = new HashSet<String>();
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT DISTINCT(MD.ITEM_CODE) FROM MOVEMENT_DAILY MD, ITEM_LOOKUP IL WHERE ");
		sql.append("MD.POS_TIMESTAMP >= To_DATE(?, 'YYYYMMDDHH24MI') ");
		sql.append("AND MD.POS_TIMESTAMP <= To_DATE(?, 'YYYYMMDDHH24MI') ");
		sql.append("AND MD.ITEM_CODE = IL.ITEM_CODE ");
		sql.append("AND (IL.PI_ANALYZE_FLAG = 'N' OR IL.PI_ANALYZE_FLAG IS NULL) ");
		logger.info("SQL - " + sql.toString());
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(weekStartDate);
		String endStr = formatter.format(weekEndDate);

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement(sql.toString());
			statement.setString(1, startStr);
			statement.setString(2, endStr);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				itemCodeSet.add(resultSet.getString("ITEM_CODE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing getItemsFromMovements");
			throw new GeneralException(
					"Error while executing getItemsFromMovements", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemCodeSet;
	}
	
	public HashMap<Integer, Movement13WeeklyDTO> get12WeekMovementDataMap(
			Connection conn, String schCSV, String departmentCode,
			String categoryid) throws GeneralException {
		HashMap<Integer, Movement13WeeklyDTO> hashMap = new HashMap<Integer, Movement13WeeklyDTO>();
		StringBuffer sb = new StringBuffer();

		sb.append("SELECT m.COMP_STR_ID, m.CHECK_DATA_ID, m.ITEM_CODE, NVL(m.QTY_REGULAR_12WK, -99) as QTY_REGULAR_12WK,");
		sb.append(" NVL(m.QTY_SALE_12WK, -99) as QTY_SALE_12WK, m.QUANTITY_REGULAR, m.QUANTITY_SALE");
		sb.append(" FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m");

		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , item_lookup i ");
		}

		sb.append(" WHERE c.SCHEDULE_ID =").append(schCSV).append("");
		sb.append(" AND c.CHECK_DATA_ID = m.CHECK_DATA_ID");

		if (!departmentCode.isEmpty()) {
			sb.append(" AND i.DEPT_ID NOT IN (").append(departmentCode)
					.append(")");
		}

		if (!categoryid.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (").append(categoryid)
					.append(")");
		}

		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" AND i.item_code = c.item_code ");
		}

		logger.debug("getWeeklyMovementDataMap - SQL : " + sb.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getWeeklyMovements");

		try {
			while (result.next()) {
				Movement13WeeklyDTO momentDto = new Movement13WeeklyDTO();
				momentDto.setCompStoreId(result.getInt("COMP_STR_ID"));
				momentDto.setItemCode(result.getInt("ITEM_CODE"));
				momentDto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				momentDto.setQuantityRegular(result
						.getDouble("QUANTITY_REGULAR"));
				momentDto.setQuantitySale(result.getDouble("QUANTITY_SALE"));
				momentDto.setQuantityRegular12Week(result
						.getDouble("QTY_REGULAR_12WK"));
				momentDto.setQuantitySale12Week(result
						.getDouble("QTY_SALE_12WK"));
				hashMap.put(result.getInt("ITEM_CODE"), momentDto);

				int size = hashMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getWeeklyMovements: processed "
							+ String.valueOf(size));
				}
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		return hashMap;
	}

	/**
	 * Method to set movement 12 week data to 0 for the given Schedule Id
	 * @param scheduleID		Schedule Id
	 * @param targetTableName	Target Table Name
	 * @param departmentCode	Departments to exclude
	 * @param categoryid		Categories to exclude
	 * @return boolean	Updation success/failure
	 * @throws GeneralException
	 */
	public boolean updateWeeklyMovement12WeekInitialData (Connection conn, String scheduleID, String targetTableName, String departmentCode, String categoryid)
	throws GeneralException
	{
		int count = 0;
		
		StringBuffer sb = new StringBuffer("UPDATE ").append(targetTableName);
		sb.append(" SET");		
		sb.append(" QTY_REGULAR_12WK = 0, QTY_SALE_12WK = 0");
		sb.append(" WHERE CHECK_DATA_ID IN");
		sb.append(" (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA c");
		
		//This is application only if Department code or category id are not empty
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , ITEM_LOOKUP i ");
		}
		
		sb.append(" WHERE c.SCHEDULE_ID = ?");

		//If department code has some value, value would be multiple separated with comma
		if (!departmentCode.isEmpty()) {		
			sb.append(" AND i.DEPT_ID NOT IN (").append(departmentCode).append(")");
		}

		//If category id has some value, value would be multiple separated with comma
		if (!categoryid.isEmpty()) {		
			sb.append(" AND i.CATEGORY_ID NOT IN (").append(categoryid).append(")");
		}
		
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" AND c.ITEM_CODE = i.ITEM_CODE ");
		}

		sb.append(" )");
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = conn.prepareStatement(sb.toString());
			pstmt.setString(1, scheduleID);
			count = pstmt.executeUpdate();
		}
		catch (SQLException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw new GeneralException("Error while updating 12 week initial data", gex);
		}
		
		return count > 0;
	}
	
	/**
	 * Method to update 12 week movement data
	 * @param conn					DB Connection
	 * @param movement12DtoList		List containing 12 week movement data
	 * @param targetTableName		Target table name
	 * @return int[]				Updation response
	 * @throws GeneralException
	 */
	public int[] updateMovement12WeekData (Connection conn, List<Movement13WeeklyDTO> movement12DtoList, String targetTableName)
	throws GeneralException
	{
		String sql = UPDATE_12WEEK_MOVEMENTS.replaceAll(Constants.TARGET_TABLE, targetTableName);
		PreparedStatement statement = null;
		int[] count = null;
		try {
			statement = conn.prepareStatement(sql);
			
			for(Movement13WeeklyDTO movementDTO : movement12DtoList){
				if (movementDTO.getQuantityRegular12Week()==-99) {
					statement.setNull(1, java.sql.Types.DOUBLE);			
				}
				else {
					statement.setDouble(1, movementDTO.getQuantityRegular12Week());
				}

				//Check for the update type
				if (movementDTO.getQuantitySale12Week() == -99) {
					statement.setNull(2, java.sql.Types.DOUBLE);
				}
				else
				{
					statement.setDouble(2, movementDTO.getQuantitySale12Week());
				}
				
				statement.setInt(3, movementDTO.getCheckDataId());
				
				statement.addBatch();
			}
			count = statement.executeBatch();
		}
		catch (SQLException gex) {
			logger.error("SQL: " + UPDATE_12WEEK_MOVEMENTS, gex);
			throw new GeneralException("Error while updating 12 week data", gex);
		}
		return count;
	}
	
	/**
	 * Method to get weekly movement data for an item
	 * @param conn			DB Connection
	 * @param scheduleIDs	Schedule Ids
	 * @param itemCodeList	List of items
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Movement13WeeklyDTO> getWeeklyMovementData (Connection conn, String scheduleIDs, List<Integer> itemCodeList)
	throws GeneralException
	{
		HashMap<Integer, Movement13WeeklyDTO> movementsMap  = new HashMap<Integer, Movement13WeeklyDTO>();
		List<Integer> tempList = new ArrayList<Integer>();
		int limitcount=0;
		
		for(Integer itemCode:itemCodeList){
			tempList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%Constants.BATCH_UPDATE_COUNT == 0)){
				Object[] values = tempList.toArray();
				retrieveWeeklyMovement(conn, movementsMap, scheduleIDs, values);
				tempList.clear();
            }
		}
		
		if(tempList.size() > 0){
			Object[] values = tempList.toArray();
			retrieveWeeklyMovement(conn, movementsMap, scheduleIDs, values);
			tempList.clear();
		}
		return movementsMap;
	}
	
	/**
	 * Method that actually queries for weekly movements
	 * @param conn			DB connection
	 * @param movementsMap	Map containing weekly movements
	 * @param scheduleIds	Schedule Ids
	 * @param values		Item Codes
	 * @throws GeneralException
	 */
	private void retrieveWeeklyMovement(Connection conn, HashMap<Integer, Movement13WeeklyDTO> movementsMap, String scheduleIds, 
			Object... values) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
	    String sql = GET_WEEKLY_MOVEMENT.replaceAll(Constants.SCHEDULE_IDS, scheduleIds);
	    Movement13WeeklyDTO movementWeeklyDTO = null;
	    try{
			statement = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	movementWeeklyDTO = new Movement13WeeklyDTO();
	        	movementWeeklyDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
	        	movementWeeklyDTO.setQuantityRegular(resultSet.getDouble("QUANTITY_REGULAR"));
				movementWeeklyDTO.setQuantitySale(resultSet.getDouble("QUANTITY_SALE"));
				movementsMap.put(movementWeeklyDTO.getItemCode(), movementWeeklyDTO);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_12WEEK_MOVEMENTS", e);
			throw new GeneralException("Error while executing GET_12WEEK_MOVEMENTS", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	    
	}
	
	/**
	 * Method to update 13 week movement data
	 * @param conn					DB connection
	 * @param movement13DtoList		List containing 13 week movement for Items
	 * @param targetTableName		Target Table Name
	 * @return	int[]				Updation status
	 * @throws GeneralException
	 */
	public int[] updateMovement13WeekData (Connection conn, List<Movement13WeeklyDTO> movement13DtoList, String targetTableName)
	throws GeneralException
	{
		String sql = UPDATE_13WEEK_MOVEMENTS.replaceAll(Constants.TARGET_TABLE, targetTableName);
		PreparedStatement statement = null;
		int[] count = null;
		
		try {
			statement = conn.prepareStatement(sql);
			
			for(Movement13WeeklyDTO movementDTO : movement13DtoList){
				if (movementDTO.getQuantityRegular13Week()==-99) {
					statement.setNull(1, java.sql.Types.DOUBLE);			
				}
				else {
					statement.setDouble(1, movementDTO.getQuantityRegular13Week());
				}

				//Check for the update type
				if (movementDTO.getQuantitySale13Week() == -99) {
					statement.setNull(2, java.sql.Types.DOUBLE);
				}
				else
				{
					statement.setDouble(2, movementDTO.getQuantitySale13Week());
				}
				
				statement.setInt(3, movementDTO.getCheckDataId());
				
				statement.addBatch();
			}
			count = statement.executeBatch();
		}
		catch (SQLException gex) {
			logger.error("SQL: " + UPDATE_13WEEK_MOVEMENTS, gex);
			throw new GeneralException("Error while updating 12 week data", gex);
		}
		return count;
	}
	
	
	 
	/*
	 * Method used to update the process flag in movement_daily
	 * Method added for repair Process. 
	 * Process used to change the non-process record to process record 
	 * Argument 1: Connection
	 * Argument 2: calendarId
	 * Argument 3: storeNumber  
	 * @throws GeneralException
	 */
	 
	 
	public void updateProcessFlag(Connection _conn, int calendarId,
			String storeNumber) throws GeneralException {

		// Query
		StringBuffer sql = new StringBuffer();
		sql.append(" update MOVEMENT_DAILY set PROCESSED_FLAG='Y'");
		sql.append(" where CALENDAR_ID=").append(calendarId);
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");
		logger.debug(" UpdateProcess Flag .... " + sql.toString());

		// Execute the query
		try {
			PristineDBUtil.execute(_conn, sql, "updateProcessFlag");
		} catch (GeneralException e) {
			logger.error(" Error While updating Process Flag ", e);
			throw new GeneralException(" Error While updating Process Flag ", e);
		}

	}

	/*
	 *  method used to get the coupons based revenue from movement daily table.
	 *  Argument 1 : Connection
	 *  Argument 2 : storeNumber
	 *  Argument 3 : Calendar Id
	 *  return HashMap contains all level aggregation(sub-category , category , department , pos-department)
	 *  HashMap Key - subcategoryid_Levelid , categoryid_levelid 
	 *  @throw GeneralException
	 */
	
	public HashMap<String, Double> getCouponBasedAggregation(Connection _conn,
			String storeNumber, int calendarid) throws GeneralException {
	 
		// return HashMap
		HashMap<String, Double> returnMap  = new HashMap<String, Double>();
		
		// get the level id from constant class
		/*int subCategoryLevelId = Constants.SUBCATEGORYLEVELID;
		int categoryLevelId    = Constants.CATEGORYLEVELID;
		int deptlevelId        = Constants.DEPARTMENTLEVELID;*/
		int posLevelId		   = Constants.POSDEPARTMENT;
		/*int storeLevelId       = Constants.STORE_LEVEL_ID;*/
				
		try {
			StringBuffer sql = new StringBuffer();
			
				sql.append(" select  MV.POS_DEPT");
				
				// logic given by suresh 
				sql.append(" ,sum(DECODE(mv.CPN_WEIGHT, 0, decode(mv.CPN_QTY, 0, 1, mv.CPN_QTY ), mv.CPN_WEIGHT) * mv.PRICE) as COUPONPRICE");
				sql.append(" from MOV_DAILY_COUPON_INFO MV");
				sql.append(" where"); 
				sql.append(" MV.CALENDAR_ID =").append(calendarid);
				sql.append(" and MV.COMP_STR_NO='").append(storeNumber).append("'");
				sql.append(" and MV.POS_DEPT not in(").append(Constants.GASPOSDEPARTMENT).append(")");
				sql.append(" and  MV.CPN_TYPE <> 6 and MV.UPC = '000000000000' ");
				// order by
				sql.append(" group by MV.POS_DEPT");
				sql.append(" Order by MV.POS_DEPT");
				
				logger.debug(" Coupon query..... " + sql.toString());
				
				CachedRowSet rst = PristineDBUtil.executeQuery(_conn, sql, "getCouponBasedAggregation");
				
			while (rst.next()) {

				/*String subCatId = rst.getString("SUB_CATEGORY_ID");
				String catId = rst.getString("CATEGORY_ID");
				String deptId = rst.getString("DEPT_ID");*/
				String posId = rst.getString("POS_DEPT");
				
				double revenue = rst.getDouble("COUPONPRICE");

				/* // for subcategory aggregation
				if (returnMap.containsKey(subCatId + "_" + subCategoryLevelId)) {
					sumCouponRevenue(returnMap,
							returnMap.get(subCatId + "_" + subCategoryLevelId),
							subCatId, subCategoryLevelId, revenue);
				} else {
					sumCouponRevenue(returnMap, 0, subCatId,
							subCategoryLevelId, revenue);
				}

				// for category Aggregation
				if (returnMap.containsKey(catId + "_" + categoryLevelId)) {
					sumCouponRevenue(returnMap,
							returnMap.get(catId + "_" + categoryLevelId),
							catId, categoryLevelId, revenue);
				} else {
					sumCouponRevenue(returnMap, 0, catId, categoryLevelId,
							revenue);
				}

				// for department Aggregation
				if (returnMap.containsKey(deptId + "_" + deptlevelId)) {
					sumCouponRevenue(returnMap,
							returnMap.get(deptId + "_" + deptlevelId), deptId,
							deptlevelId, revenue);
				} else {
					sumCouponRevenue(returnMap, 0, deptId, deptlevelId, revenue);
				}
				 */
				// for pos department aggregation
				if (returnMap.containsKey(posId + "_" + posLevelId)) {
					sumCouponRevenue(returnMap,
							returnMap.get(posId + "_" + posLevelId), posId,
							posLevelId, revenue);
				} else {
					sumCouponRevenue(returnMap, 0, posId, posLevelId, revenue);
				}
				
					/*// for store level aggregation
					if (returnMap.containsKey(storeNumber+"_"+storeLevelId)) {
						sumCouponRevenue(returnMap,
								returnMap.get(storeNumber + "_" + storeLevelId), storeNumber,
								storeLevelId, revenue);
					} else {
						sumCouponRevenue(returnMap, 0, storeNumber, storeLevelId, revenue);
					}		
					 */
			}
			
			rst.close();
			
		} catch (SQLException e) {
			 logger.error(" Error while fetching result ...." , e);
			 throw new GeneralException(" Error while fetching result ...." , e);
		} catch (GeneralException e) {
			logger.error(" Error while fetching result ...." , e);
			 throw new GeneralException(" Error while fetching result ...." , e);
		}
			
		logger.debug(" Return Coupon  Map size......" + returnMap.size());
		
		return returnMap;
	}

	/*
	 * Method used to new/add the coupon revenue 
	 * Argument 1 : returnMap
	 * Arguemnt 2 : newRevenue
	 * Argument 2 : productId
	 * Argument 3 : productLevelId
	 * Argument 4 : oldrevenue
	 * 
	 */
	private void sumCouponRevenue(HashMap<String, Double> returnMap,
			double newRevenue, String productId, int productLevelId,
			double oldrevenue) {

		double totRevenue = newRevenue + oldrevenue;

		// logger.info(" Product Id..." + productId +"...Product LevelId.." +
		// productLevelId +"....Revenue.." + totRevenue);
		returnMap.put(productId, totRevenue);

	}
	
	
	/*
	 * Method used to get the distinct item code for given calendar and store no 
	 * Argument 1 : conn
	 * Argument 2 : Store number
	 * Argument 3 : calendar_id
	 * @throw GeneralException
	 *  
	 */
	
	public List<String> getDistinctItemCode(Connection _conn,
			String storeNumber, int calendar_id) throws GeneralException  {
		
		// Return List
		List<String> returnList = new ArrayList<String>();
		
		try {
			// Query
			StringBuffer sql = new StringBuffer();
			
			sql.append(" select distinct ITEM_CODE from MOVEMENT_DAILY");
			sql.append(" where CALENDAR_ID=").append(calendar_id);
			sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");
			
			logger.debug(" Item code query...." + sql.toString());
			
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getDistinctItemCode");
			
			while( result.next()){
				
				returnList.add(result.getString("ITEM_CODE"));
			}
			
			result.close();
			
			logger.debug(" Movement Item code List size.... " + returnList.size());
		} catch (SQLException e) {
			 logger.error(" Error while fetching the distinct item code in movement daily " , e);
			 throw new GeneralException(" Error while fetching the distinct item code in movement daily " , e);
		} catch (GeneralException e) {
			logger.error(" Error while fetching the distinct item code in movement daily " , e);
			 throw new GeneralException(" Error while fetching the distinct item code in movement daily " , e);
		}
		
			
		return returnList;
	}
}
/*
  CREATE TABLE MOVEMENT_DAILY
  (
    COMP_STR_NO			VARCHAR2(10 BYTE) NOT NULL,
    UPC         		VARCHAR2(20 BYTE) NOT NULL,
    POS_TIMESTAMP		DATE NOT NULL,
    UNIT_PRICE       	NUMBER(5,2) NOT NULL,
    SALE_FLAG   		CHAR(1 BYTE) NOT NULL,
    QUANTITY    		NUMBER(6,0) DEFAULT 0,
    WEIGHT      		NUMBER(8,4) DEFAULT 0,
    PRICE       		NUMBER(6,2) NOT NULL,
    WEEKDAY				VARCHAR2(10 BYTE) NOT NULL,
    TRANSACTION_NO		NUMBER(8,0) NOT NULL,
    CUSTOMER_CARD_NO	VARCHAR2(30 BYTE)
  )
  TABLESPACE "USERS";

  ALTER TABLE MOVEMENT_DAILY RENAME COLUMN DAY_OFF_WEEK to WEEKDAY;
  ALTER TABLE MOVEMENT_DAILY ADD TRANSACTION_NO NUMBER(8,0) DEFAULT 0;
  
ALTER TABLE MOVEMENT_DAILY MODIFY UNIT_PRICE NUMBER(7,2);
ALTER TABLE MOVEMENT_DAILY MODIFY PRICE NUMBER(8,2);

ALTER TABLE MOVEMENT_DAILY ADD ITEM_CODE NUMBER(20,0);
ALTER TABLE MOVEMENT_DAILY ADD CALENDAR_ID NUMBER(8,0);

  CREATE TABLE MOVEMENT_WEEKLY
  (
    COMP_STR_ID			NUMBER(5,0) NOT NULL,
    ITEM_CODE			NUMBER(6,0) NOT NULL,
    CHECK_DATA_ID		NUMBER(8,0) NOT NULL PRIMARY KEY,
    REVENUE_REGULAR		NUMBER(8,2) DEFAULT 0,
    QUANTITY_REGULAR  	NUMBER(10,4) DEFAULT 0,
    REVENUE_SALE    	NUMBER(8,2) DEFAULT 0,
    QUANTITY_SALE  		NUMBER(10,4) DEFAULT 0,
    LIST_COST  			NUMBER(7,2),
    EFF_LIST_COST_DATE	DATE,
    DEAL_COST  			NUMBER(7,2),
    DEAL_START_DATE		DATE,
    DEAL_END_DATE		DATE,
    COST_CHG_DIRECTION  NUMBER(1),
    MARGIN_CHG_DIRECTION  NUMBER(1),
    MARGIN_PCT			NUMBER(5,2)
  )
  TABLESPACE "USERS";

 Alter table movement_weekly add List_cost number (7,2);
Alter table movement_weekly add Eff_List_Cost_Date date;
Alter table movement_weekly add Deal_cost number (7,2);
Alter table movement_weekly add Deal_Start_date Date;
Alter table movement_weekly add Deal_End_date Date;
Alter table movement_weekly add Cost_Chg_Direction Number (1);

Alter table movement_weekly add Margin_Chg_Direction Number (1);
Alter table movement_weekly add Margin_Pct Number (5,2);

  CREATE INDEX Movement_Weekly_Index ON MOVEMENT_WEEKLY (COMP_STR_ID, ITEM_CODE)
  	  TABLESPACE "USERS";
  ALTER TABLE MOVEMENT_WEEKLY ADD (
      CONSTRAINT FK_MOVEMENT_WEEKLY_COMP_STORE
      FOREIGN KEY (COMP_STR_ID)
      REFERENCES COMPETITOR_STORE(COMP_STR_ID));
  ALTER TABLE MOVEMENT_WEEKLY ADD (
 	  CONSTRAINT FK_MOVEMENT_WEEKLY_ITEMLOOKUP
 	  FOREIGN KEY (ITEM_CODE) 
 	  REFERENCES ITEM_LOOKUP (ITEM_CODE));
  ALTER TABLE MOVEMENT_WEEKLY ADD (
 	  CONSTRAINT FK_MOVEMENT_WEEKLY_COMP_DATA
 	  FOREIGN KEY (CHECK_DATA_ID) 
 	  REFERENCES COMPETITIVE_DATA (CHECK_DATA_ID));

  CREATE TABLE MOVEMENT_WEEKLY_LIG
  (
    COMP_STR_ID			NUMBER(5,0) NOT NULL,
    ITEM_CODE			NUMBER(6,0) NOT NULL,
    CHECK_DATA_ID		NUMBER(8,0) NOT NULL PRIMARY KEY,
    REVENUE_REGULAR		NUMBER(8,2) DEFAULT 0,
    QUANTITY_REGULAR  	NUMBER(10,4) DEFAULT 0,
    REVENUE_SALE    	NUMBER(8,2) DEFAULT 0,
    QUANTITY_SALE  		NUMBER(10,4) DEFAULT 0,
    LIST_COST  			NUMBER(7,2),
    EFF_LIST_COST_DATE	DATE,
    DEAL_COST  			NUMBER(7,2),
    DEAL_START_DATE		DATE,
    DEAL_END_DATE		DATE,
    COST_CHG_DIRECTION  NUMBER(1),
    MARGIN_CHG_DIRECTION  NUMBER(1),
    MARGIN_PCT			NUMBER(5,2)
  )
  TABLESPACE "USERS";
 
  CREATE INDEX Movement_Weekly_Lig_Index ON MOVEMENT_WEEKLY_LIG (COMP_STR_ID, ITEM_CODE)
  	  TABLESPACE "USERS";
  ALTER TABLE MOVEMENT_WEEKLY_LIG ADD (
      CONSTRAINT FK_MOVEMENT_WKLY_LIG_COMP_STR
      FOREIGN KEY (COMP_STR_ID)
      REFERENCES COMPETITOR_STORE(COMP_STR_ID));
  ALTER TABLE MOVEMENT_WEEKLY_LIG ADD (
 	  CONSTRAINT FK_MOVEMENT_WKLY_LIG_ITEMLKUP
 	  FOREIGN KEY (ITEM_CODE) 
 	  REFERENCES ITEM_LOOKUP (ITEM_CODE));
  ALTER TABLE MOVEMENT_WEEKLY_LIG ADD (
 	  CONSTRAINT FK_MOVEMENT_WKLY_LIG_COMP_DAT
 	  FOREIGN KEY (CHECK_DATA_ID) 
 	  REFERENCES COMPETITIVE_DATA (CHECK_DATA_ID));

*

update MOVEMENT_DAILY set TRANSACTION_NO = 830175 where COMP_STR_NO = '0209' and UPC = '00000000741'
and POS_TIMESTAMP = To_DATE('201103132339', 'YYYYMMDDHH24MI') and UNIT_PRICE = 3.99 and SALE_FLAG = 'N' and QUANTITY = 0.0
and WEIGHT = 2.5789 and PRICE = 10.29

-- upc level weekly revenue and scanned units
select sum(price) as revenue, sum(quantity) as scans, m.upc, c.name as category, d.name as dept from
(select m.upc, price, (case when quantity > 0 then quantity else weight end) as quantity from movement_daily where pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI')
and pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI') and comp_str_no = '0363') m, item_lookup i, category c, department d
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.category_id = c.id and i.dept_id = d.id
group by d.name, c.name, m.upc order by upper(d.name), upper(c.name), upper(m.upc)

-- Category level weekly revenue and scanned units
select sum(price) as revenue, sum(quantity) as scans, c.name as category, d.name as dept from
(select upc, price, (case when quantity > 0 then quantity else weight end) as quantity from movement_daily where pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI')
and pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI') and comp_str_no = '0363') m, item_lookup i, category c, department d
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.category_id = c.id and i.dept_id = d.id
group by d.name, c.name order by upper(d.name), upper(c.name)

-- Dept level weekly revenue
select d.name as dept, sum(price) as revenue from
(select upc, price, (case when quantity > 0 then quantity else weight end) as quantity from movement_daily
where pos_timestamp >= To_DATE('201107240000', 'YYYYMMDDHH24MI') and pos_timestamp <= To_DATE('201108302359', 'YYYYMMDDHH24MI')
and comp_str_no = '0363') m, item_lookup i, department d
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.dept_id = d.id and d.id != 37
group by d.name order by upper(d.name)

-- RX items
select upc, sum(price) as revenue from movement_daily
where pos_timestamp >= To_DATE('201107310000', 'YYYYMMDDHH24MI') and pos_timestamp <= To_DATE('201108062359', 'YYYYMMDDHH24MI')
and comp_str_no = '0363' and (upc like '%00000000101' or upc like '%0000000102') group by upc order by upc

-- TOPS weekly revenue
select sum(price) as revenue, sum(quantity) as scans, c.name as category, d.name as dept from
(select upc, price, (case when quantity > 0 then quantity else weight end) as quantity from movement_daily where pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI')
and pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI') and comp_str_no = '0363') m, item_lookup i, category c, department d
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.category_id = c.id and i.dept_id = d.id
group by d.name, c.name order by upper(d.name), upper(c.name)
440
669
439
441
645
666

// Retrieve count for 15 Mar 11
select count(*) from movement_daily where pos_timestamp > To_DATE('20110314235959', 'YYYYMMDDHH24MISS')
and pos_timestamp < To_DATE('20110316000000', 'YYYYMMDDHH24MISS')

// Total margin, visit count - weekly movement
select * from MOVEMENT_WEEKLY m   where m.COMP_STR_ID = 5670   and m.CHECK_DATA_ID = 22250047   and m.ITEM_CODE = 145256
update MOVEMENT_WEEKLY set total_margin = null, margin_pct = null, visit_count = null, qty_regular_13wk = null, qty_sale_13wk = null
where COMP_STR_ID = 5670 and CHECK_DATA_ID = 22250047 and ITEM_CODE = 145256

// LIG
select m.COMP_STR_NO, i.RET_LIR_ID, l.ret_lir_item_code, count(distinct m.transaction_no) as VISIT_COUNT from MOVEMENT_DAILY m, ITEM_LOOKUP i, retailer_like_item_group l
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.ret_lir_id = l.ret_lir_id
and m.pos_timestamp >= To_DATE('201107030000', 'YYYYMMDDHH24MI') and m.pos_timestamp <= To_DATE('201107092359', 'YYYYMMDDHH24MI') and m.transaction_no > 0
group by m.COMP_STR_NO, i.RET_LIR_ID, l.ret_lir_item_code order by m.COMP_STR_NO, i.RET_LIR_ID

// LIG quantity
select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE from MOVEMENT_WEEKLY_LIG m, COMPETITIVE_DATA_VIEW d, item_lookup i, retailer_like_item_group l
where m.COMP_STR_ID = d.COMP_STR_ID and i.ret_lir_id = l.ret_lir_id and m.ITEM_CODE = l.RET_LIR_ITEM_CODE and i.ret_lir_id is not null
and m.COMP_STR_ID = 5670 and m.ITEM_CODE = 420841 and i.ret_lir_id = 8795
and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') >= '2011/03/01' and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') <= '2011/07/03'

// 13 week data
select m.ITEM_CODE, sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE from MOVEMENT_WEEKLY m, COMPETITIVE_DATA_VIEW d
where m.COMP_STR_ID = d.COMP_STR_ID and m.CHECK_DATA_ID = d.CHECK_DATA_ID and m.COMP_STR_ID = 5704 and m.ITEM_CODE in (11804,11805,11807,204999,204997,12081,12082) 
and d.START_DATE >= '24-Apr-11' and d.START_DATE <= '17-Jul-11'
group by m.ITEM_CODE

select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE
from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d where m.ITEM_CODE = 
			sb.append(" and d.SCHEDULE_ID in (").append(schCSV).append(")");
		}
		sb.append(" and m.CHECK_DATA_ID = d.CHECK_DATA_ID");

select comp_str_id, item_code, total_margin, margin_pct, visit_count, qty_regular_13wk, qty_sale_13wk from movement_weekly
where comp_str_id = 5704 and qty_regular_13wk > 0 and qty_sale_13wk > 0 and item_code in (11804,11805,11807,204999,204997,12081,12082)

-- getVisitWeekly
select COMP_STR_NO, UPC, sum(VISIT_COUNT) as VISIT_COUNT from
( select COMP_STR_NO, WEEKDAY, UPC, count(distinct transaction_no) as VISIT_COUNT from MOVEMENT_DAILY
where pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI') and pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI')
and transaction_no > 0 and COMP_STR_NO ='0363' group by COMP_STR_NO, WEEKDAY, UPC )
group by COMP_STR_NO, UPC

-- getLIGVisitWeekly
select COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE, sum(VISIT_COUNT) as VISIT_COUNT from
( select m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY, count(distinct m.transaction_no) as VISIT_COUNT
from MOVEMENT_DAILY m, ITEM_LOOKUP i, retailer_like_item_group l
where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.ret_lir_id = l.ret_lir_id
and m.pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI') and pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI') and
m.transaction_no > 0and m.COMP_STR_NO ='0363'
group by m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY )
group by COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE
order by COMP_STR_NO, RET_LIR_ID

select
(case when m.DEAL_COST is null then m.LIST_COST
 when m.DEAL_COST = 0 then m.LIST_COST
 else m.DEAL_COST
 end) COST
from MOVEMENT_WEEKLY m
where m.COMP_STR_ID = 5704
and m.CHECK_DATA_ID = 44330640
and m.ITEM_CODE = 43413

and m.CHECK_DATA_ID = 37769057
and m.ITEM_CODE = 1192
*/
