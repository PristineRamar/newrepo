package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dto.Movement13WeeklyDTO;
import com.pristine.dto.MovementDTO;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.TransactionLogDTO;
import com.pristine.dto.customer.HouseholdSummaryDailyInDTO;
import com.pristine.dto.salesanalysis.HouseholdDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;

@SuppressWarnings("unused")
public class MovementDAO implements IDAO {
	static Logger logger = Logger.getLogger("MovementDAO");

	private static final String GET_WEEKLY_MOVEMENT = "SELECT m.ITEM_CODE, sum(NVL(m.QUANTITY_REGULAR, 0)) as QUANTITY_REGULAR, sum(NVL(m.QUANTITY_SALE,0)) as QUANTITY_SALE "
			+ " FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m "
			+ " WHERE c.SCHEDULE_ID in (%SCHEDULE_IDS%) AND c.CHECK_DATA_ID = m.CHECK_DATA_ID "
			+ " AND m.ITEM_CODE in (%s) group by m.ITEM_CODE";

	private static final String UPDATE_12WEEK_MOVEMENTS = "UPDATE %TARGET_TABLE% SET QTY_REGULAR_12WK = ?, QTY_SALE_12WK = ? where CHECK_DATA_ID = ?";

	private static final String UPDATE_13WEEK_MOVEMENTS = "UPDATE %TARGET_TABLE% SET QTY_REGULAR_13WK = ?, QTY_SALE_13WK = ? where CHECK_DATA_ID = ?";
	
	//COMMENTED BY KIRTHI 13/12/2019
	
	/*
	 * private static final String GET_REG_PRICE_FROM_TLOG =
	 * " SELECT UNIT_PRICE,ITEM_ID FROM ( " +
	 * " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
	 * +
	 * " SELECT UNIT_PRICE, ITEM_ID, COUNT(*) NOOFOCCURENCE FROM (SELECT CASE WHEN WEIGHT > 0 THEN REGULAR_AMT/WEIGHT ELSE REGULAR_AMT/QUANTITY "
	 * +
	 * " END AS UNIT_PRICE, ITEM_ID FROM (SELECT ABS(REGULAR_AMT) AS REGULAR_AMT, ABS(WEIGHT) AS WEIGHT, ABS(QUANTITY) AS QUANTITY, ITEM_ID FROM %TABLE_NAME% WHERE REGULAR_AMT <> 0 AND STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID_2 IN (?)) AND ITEM_ID IN (%ITEM_CODES%) "
	 * + " AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR  " +
	 * " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?))) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 "
	 * ;
	 */
	
	//CHANGED PRICE_ZONE_ID_2 TO PRICE_ZONE_ID
	private static final String GET_REG_PRICE_FROM_TLOG = " SELECT UNIT_PRICE,ITEM_ID FROM ( "
			+ " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
			+ " SELECT UNIT_PRICE, ITEM_ID, COUNT(*) NOOFOCCURENCE FROM (SELECT CASE WHEN WEIGHT > 0 THEN REGULAR_AMT/WEIGHT ELSE REGULAR_AMT/QUANTITY "
			+ " END AS UNIT_PRICE, ITEM_ID FROM (SELECT ABS(REGULAR_AMT) AS REGULAR_AMT, ABS(WEIGHT) AS WEIGHT, ABS(QUANTITY) AS QUANTITY, ITEM_ID "
			+ " FROM %TABLE_NAME% TL "
			+ " LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TL.TRX_TYPE = TTL.TRX_TYPE "
			+ " WHERE REGULAR_AMT <> 0 AND QUANTITY <> 0 "
			+ " AND STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID IN (?)) AND ITEM_ID IN (%ITEM_CODES%) "
			+ " AND (TTL.EXCLUDE_SALES_CALC IS NULL OR TTL.EXCLUDE_SALES_CALC = 'N') AND (TTL.EXCLUDE_UNIT_CALC IS NULL OR TTL.EXCLUDE_UNIT_CALC = 'N') "
			+ " AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR  "
			+ " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?))) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 ";
	
	
	private static final String GET_REG_PRICE_FROM_TLOG_PLM = "SELECT UNIT_PRICE,ITEM_ID FROM ( "
			+ " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
			+ " SELECT UNIT_PRICE, ITEM_ID, COUNT(*) NOOFOCCURENCE FROM (SELECT CASE WHEN WEIGHT > 0 THEN REGULAR_AMT/WEIGHT ELSE REGULAR_AMT/QUANTITY "
			+ " END AS UNIT_PRICE, ITEM_ID FROM (SELECT REGULAR_AMT, WEIGHT, QUANTITY, ITEM_ID FROM %TABLE_NAME% TL "
			+ " LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TL.TRX_TYPE = TTL.TRX_TYPE "
			+ " WHERE REGULAR_AMT > 0 and (WEIGHT > 0 OR QUANTITY > 0) "
			+ " AND STORE_ID IN (SELECT DISTINCT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN "
			+ " (SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE LOCATION_ID = ? )) AND ITEM_ID IN (%ITEM_CODES%) "
			+ " AND (TTL.EXCLUDE_SALES_CALC IS NULL OR TTL.EXCLUDE_SALES_CALC = 'N') AND (TTL.EXCLUDE_UNIT_CALC IS NULL OR TTL.EXCLUDE_UNIT_CALC = 'N') "
			+ " AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR  "
			+ " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?))) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 ";

	/*
	 * private static final String GET_SALE_PRICE_FROM_TLOG =
	 * " SELECT UNIT_PRICE,ITEM_ID FROM ( " +
	 * " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
	 * + " SELECT UNIT_PRICE,ITEM_ID,COUNT(*) AS NOOFOCCURENCE FROM %TABLE_NAME% " +
	 * " WHERE STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID_2 IN (?)) AND ITEM_ID IN (%ITEM_CODES%) AND "
	 * +
	 * " UNIT_PRICE > 0 AND SALE_TYPE='Y' AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
	 * +
	 * " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 "
	 * ;
	 */
	
	private static final String GET_SALE_PRICE_FROM_TLOG = " SELECT UNIT_PRICE,ITEM_ID, SALE_TYPE FROM ( "
			+ " SELECT UNIT_PRICE,ITEM_ID, SALE_TYPE, RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
			+ " SELECT UNIT_PRICE,ITEM_ID, SALE_TYPE, COUNT(*) AS NOOFOCCURENCE FROM %TABLE_NAME% TL "
			+ " LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TL.TRX_TYPE = TTL.TRX_TYPE "
			+ " WHERE STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID IN (?)) AND ITEM_ID IN (%ITEM_CODES%) "
			+ " AND (TTL.EXCLUDE_SALES_CALC IS NULL OR TTL.EXCLUDE_SALES_CALC = 'N') AND (TTL.EXCLUDE_UNIT_CALC IS NULL OR TTL.EXCLUDE_UNIT_CALC = 'N') "
			+ " AND UNIT_PRICE > 0 AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
			+ " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?) "
			+ " GROUP BY UNIT_PRICE,ITEM_ID, SALE_TYPE)) WHERE UNIT_PRICE_RANK=1 ";
	
	private static final String GET_SALE_PRICE_FROM_TLOG_PLM = " SELECT UNIT_PRICE,ITEM_ID FROM ( "
			+ " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
			+ " SELECT UNIT_PRICE,ITEM_ID,COUNT(*) AS NOOFOCCURENCE FROM %TABLE_NAME% TL "
			+ " LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TL.TRX_TYPE = TTL.TRX_TYPE "
			+ " WHERE STORE_ID IN (SELECT DISTINCT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN "
			+ " (SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE LOCATION_ID = ? )) AND ITEM_ID IN (%ITEM_CODES%) "
			+ " AND (TTL.EXCLUDE_SALES_CALC IS NULL OR TTL.EXCLUDE_SALES_CALC = 'N') AND (TL.EXCLUDE_UNIT_CALC IS NULL OR TL.EXCLUDE_UNIT_CALC = 'N') "
			+ " AND UNIT_PRICE > 0 AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
			+ " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 ";
		
	private static final String GET_SALE_PRICE_FROM_TLOG_FOR_STORE = " SELECT UNIT_PRICE,ITEM_ID FROM ( "
			+ " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
			+ " SELECT UNIT_PRICE,ITEM_ID,COUNT(*) AS NOOFOCCURENCE FROM %TABLE_NAME% "
			+ " WHERE STORE_ID = ? AND ITEM_ID IN (%ITEM_CODES%) AND "
			+ " UNIT_PRICE > 0 AND SALE_TYPE='Y' AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
			+ " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 ";
	
	
	private static final String GET_REG_PRICE_FROM_TLOG_FOR_STORE = " SELECT UNIT_PRICE,ITEM_ID FROM ( "
			+ " SELECT UNIT_PRICE,ITEM_ID,RANK() OVER (PARTITION BY ITEM_ID ORDER BY NOOFOCCURENCE DESC) UNIT_PRICE_RANK FROM ("
			+ " SELECT UNIT_PRICE, ITEM_ID, COUNT(*) NOOFOCCURENCE FROM (SELECT CASE WHEN WEIGHT > 0 THEN REGULAR_AMT/WEIGHT ELSE REGULAR_AMT/QUANTITY "
			+ " END AS UNIT_PRICE, ITEM_ID FROM (SELECT ABS(REGULAR_AMT) AS REGULAR_AMT, ABS(WEIGHT) AS WEIGHT, ABS(QUANTITY) AS QUANTITY, "
			+ " ITEM_ID FROM %TABLE_NAME% WHERE REGULAR_AMT <> 0 AND STORE_ID = ? AND ITEM_ID IN (%ITEM_CODES%) "
			+ " AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR  "
			+ " WHERE START_DATE BETWEEN TO_DATE(?, 'MM/DD/YYYY') AND TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE=?))) GROUP BY UNIT_PRICE,ITEM_ID)) WHERE UNIT_PRICE_RANK=1 ";
	
	private  final int commitCount = Constants.LIMIT_COUNT;
	public ArrayList<String> getTopsStores(Connection conn, Date start, Date end)
			throws GeneralException {
		ArrayList<String> stores = new ArrayList<String>();

		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT cs.comp_str_no FROM schedule s, competitor_store cs");
		sb.append(" WHERE s.comp_str_id = cs.comp_str_id");
		sb.append(" AND TO_CHAR(s.start_date, 'MM/DD/YYYY') = '")
				.append(startStr).append("'");
		sb.append(" AND cs.comp_chain_id = 50"); // TOPS chain id is 50

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
					"getTopsStores");
			while (result.next()) {
				String storeNo = result.getString("comp_str_no");
				stores.add(storeNo);
			}
		} catch (Exception ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return stores;
	}

	public ArrayList<String> getTopsMovementStores(Connection conn, Date start,
			Date end) throws GeneralException {
		ArrayList<String> stores = new ArrayList<String>();

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);

		StringBuffer sb = new StringBuffer(
				"select distinct COMP_STR_NO from MOVEMENT_DAILY where");
		sb.append(" POS_TIMESTAMP >= ").append("To_DATE('").append(startStr)
				.append("', 'YYYYMMDDHH24MI')");
		sb.append(" and POS_TIMESTAMP <= ").append("To_DATE('").append(endStr)
				.append("', 'YYYYMMDDHH24MI')");

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
					"getTopsMovementStores");
			while (result.next()) {
				String storeNum = result.getString("COMP_STR_NO");
				stores.add(storeNum);
			}
		} catch (Exception ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return stores;
	}

	public boolean insertDailyMovement(Connection conn, MovementDTO movement,
			boolean deleteIfPresent) throws GeneralException {
		StringBuffer sb = new StringBuffer("insert into MOVEMENT_DAILY (");
		sb.append(" COMP_STR_NO, UPC, POS_TIMESTAMP, WEEKDAY, UNIT_PRICE, PRICE, SALE_FLAG, QUANTITY, WEIGHT");
		sb.append(", TRANSACTION_NO, CUSTOMER_CARD_NO) values (");
		sb.append("'").append(movement.getItemStore()).append("'");
		String upc = PrestoUtil.castUPC(movement.getItemUPC(), true);
		sb.append(", '").append(upc).append("'");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String dateStr = formatter.format(movement.getItemDateTime());
		sb.append(", To_DATE('").append(dateStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(", '").append(movement.getDayOfWeek()).append("'");

		double unitPrice = movement.getItemNetPrice();
		double price = movement.getExtendedNetPrice();
		char saleFlag = (0 <= price && price < movement.getExtendedGrossPrice()) ? 'Y'
				: 'N';
		sb.append(", ").append(unitPrice);
		sb.append(", ").append(price);
		sb.append(", '").append(saleFlag).append("'");

		sb.append(", ").append(movement.getExtnQty());
		sb.append(", ").append(movement.getExtnWeight());
		sb.append(", ").append(movement.getTransactionNo());
		sb.append(", '").append(movement.getCustomerId()).append("'");

		sb.append(")"); // close

		// String sql = sb.toString();
		try {
			PristineDBUtil.execute(conn, sb, "MovementDAO - Insert");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}

		return true;
	}

	public boolean updateDailyMovementTransactions(Connection conn,
			MovementDTO movement, boolean logError) throws GeneralException {
		StringBuffer sb = new StringBuffer("update MOVEMENT_DAILY set");
		sb.append(" TRANSACTION_NO = ").append(movement.getTransactionNo());
		sb.append(" where COMP_STR_NO = '").append(movement.getItemStore())
				.append("'");
		String upc = PrestoUtil.castUPC(movement.getItemUPC(), true);
		sb.append(" and UPC = '").append(upc).append("'");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		String dateStr = formatter.format(movement.getItemDateTime());
		sb.append(" and POS_TIMESTAMP = To_DATE('").append(dateStr)
				.append("', 'YYYYMMDDHH24MISS')");

		/*
		 * char saleFlag; double unitPrice = movement.getItemNetPrice(); double
		 * price; if ( 0 < unitPrice && unitPrice < movement.getItemGrossPrice()
		 * ) { saleFlag = 'Y'; price = movement.getExtendedNetPrice(); } else {
		 * saleFlag = 'N'; unitPrice = movement.getItemGrossPrice(); price =
		 * movement.getExtendedGrossPrice(); }
		 * sb.append(" and UNIT_PRICE = ").append(unitPrice);
		 * sb.append(" and SALE_FLAG = '").append(saleFlag).append("'");
		 * 
		 * sb.append(" and QUANTITY = ").append(movement.getExtnQty());
		 * sb.append(" and WEIGHT = ").append(movement.getExtnWeight());
		 * sb.append(" and PRICE = ").append(price);
		 */

		if (movement.getCustomerId().length() > 0)
			sb.append(" and CUSTOMER_CARD_NO = '")
					.append(movement.getCustomerId()).append("'");

		// String sql = sb.toString();
		int count = 0;
		try {
			count = PristineDBUtil.executeUpdate(conn, sb,
					"MovementDAO - UpdateDailyTransactions");
			if (count > 1) {
				String msg = "Updated " + count + " records for store="
						+ movement.getItemStore() + " time=" + dateStr
						+ " ups=" + upc;
				logger.info(msg);
			}
		} catch (GeneralException gex) {
			if (logError) {
				logger.error("SQL: " + sb.toString(), gex);
			}
			throw gex;
		}

		return count > 0;
	}

	public boolean deleteDailyMovement(Connection conn, MovementDTO movement,
			boolean logError) throws GeneralException {
		StringBuffer sb = new StringBuffer("delete from MOVEMENT_DAILY where");
		sb.append(" COMP_STR_NO = '").append(movement.getItemStore())
				.append("'");
		String upc = PrestoUtil.castUPC(movement.getItemUPC(), true);
		sb.append(" and UPC = '").append(upc).append("'");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String dateStr = formatter.format(movement.getItemDateTime());
		sb.append(" and POS_TIMESTAMP = To_DATE('").append(dateStr)
				.append("', 'YYYYMMDDHH24MI')");

		char saleFlag;
		double unitPrice = movement.getItemNetPrice();
		double price;
		if (0 < unitPrice && unitPrice < movement.getItemGrossPrice()) {
			saleFlag = 'Y';
			price = movement.getExtendedNetPrice();
		} else {
			saleFlag = 'N';
			unitPrice = movement.getItemGrossPrice();
			price = movement.getExtendedGrossPrice();
		}
		sb.append(" and UNIT_PRICE = ").append(unitPrice);
		sb.append(" and SALE_FLAG = '").append(saleFlag).append("'");

		sb.append(" and QUANTITY = ").append(movement.getExtnQty());
		sb.append(" and WEIGHT = ").append(movement.getExtnWeight());
		sb.append(" and PRICE = ").append(price);
		sb.append(" and CUSTOMER_CARD_NO = '").append(movement.getCustomerId())
				.append("'");

		try {
			PristineDBUtil.execute(conn, sb, "MovementDAO - Delete");
		} catch (GeneralException gex) {
			if (logError) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}

		return true;
	}

	// Price Index By Price Zone - Added another argument to differentiate between store and zone
	public List<MovementWeeklyDTO> getQuantityMovementsForPeriod (Connection conn, String storeNum,
			Date start, Date end, boolean sale, int processLevelTypeId, boolean isGloablZone)
	throws GeneralException
	{
		CachedRowSet result = getQuantityMovementsForPeriodRowSet (conn, storeNum, start, end, sale, processLevelTypeId,isGloablZone);
		return populateList (result, sale);
	}
	
	
	// Price Index By Price Zone - Added another argument to differentiate between store and zone
	// Changes made to run SQL with PreparedStatement instead of Statement
	public CachedRowSet getQuantityMovementsForPeriodRowSet (Connection conn, String storeNum,
			Date start, Date end, boolean sale, int processLevelTypeId, boolean isGloablZone)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);

		//StringBuffer sb = new StringBuffer("select COMP_STR_NO, UPC, sum(PRICE) as PRICE, sum(QUANTITY) as QUANTITY from MOVEMENT_DAILY where");
		// Changes to consider authorized items for store/zone - ITEM_CODE added to query
		StringBuffer sb = new StringBuffer("select UPC, ITEM_CODE, sum(PRICE) as PRICE, sum(QUANTITY) as QUANTITY from MOVEMENT_DAILY where");
		sb.append(" CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= To_DATE(?, 'YYYYMMDDHH24MI')");
		sb.append(" and START_DATE <= To_DATE(?, 'YYYYMMDDHH24MI') AND ROW_TYPE = 'D') ");
		char saleFlag = sale ? 'Y' : 'N';
		sb.append(" and SALE_FLAG = ?");	
		sb.append(" and QUANTITY <> 0");	// need to include returns
		if ( storeNum != null ) {
			appendStoresQueryForGivenLocation(processLevelTypeId, sb,isGloablZone);
		}
		//sb.append(" and UPC in ('07078424030', '07078427352', '07078428240', '03010001610', '03000031018', '04400000028')");
		sb.append(" group by UPC, ITEM_CODE");
		sb.append(" order by UPC, ITEM_CODE");
		//sb.append(" group by COMP_STR_NO, UPC");
		//sb.append(" order by COMP_STR_NO, UPC");
	
		String sql = sb.toString();
		logger.debug ("getQuantityMovementsForPeriod SQL: " + sql);
		PreparedStatement statement = null;
		ResultSet rs = null;
		CachedRowSet result = null;
		try {
			statement = conn.prepareStatement(sb.toString());
			statement.setString(1, startStr);
			statement.setString(2, endStr);
			statement.setString(3, String.valueOf(saleFlag));
			statement.setString(4, storeNum);
			rs = statement.executeQuery();
			result = PristineDBUtil.getCachedRowSet(rs);
		}
		catch (SQLException ex) {
			logger.error("Error in getQuantityMovementsForPeriod. SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}

		return result;
	}

	
	private void appendStoresQueryForGivenLocation(int locationLevelId, StringBuffer sb, boolean isGloablZone){
		if(locationLevelId == Constants.STORE_LEVEL_ID)
			sb.append("   and COMP_STR_NO = ? ");
		else if (locationLevelId == Constants.CHAIN_LEVEL_ID)
			sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ?) ");
		else if (locationLevelId == Constants.DIVISION_LEVEL_ID){
			sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE "
					+ " COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') ");
			sb.append(" and DIVISION_ID = ?) ");
		}
		else if (locationLevelId == Constants.REGION_LEVEL_ID){
			sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE "
					+ " COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') ");
			sb.append(" and REGION_ID = ?)");
		}
		else if (locationLevelId == Constants.DISTRICT_LEVEL_ID){
			sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE "
					+ " COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y')) ");
			sb.append(" and DISTRICT_ID = ?");
		}
		else if (isGloablZone) {
			sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ "
					+ "WHERE RPZ.ZONE_NUM = ? AND CS.PRICE_ZONE_ID_3 = RPZ.PRICE_ZONE_ID) ");
		} else {
			sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ "
					+ "WHERE RPZ.ZONE_NUM = ? AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID) ");
		}
	}
	
	// Price Index By Price Zone - Added another argument to differentiate between store and zone
	public List<MovementWeeklyDTO> getWeightMovementsForPeriod (Connection conn, String storeNum,
			Date start, Date end, boolean sale, int processLevelTypeId,boolean isGloablZone)
	throws GeneralException
	{
		CachedRowSet result = getWeightMovementsForPeriodRowSet (conn, storeNum, start, end, sale, processLevelTypeId,isGloablZone);
		return populateList (result, sale);
	}

	// Price Index By Price Zone - Added another argument to differentiate between store and zone
	// Changes made to run SQL with PreparedStatement instead of Statement
	public CachedRowSet getWeightMovementsForPeriodRowSet (Connection conn, String storeNum,
			Date start, Date end, boolean sale, int processLevelTypeId,boolean isGloablZone)
	throws GeneralException
	{
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		//StringBuffer sb = new StringBuffer("select COMP_STR_NO, UPC, sum(PRICE) as PRICE, sum(WEIGHT) as QUANTITY from MOVEMENT_DAILY where");
		// Changes to consider authorized items for store/zone - ITEM_CODE added to query
		StringBuffer sb = new StringBuffer("select UPC, ITEM_CODE, sum(PRICE) as PRICE, sum(WEIGHT) as QUANTITY from MOVEMENT_DAILY where");
		sb.append(" CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= To_DATE(?, 'MM/dd/yyyy')");
		sb.append(" and START_DATE <= To_DATE(?, 'MM/dd/yyyy') AND ROW_TYPE = 'D')");
		char saleFlag = sale ? 'Y' : 'N';
		sb.append(" and SALE_FLAG = ?");	
		sb.append(" and WEIGHT <> 0");	// need to include returns
		if ( storeNum != null ) {
			appendStoresQueryForGivenLocation(processLevelTypeId, sb, isGloablZone);
		}
		//sb.append(" and UPC in ('20809900000', '20567900000', '20173600000', '20209700000')");
		sb.append(" group by UPC, ITEM_CODE");
		sb.append(" order by UPC, ITEM_CODE");
		//sb.append(" group by COMP_STR_NO, UPC");
		//sb.append(" order by COMP_STR_NO, UPC");
	
		String sql = sb.toString();
		logger.debug ("getWeightMovementsForPeriod SQL: " + sql);
		PreparedStatement statement = null;
		ResultSet rs = null;
		CachedRowSet result = null;
		try {
			statement = conn.prepareStatement(sb.toString());
			statement.setString(1, startStr);
			statement.setString(2, endStr);
			statement.setString(3, String.valueOf(saleFlag));
			statement.setString(4, storeNum);
			rs = statement.executeQuery();
			result = PristineDBUtil.getCachedRowSet(rs);
		}catch (SQLException ex) {
			logger.error("Error in getWeightMovementsForPeriod. SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}
			
		return result;
	}

	private List<MovementWeeklyDTO> populateList(CachedRowSet result,
			boolean sale) {
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try {
			while (result.next()) {
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				loadWeeklyDTO(dto, result);
				dto.setSaleFlag(sale);
				list.add(dto);
			}
		} catch (SQLException ex) {
			logger.error("Error:", ex);
		}

		return list;
	}
	
	// Price Index Support By Price Zone - New argument added to differentiate between store and zone
	public List<MovementWeeklyDTO> getVisitWeekly (Connection conn, String storeNum, Date start, Date end, boolean ignoreTransactionNumber, int processLevelTypeId,boolean isGloablZone)
	throws GeneralException
	{
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		CachedRowSet result = getVisitWeeklyRowSet(conn, storeNum, start, end, ignoreTransactionNumber, processLevelTypeId,isGloablZone);
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			while (result.next())
			{
				try
				{
					MovementWeeklyDTO dto = new MovementWeeklyDTO();
					//dto.setItemStore(result.getString("COMP_STR_NO"));
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
 
	// Price Index Support By Price Zone - New argument added to differentiate between store and zone
	// Changes made in function to execute SQL with PreparedStatement instead of Statement
	public CachedRowSet getVisitWeeklyRowSet (Connection conn, String storeNum, Date start, Date end, boolean ignoreTransactionNumber, int processLevelTypeId,boolean isGloablZone)
	throws GeneralException
	{
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		StringBuffer sb = new StringBuffer();
		if (!ignoreTransactionNumber)
			//sb.append("  select COMP_STR_NO, WEEKDAY, UPC, ITEM_CODE, count(distinct transaction_no) as VISIT_COUNT from MOVEMENT_DAILY");
			sb.append("  select UPC, ITEM_CODE, count(distinct transaction_no) as VISIT_COUNT from MOVEMENT_DAILY ");
		else
			//sb.append("  select COMP_STR_NO, WEEKDAY, UPC, ITEM_CODE, 0 as VISIT_COUNT from MOVEMENT_DAILY");
			sb.append("  select UPC, ITEM_CODE, 0 as VISIT_COUNT from MOVEMENT_DAILY");
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		sb.append("   where CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= To_DATE(?, 'MM/dd/yyyy')");
		String endStr = formatter.format(end);
		sb.append(" and START_DATE <= To_DATE(?, 'MM/dd/yyyy')");
		sb.append(" and ROW_TYPE = 'D') ");
		if (!ignoreTransactionNumber)
			sb.append("   and transaction_no > 0");
		if (storeNum != null) {
			appendStoresQueryForGivenLocation(processLevelTypeId, sb,isGloablZone);
		}
		//sb.append("   group by COMP_STR_NO, WEEKDAY, UPC, ITEM_CODE");
		sb.append("   group by UPC, ITEM_CODE");

		String sql = sb.toString();
		logger.debug("getVisitWeekly SQL: " + sql);
		PreparedStatement statement = null;
		ResultSet rs = null;
		CachedRowSet result = null;
		try {
			statement = conn.prepareStatement(sb.toString());
			logger.debug(startStr + "\t" + endStr + "\t" + storeNum);
			statement.setString(1, startStr);
			statement.setString(2, endStr);
			statement.setString(3, storeNum);
			rs = statement.executeQuery();
			result = PristineDBUtil.getCachedRowSet(rs);
		} catch (SQLException ex) {
			logger.error("Error in getVisitWeeklySQL. SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}

		return result;
	}
	
	
	// Price Index Support By Price Zone - New argument added to differentiate between store and zone
	// Changes made in function to execute SQL with PreparedStatement instead of Statement	
	public List<MovementWeeklyDTO> getLIGVisitWeekly (Connection conn, String storeNum, Date start, Date end, boolean ignoreTransactionNumber, int processLevelTypeId, boolean isGloablZone)	
	throws GeneralException
	{
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		
		StringBuffer sb = new StringBuffer();
		//sb.append("select COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE, sum(VISIT_COUNT) as VISIT_COUNT from");
		//sb.append("select RET_LIR_ID, RET_LIR_ITEM_CODE, sum(VISIT_COUNT) as VISIT_COUNT from");
		//sb.append(" (");
		if (!ignoreTransactionNumber)
			//sb.append("  select m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY, count(distinct m.transaction_no) as VISIT_COUNT");
			sb.append("  select i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, count(distinct m.transaction_no) as VISIT_COUNT");
		else
			//sb.append("  select m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY, 0 as VISIT_COUNT");				
			sb.append("  select i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, 0 as VISIT_COUNT");
		
		sb.append("   from MOVEMENT_DAILY m, ITEM_LOOKUP i, retailer_like_item_group l ");
		// sb.append("   where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.ret_lir_id = l.ret_lir_id");
		sb.append("   where m.item_code = i.item_code and i.ret_lir_id = l.ret_lir_id");
		
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		sb.append("   and m.calendar_id in (select calendar_id from retail_calendar where start_date >= To_DATE(?, 'MM/dd/yyyy')");
		String endStr = formatter.format(end);
		sb.append("   and start_date <= To_DATE(?, 'MM/dd/yyyy') and row_type = 'D')");
		if (!ignoreTransactionNumber)
			sb.append("   and m.transaction_no > 0");
		if ( storeNum != null ) {
				/*if(processLevelTypeId == Constants.STORE_LEVEL_TYPE_ID)
					sb.append("   and COMP_STR_NO = ? ");
				else if (processLevelTypeId == Constants.CHAIN_LEVEL_TYPE_ID)
					sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ?) ");
				else
					sb.append(" and COMP_STR_NO IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE RPZ.ZONE_NUM = ? AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID) ");*/
			appendStoresQueryForGivenLocation(processLevelTypeId, sb, isGloablZone);
		}
		//sb.append("   group by m.COMP_STR_NO, i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY");
		sb.append("   group by i.RET_LIR_ID, l.RET_LIR_ITEM_CODE");
		//sb.append(" )");
		//sb.append(" group by COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE");
		//sb.append(" group by RET_LIR_ID, RET_LIR_ITEM_CODE");
		//sb.append(" order by COMP_STR_NO, RET_LIR_ID");
		sb.append(" order by RET_LIR_ID");

		String sql = sb.toString();
		logger.debug ("getLIGVisitWeekly SQL: " + sql);
		PreparedStatement statement = null;
		ResultSet rs = null;
		CachedRowSet result = null;
		try {
			statement = conn.prepareStatement(sb.toString());
			statement.setString(1, startStr);
			statement.setString(2, endStr);
			statement.setString(3, storeNum);
			rs = statement.executeQuery();
			result = PristineDBUtil.getCachedRowSet(rs);
		} catch (SQLException ex) {
			logger.error("Error in getVisitWeeklySQL. SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}
		
		try
		{
			while (result.next())
			{
				try
				{
					MovementWeeklyDTO dto = new MovementWeeklyDTO();
					//dto.setItemStore(result.getString("COMP_STR_NO"));
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

	public void getCostWeekly (Connection conn, MovementWeeklyDTO dto, int processLevelTypeId)
	throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select m.LIST_COST");
		sb.append(",  (case when m.DEAL_COST is null then m.LIST_COST");
		sb.append("   when m.DEAL_COST <= 0 then m.LIST_COST");
		sb.append("   else m.DEAL_COST");
		sb.append("   end) COST");
		sb.append(" from MOVEMENT_WEEKLY m");
		if(processLevelTypeId == Constants.ZONE_LEVEL_TYPE_ID)
			sb.append(" where m.PRICE_ZONE_ID = ?");
		else
			sb.append(" where m.COMP_STR_ID = ?");
		sb.append(" and m.CHECK_DATA_ID = ?");
		sb.append(" and m.ITEM_CODE = ?");
		
		//String sql = sb.toString();
		//logger.debug ("getVisitWeekly SQL: " + sql);
		//CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getRevenueCostWeekly");
		PreparedStatement statement = null;
		ResultSet rs = null;
		try
		{
			statement = conn.prepareStatement(sb.toString());
			if(processLevelTypeId == Constants.ZONE_LEVEL_TYPE_ID)
				statement.setInt(1, dto.getPriceZoneId());
			else
				statement.setInt(1, dto.getCompStoreId());
			statement.setInt(2, dto.getCheckDataId());
			statement.setInt(3, dto.getItemCode());
			rs = statement.executeQuery();
			while (rs.next()) {
				dto.setListCost(rs.getDouble("LIST_COST"));
				dto.setDealCost(rs.getDouble("COST"));
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}
	}

	public void get13WeekData(Connection conn, MovementWeeklyDTO dto,
			String schCSV) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE");
		sb.append(" from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d");
		sb.append(" where d.SCHEDULE_ID in (").append(schCSV).append(")");
		sb.append(" and m.ITEM_CODE = ").append(dto.getItemCode());
		sb.append(" and m.CHECK_DATA_ID = d.CHECK_DATA_ID");
		// logger.debug ("get13WeekData SQL: " + sb.toString());

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"get13WeekData");
		try {
			while (result.next()) {
				double d = dto.getQuantityRegular()
						+ result.getDouble("QUANTITY_REGULAR");
				dto.setQuantityRegular13Week(d);
				d = dto.getQuantitySale() + result.getDouble("QUANTITY_SALE");
				dto.setQuantitySale13Week(d);
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public void get13WeekData(Connection conn, Date lastStartDate,
			MovementWeeklyDTO dto) throws GeneralException {
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

		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') >= '")
				.append(startStr).append("'");
		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') <= '")
				.append(endStr).append("'");

		// logger.debug ("get13WeekData SQL: " + sb.toString());

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"get13WeekData");
		try {
			while (result.next()) {
				double d = dto.getQuantityRegular()
						+ result.getDouble("QUANTITY_REGULAR");
				dto.setQuantityRegular13Week(d);
				d = dto.getQuantitySale() + result.getDouble("QUANTITY_SALE");
				dto.setQuantitySale13Week(d);
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	public void get13WeekLIGData(Connection conn, Date lastStartDate,
			MovementWeeklyDTO dto) throws GeneralException {
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

		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') >= '")
				.append(startStr).append("'");
		sb.append(" and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') <= '")
				.append(endStr).append("'");

		// sb.append(" group by i.ret_lir_id");

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"get13WeekLIGData");
		try {
			while (result.next()) {
				double d = dto.getQuantityRegular()
						+ result.getDouble("QUANTITY_REGULAR");
				dto.setQuantityRegular13Week(d);
				d = dto.getQuantitySale() + result.getDouble("QUANTITY_SALE");
				dto.setQuantitySale13Week(d);
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
	}

	// Price Index By Price Zone - Added a new argument to differentiate between store and zone
	// Changes to execute SQL with PreparedStatement instead of Statement
	public List<MovementWeeklyDTO> getWeeklyMovements (Connection conn, int storeId, int scheduleId)
	throws GeneralException
	{
		Date date;

		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		StringBuffer sb = new StringBuffer();
		sb.append("select m.*, l.ret_lir_id, l.ret_lir_item_code");
		sb.append(",to_char(m.eff_list_cost_date, 'MM/DD/YYYY') as date_eff_list_cost");
		sb.append(",to_char(m.deal_start_date, 'MM/DD/YYYY') as date_deal_start");
		sb.append(",to_char(m.deal_end_date, 'MM/DD/YYYY')as date_deal_end");

		sb.append(" from movement_weekly m, item_lookup i, retailer_like_item_group l, competitive_data_view d");
		sb.append(" where m.check_data_id = d.check_data_id");
		sb.append(" and d.schedule_id = ?");
		sb.append(" and m.item_code = i.item_code and i.ret_lir_id = l.ret_lir_id and i.ret_lir_id is not null");
		sb.append(" order by m.comp_str_id, l.ret_lir_id");

		String sql = sb.toString();
		logger.debug("getWeeklyMovements SQL: " + sql);
		PreparedStatement statement = null;
		ResultSet rs = null;
		CachedRowSet result = null;
		
		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try
		{
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, scheduleId);
			rs = statement.executeQuery();
			result = PristineDBUtil.getCachedRowSet(rs);
			while (result.next())
			{
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				dto.setCompStoreId(result.getInt("COMP_STR_ID"));
				dto.setPriceZoneId(result.getInt("PRICE_ZONE_ID"));
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
				// dto.setEffListCostDate(result.getDate("EFF_LIST_COST_DATE"));

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

	public List<MovementWeeklyDTO> getWeeklyMovementsFor13Week(Connection conn,
			int storeId, Date start, Date end) throws GeneralException {
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("select m.CHECK_DATA_ID, m.ITEM_CODE, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE");
		sb.append(" from movement_weekly m, competitive_data_view d");
		sb.append(" where m.check_data_id = d.check_data_id");
		sb.append(" and to_char(d.start_date, 'MM/DD/YYYY') = '")
				.append(startStr).append("'");
		sb.append(" and m.comp_str_id = ").append(storeId);
		// sb.append(" and m.comp_str_id = 5662 and l.ret_lir_id in (1, 2, 16, 3, 4, 5, 10, 11, 13, 14, 273, 274)");
		// sb.append(" and m.comp_str_id = 5704 and m.ITEM_CODE in (41898,35963,49690,35969,35980,50446,55418,55422,42000)");
		// sb.append(" and l.ret_lir_id in (381, 1)");

		String sql = sb.toString();
		logger.debug("getWeeklyMovementsFor13Week SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getWeeklyMovementsFor13Week");

		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try {
			while (result.next()) {
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				dto.setCompStoreId(storeId);
				dto.setItemCode(result.getInt("ITEM_CODE"));
				dto.setCheckDataId(result.getInt("CHECK_DATA_ID"));

				dto.setRevenueRegular(result.getDouble("REVENUE_REGULAR"));
				dto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				dto.setRevenueSale(result.getDouble("REVENUE_SALE"));
				dto.setQuantitySale(result.getDouble("QUANTITY_SALE"));

				list.add(dto);
				int size = list.size();
				if ((size % 25000) == 0) {
					logger.debug("getWeeklyMovementsFor13Week: processed "
							+ String.valueOf(size));
				}
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return list;
	}

	public List<MovementWeeklyDTO> getWeeklyLIGMovementsFor13Week(
			Connection conn, int storeId, Date start, Date end)
			throws GeneralException {
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("select m.CHECK_DATA_ID, m.ITEM_CODE, i.RET_LIR_ID, m.REVENUE_REGULAR, m.QUANTITY_REGULAR, m.REVENUE_SALE, m.QUANTITY_SALE");
		sb.append(" from movement_weekly_lig m, competitive_data_view_lig d, item_lookup i");
		sb.append(" where m.check_data_id = d.check_data_id");
		sb.append(" and m.item_code = i.item_code");
		sb.append(" and to_char(d.start_date, 'MM/DD/YYYY') = '")
				.append(startStr).append("'");
		sb.append(" and m.comp_str_id = ").append(storeId);
		// sb.append(" and m.comp_str_id = 5662 and l.ret_lir_id in (1, 2, 16, 3, 4, 5, 10, 11, 13, 14, 273, 274)");
		// sb.append(" and m.comp_str_id = 5704 and m.ITEM_CODE in (41898,35963,49690,35969,35980,50446,55418,55422,42000)");
		// sb.append(" and l.ret_lir_id in (381, 1)");

		String sql = sb.toString();
		logger.debug("getWeeklyLIGMovementsFor13Week SQL: " + sql);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getWeeklyLIGMovementsFor13Week");

		List<MovementWeeklyDTO> list = new ArrayList<MovementWeeklyDTO>();
		try {
			while (result.next()) {
				MovementWeeklyDTO dto = new MovementWeeklyDTO();
				dto.setCompStoreId(storeId);
				dto.setItemCode(result.getInt("ITEM_CODE"));
				dto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				dto.setLirId(result.getInt("RET_LIR_ID"));

				dto.setRevenueRegular(result.getDouble("REVENUE_REGULAR"));
				dto.setQuantityRegular(result.getDouble("QUANTITY_REGULAR"));
				dto.setRevenueSale(result.getDouble("REVENUE_SALE"));
				dto.setQuantitySale(result.getDouble("QUANTITY_SALE"));

				list.add(dto);
				int size = list.size();
				if ((size % 25000) == 0) {
					logger.debug("getWeeklyMovementsFor13Week: processed "
							+ String.valueOf(size));
				}
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return list;
	}

	public boolean updateWeeklyMovement(Connection conn,
			MovementWeeklyDTO movement, boolean update) throws GeneralException {
		StringBuffer sb;
		int count = 0;
		if (update) {
			sb = new StringBuffer("update MOVEMENT_WEEKLY set");

			sb.append(" REVENUE_REGULAR = ").append(
					movement.getRevenueRegular());
			sb.append(", QUANTITY_REGULAR = ").append(
					movement.getQuantityRegular());
			sb.append(", REVENUE_SALE = ").append(movement.getRevenueSale());
			sb.append(", QUANTITY_SALE = ").append(movement.getQuantitySale());
			// sb.append(", TOTAL_MARGIN = ").append(movement.getMargin());

			// Added on 18th Oct 2012 for Ahold, to set upper limit for the
			// margin
			// The margin goes beyond field size, which is actually an invalid
			// data
			double margin = movement.getMargin();
			if (margin > 99999.99)
				margin = 99999.99;
			else if (margin < -99999.99)
				margin = -99999.99;

			sb.append(", TOTAL_MARGIN = ").append(margin);

			/*
			 * Margin percent greater than or equal 1000% (on the positive and
			 * negative side) to be stored as 999.99 with appropriate sign
			 */
			double marginPct = movement.getMarginPercent();
			if (marginPct > 999.99)
				marginPct = 999.99;
			else if (marginPct < -999.99)
				marginPct = -999.99;

			sb.append(", MARGIN_PCT = ").append(marginPct);
			sb.append(", VISIT_COUNT = ").append(movement.getVisitCount());
			// sb.append(", QTY_REGULAR_13WK = ").append(movement.getQuantityRegular13Week());
			// sb.append(", QTY_SALE_13WK = ").append(movement.getQuantitySale13Week());

			sb.append(" where CHECK_DATA_ID = ").append(
					movement.getCheckDataId());

			try {
				count = PristineDBUtil.executeUpdate(conn, sb,
						"MovementDAO - UpdateWeekly");
			} catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}

		if (count == 0) {
			// Nothing was updated, insert record...
			sb = new StringBuffer("insert into MOVEMENT_WEEKLY (");

			sb.append("COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID");
			sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE");
			sb.append(", TOTAL_MARGIN, MARGIN_PCT, VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK, PRICE_ZONE_ID");
			
			sb.append(") values (");

			sb.append(movement.getCompStoreId());
			sb.append(", ").append(movement.getItemCode());
			sb.append(", ").append(movement.getCheckDataId());

			sb.append(", ").append(movement.getRevenueRegular());
			sb.append(", ").append(movement.getQuantityRegular());
			sb.append(", ").append(movement.getRevenueSale());
			sb.append(", ").append(movement.getQuantitySale());

			// sb.append(", ").append(movement.getMargin());

			double margin = movement.getMargin();
			if (margin > 99999.99)
				margin = 99999.99;
			else if (margin < -99999.99)
				margin = -99999.99;

			sb.append(", ").append(margin);

			/*
			 * Margin percent greater than or equal 1000% (on the positive and
			 * negative side) to be stored as 999.99 with appropriate sign
			 */
			double marginPct = movement.getMarginPercent();
			if (marginPct > 999.99)
				marginPct = 999.99;
			else if (marginPct < -999.99)
				marginPct = -999.99;

			sb.append(", ").append(marginPct);
			sb.append(", ").append(movement.getVisitCount());
			sb.append(", ").append(movement.getQuantityRegular13Week());
			sb.append(", ").append(movement.getQuantitySale13Week());
			sb.append(", ").append(movement.getPriceZoneId());
			
			sb.append(")");		// close
			
			//String sql = sb.toString();
			try {
				PristineDBUtil.execute(conn, sb, "MovementDAO - InsertWeekly");
			} catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				throw gex;
			}
		}

		return true;
	}

	public boolean updateWeeklyMovement13WeekData(Connection conn,
			MovementWeeklyDTO movement) throws GeneralException {
		int count = 0;

		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY set");
		sb.append(" QTY_REGULAR_13WK = ").append(
				movement.getQuantityRegular13Week());
		sb.append(", QTY_SALE_13WK = ")
				.append(movement.getQuantitySale13Week());
		sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());

		try {
			count = PristineDBUtil.executeUpdate(conn, sb,
					"MovementDAO - UpdateWeekly13WeekData");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}

		return count > 0;
	}

	public boolean updateWeeklyLIGMovement13WeekData(Connection conn,
			MovementWeeklyDTO movement) throws GeneralException {
		int count = 0;

		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set");
		sb.append(" QTY_REGULAR_13WK = ").append(
				movement.getQuantityRegular13Week());
		sb.append(", QTY_SALE_13WK = ")
				.append(movement.getQuantitySale13Week());
		sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());

		try {
			count = PristineDBUtil.executeUpdate(conn, sb,
					"MovementDAO - UpdateWeeklyLIG13WeekData");
		} catch (GeneralException gex) {
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
	
	public int getCheckDataID (Connection conn, int itemCode, int scheduleId)
	throws GeneralException
	{
		/*SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
		String startStr = formatter.format(start);
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT check_data_id FROM competitive_data_view_lig");
		sb.append(" WHERE comp_str_id = ").append(storeId);
		//sb.append(" WHERE comp_str_id = 5720");
		sb.append(" AND item_code = ").append(itemCode);
		sb.append(" AND to_char(start_date, 'MM/DD/YYYY') = '").append(startStr).append("'");*/
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT check_data_id FROM competitive_data_view_lig");
		sb.append(" WHERE schedule_id = ?");
		sb.append(" AND item_code = ?");
		
		PreparedStatement statement = null;
		ResultSet result = null;
		int ret = -1;
		try
		{
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, scheduleId);
			statement.setInt(2, itemCode);
			result = statement.executeQuery();
			while (result.next()) {
				ret = result.getInt("check_data_id");
			}
		}
		catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}

		return ret;
	}

	public boolean insertWeeklyLIGMovement(Connection conn,
			MovementWeeklyDTO movement) throws GeneralException {
		boolean ret = true;

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

		// Try to update LIG entry
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set");
		sb.append(" REVENUE_REGULAR = ").append(movement.getRevenueRegular());
		sb.append(", QUANTITY_REGULAR = ")
				.append(movement.getQuantityRegular());
		sb.append(", REVENUE_SALE = ").append(movement.getRevenueSale());
		sb.append(", QUANTITY_SALE = ").append(movement.getQuantitySale());

		sb.append(", LIST_COST = ").append(movement.getListCost());
		sb.append(", EFF_LIST_COST_DATE = ");
		String dateStr;
		if (movement.getEffListCostDate() != null) {
			dateStr = formatter.format(movement.getEffListCostDate());
			sb.append("To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
		} else {
			sb.append("NULL");
		}

		sb.append(", DEAL_COST = ").append(movement.getDealCost());
		sb.append(", DEAL_START_DATE = ");
		if (movement.getDealStartDate() != null) {
			dateStr = formatter.format(movement.getDealStartDate());
			sb.append("To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
		} else {
			sb.append("NULL");
		}
		sb.append(", DEAL_END_DATE = ");
		if (movement.getDealEndDate() != null) {
			dateStr = formatter.format(movement.getDealEndDate());
			sb.append("To_DATE('").append(dateStr).append("', 'YYYYMMDD')");
		} else {
			sb.append("NULL");
		}

		sb.append(", COST_CHG_DIRECTION = ").append(
				movement.getCostChangeDirection());
		sb.append(", MARGIN_CHG_DIRECTION = ").append(
				movement.getMarginChangeDirection());

		/*
		 * Margin percent greater than or equal 1000% (on the positive and
		 * negative side) to be stored as 999.99 with appropriate sign
		 */
		double marginPct = movement.getMarginPercent();
		if (marginPct > 999.99)
			marginPct = 999.99;
		else if (marginPct < -999.99)
			marginPct = -999.99;

		sb.append(", MARGIN_PCT = ").append(marginPct);
		sb.append(", TOTAL_MARGIN = ").append(movement.getMargin());
		sb.append(", VISIT_COUNT = ").append(movement.getVisitCount());

		sb.append(" where CHECK_DATA_ID = ").append(movement.getCheckDataId());

		// String sql = sb.toString();
		int count = 0;
		try {
			count = PristineDBUtil.executeUpdate(conn, sb,
					"MovementDAO - insertWeeklyLIGMovement: update");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			ret = false;
		}

		int storeId = movement.getCompStoreId();
		int itemCode = movement.getItemCode();
		int checkDataId = movement.getCheckDataId();
		// String logstr = "storeId = " + storeId + ", itemCode = " + itemCode +
		// ", checkDataId = " + checkDataId;
		if (count == 0) {
			// Nothing was updated, insert record...
			sb = new StringBuffer(
					"insert into MOVEMENT_WEEKLY_LIG (COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID");
			sb.append(", REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE");
			sb.append(", LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE");
			sb.append(", COST_CHG_DIRECTION, MARGIN_CHG_DIRECTION, MARGIN_PCT, TOTAL_MARGIN");
			sb.append(", VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK, PRICE_ZONE_ID");
			
			sb.append(") values (");

			sb.append(storeId);
			sb.append(", ").append(itemCode);
			sb.append(", ").append(checkDataId);

			sb.append(", ").append(movement.getRevenueRegular());
			sb.append(", ").append(movement.getQuantityRegular());
			sb.append(", ").append(movement.getRevenueSale());
			sb.append(", ").append(movement.getQuantitySale());

			sb.append(", ").append(movement.getListCost());
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
			sb.append(", ").append(movement.getMarginChangeDirection());

			/*
			 * Margin percent greater than or equal 1000% (on the positive and
			 * negative side) to be stored as 999.99 with appropriate sign
			 */
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
			sb.append(", ").append(movement.getPriceZoneId());
			sb.append(")");		// close
			
			//String sql = sb.toString();
			try {
				PristineDBUtil.execute(conn, sb,
						"MovementDAO - InsertWeeklyLIG");
			} catch (GeneralException gex) {
				logger.error("SQL: " + sb.toString(), gex);
				ret = false;
				// throw gex;
			}

			// logger.debug("Inserted: " + logstr);
		} else {
			// logger.debug("Updated: " + logstr);
		}

		return ret;
	}

	public boolean updateWeeklyLIGMovement(Connection conn,
			MovementWeeklyDTO movement) throws GeneralException {
		boolean ret = true;

		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set");
		sb.append(" VISIT_COUNT = ").append(movement.getVisitCount());
		sb.append(" where COMP_STR_ID = ").append(movement.getCompStoreId());
		sb.append(" and ITEM_CODE = ").append(movement.getItemCode());
		sb.append(" and CHECK_DATA_ID = ").append(movement.getCheckDataId());

		// String sql = sb.toString();
		try {
			PristineDBUtil.execute(conn, sb,
					"MovementDAO - updateWeeklyLIGMovement");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			ret = false;
		}

		return ret;
	}
	
	public MovementWeeklyDTO loadWeeklyDTO (MovementWeeklyDTO dto, CachedRowSet row) throws SQLException
	{
		//dto.setItemStore(row.getString("COMP_STR_NO"));
		dto.setItemUPC(row.getString("UPC"));
		dto.setItemCode(row.getInt("ITEM_CODE")); // Changes to consider authorized items for store/zone
		dto.setTotalPrice(row.getDouble("PRICE"));
		dto.setExtnQty(row.getDouble("QUANTITY"));
		// dto.setExtnWeight(row.getDouble("WEIGHT"));
		return dto;
	}

	private static Date get13WeekFirstStartDate(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, -7 * 12); // 13 weeks
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}

	private static Date getPreviousWeekStartDate(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, -7); // previous week
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}

	/*
	 * ****************************************************************
	 * Function to get the weekly movement data for given schedule Argument 1:
	 * DB connection Argument 2: Schedule ID Return : ArrayList
	 * 
	 * @throws GeneralException, SQLException
	 * ****************************************************************
	 */
	public ArrayList<Movement13WeeklyDTO> getWeeklyMovementDataList(
			Connection conn, String schCSV, String departmentCode,
			String categoryid) throws GeneralException {
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

		logger.info("getWeeklyMovementDataList - SQL : " + sb.toString());
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
				momentDto.setQuantityRegular13Week(result
						.getDouble("QTY_REGULAR_13WK"));
				momentDto.setQuantitySale13Week(result
						.getDouble("QTY_SALE_13WK"));
				listMoment.add(momentDto);

				int size = listMoment.size();
				if ((size % 25000) == 0) {
					logger.debug("getWeeklyMovements: processed "
							+ String.valueOf(size));
				}
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return listMoment;
	}

	/*
	 * ****************************************************************
	 * Function to get the weekly movement data for given schedule Argument 1:
	 * DB connection Argument 2: Schedule ID Return : HashMap
	 * 
	 * @throws GeneralException, SQLException
	 * ****************************************************************
	 */
	public HashMap<Integer, Movement13WeeklyDTO> getWeeklyMovementDataMap(
			Connection conn, String schCSV, String departmentCode,
			String categoryid) throws GeneralException {
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

		logger.info("getWeeklyMovementDataMap - SQL : " + sb.toString());
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
				momentDto.setQuantityRegular13Week(result
						.getDouble("QTY_REGULAR_13WK"));
				momentDto.setQuantitySale13Week(result
						.getDouble("QTY_SALE_13WK"));
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

	/*
	 * ****************************************************************
	 * Function to get the 13 week movement data for an item Argument 1: DB
	 * connection Argument 2: Schedule IDs Argument 3: Item Code Return :
	 * Movement13WeeklyDTO
	 * 
	 * @throws GeneralException, SQLException
	 * ****************************************************************
	 */
	public Movement13WeeklyDTO getWeeklyMovements13WeekData(Connection conn,
			String scheduleIDs, int itemCode) throws GeneralException {

		Movement13WeeklyDTO movementWeeklyDTO = new Movement13WeeklyDTO();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT sum(NVL(m.QUANTITY_REGULAR, 0)) as QUANTITY_REGULAR,");
		sb.append(" sum(NVL(m.QUANTITY_SALE,0)) as QUANTITY_SALE,");
		sb.append(" count(m.CHECK_DATA_ID) as REC_COUNT");
		sb.append(" FROM COMPETITIVE_DATA c, MOVEMENT_WEEKLY m");
		sb.append(" WHERE c.SCHEDULE_ID in (").append(scheduleIDs);
		sb.append(" ) AND c.CHECK_DATA_ID = m.CHECK_DATA_ID");
		sb.append(" AND m.ITEM_CODE = ").append(itemCode);

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getWeeklyMovementsFor13Week");

		try {
			movementWeeklyDTO.setItemCode(itemCode);
			while (result.next()) {
				movementWeeklyDTO.setQuantityRegular13Week(result
						.getDouble("QUANTITY_REGULAR"));
				movementWeeklyDTO.setQuantitySale13Week(result
						.getDouble("QUANTITY_SALE"));
				movementWeeklyDTO.setWeek13RecordCount(result
						.getDouble("REC_COUNT"));
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return movementWeeklyDTO;
	}

	/*
	 * ****************************************************************
	 * Function to get the weekly movement data for LIG Argument 1: DB
	 * connection Argument 2: Schedule ID Return : ArrayList
	 * 
	 * @throws GeneralException, SQLException
	 * ****************************************************************
	 */
	public ArrayList<Movement13WeeklyDTO> getMovement13WeeklyDataForLIG(
			Connection conn, String schCSV, String departmentCode,
			String categoryId) throws GeneralException {
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
			sb.append(" AND i.DEPT_ID NOT IN (").append(departmentCode)
					.append(")");
		}

		if (!categoryId.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (").append(categoryId)
					.append(")");
		}

		sb.append(" ORDER BY i.RET_LIR_ID");

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getWeeklyMovements");

		try {
			while (result.next()) {
				Movement13WeeklyDTO movementDto = new Movement13WeeklyDTO();
				movementDto.setCompStoreId(result.getInt("COMP_STR_ID"));
				movementDto.setItemCode(result.getInt("ITEM_CODE"));
				movementDto.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				movementDto.setRetLIRId(result.getInt("RET_LIR_ID"));
				movementDto.setRetLIRItemCode(result
						.getInt("RET_LIR_ITEM_CODE"));
				movementDto.setQuantityRegular13Week(result
						.getDouble("QTY_REGULAR_13WK"));
				movementDto.setQuantitySale13Week(result
						.getDouble("QTY_SALE_13WK"));
				listMovement.add(movementDto);
				// logger.debug("Item Code for Week 1 " +
				// result.getInt("ITEM_CODE"));
				int size = listMovement.size();
				if ((size % 25000) == 0) {
					logger.debug("getWeeklyMovements: processed "
							+ String.valueOf(size));
				}
			}
		} catch (SQLException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}

		return listMovement;
	}

	/*
	 * ****************************************************************
	 * Function to set movement 13 week data to 0 for the current week Argument
	 * 1: DB connection Argument 2: Schedule ID Return : boolean
	 * 
	 * @throws GeneralException, SQLException
	 * ****************************************************************
	 */
	public boolean updateWeeklyMovement13WeekInitialData(Connection conn,
			String scheduleID, String targetTableName, String departmentCode,
			String categoryid) throws GeneralException {
		int count = 0;

		StringBuffer sb = new StringBuffer("UPDATE ").append(targetTableName);
		sb.append(" SET");
		sb.append(" QTY_REGULAR_13WK = 0, QTY_SALE_13WK = 0");
		sb.append(" WHERE CHECK_DATA_ID IN");
		sb.append(" (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA c");

		// This is application only if Department code or category id are not
		// empty
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , ITEM_LOOKUP i ");
		}

		sb.append(" WHERE c.SCHEDULE_ID = ").append(scheduleID);

		// If department code has some value, value would be multiple separated
		// with comma
		if (!departmentCode.isEmpty()) {
			sb.append(" AND i.DEPT_ID NOT IN (").append(departmentCode)
					.append(")");
		}

		// If category id has some value, value would be multiple separated with
		// comma
		if (!categoryid.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (").append(categoryid)
					.append(")");
		}

		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" AND c.ITEM_CODE = i.ITEM_CODE ");
		}

		sb.append(" )");

		try {
			count = PristineDBUtil.executeUpdate(conn, sb,
					"MovementDAO - UpdateWeekly13WeekData");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}

		return count > 0;
	}

	/*
	 * ****************************************************************
	 * Function update the movement 13 week data Update the can be 13 week
	 * movement count or null Argument 1: DB connection Argument 2: movement 13
	 * week data (DTO) Return: boolean
	 * 
	 * @throws GeneralException, SQLException
	 * ****************************************************************
	 */
	public boolean updateMovement13WeekData(Connection conn,
			Movement13WeeklyDTO movement13Dto, String targetTableName)
			throws GeneralException {
		int count = 0;

		StringBuffer sb = new StringBuffer("UPDATE ").append(targetTableName)
				.append(" SET");

		// Check for the update type
		if (movement13Dto.getQuantityRegular13Week() == -99) {
			sb.append(" QTY_REGULAR_13WK = null");
		} else {
			sb.append(" QTY_REGULAR_13WK = ").append(
					movement13Dto.getQuantityRegular13Week());
		}

		// Check for the update type
		if (movement13Dto.getQuantitySale13Week() == -99) {
			sb.append(", QTY_SALE_13WK = null");
		} else {
			sb.append(", QTY_SALE_13WK = ").append(
					movement13Dto.getQuantitySale13Week());
		}

		sb.append(" where CHECK_DATA_ID = ").append(
				movement13Dto.getCheckDataId());

		try {
			count = PristineDBUtil.executeUpdate(conn, sb,
					"MovementDAO - UpdateWeekly13WeekData");
		} catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw gex;
		}

		return count > 0;
	}

	/*
	 * ****************************************************************
	 * Method used to get the Com_store_no,Visitor count Argument 1: Connection
	 * Argument 2: Store Number Argument 3: Calendar Id Argument 4: specialDTO
	 * Argument 5: Categories to be excluded return :List<SummaryDailyDTO>
	 * 
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */

	public HashMap<String, Integer> VisitorSummary(Connection _Conn,
			String storeNumber, int calendar_id, int productLevelId,
			String categoryIdStr) throws GeneralException {

		// logger.info("Visitorsummary begin");

		HashMap<String, Integer> returnMap = new HashMap<String, Integer>();

		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		
		String product = "";
		// chekc the product id for visit count
		if (productLevelId == Constants.SUBCATEGORYLEVELID)
			product = "I.SUB_CATEGORY_ID";
		else if (productLevelId == Constants.CATEGORYLEVELID)
			product = "I.CATEGORY_ID";
		else if (productLevelId == Constants.DEPARTMENTLEVELID)
			product = "I.DEPT_ID";
		else if (productLevelId == Constants.POSDEPARTMENT)
			product = "M.POS_DEPARTMENT";
		else if (productLevelId == Constants.LOCATIONLEVELID)
			product = "M.COMP_STR_NO";
		else
			product = "PGR.PRODUCT_ID";

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select ").append(product);
			sql.append(" ,COUNT(DISTINCT M.TRANSACTION_NO) AS VISIT_COUNT");
			sql.append(" from MOVEMENT_DAILY M");

			// get the finance department Visit Count
			if (productLevelId == Constants.FINANCEDEPARTMENT) {
				sql.append(" Inner join PRODUCT_GROUP_RELATION_T PGR on PGR.CHILD_PRODUCT_ID = M.POS_DEPARTMENT");
			} else {
				sql.append(" Inner Join ITEM_LOOKUP I on  M.ITEM_CODE= I.ITEM_CODE");
			}

			// get the portfolio Visit Count
			if (productLevelId == Constants.PORTFOLIO) {
				sql.append(" inner join PRODUCT_GROUP_RELATION_T PGR on PGR.CHILD_PRODUCT_ID = I.CATEGORY_ID");
			}

			sql.append(" where  CALENDAR_ID=").append(calendar_id);
			sql.append(" and  M.COMP_STR_NO='").append(storeNumber).append("'");

			if (productLevelId == Constants.FINANCEDEPARTMENT) {
				/*
				 * sql.append(" and PGR.PRODUCT_LEVEL_ID=").append(productLevelId
				 * );
				 */
				sql.append(" and M.POS_DEPARTMENT <= " + maxPOS);
			} else if (productLevelId == Constants.PORTFOLIO) {
				/*
				 * sql.append(" and PGR.PRODUCT_LEVEL_ID=").append(productLevelId
				 * );
				 */
				sql.append(" and ( M.POS_DEPARTMENT <=" + maxPOS + " and M.POS_DEPARTMENT <> "+ Constants.POS_GASDEPARTMENT + " )");
			} else {
				sql.append(" and ( M.POS_DEPARTMENT <=" + maxPOS + " and M.POS_DEPARTMENT <> "+ Constants.POS_GASDEPARTMENT + " )");
			}

			if (productLevelId != Constants.FINANCEDEPARTMENT
					&& categoryIdStr != null && !categoryIdStr.isEmpty()) {
				sql.append(" AND I.CATEGORY_ID NOT IN (").append(categoryIdStr)
						.append(")");
			}

			sql.append("  and TRANSACTION_NO > 0");
			/*
			 * if (specialDTO.getExcludeDepartments() != null) {
			 * sql.append("  and ( I.DEPT_ID not in (");
			 * sql.append(specialDTO.getExcludeDepartments());
			 * sql.append("   )"); if (specialDTO.getIncludeItems() != null) {
			 * sql.append(" or I.UPC in (");
			 * sql.append(specialDTO.getIncludeItems()); sql.append(")"); }
			 * sql.append("   )"); }
			 */

			sql.append(" group by ").append(product);

			logger.debug("GetVisitorSummary SQL: " + sql.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,
					"GetVisitorSummary");

			// logger.info("Total Store count " + result.size());

			while (result.next()) {

				returnMap
						.put(result.getString(1), result.getInt("VISIT_COUNT"));
			}

			result.close();

		} catch (SQLException sql) {
			logger.error("Error While Fetching VisitSummary ", sql);
			throw new GeneralException("Error While Fetching VisitSummary", sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching VisitSummary ", e);
			throw new GeneralException("Error While Fetching VisitSummary", e);
		}

		return returnMap;
	}

	public HashMap<Double, Integer> VisitorSummarybyItem(Connection _Conn,
			String storeNumber, int calendar_id) throws GeneralException {

		HashMap<Double, Integer> returnMap = new HashMap<Double, Integer>();

		try {

			StringBuffer sql = new StringBuffer();
			sql.append("select ITEM_CODE, count(*) as VISIT_COUNT from");
			sql.append(" (select DISTINCT TRANSACTION_NO, ITEM_CODE from");
			sql.append(" movement_daily where CALENDAR_ID = ");
			sql.append(calendar_id);
			sql.append(" and COMP_STR_NO = '").append(storeNumber).append("'");
			sql.append(" and ITEM_CODE>0 ORDER BY TRANSACTION_NO, ITEM_CODE)");
			sql.append(" group by ITEM_CODE order by ITEM_CODE");

			logger.debug("VisitorSummarybyItem SQL: " + sql.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,
					"VisitorSummarybyItem");

			while (result.next()) {

				returnMap.put(result.getDouble("ITEM_CODE"),
						result.getInt("VISIT_COUNT"));
			}

			result.close();

		} catch (SQLException sql) {
			logger.error("Error While Fetching VisitSummary ", sql);
			throw new GeneralException("Error While Fetching VisitSummary", sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching VisitSummary ", e);
			throw new GeneralException("Error While Fetching VisitSummary", e);
		}

		return returnMap;
	}

	/*
	 * Method used to get the Movement data for daily aggregation. Argument 1 :
	 * _Conn Argument 2 : storeNumber Argument 3 : calendar_id Argument 4 :
	 * specialDTO
	 * 
	 * @throws GeneralException
	 */

	// Added categoryIdStr parameter to exclude specific categories during
	// revenue calculation
	public List<MovementDailyAggregateDTO> getMovementDailyData(
			Connection _Conn, String storeNumber, int calendar_id,
			String categoryIdStr) throws GeneralException {

		// logger.info(" Daily Summary Process Dao Begins  ");

		List<MovementDailyAggregateDTO> movementDataList = new ArrayList<MovementDailyAggregateDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select I.SUB_CATEGORY_ID,I.CATEGORY_ID,I.DEPT_ID, MV.ITEM_CODE, MV.SALE_FLAG as FLAG");

		// add uom_id
		sql.append(" ,I.UOM_ID, MV.POS_DEPARTMENT");

		// add the movement by volume
		// multiplay the quantity * item size.
		sql.append(" ,sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(" ,sum(MV.QUANTITY) as QUANTITY");

		// Actual weight to Margin calculation - Britto 10/03/2012
		sql.append(" ,sum(MV.WEIGHT) as WEIGHT_ACTUAL");

		sql.append(" ,count(case when MV.WEIGHT=0  then null  else 1  end) as WEIGHT");
		sql.append(" ,sum( MV.PRICE) as REVENUE");

		// code added for gas revenue
		sql.append(" ,sum(MV.EXTENDED_GROSS_PRICE) as GROSSREVENUE");
		sql.append(" from MOVEMENT_DAILY MV Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE");
		sql.append(" where  MV.CALENDAR_ID =").append(calendar_id);
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");


		//POS department change begins -- Britto 07/04/2013
		//POS department changeMax POS depart id is changed from 37 to 39
		// add the new conidition pos_department < =37 
		//sql.append(" and MV.POS_DEPARTMENT <=").append(
		//		Constants.POS_GASDEPARTMENT);
		
		//New code to support any level of POS based on configuration
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
		
		if (Integer.parseInt(maxPOS) > 0)
			sql.append(" and MV.POS_DEPARTMENT <= ").append(maxPOS);
		//POS department change ends -- Britto 07/04/2013

		
		
		// Exclude categories for revenue calculation
		if (categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" and (I.CATEGORY_ID NOT IN (").append(categoryIdStr)
					.append(") OR I.CATEGORY_ID IS NULL)");

		sql.append(" group by DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID, MV.ITEM_CODE,MV.SALE_FLAG,I.UOM_ID,MV.POS_DEPARTMENT");
		sql.append(" order by DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID, MV.ITEM_CODE, MV.SALE_FLAG");

		logger.debug("GetMovementDailyData SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
					"getMovementDailyData");

			if (rst.size() > 0) {
				while (rst.next()) {

					MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
					objMovementDto.set_subcategoryid(rst
							.getString("SUB_CATEGORY_ID"));
					objMovementDto.set_categoryid(rst.getString("CATEGORY_ID"));
					objMovementDto.set_departmentid(rst.getString("DEPT_ID"));

					objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
					/* movement.setupc(rst.getString("UPC")); */
					objMovementDto.setFlag(rst.getString("FLAG"));
					objMovementDto.setuomId(rst.getString("UOM_ID"));
					objMovementDto.setPosDepartment(rst
							.getInt("POS_DEPARTMENT"));

					if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
						objMovementDto.set_regularQuantity(rst
								.getDouble("QUANTITY")
								+ rst.getDouble("WEIGHT"));
						objMovementDto.setActualWeight(rst
								.getDouble("WEIGHT_ACTUAL"));
						objMovementDto.setActualQuantity(rst
								.getDouble("QUANTITY"));
						objMovementDto.setregMovementVolume(rst
								.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
						objMovementDto.setRevenueRegular(rst
								.getDouble("REVENUE"));
						objMovementDto.setregGrossRevenue(rst
								.getDouble("GROSSREVENUE"));
					}
					if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
						objMovementDto.set_saleQuantity(rst
								.getDouble("QUANTITY")
								+ rst.getDouble("WEIGHT"));
						objMovementDto.setActualWeight(rst
								.getDouble("WEIGHT_ACTUAL"));
						objMovementDto.setActualQuantity(rst
								.getDouble("QUANTITY"));
						objMovementDto.setsaleMovementVolume(rst
								.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
						objMovementDto.setRevenueSale(rst.getDouble("REVENUE"));
						objMovementDto.setsaleGrossRevenue(rst
								.getDouble("GROSSREVENUE"));
					}
					movementDataList.add(objMovementDto);
				}
			} else {
				logger.warn("There is no movement data");
			}

			/*
			 * logger.info(" Daily Summary Process Count " +
			 * movementDataList.size());
			 */

			rst.close();

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}

		return movementDataList;
	}
	
	

	public List<ProductMetricsDataDTO> getMovementForItemSearch(
			Connection _Conn, int storeId, int calendar_id, 
			double itemC, String categoryIdStr)
			throws GeneralException {

		List<ProductMetricsDataDTO> movementDataList = new ArrayList<ProductMetricsDataDTO>();

		StringBuffer sql = new StringBuffer();
		
		/* SQL:
		 * select MV.ITEM_CODE
		, count(case when MV.SALE_FLAG = 'Y' then 1 else null end) SALE_FLAG_COUNT 
		, sum(case when MV.SALE_FLAG = 'N'  then MV.QUANTITY * I.ITEM_SIZE else 0 end) as REG_VOLUME
		, sum(case when MV.SALE_FLAG = 'Y'  then MV.QUANTITY * I.ITEM_SIZE else 0 end) as SALE_VOLUME
		, sum(case when MV.SALE_FLAG = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY
		, sum(case when MV.SALE_FLAG = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY
		, sum(case when MV.SALE_FLAG = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL
		, sum(case when MV.SALE_FLAG = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL
		, count(case when MV.SALE_FLAG = 'N'  and  MV.WEIGHT<>0  then 1 else null end ) as REG_WEIGHT
		, count(case when MV.SALE_FLAG = 'Y'  and MV.WEIGHT<>0  then 1 else null end) as SALE_WEIGHT
		, sum( case when MV.SALE_FLAG = 'N'  then MV.PRICE else 0 end) as REG_REVENUE
		, sum( case when MV.SALE_FLAG = 'Y'  then MV.PRICE else 0 end) as SALE_REVENUE
		 from MOVEMENT_DAILY MV
		Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE
		where  MV.CALENDAR_ID =<calid>
		and COMP_STR_NO=<storeno>
		and MV.POS_DEPARTMENT <= 37
       and MV.ITEM_CODE=<itemcode>
   		group by MV.ITEM_CODE

		 * 
		 */

		
		/*
		 OLD CODE
		 		sql.append("select MV.ITEM_CODE		, " +
				"count(case when MV.SALE_FLAG = 'Y' then 1 else null end) SALE_FLAG_COUNT, "+
				"sum(case when MV.SALE_FLAG = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY		, " +
				"sum(case when MV.SALE_FLAG = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY		, " +
				"sum(case when MV.SALE_FLAG = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL		, " +
				"sum(case when MV.SALE_FLAG = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL		, " +
				"count(case when MV.SALE_FLAG = 'N'  and  MV.WEIGHT<>0  then 1 else null end ) as REG_WEIGHT		, " +
				"count(case when MV.SALE_FLAG = 'Y'  and MV.WEIGHT<>0  then 1 else null end) as SALE_WEIGHT		, " +
				"sum( case when MV.SALE_FLAG = 'N'  then MV.PRICE else 0 end) as REG_REVENUE		, " +
				"sum( case when MV.SALE_FLAG = 'Y'  then MV.PRICE else 0 end) as SALE_REVENUE		 " +
				"from MOVEMENT_DAILY MV		" +
				"where  MV.CALENDAR_ID =" + calendar_id +
				"and COMP_STR_NO='"+storeNumber+"'" +
				"and MV.POS_DEPARTMENT <= 37       ") ;
		if(itemC != -1){
					sql.append(" and MV.ITEM_CODE=").append(itemC).append("");
				}
				sql.append("group by MV.ITEM_CODE");

		  
		  
		 */
		
		/* NEW SQL
		 * 
		 *
		 *
						select MV.ITEM_ID  ITEM_CODE		,
				count(case when MV.SALE_TYPE = 'Y' then 1 else null end) SALE_FLAG_COUNT,
				sum(case when MV.SALE_TYPE = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY		, 
				sum(case when MV.SALE_TYPE = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY		, 
				sum(case when MV.SALE_TYPE = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL		, 
				sum(case when MV.SALE_TYPE = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL		, 
				count(case when MV.SALE_TYPE = 'N'  and  MV.WEIGHT<>0  then 1 else null end ) as REG_WEIGHT		, 
				count(case when MV.SALE_TYPE = 'Y'  and MV.WEIGHT<>0  then 1 else null end) as SALE_WEIGHT		, 
				sum( case when MV.SALE_TYPE = 'N'  then MV.net_amt else 0 end) as REG_REVENUE		, 
				sum( case when MV.SALE_TYPE = 'Y'  then MV.net_amt else 0 end) as SALE_REVENUE		 
				from transaction_log MV

				where  MV.CALENDAR_ID =3557
				and store_id = 5651
        
				and mv.POS_DEPARTMENT_id <= 37       
				group by MV.ITEM_ID

		
		
		
		 * 
		 */
		
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		
		sql.append("select count(distinct trx_no) TRX_COUNT, MV.ITEM_ID ITEM_CODE			, " +
				"count(case when MV.SALE_TYPE = 'Y' then 1 else null end) SALE_FLAG_COUNT, "+
				"sum(case when MV.SALE_TYPE = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY		, " +
				"sum(case when MV.SALE_TYPE = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY		, " +
				"sum(case when MV.SALE_TYPE = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL		, " +
				"sum(case when MV.SALE_TYPE = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL		, " +
				"sum(case when MV.SALE_TYPE = 'N'  and  MV.WEIGHT<>0  then MV.WEIGHT else null end ) as REG_WEIGHT		, " +
				"sum(case when MV.SALE_TYPE = 'Y'  and MV.WEIGHT<>0  then MV.WEIGHT else null end) as SALE_WEIGHT		, " +
				"sum( case when MV.SALE_TYPE = 'N'  then MV.net_amt else 0 end) as REG_REVENUE		, " +
				"sum( case when MV.SALE_TYPE = 'Y'  then MV.net_amt else 0 end) as SALE_REVENUE		 " +
				"from TRANSACTION_LOG MV		");
				
		//Exclude Category
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" join ITEM_LOOKUP I on MV.ITEM_ID = I.ITEM_CODE " +
			" and (I.CATEGORY_ID not in (" + categoryIdStr + ") or I.CATEGORY_ID is null)");
				
		sql.append("where  MV.CALENDAR_ID = ? "+
				" and STORE_ID= ? " +
				" and MV.POS_DEPARTMENT_ID <= " + maxPOS) ;
		if(itemC != -1){
					sql.append(" and MV.ITEM_ID=").append(itemC).append("");
				}
				sql.append("group by MV.ITEM_ID");

		logger.debug("getMovementForItemSearch SQL:" + sql.toString());
		PreparedStatement statement = null;
		ResultSet rst = null;
		try {
			statement = _Conn.prepareStatement(sql.toString());
			statement.setInt(1, calendar_id);
			statement.setInt(2, storeId);
			rst = statement.executeQuery();

			while (rst.next()) {
				ProductMetricsDataDTO objMovementDto = new ProductMetricsDataDTO(); 
				objMovementDto.setProductId((int) rst.getDouble("ITEM_CODE"));
					
					// Setting the Sale Flag
				
				//Commented by Pradeep on 08/10/2015 for avoiding confusion. 
				//Sale price is set from RETAIL_PRICE_INFO, So eleminated setting SALE_FLAG from here.
			/*	if(rst.getDouble("SALE_FLAG_COUNT")>0){
					objMovementDto.setSaleFlag("Y");
				}else{
					objMovementDto.setSaleFlag("N");
				}*/
					
				// Setting Regular data
				objMovementDto.setRegularMovement(rst.getDouble("REG_QUANTITY") + rst.getDouble("REG_WEIGHT"));
				objMovementDto.setRegularWeight(rst.getDouble("REG_WEIGHT_ACTUAL"));
				objMovementDto.setRegularQuantity(rst.getDouble("REG_QUANTITY"));
				objMovementDto.setRegularRevenue(rst.getDouble("REG_REVENUE"));
					
				// Setting Sale Data
				objMovementDto.setSaleMovement(rst.getDouble("SALE_QUANTITY") + rst.getDouble("SALE_WEIGHT"));
				objMovementDto.setSaleWeight(rst.getDouble("SALE_WEIGHT_ACTUAL"));
				objMovementDto.setSaleQuantity(rst.getDouble("SALE_QUANTITY"));
				objMovementDto.setSaleRevenue(rst.getDouble("SALE_REVENUE"));
				objMovementDto.setTotalVisits(rst.getDouble("TRX_COUNT"));
				objMovementDto.setAvgOrderSize(objMovementDto.getRegularRevenue() + objMovementDto.getSaleRevenue() / objMovementDto.getTotalVisits());

				movementDataList.add(objMovementDto);
			}

			logger.debug("Total Items:" + movementDataList.size());
			
			if(movementDataList.size() == 0)
				logger.warn("No movement data found");

		} catch (Exception exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}finally{
			PristineDBUtil.close(rst);
			PristineDBUtil.close(statement);
		}

		return movementDataList;
	}
	
	/**
	 * 
	 * @param _Conn
	 * @param storeId
	 * @param calendar_id
	 * @param itemC
	 * @param categoryIdStr
	 * @return
	 * @throws GeneralException
	 */
	
	public List<ProductMetricsDataDTO> getWeeklyMovementForItemSearch(
			Connection _Conn, int storeId, String weekCalIds, 
			double itemC, String categoryIdStr)
			throws GeneralException {

		List<ProductMetricsDataDTO> movementDataList = new ArrayList<ProductMetricsDataDTO>();

		StringBuffer sql = new StringBuffer();
		
		/* SQL:
		 * select MV.ITEM_CODE
		, count(case when MV.SALE_FLAG = 'Y' then 1 else null end) SALE_FLAG_COUNT 
		, sum(case when MV.SALE_FLAG = 'N'  then MV.QUANTITY * I.ITEM_SIZE else 0 end) as REG_VOLUME
		, sum(case when MV.SALE_FLAG = 'Y'  then MV.QUANTITY * I.ITEM_SIZE else 0 end) as SALE_VOLUME
		, sum(case when MV.SALE_FLAG = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY
		, sum(case when MV.SALE_FLAG = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY
		, sum(case when MV.SALE_FLAG = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL
		, sum(case when MV.SALE_FLAG = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL
		, count(case when MV.SALE_FLAG = 'N'  and  MV.WEIGHT<>0  then 1 else null end ) as REG_WEIGHT
		, count(case when MV.SALE_FLAG = 'Y'  and MV.WEIGHT<>0  then 1 else null end) as SALE_WEIGHT
		, sum( case when MV.SALE_FLAG = 'N'  then MV.PRICE else 0 end) as REG_REVENUE
		, sum( case when MV.SALE_FLAG = 'Y'  then MV.PRICE else 0 end) as SALE_REVENUE
		 from MOVEMENT_DAILY MV
		Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE
		where  MV.CALENDAR_ID =<calid>
		and COMP_STR_NO=<storeno>
		and MV.POS_DEPARTMENT <= 37
       and MV.ITEM_CODE=<itemcode>
   		group by MV.ITEM_CODE

		 * 
		 */

		
		/*
		 OLD CODE
		 		sql.append("select MV.ITEM_CODE		, " +
				"count(case when MV.SALE_FLAG = 'Y' then 1 else null end) SALE_FLAG_COUNT, "+
				"sum(case when MV.SALE_FLAG = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY		, " +
				"sum(case when MV.SALE_FLAG = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY		, " +
				"sum(case when MV.SALE_FLAG = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL		, " +
				"sum(case when MV.SALE_FLAG = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL		, " +
				"count(case when MV.SALE_FLAG = 'N'  and  MV.WEIGHT<>0  then 1 else null end ) as REG_WEIGHT		, " +
				"count(case when MV.SALE_FLAG = 'Y'  and MV.WEIGHT<>0  then 1 else null end) as SALE_WEIGHT		, " +
				"sum( case when MV.SALE_FLAG = 'N'  then MV.PRICE else 0 end) as REG_REVENUE		, " +
				"sum( case when MV.SALE_FLAG = 'Y'  then MV.PRICE else 0 end) as SALE_REVENUE		 " +
				"from MOVEMENT_DAILY MV		" +
				"where  MV.CALENDAR_ID =" + calendar_id +
				"and COMP_STR_NO='"+storeNumber+"'" +
				"and MV.POS_DEPARTMENT <= 37       ") ;
		if(itemC != -1){
					sql.append(" and MV.ITEM_CODE=").append(itemC).append("");
				}
				sql.append("group by MV.ITEM_CODE");

		  
		  
		 */
		
		/* NEW SQL
		 * 
		 *
		 *
						select MV.ITEM_ID  ITEM_CODE		,
				count(case when MV.SALE_TYPE = 'Y' then 1 else null end) SALE_FLAG_COUNT,
				sum(case when MV.SALE_TYPE = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY		, 
				sum(case when MV.SALE_TYPE = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY		, 
				sum(case when MV.SALE_TYPE = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL		, 
				sum(case when MV.SALE_TYPE = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL		, 
				count(case when MV.SALE_TYPE = 'N'  and  MV.WEIGHT<>0  then 1 else null end ) as REG_WEIGHT		, 
				count(case when MV.SALE_TYPE = 'Y'  and MV.WEIGHT<>0  then 1 else null end) as SALE_WEIGHT		, 
				sum( case when MV.SALE_TYPE = 'N'  then MV.net_amt else 0 end) as REG_REVENUE		, 
				sum( case when MV.SALE_TYPE = 'Y'  then MV.net_amt else 0 end) as SALE_REVENUE		 
				from transaction_log MV

				where  MV.CALENDAR_ID =3557
				and store_id = 5651
        
				and mv.POS_DEPARTMENT_id <= 37       
				group by MV.ITEM_ID

		
		
		
		 * 
		 */
		
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		String minPOS = PropertyManager.getProperty("SALES_ANALYSIS.MIN_REV_POS_DEPARTMENT", "0");
		
		sql.append("select count(distinct trx_no) TRX_COUNT, MV.ITEM_ID ITEM_CODE			, " +
				"count(case when MV.SALE_TYPE = 'Y' then 1 else null end) SALE_FLAG_COUNT, "+
				"sum(case when MV.SALE_TYPE = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY		, " +
				"sum(case when MV.SALE_TYPE = 'Y'  then MV.QUANTITY else 0 end) as SALE_QUANTITY		, " +
				"sum(case when MV.SALE_TYPE = 'N'  then MV.WEIGHT else 0 end) as REG_WEIGHT_ACTUAL		, " +
				"sum(case when MV.SALE_TYPE = 'Y'  then MV.WEIGHT else 0 end) as SALE_WEIGHT_ACTUAL		, " +
				"sum(case when MV.SALE_TYPE = 'N'  and  MV.WEIGHT<>0  then MV.WEIGHT else null end ) as REG_WEIGHT		, " +
				"sum(case when MV.SALE_TYPE = 'Y'  and MV.WEIGHT<>0  then MV.WEIGHT else null end) as SALE_WEIGHT		, " +
				"sum( case when MV.SALE_TYPE = 'N'  then MV.net_amt else 0 end) as REG_REVENUE		, " +
				"sum( case when MV.SALE_TYPE = 'Y'  then MV.net_amt else 0 end) as SALE_REVENUE	FROM	 ");
		
		
		String tableName = PropertyManager.getProperty("T_LOG_SOURCE_TABLE", "TRANSACTION_LOG");
		sql.append(tableName).append(" MV ");
				
		//Exclude Category
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" join ITEM_LOOKUP I on MV.ITEM_ID = I.ITEM_CODE " +
			" and (I.CATEGORY_ID not in (" + categoryIdStr + ") or I.CATEGORY_ID is null)");
				
		sql.append("where  MV.CALENDAR_ID IN ( "+ weekCalIds + ") " +
				" and STORE_ID= ? " +
				" and MV.POS_DEPARTMENT_ID <= " + maxPOS) ;
		
		sql.append(" and MV.POS_DEPARTMENT_ID >= " + minPOS);
		if(itemC != -1){
					sql.append(" and MV.ITEM_ID=").append(itemC).append("");
				}
				sql.append("group by MV.ITEM_ID");

		logger.debug("getMovementForItemSearch SQL:" + sql.toString());
		PreparedStatement statement = null;
		ResultSet rst = null;
		try {
			statement = _Conn.prepareStatement(sql.toString());
			//statement.setString(1, weekCalIds);
			statement.setInt(1, storeId);
			rst = statement.executeQuery();

			while (rst.next()) {
				ProductMetricsDataDTO objMovementDto = new ProductMetricsDataDTO(); 
				objMovementDto.setProductId((int) rst.getDouble("ITEM_CODE"));
					
					// Setting the Sale Flag
				
				//Commented by Pradeep on 08/10/2015 for avoiding confusion. 
				//Sale price is set from RETAIL_PRICE_INFO, So eleminated setting SALE_FLAG from here.
			/*	if(rst.getDouble("SALE_FLAG_COUNT")>0){
					objMovementDto.setSaleFlag("Y");
				}else{
					objMovementDto.setSaleFlag("N");
				}*/
					
				// Setting Regular data
				objMovementDto.setRegularMovement(rst.getDouble("REG_QUANTITY") + rst.getDouble("REG_WEIGHT"));
				objMovementDto.setRegularWeight(rst.getDouble("REG_WEIGHT_ACTUAL"));
				objMovementDto.setRegularQuantity(rst.getDouble("REG_QUANTITY"));
				objMovementDto.setRegularRevenue(rst.getDouble("REG_REVENUE"));
					
				// Setting Sale Data
				objMovementDto.setSaleMovement(rst.getDouble("SALE_QUANTITY") + rst.getDouble("SALE_WEIGHT"));
				objMovementDto.setSaleWeight(rst.getDouble("SALE_WEIGHT_ACTUAL"));
				objMovementDto.setSaleQuantity(rst.getDouble("SALE_QUANTITY"));
				objMovementDto.setSaleRevenue(rst.getDouble("SALE_REVENUE"));
				objMovementDto.setTotalVisits(rst.getDouble("TRX_COUNT"));
				objMovementDto.setAvgOrderSize((objMovementDto.getRegularRevenue() + objMovementDto.getSaleRevenue()) / objMovementDto.getTotalVisits());

				movementDataList.add(objMovementDto);
			}

			logger.debug("Total Items:" + movementDataList.size());
			
			if(movementDataList.size() == 0)
				logger.warn("No movement data found");

		} catch (Exception exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}finally{
			PristineDBUtil.close(rst);
			PristineDBUtil.close(statement);
		}

		return movementDataList;
	}

	
	
	/**
	 * 
	 * @param _Conn
	 * @param storeId
	 * @param calendar_id
	 * @param itemC
	 * @param categoryIdStr
	 * @return
	 * @throws GeneralException
	 */
	
	public List<ProductMetricsDataDTO> getWeeklyMovementAtZoneLevel(
			Connection _Conn, String weekCalIds, 
			double itemC, String categoryIdStr, boolean isAholdIMSAggr, boolean useProductLocationMapforZoneIMSAggr, PriceZoneDTO priceZoneDTO)
			throws GeneralException {

		List<ProductMetricsDataDTO> movementDataList = new ArrayList<ProductMetricsDataDTO>();

		StringBuffer sql = new StringBuffer();
			
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		String minPOS = PropertyManager.getProperty("SALES_ANALYSIS.MIN_REV_POS_DEPARTMENT", "0");
		
		String excludeGrossUnitsTrxTypes = PropertyManager.getProperty("EXCLUDE_TRX_TYPES_FOR_GROSS_UNITS", "0");
		String excludeGrossSalesTrxTypes = PropertyManager.getProperty("EXCLUDE_TRX_TYPES_FOR_GROSS_SALES", "0");
		//Proprerty to be added only for SChnucks with value FALSE , for other clients it will be TRUE by default
		boolean considerWtandQty=Boolean.parseBoolean(PropertyManager.getProperty("CONSIDER_WEIGHT_AND_QTY_FOR_MOVEMENT", "TRUE"));
		
		sql.append("select count(distinct trx_no) TRX_COUNT, MV.ITEM_ID ITEM_CODE			, ");
		sql.append("count(case when MV.SALE_TYPE = 'Y' then 1 else null end) SALE_FLAG_COUNT, ");
		sql.append("sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_unit_calc = 'Y' then 0 else MV.QUANTITY end) as REG_QUANTITY,");
		sql.append("sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_unit_calc = 'Y' then 0 else MV.QUANTITY end) as SALE_QUANTITY,");
		sql.append("sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_unit_calc = 'Y' then 0 else MV.WEIGHT end) as REG_WEIGHT_ACTUAL,");
		sql.append("sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_unit_calc = 'Y' then 0 else MV.WEIGHT end) as SALE_WEIGHT_ACTUAL,");
		sql.append("sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_unit_calc = 'Y' and MV.WEIGHT <> 0  then NULL else MV.WEIGHT end) as REG_WEIGHT,");
		sql.append("sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_unit_calc = 'Y' and MV.WEIGHT <> 0  then NULL else MV.WEIGHT end) as SALE_WEIGHT,");
		sql.append("sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_sales_calc = 'Y'  then 0 else MV.net_amt end) as REG_REVENUE,");
		sql.append("sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_sales_calc = 'Y' then 0 else MV.net_amt end) as SALE_REVENUE, ");
		sql.append("sum(case when MV.TRX_TYPE IN (");
		sql.append(excludeGrossUnitsTrxTypes).append(") then 0 else MV.QUANTITY end) as GROSS_UNITS,");
		sql.append("sum(case when MV.TRX_TYPE IN (");
		sql.append(excludeGrossSalesTrxTypes).append(") then 0 else MV.NET_AMT end) as GROSS_SALES ");
		sql.append(" FROM (SELECT ITEM_ID, TRX_NO, SALE_TYPE, QUANTITY, WEIGHT, NET_AMT, CALENDAR_ID, STORE_ID, POS_DEPARTMENT_ID,  ");
		sql.append(" CASE WHEN TTL.EXCLUDE_UNIT_CALC IS NULL THEN 'N' ELSE TTL.EXCLUDE_UNIT_CALC END AS EXCLUDE_UNIT_CALC,  ");
		sql.append(" CASE WHEN TTL.EXCLUDE_SALES_CALC IS NULL THEN 'N' ELSE TTL.EXCLUDE_SALES_CALC END AS EXCLUDE_SALES_CALC,  ");
		sql.append(" TTL.TRX_TYPE FROM TRANSACTION_LOG TL LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TTL.TRX_TYPE = TL.TRX_TYPE) MV ");
		
		//Exclude Category
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" join ITEM_LOOKUP I on MV.ITEM_ID = I.ITEM_CODE " +
			" and I.LIR_IND = 'N' AND (I.CATEGORY_ID in (" + categoryIdStr + ") or I.CATEGORY_ID is null)");
				
		sql.append("where  MV.CALENDAR_ID IN ( " + weekCalIds + ") " + " and STORE_ID IN ( ");
		if(isAholdIMSAggr) {
			sql.append("  select COMP_STR_ID from COMPETITOR_STORE");		
			sql.append("  where COMP_STR_NO IN ( ");
			if(useProductLocationMapforZoneIMSAggr) {			
				sql.append("SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT DISTINCT STORE_ID FROM PR_PRODUCT_LOCATION_STORE ");
				sql.append("WHERE PRODUCT_LOCATION_MAPPING_ID IN (SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING ");
				sql.append("WHERE LOCATION_ID = ").append(priceZoneDTO.getPriceZoneId()).append("))))");			
			} else {
				sql.append("SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE ");
				sql.append("CS.PRICE_ZONE_ID_2 = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ")
						.append(priceZoneDTO.getPriceZoneId()).append("))");
			}
		}
		else 
		{
			if (priceZoneDTO.isGlobalZone()) {
				sql.append(" select COMP_STR_ID from COMPETITOR_STORE ");
				sql.append(
						" where COMP_STR_NO IN (SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE ");
				sql.append(" CS.PRICE_ZONE_ID_3 = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ");
				sql.append(priceZoneDTO.getPriceZoneId()).append("))");

			} else {
				sql.append(" select COMP_STR_ID from COMPETITOR_STORE ");
				sql.append(
						" where COMP_STR_NO IN (SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE ");
				sql.append(" CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ");
				sql.append(priceZoneDTO.getPriceZoneId()).append("))");
			}
			
		}
			//COMMENTED FOR AZ	
        sql.append(" and MV.POS_DEPARTMENT_ID <= ").append(maxPOS) ;
		
		//if(Boolean.parseBoolean(PropertyManager.getProperty("IMS_ZONE_AGGR_FOR_AHOLD", "FALSE"))){
			//sql.append(" AND MV.ITEM_ID IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE LIR_IND='N')");
		//}
		
		//COMMENTED FOR AZ
		sql.append(" and MV.POS_DEPARTMENT_ID >= " + minPOS);
		if (itemC != -1) {
			sql.append(" and MV.ITEM_ID=").append(itemC).append("");
		}
		sql.append(" group by MV.ITEM_ID");

		logger.debug("getMovementForItemSearch SQL:" + sql.toString());
		PreparedStatement statement = null;
		ResultSet rst = null;
		try {
			statement = _Conn.prepareStatement(sql.toString());
			//statement.setString(1, weekCalIds);
			//statement.setInt(1, storeId);
			rst = statement.executeQuery();

			while (rst.next()) {
				ProductMetricsDataDTO objMovementDto = new ProductMetricsDataDTO(); 
				objMovementDto.setProductId((int) rst.getDouble("ITEM_CODE"));
				objMovementDto.setRegularWeight(rst.getDouble("REG_WEIGHT_ACTUAL"));
				objMovementDto.setRegularQuantity(rst.getDouble("REG_QUANTITY"));
				objMovementDto.setRegularRevenue(rst.getDouble("REG_REVENUE"));
					
				// Setting Sale Data
					
				objMovementDto.setSaleWeight(rst.getDouble("SALE_WEIGHT_ACTUAL"));
				objMovementDto.setSaleQuantity(rst.getDouble("SALE_QUANTITY"));
				objMovementDto.setSaleRevenue(rst.getDouble("SALE_REVENUE"));
				objMovementDto.setTotalVisits(rst.getDouble("TRX_COUNT"));
				objMovementDto.setAvgOrderSize((objMovementDto.getRegularRevenue() 
						+ objMovementDto.getSaleRevenue()) / objMovementDto.getTotalVisits());
				
				//Added by Karishma only  for Schnucks,we will use the WEIGHT info FOR MOVEMENT if its present else use Qty
				if (considerWtandQty) {
					objMovementDto.setRegularMovement(rst.getDouble("REG_QUANTITY") + rst.getDouble("REG_WEIGHT"));
					objMovementDto.setSaleMovement(rst.getDouble("SALE_QUANTITY") + rst.getDouble("SALE_WEIGHT"));
				} else {
					if (rst.getDouble("REG_WEIGHT") > 0) {
						objMovementDto.setRegularMovement(rst.getDouble("REG_WEIGHT"));
					} else
						objMovementDto.setRegularMovement(rst.getDouble("REG_QUANTITY"));

					if (rst.getDouble("SALE_WEIGHT") > 0) {
						objMovementDto.setSaleMovement(rst.getDouble("SALE_WEIGHT"));
					} else
						objMovementDto.setSaleMovement(rst.getDouble("SALE_QUANTITY"));

				}
			

				//SummaryDataDTO objMovementDto = new SummaryDataDTO();
				//objMovementDto.setLocationId(rst.getInt("COMP_STR_ID"));
				//objMovementDto.set_storeNumber(rst.getString("COMP_STR_NO"));
				/*
				 * objMovementDto.set_storeState(rst.getString("STATE"));
				 * objMovementDto.set_storeOpenDate(rst.getString("OPEN_DATE"));
				 */
				
				objMovementDto.setGrossUnits(rst.getDouble("GROSS_UNITS"));
				objMovementDto.setGrossSales(rst.getDouble("GROSS_SALES"));
				movementDataList.add(objMovementDto);
			}

			logger.debug("Total Items:" + movementDataList.size());
			
			if(movementDataList.size() == 0)
				logger.warn("No movement data found");

		} catch (Exception exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}finally{
			PristineDBUtil.close(rst);
			PristineDBUtil.close(statement);
		}

		return movementDataList;
	}

	
	
	
	public List<ProductMetricsDataDTO> getMovementForItemSearchOld(
			Connection _Conn, String storeNumber, int calendar_id, double itemC)
			throws GeneralException {

		List<ProductMetricsDataDTO> movementDataList = new ArrayList<ProductMetricsDataDTO>();

		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		
		StringBuffer sql = new StringBuffer();

		sql.append("select MV.ITEM_CODE, MV.SALE_FLAG as FLAG");
		sql.append(", sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(", sum(MV.QUANTITY) as QUANTITY");
		sql.append(", sum(MV.WEIGHT) as WEIGHT_ACTUAL");
		sql.append(", count(case when MV.WEIGHT=0  then null else 1 end) as WEIGHT");
		sql.append(", sum( MV.PRICE) as REVENUE");
		sql.append(" from MOVEMENT_DAILY MV");
		sql.append(" Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE");
		sql.append(" where  MV.CALENDAR_ID =").append(calendar_id);
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");
		sql.append(" and MV.POS_DEPARTMENT <= " + maxPOS);
		if(itemC != -1){
			sql.append(" and MV.ITEM_CODE=").append(itemC).append("");
		}
		sql.append(" group by MV.ITEM_CODE, MV.SALE_FLAG");
		sql.append(" order by MV.ITEM_CODE, MV.SALE_FLAG");

		logger.debug("getMovementForItemSearch SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
					"getMovementForItemSearch");

			if (rst.size() > 0) {

				double lastItemCode = 0;
				ProductMetricsDataDTO objMovementDto = null;
				// ProductMetricsDataDTO objMovementDto = new ProductMetricsDataDTO();

				while (rst.next()) {

					double itemCode = rst.getDouble("ITEM_CODE");
					objMovementDto = new ProductMetricsDataDTO(); 
					if (lastItemCode != itemCode) {

						if (lastItemCode > 0) {
							movementDataList.add(objMovementDto);
							// logger.debug("Processed Item Code:" + objMovementDto.getProductId());

						}

//						objMovementDto = new ProductMetricsDataDTO();
						objMovementDto.setProductId((int) rst
								.getDouble("ITEM_CODE"));
						objMovementDto.setSaleFlag(rst.getString("FLAG"));

						if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
							objMovementDto.setRegularMovement(rst
									.getDouble("QUANTITY")
									+ rst.getDouble("WEIGHT"));
							objMovementDto.setRegularWeight(rst
									.getDouble("WEIGHT_ACTUAL"));
							objMovementDto.setRegularQuantity(rst
									.getDouble("QUANTITY"));
							objMovementDto.setRegularRevenue(rst
									.getDouble("REVENUE"));
						} else if (rst.getString("FLAG").trim()
								.equalsIgnoreCase("Y")) {
							objMovementDto.setSaleMovement(rst
									.getDouble("QUANTITY")
									+ rst.getDouble("WEIGHT"));
							objMovementDto.setSaleWeight(rst
									.getDouble("WEIGHT_ACTUAL"));
							objMovementDto.setSaleQuantity(rst
									.getDouble("QUANTITY"));
							objMovementDto.setSaleRevenue(rst
									.getDouble("REVENUE"));
						}
					} else {
						if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
							objMovementDto.setRegularMovement(objMovementDto
									.getRegularMovement()
									+ rst.getDouble("QUANTITY")
									+ rst.getDouble("WEIGHT"));
							objMovementDto.setRegularWeight(objMovementDto
									.getRegularWeight()
									+ rst.getDouble("WEIGHT_ACTUAL"));
							objMovementDto.setRegularQuantity(objMovementDto
									.getRegularQuantity()
									+ rst.getDouble("QUANTITY"));
							objMovementDto.setRegularRevenue(objMovementDto
									.getRegularRevenue()
									+ rst.getDouble("REVENUE"));
						} else if (rst.getString("FLAG").trim()
								.equalsIgnoreCase("Y")) {
							objMovementDto.setSaleMovement(objMovementDto
									.getSaleMovement()
									+ rst.getDouble("QUANTITY")
									+ rst.getDouble("WEIGHT"));
							objMovementDto.setSaleWeight(objMovementDto
									.getSaleWeight()
									+ rst.getDouble("WEIGHT_ACTUAL"));
							objMovementDto.setSaleQuantity(objMovementDto
									.getSaleQuantity()
									+ rst.getDouble("QUANTITY"));
							objMovementDto.setSaleRevenue(objMovementDto
									.getSaleRevenue()
									+ rst.getDouble("REVENUE"));
						}
					}

					lastItemCode = itemCode;
				}
				movementDataList.add(objMovementDto);
				// logger.debug("Processed Item Code:" + objMovementDto.getProductId());
				logger.debug("Total Items:" + movementDataList.size());

			} else {
				logger.warn("There is no movement data");
			}

			rst.close();

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}

		return movementDataList;
	}

	public void insertMovementDaily(Connection _Conn,
		ArrayList<MovementDTO> movementList, String targetTable) 
										throws GeneralException {
		try {

			PreparedStatement psmt = _Conn
					.prepareStatement(InsertMovementSql(targetTable));

			MovementDTO objMDto = new MovementDTO();

			for (int recordCnt = 0; recordCnt < movementList.size(); recordCnt++) {

				objMDto = movementList.get(recordCnt);
				addMovementData(objMDto, psmt);
			}
			//logger.info("Executing batch...");
			psmt.executeBatch();
			psmt.close();
			_Conn.commit();
			//logger.info("Executing batch is completed.");
			movementList = null;

		} catch (Exception se) {
			logger.debug(se);
			throw new GeneralException("addMovementData", se);
		}

	}

	/*
	 * ****************************************************************
	 * Method to Create the insert script and add the script into batch List
	 * Argument 1 : movementDto
	 * 
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */

	public void addMovementData(MovementDTO movementDto, PreparedStatement psmt)
			throws GeneralException {

		try {
			// COMP_STR_NO
			psmt.setObject(1, movementDto.getItemStore());

			// UPC
			String upc = PrestoUtil.castUPC(movementDto.getItemUPC(), true);
			psmt.setObject(2, upc);

			// POS_TIMESTAMP
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
			String dateStr = formatter.format(movementDto.getItemDateTime());
			psmt.setObject(3, dateStr);

			// WEEKDAY
			psmt.setObject(4, movementDto.getDayOfWeek());

			// UNIT_PRICE
			double unitPrice = movementDto.getItemNetPrice();
			psmt.setObject(5, unitPrice);

			// PRICE
			double price = movementDto.getExtendedNetPrice();
			psmt.setObject(6, price);

			// SALE_FLAG
			if ((0 <= price) && (price < movementDto.getExtendedGrossPrice())) {
				// logger.debug("SALE FLAG...............Y");
				psmt.setObject(7, "Y");
			} else {
				// logger.debug("SALE FLAG...............N");
				psmt.setObject(7, "N");
			}

			// QUANTITY
			psmt.setObject(8, movementDto.getExtnQty());

			// WEIGHT
			psmt.setObject(9, movementDto.getExtnWeight());

			// TRANSACTION_NO
			psmt.setObject(10, movementDto.getTransactionNo());

			// CUSTOMER_CARD_NO
			psmt.setObject(11, movementDto.getCustomerId());

			// ITEM_CODE
			psmt.setObject(12, movementDto.getItemCode());

			// CALENDAR_ID
			psmt.setObject(13, movementDto.getCalendarId());

			// pos Department
			psmt.setObject(14, movementDto.getPosDepartment());

			// Extended Gross Price
			psmt.setObject(15, movementDto.getExtendedGrossPrice());

			// Store coupon used
			if (movementDto.getStoreCpnUsed() == null)
				psmt.setNull(16, Types.CHAR); 
			else
				psmt.setObject(16, movementDto.getStoreCpnUsed());

			// mfr coupon used
			if (movementDto.getMfrCpnUsed() == null)
				psmt.setNull(17, Types.CHAR); 
			else				
				psmt.setObject(17, movementDto.getMfrCpnUsed());

			// IDW-ITEM-OPERATOR
			//psmt.setObject(18, movementDto.getItemOperator());
			logger.debug(movementDto.getItemStore() + ", " +
					movementDto.getItemUPC() + ", " + movementDto.getItemDateTime() + ", " + movementDto.getDayOfWeek()
					+ ", " + movementDto.getItemNetPrice() + ", " + movementDto.getExtendedNetPrice()
					+ ", " + "Y" +  ", " + movementDto.getExtnQty()
					+ ", " + movementDto.getExtnWeight()
					+ ", " + movementDto.getTransactionNo()
					+ ", " + movementDto.getCustomerId()
					+ ", " + movementDto.getItemCode()
					+ ", " + movementDto.getCalendarId()
					+ ", " + movementDto.getPosDepartment()
					+ ", " + movementDto.getExtendedGrossPrice()
					+ ", " + movementDto.getStoreCpnUsed()
					+ ", " +movementDto.getMfrCpnUsed());
			psmt.addBatch();

		} catch (Exception sql) {
			logger.debug(sql);
			
			logger.error("UPC in error - " + movementDto.getItemUPC());
			throw new GeneralException("addMovementData", sql);

		}
	}

	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */

	private String InsertMovementSql(String targetTable) {

		StringBuffer Sql = new StringBuffer();
		Sql.append("insert into " ).append(targetTable).append(" (");
		Sql.append(" COMP_STR_NO, UPC, POS_TIMESTAMP, WEEKDAY, UNIT_PRICE, ");
		Sql.append(" PRICE, SALE_FLAG, QUANTITY, WEIGHT, TRANSACTION_NO, ");
		Sql.append(" CUSTOMER_CARD_NO, ITEM_CODE, CALENDAR_ID,");
		Sql.append(" POS_DEPARTMENT, EXTENDED_GROSS_PRICE,");
		//Sql.append(" STORE_COUPON_USED, MFR_COUPON_USED, ITEM_OPERATOR)");
		Sql.append(" STORE_COUPON_USED, MFR_COUPON_USED)");
		Sql.append(" values (?, ?, To_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ");
		//Sql.append(" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		Sql.append(" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		logger.debug("InsertMovementSql " + Sql.toString());
		return Sql.toString();
	}

	/*
	 * Method used to get the movement data for finance department aggregation
	 * Argument 1 : Connection Argument 2 : storeNumber Argument 3 : calendarId
	 * Argument 4 : specialDTO Argument 5 : productLevelId Argument 6 :
	 * Salesaggregationbusiness Argument 7 : Repair Flag
	 * 
	 * @throws GeneralException
	 * 
	 * @catch Exception
	 * 
	 * 
	 * public List<MovementDailyAggregateDTO> financeAggregation(Connection
	 * conn, String storeNumber, int calendarId, SpecialCriteriaDTO specialDTO,
	 * int productLevelId, Salesaggregationbusiness summaryBusiness, String
	 * repairFlag) throws GeneralException {
	 * 
	 * List<MovementDailyAggregateDTO> financeAggergation = new
	 * ArrayList<MovementDailyAggregateDTO>(); StringBuffer sql = new
	 * StringBuffer(); sql.append(
	 * " SELECT  MV.UPC,PR.PRODUCT_ID as PRODUCT_ID, MV.SALE_FLAG AS FLAG,");
	 * sql.append(" SUM(MV.QUANTITY * I.ITEM_SIZE) AS VOLUME");
	 * sql.append(" ,SUM(MV.QUANTITY) AS QUANTITY");
	 * sql.append(" ,COUNT(CASE WHEN MV.WEIGHT=0  THEN NULL ELSE 1  END)AS WEIGHT ,"
	 * ); sql.append(" SUM( MV.PRICE) AS REVENUE,I.UOM_ID");
	 * sql.append(" FROM MOVEMENT_DAILY MV");
	 * sql.append(" inner JOIN PRODUCT_GROUP_RELATION PR");
	 * sql.append(" ON MV.ITEM_CODE=PR.CHILD_PRODUCT_ID");
	 * sql.append(" inner Join item_lookup I  ");
	 * sql.append(" ON MV.ITEM_CODE=I.ITEM_CODE");
	 * sql.append(" WHERE MV.CALENDAR_ID = "+calendarId+"");
	 * sql.append(" AND COMP_STR_NO      ='"+storeNumber+"'");
	 * sql.append(" AND PR.PRODUCT_LEVEL_ID="+productLevelId+"");
	 * 
	 * // code added for partial repair flag
	 * sql.append(" and MV.PROCESS_FLAG='N'");
	 * 
	 * // code blocked for Movement by volume if
	 * (specialDTO.getExcludeDepartments() != null) {
	 * sql.append("  and ( I.DEPT_ID not in (");
	 * sql.append(specialDTO.getExcludeDepartments()); sql.append("   )"); if
	 * (specialDTO.getIncludeItems() != null) { sql.append("  or MV.UPC in (");
	 * sql.append(specialDTO.getIncludeItems()+",");
	 * sql.append(specialDTO.getIncludeGas()); sql.append("   )"); }
	 * sql.append(" )"); }
	 * 
	 * // add the upc in group by process
	 * sql.append("  GROUP BY MV.UPC,PRODUCT_ID, MV.SALE_FLAG,I.UOM_ID");
	 * sql.append("  ORDER BY PRODUCT_ID,MV.UPC,  MV.SALE_FLAG");
	 * logger.info("Finance Department Aggregation SQL:" + sql.toString());
	 * 
	 * try { CachedRowSet rst = PristineDBUtil.executeQuery(conn, sql,
	 * "Finance Department Aggregation");
	 * 
	 * while (rst.next()) { MovementDailyAggregateDTO movement = new
	 * MovementDailyAggregateDTO();
	 * movement.setProductId(rst.getString("PRODUCT_ID"));
	 * movement.setProductLevelId(productLevelId);
	 * movement.setupc(rst.getString("UPC"));
	 * movement.setuomId(rst.getString("UOM_ID"));
	 * if(rst.getString("FLAG").equalsIgnoreCase("Y")){
	 * movement.set_saleQuantity(rst.getDouble("QUANTITY")+
	 * rst.getDouble("WEIGHT"));
	 * movement.setsaleMovementVolume(rst.getDouble("VOLUME")+
	 * rst.getDouble("WEIGHT"));
	 * movement.setRevenueSale(rst.getDouble("REVENUE")); } else{
	 * movement.set_regularQuantity(rst.getDouble("QUANTITY")+
	 * rst.getDouble("WEIGHT"));
	 * movement.setregMovementVolume(rst.getDouble("VOLUME")+
	 * rst.getDouble("WEIGHT"));
	 * movement.setRevenueRegular(rst.getDouble("REVENUE")); }
	 * 
	 * financeAggergation.add(movement); }
	 * logger.info(" Daily Summary Process Count " + movementDataList.size());
	 * 
	 * } catch (Exception exe) { logger.error(exe); throw new
	 * GeneralException("Fetch Finance Departemnt Aggregation Proceess", exe); }
	 * 
	 * return financeAggergation; }
	 */

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
	 * This method returns distinct item codes from movements specified between
	 * given dates.
	 * 
	 * @param conn
	 *            Connection
	 * @param weekStartDate
	 *            Week Start Date
	 * @param weekEndDate
	 *            Week End Date
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
	 * 
	 * @param scheduleID
	 *            Schedule Id
	 * @param targetTableName
	 *            Target Table Name
	 * @param departmentCode
	 *            Departments to exclude
	 * @param categoryid
	 *            Categories to exclude
	 * @return boolean Updation success/failure
	 * @throws GeneralException
	 */
	public boolean updateWeeklyMovement12WeekInitialData(Connection conn,
			String scheduleID, String targetTableName, String departmentCode,
			String categoryid) throws GeneralException {
		int count = 0;

		StringBuffer sb = new StringBuffer("UPDATE ").append(targetTableName);
		sb.append(" SET");
		sb.append(" QTY_REGULAR_12WK = 0, QTY_SALE_12WK = 0");
		sb.append(" WHERE CHECK_DATA_ID IN");
		sb.append(" (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA c");

		// This is application only if Department code or category id are not
		// empty
		if ((!departmentCode.isEmpty()) || (!categoryid.isEmpty())) {
			sb.append(" , ITEM_LOOKUP i ");
		}

		sb.append(" WHERE c.SCHEDULE_ID = ?");

		// If department code has some value, value would be multiple separated
		// with comma
		if (!departmentCode.isEmpty()) {
			sb.append(" AND i.DEPT_ID NOT IN (").append(departmentCode)
					.append(")");
		}

		// If category id has some value, value would be multiple separated with
		// comma
		if (!categoryid.isEmpty()) {
			sb.append(" AND i.CATEGORY_ID NOT IN (").append(categoryid)
					.append(")");
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
		} catch (SQLException gex) {
			logger.error("SQL: " + sb.toString(), gex);
			throw new GeneralException(
					"Error while updating 12 week initial data", gex);
		}

		return count > 0;
	}

	/**
	 * Method to update 12 week movement data
	 * 
	 * @param conn
	 *            DB Connection
	 * @param movement12DtoList
	 *            List containing 12 week movement data
	 * @param targetTableName
	 *            Target table name
	 * @return int[] Updation response
	 * @throws GeneralException
	 */
	public int[] updateMovement12WeekData(Connection conn,
			List<Movement13WeeklyDTO> movement12DtoList, String targetTableName)
			throws GeneralException {
		String sql = UPDATE_12WEEK_MOVEMENTS.replaceAll(Constants.TARGET_TABLE,
				targetTableName);
		PreparedStatement statement = null;
		int[] count = null;
		try {
			statement = conn.prepareStatement(sql);

			for (Movement13WeeklyDTO movementDTO : movement12DtoList) {
				if (movementDTO.getQuantityRegular12Week() == -99) {
					statement.setNull(1, java.sql.Types.DOUBLE);
				} else {
					statement.setDouble(1,
							movementDTO.getQuantityRegular12Week());
				}

				// Check for the update type
				if (movementDTO.getQuantitySale12Week() == -99) {
					statement.setNull(2, java.sql.Types.DOUBLE);
				} else {
					statement.setDouble(2, movementDTO.getQuantitySale12Week());
				}

				statement.setInt(3, movementDTO.getCheckDataId());

				statement.addBatch();
			}
			count = statement.executeBatch();
		} catch (SQLException gex) {
			logger.error("SQL: " + UPDATE_12WEEK_MOVEMENTS, gex);
			throw new GeneralException("Error while updating 12 week data", gex);
		}
		return count;
	}

	/**
	 * Method to get weekly movement data for an item
	 * 
	 * @param conn
	 *            DB Connection
	 * @param scheduleIDs
	 *            Schedule Ids
	 * @param itemCodeList
	 *            List of items
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Movement13WeeklyDTO> getWeeklyMovementData(
			Connection conn, String scheduleIDs, List<Integer> itemCodeList)
			throws GeneralException {
		HashMap<Integer, Movement13WeeklyDTO> movementsMap = new HashMap<Integer, Movement13WeeklyDTO>();
		List<Integer> tempList = new ArrayList<Integer>();
		int limitcount = 0;

		for (Integer itemCode : itemCodeList) {
			tempList.add(itemCode);
			limitcount++;
			if (limitcount > 0
					&& (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = tempList.toArray();
				retrieveWeeklyMovement(conn, movementsMap, scheduleIDs, values);
				tempList.clear();
			}
		}

		if (tempList.size() > 0) {
			Object[] values = tempList.toArray();
			retrieveWeeklyMovement(conn, movementsMap, scheduleIDs, values);
			tempList.clear();
		}
		return movementsMap;
	}

	/**
	 * Method that actually queries for weekly movements
	 * 
	 * @param conn
	 *            DB connection
	 * @param movementsMap
	 *            Map containing weekly movements
	 * @param scheduleIds
	 *            Schedule Ids
	 * @param values
	 *            Item Codes
	 * @throws GeneralException
	 */
	private void retrieveWeeklyMovement(Connection conn,
			HashMap<Integer, Movement13WeeklyDTO> movementsMap,
			String scheduleIds, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		String sql = GET_WEEKLY_MOVEMENT.replaceAll(Constants.SCHEDULE_IDS,
				scheduleIds);
		Movement13WeeklyDTO movementWeeklyDTO = null;
		try {
			statement = conn.prepareStatement(String.format(sql,
					PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				movementWeeklyDTO = new Movement13WeeklyDTO();
				movementWeeklyDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				movementWeeklyDTO.setQuantityRegular(resultSet
						.getDouble("QUANTITY_REGULAR"));
				movementWeeklyDTO.setQuantitySale(resultSet
						.getDouble("QUANTITY_SALE"));
				movementsMap.put(movementWeeklyDTO.getItemCode(),
						movementWeeklyDTO);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_12WEEK_MOVEMENTS", e);
			throw new GeneralException(
					"Error while executing GET_12WEEK_MOVEMENTS", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

	}

	/**
	 * Method to update 13 week movement data
	 * 
	 * @param conn
	 *            DB connection
	 * @param movement13DtoList
	 *            List containing 13 week movement for Items
	 * @param targetTableName
	 *            Target Table Name
	 * @return int[] Updation status
	 * @throws GeneralException
	 * @throws SQLException 
	 */
	public int[] updateMovement13WeekData(Connection conn,
			List<Movement13WeeklyDTO> movement13DtoList, String targetTableName)
			throws GeneralException, SQLException {
		String sql = UPDATE_13WEEK_MOVEMENTS.replaceAll(Constants.TARGET_TABLE,
				targetTableName);
		PreparedStatement statement = null;
		int[] count = null;

		try {
			statement = conn.prepareStatement(sql);

			for (Movement13WeeklyDTO movementDTO : movement13DtoList) {
				if (movementDTO.getQuantityRegular13Week() == -99) {
					statement.setNull(1, java.sql.Types.DOUBLE);
				} else {
					statement.setDouble(1,
							movementDTO.getQuantityRegular13Week());
				}

				// Check for the update type
				if (movementDTO.getQuantitySale13Week() == -99) {
					statement.setNull(2, java.sql.Types.DOUBLE);
				} else {
					statement.setDouble(2, movementDTO.getQuantitySale13Week());
				}

				statement.setInt(3, movementDTO.getCheckDataId());

				statement.addBatch();
			}
			count = statement.executeBatch();
		} catch (SQLException gex) {
			logger.error("SQL: " + UPDATE_13WEEK_MOVEMENTS, gex);
			throw new GeneralException("Error while updating 12 week data", gex);
		}finally {
			if(statement!=null) {
				PristineDBUtil.close(statement);
			}
		}
		return count;
	}

	/*
	 * Method used to update the process flag in movement_daily Method added for
	 * repair Process. Process used to change the non-process record to process
	 * record Argument 1: Connection Argument 2: calendarId Argument 3:
	 * storeNumber
	 * 
	 * @throws GeneralException
	 */

	public void updateProcessFlag(Connection _conn, int calendarId,
			String storeNumber) throws GeneralException {

		// Query
		StringBuffer sql = new StringBuffer();
		sql.append(" update MOVEMENT_DAILY set PROCESSED_FLAG='Y'");
		sql.append(" where CALENDAR_ID=").append(calendarId);
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");
		logger.debug(" updateProcessFlag SQL:" + sql.toString());

		// Execute the query
		try {
			PristineDBUtil.execute(_conn, sql, "updateProcessFlag");
		} catch (GeneralException e) {
			logger.error(" Error While updating Process Flag ", e);
			throw new GeneralException(" Error While updating Process Flag ", e);
		}

	}

	/*
	 * Method used to update the process flag in movement_daily Method added for
	 * repair Process. Process used to change the non-process record to process
	 * record Argument 1: Connection 
	 * Argument 2: calendarId 
	 * Argument 3: storeId
	 * @throws GeneralException
	 */

	public void updateTLProcessFlag(Connection _conn, int calendarId,
								int storeId) throws GeneralException {
		// SQL
		StringBuffer sql = new StringBuffer();
		sql.append(" update TRANSACTION_LOG set PROCESSED_FLAG='Y'");
		sql.append(" where CALENDAR_ID=").append(calendarId);
		sql.append(" and STORE_ID = ").append(storeId);
		
		logger.debug("UpdateTLProcessFlag SQL:" + sql.toString());

		// Execute the query
		try {
			PristineDBUtil.execute(_conn, sql, "updateProcessFlag");
		} catch (GeneralException e) {
			logger.error(" Error While updating Process Flag ", e);
			throw new GeneralException(" Error While updating Process Flag ", e);
		}
	}	
	
	
	/*
	 * method used to get the coupons based revenue from movement daily table.
	 * Argument 1 : Connection Argument 2 : storeNumber Argument 3 : Calendar Id
	 * return HashMap contains all level aggregation(sub-category , category ,
	 * department , pos-department) HashMap Key - subcategoryid_Levelid ,
	 * categoryid_levelid
	 * 
	 * @throw GeneralException
	 */

	public HashMap<String, Double> getCouponBasedAggregation(Connection _conn,
			String storeNumber, int calendarid) throws GeneralException {

		// return HashMap
		HashMap<String, Double> returnMap = new HashMap<String, Double>();

		// get the level id from constant class
		/*
		 * int subCategoryLevelId = Constants.SUBCATEGORYLEVELID; int
		 * categoryLevelId = Constants.CATEGORYLEVELID; int deptlevelId =
		 * Constants.DEPARTMENTLEVELID;
		 */
		int posLevelId = Constants.POSDEPARTMENT;
		/* int storeLevelId = Constants.STORE_LEVEL_ID; */

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select  MV.POS_DEPT");

			// logic given by suresh
			sql.append(" ,sum(DECODE(mv.CPN_WEIGHT, 0, decode(mv.CPN_QTY, 0, 1, mv.CPN_QTY ), mv.CPN_WEIGHT) * mv.PRICE) as COUPONPRICE");
			sql.append(" from MOV_DAILY_COUPON_INFO MV");
			sql.append(" where");
			sql.append(" MV.CALENDAR_ID =").append(calendarid);
			sql.append(" and MV.COMP_STR_NO='").append(storeNumber).append("'");
			sql.append(" and MV.POS_DEPT not in(")
					.append(Constants.GASPOSDEPARTMENT).append(")");
			sql.append(" and  MV.CPN_TYPE <> 6 and MV.UPC = '000000000000' ");
			// order by
			sql.append(" group by MV.POS_DEPT");
			sql.append(" Order by MV.POS_DEPT");

			logger.debug(" Coupon query..... " + sql.toString());

			CachedRowSet rst = PristineDBUtil.executeQuery(_conn, sql,
					"getCouponBasedAggregation");

			while (rst.next()) {

				/*
				 * String subCatId = rst.getString("SUB_CATEGORY_ID"); String
				 * catId = rst.getString("CATEGORY_ID"); String deptId =
				 * rst.getString("DEPT_ID");
				 */
				String posId = rst.getString("POS_DEPT");

				double revenue = rst.getDouble("COUPONPRICE");

				/*
				 * // for subcategory aggregation if
				 * (returnMap.containsKey(subCatId + "_" + subCategoryLevelId))
				 * { sumCouponRevenue(returnMap, returnMap.get(subCatId + "_" +
				 * subCategoryLevelId), subCatId, subCategoryLevelId, revenue);
				 * } else { sumCouponRevenue(returnMap, 0, subCatId,
				 * subCategoryLevelId, revenue); }
				 * 
				 * // for category Aggregation if (returnMap.containsKey(catId +
				 * "_" + categoryLevelId)) { sumCouponRevenue(returnMap,
				 * returnMap.get(catId + "_" + categoryLevelId), catId,
				 * categoryLevelId, revenue); } else {
				 * sumCouponRevenue(returnMap, 0, catId, categoryLevelId,
				 * revenue); }
				 * 
				 * // for department Aggregation if
				 * (returnMap.containsKey(deptId + "_" + deptlevelId)) {
				 * sumCouponRevenue(returnMap, returnMap.get(deptId + "_" +
				 * deptlevelId), deptId, deptlevelId, revenue); } else {
				 * sumCouponRevenue(returnMap, 0, deptId, deptlevelId, revenue);
				 * }
				 */
				// for pos department aggregation
				if (returnMap.containsKey(posId + "_" + posLevelId)) {
					sumCouponRevenue(returnMap,
							returnMap.get(posId + "_" + posLevelId), posId,
							posLevelId, revenue);
				} else {
					sumCouponRevenue(returnMap, 0, posId, posLevelId, revenue);
				}

				/*
				 * // for store level aggregation if
				 * (returnMap.containsKey(storeNumber+"_"+storeLevelId)) {
				 * sumCouponRevenue(returnMap, returnMap.get(storeNumber + "_" +
				 * storeLevelId), storeNumber, storeLevelId, revenue); } else {
				 * sumCouponRevenue(returnMap, 0, storeNumber, storeLevelId,
				 * revenue); }
				 */
			}

			rst.close();

		} catch (SQLException e) {
			logger.error(" Error while fetching result ....", e);
			throw new GeneralException(" Error while fetching result ....", e);
		} catch (GeneralException e) {
			logger.error(" Error while fetching result ....", e);
			throw new GeneralException(" Error while fetching result ....", e);
		}

		logger.debug(" Return Coupon  Map size......" + returnMap.size());

		return returnMap;
	}

	/*
	 * Method used to new/add the coupon revenue Argument 1 : returnMap Arguemnt
	 * 2 : newRevenue Argument 2 : productId Argument 3 : productLevelId
	 * Argument 4 : oldrevenue
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
	 * Argument 1 : conn Argument 2 : Store number Argument 3 : calendar_id
	 * 
	 * @throw GeneralException
	 */

	public List<String> getDistinctItemCodeMD(Connection _conn,
			String storeNumber, int calendar_id) throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet result = null;
		try {

			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct ITEM_CODE from MOVEMENT_DAILY T");
			sql.append(" where CALENDAR_ID= ? ");
			sql.append(" and COMP_STR_NO= ? ");
			sql.append(" and POS_DEPARTMENT <=" + maxPOS);
			logger.debug("getDistinctItemCode SQL:" + sql.toString());
			
			statement = _conn.prepareStatement(sql.toString());
			statement.setInt(1, calendar_id);
			statement.setString(2, storeNumber);
			result = statement.executeQuery();

			while (result.next()) {

				returnList.add(result.getString("ITEM_CODE"));
			}

			logger.debug("Movement Item code List size:" + returnList.size());
		} catch (SQLException e) {
			logger.error(" Error while fetching the distinct item code in movement daily ",	e);
			throw new GeneralException(" Error while fetching the distinct item code in movement daily ", e);
		}finally{
			PristineDBUtil.close(result);
			PristineDBUtil.close(statement);
		}
		return returnList;
	}	
	
	/*
	 * Method used to get the distinct item code for given calendar and store no
	 * Argument 1 : conn 
	 * Argument 2 : Store Id 
	 * Argument 3 : calendar_id
	 * @throw GeneralException
	 */

	public List<String> getDistinctItemCodeTL(Connection _conn,
			int storeId, int calendar_id) throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet result = null;
		try {

			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct ITEM_ID AS ITEM_CODE from TRANSACTION_LOG T");
			sql.append(" where CALENDAR_ID = ? ");
			sql.append(" and STORE_ID = ? ");
			sql.append(" and POS_DEPARTMENT_ID <=" + maxPOS);
			logger.debug("getDistinctItemCodeTL SQL:" + sql.toString());
			
			statement = _conn.prepareStatement(sql.toString());
			statement.setInt(1, calendar_id);
			statement.setInt(2, storeId);
			result = statement.executeQuery();

			while (result.next()) {

				returnList.add(result.getString("ITEM_CODE"));
			}

			logger.debug("Movement Item code List size:" + returnList.size());
		} catch (SQLException e) {
			logger.error(" Error while fetching the distinct item code in movement daily ",	e);
			throw new GeneralException(" Error while fetching the distinct item code in movement daily ", e);
		}finally{
			PristineDBUtil.close(result);
			PristineDBUtil.close(statement);
		}
		return returnList;
	}	
		
	public List<String> getDistinctItemCodeTLForWeek(Connection _conn,
						int storeId, RetailCalendarDTO objCalendarDto) 
											throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet result = null;
		try {

			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct ITEM_ID AS ITEM_CODE from TRANSACTION_LOG T");
			sql.append(" WHERE CALENDAR_ID IN");
			sql.append(" (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE");
			sql.append(" START_DATE BETWEEN To_Date('").append(objCalendarDto.getStartDate()).append("', 'dd-MM-yyyy')");
			sql.append(" AND to_date('").append(objCalendarDto.getEndDate()).append("', 'dd-MM-yyyy') AND ROW_TYPE = 'D')");
			sql.append(" and STORE_ID = ").append(storeId);
			sql.append(" and POS_DEPARTMENT_ID <=").append(maxPOS);
			
			// TempORARY SQL
			sql.append(" AND ROWNUM < 101");
			
			logger.debug("getDistinctItemCodeTL SQL:" + sql.toString());
			
			statement = _conn.prepareStatement(sql.toString());
			result = statement.executeQuery();

			while (result.next()) {

				returnList.add(result.getString("ITEM_CODE"));
			}

			logger.debug("Movement Item code List size:" + returnList.size());
		} catch (SQLException e) {
			logger.error(" Error while fetching the distinct item code in movement daily ",	e);
			throw new GeneralException(" Error while fetching the distinct item code in movement daily ", e);
		}finally{
			PristineDBUtil.close(result);
			PristineDBUtil.close(statement);
		}
		return returnList;
	}	
			
	
	
	/*
	 * Method used to get the distinct item code for given calendar and store no
	 * Argument 1 : conn Argument 2 : Store number Argument 3 : calendar_id
	 * 
	 * @throw GeneralException
	 */

	public List<String> getDistinctItemCode(Connection _conn,
			String storeNumber, int calendar_id) throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet result = null;
		try {

			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct ITEM_ID ITEM_CODE from TRANSACTION_LOG T, COMPETITOR_STORE CS");
			sql.append(" where T.store_id = CS.comp_str_id and CALENDAR_ID= ? ");
			sql.append(" and COMP_STR_NO= ? ");
			sql.append(" and POS_DEPARTMENT_ID <=" + maxPOS);
			logger.debug("getDistinctItemCode SQL:" + sql.toString());
			
			statement = _conn.prepareStatement(sql.toString());
			statement.setInt(1, calendar_id);
			statement.setString(2, storeNumber);
			result = statement.executeQuery();

			while (result.next()) {

				returnList.add(result.getString("ITEM_CODE"));
			}

			logger.debug("Movement Item code List size:" + returnList.size());
		} catch (SQLException e) {
			logger.error(" Error while fetching the distinct item code in movement daily ",	e);
			throw new GeneralException(" Error while fetching the distinct item code in movement daily ", e);
		}finally{
			PristineDBUtil.close(result);
			PristineDBUtil.close(statement);
		}
		return returnList;
	}

	public List<String> getDistinctStores(Connection _conn,
			int calendar_id) throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();

		try {
			String tableName = PropertyManager.getProperty("T_LOG_SOURCE_TABLE", "TRANSACTION_LOG");
			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
			
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct COMP_STR_NO from ");
			sql.append(tableName);
			sql.append(" T, COMPETITOR_STORE CS");
			sql.append(" where T.store_id = CS.comp_str_id and CALENDAR_ID=").append(calendar_id);
			sql.append(" and POS_DEPARTMENT_ID <=" + maxPOS);
			
			logger.debug("getDistinctStores SQL:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getDistinctStores");

			while (result.next()) {

				returnList.add(result.getString("COMP_STR_NO"));
			}

			result.close();

			logger.debug("Movement Store List size:" + returnList.size());
		} catch (Exception e) {
			logger.error(
					" Error while fetching the distinct Store list in movement daily ",
					e);
			throw new GeneralException(
					" Error while fetching the distinct Store list in movement daily ",
					e);
		} 

		return returnList;
	}

	
	
	// Added categoryIdStr parameter to exclude specific categories during
	// revenue calculation
	public List<MovementDailyAggregateDTO> getMovemenDataByTransaction(
			Connection _Conn, String storeNumber, int calendar_id,
			String categoryIdStr) throws GeneralException {

		List<MovementDailyAggregateDTO> movementDataList = new ArrayList<MovementDailyAggregateDTO>();

		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		
		StringBuffer sql = new StringBuffer();

		sql.append(" select NVL(MV.CUSTOMER_CARD_NO, 0) AS CUSTOMER_CARD_NO, TRANSACTION_NO, MV.ITEM_CODE, MV.SALE_FLAG as FLAG");

		// add uom_id
		sql.append(" ,I.UOM_ID");

		// add the movement by volume - multiply the quantity * item size.
		sql.append(" ,sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(" ,sum(QUANTITY) as QUANTITY");

		// Actual weight to Margin calculation
		sql.append(" ,sum(MV.WEIGHT) as WEIGHT_ACTUAL");

		sql.append(" ,count(case when MV.WEIGHT=0  then null  else 1 end) as WEIGHT");
		sql.append(" ,sum(MV.PRICE) as REVENUE");

		// code added for gas revenue
		sql.append(" ,sum(MV.EXTENDED_GROSS_PRICE) as GROSSREVENUE");
		sql.append(" from MOVEMENT_DAILY MV Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE");
		sql.append(" where  MV.CALENDAR_ID =").append(calendar_id);
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");

		// add the new condition pos_department < =37
		sql.append(" and ( MV.POS_DEPARTMENT <=" + maxPOS + " and MV.POS_DEPARTMENT <> "+ Constants.POS_GASDEPARTMENT + " )");
		
		
		// Exclude categories for revenue calculation
		if (categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr)
					.append(")");

		sql.append(" group by MV.CUSTOMER_CARD_NO, MV.TRANSACTION_NO, MV.ITEM_CODE, MV.SALE_FLAG, I.UOM_ID");
		sql.append(" order by CUSTOMER_CARD_NO, TRANSACTION_NO");

		logger.debug("getMovemenDataByTransaction SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
					"getMovemenDataByTransaction");

			while (rst.next()) {

				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();

				objMovementDto.setCustomerCard(rst
						.getString("CUSTOMER_CARD_NO"));
				objMovementDto.setTransactionNo(rst.getInt("TRANSACTION_NO"));
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));

				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					objMovementDto.set_regularQuantity(rst
							.getDouble("QUANTITY") + rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setregMovementVolume(rst.getDouble("VOLUME")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setRevenueRegular(rst.getDouble("REVENUE"));
					objMovementDto.setregGrossRevenue(rst
							.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					objMovementDto.set_saleQuantity(rst.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setsaleMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					objMovementDto.setRevenueSale(rst.getDouble("REVENUE"));
					objMovementDto.setsaleGrossRevenue(rst
							.getDouble("GROSSREVENUE"));
				}
				movementDataList.add(objMovementDto);
			}

			/*
			 * logger.info(" Daily Summary Process Count " +
			 * movementDataList.size());
			 */

			rst.close();

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}

		return movementDataList;
	}


	public List<MovementDailyAggregateDTO> getMovementDailyDataV2(
						Connection _Conn, String storeNumber, int calendar_id, 
					String categoryIdStr, ArrayList<Integer> leastProductLevel, 
				HashMap<Integer, Integer> gasItems) throws GeneralException {

		List<MovementDailyAggregateDTO> movementDataList = 
				new ArrayList<MovementDailyAggregateDTO>();

		StringBuffer sql = new StringBuffer();
		StringBuffer selectSB = new StringBuffer();
		StringBuffer joinSB = new StringBuffer();
		StringBuffer whereSB = new StringBuffer();
		StringBuffer groupSB = new StringBuffer();
		StringBuffer orderSB = new StringBuffer();

		for (int i=0; i < leastProductLevel.size(); i++ ){
			selectSB.append(" PR" + i +".PRODUCT_ID as PRODUCT_ID" + i + ", ");
			joinSB.append(" LEFT OUTER JOIN PRODUCT_GROUP_RELATION PR");
			joinSB.append(i).append(" ON MV.ITEM_CODE = PR"+i+ ".CHILD_PRODUCT_ID");
			joinSB.append(" AND PR" + i +".PRODUCT_LEVEL_ID=" + leastProductLevel.get(i));
			groupSB.append("PR" + i + ".PRODUCT_ID, ");
			orderSB.append("PR" + i + ".PRODUCT_ID, ");
		}
		
		sql.append("select").append(selectSB);
		sql.append(" MV.ITEM_CODE, MV.SALE_FLAG as FLAG");
		sql.append(", I.UOM_ID, MV.POS_DEPARTMENT");
		sql.append(", NVL(I.PRIVATE_LABEL_IND, 'N') AS PRIVATE_LABEL_IND");

		sql.append(", sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(", sum(MV.QUANTITY) as QUANTITY");
		sql.append(", sum(MV.WEIGHT) as WEIGHT_ACTUAL");
		sql.append(", count(case when MV.WEIGHT=0  then null else 1 end) as WEIGHT");
		sql.append(", sum( MV.PRICE) as REVENUE");
		sql.append(", sum(MV.EXTENDED_GROSS_PRICE) as GROSSREVENUE");
		sql.append(" from MOVEMENT_DAILY MV");
		sql.append(" Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE");
		sql.append(joinSB);
		sql.append(" where  MV.CALENDAR_ID =").append(calendar_id); 
		sql.append(" and COMP_STR_NO='").append(storeNumber).append("'");
		
		//New code to support any level of POS based on configuration
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
		
		if (Integer.parseInt(maxPOS) > 0){
			sql.append(" and MV.POS_DEPARTMENT > 0");
			sql.append(" and MV.POS_DEPARTMENT <= ").append(maxPOS);
		}
		//POS department change ends -- Britto 07/04/2013
		
		sql.append(whereSB);
		//sql.append(" and rownum < 100");
		// Exclude categories for revenue calculation
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" and (I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(") OR I.CATEGORY_ID IS NULL)");
		
		sql.append(" group by ").append(groupSB);
		sql.append(" MV.ITEM_CODE, I.PRIVATE_LABEL_IND, MV.SALE_FLAG, I.UOM_ID, MV.POS_DEPARTMENT");
		sql.append(" order by ").append(orderSB);
		sql.append(" MV.ITEM_CODE, MV.SALE_FLAG");
				
		logger.debug("GetMovementDailyData SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
				"getMovementDailyData");
			
			if (rst.size() > 0 ) {
				while (rst.next()) {

					MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
					
					HashMap<Integer, Integer> parentProductData = new HashMap<Integer, Integer>();
					for (int i=0; i < leastProductLevel.size(); i++ ){
						parentProductData.put(leastProductLevel.get(i) , rst.getInt("PRODUCT_ID" + i));
					}
	
					objMovementDto.setProductData(parentProductData);
					
					objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
					objMovementDto.setFlag(rst.getString("FLAG"));
					objMovementDto.setuomId(rst.getString("UOM_ID"));
					
					objMovementDto.setPosDepartment(
												rst.getInt("POS_DEPARTMENT"));

					if (rst.getString("PRIVATE_LABEL_IND").trim().equalsIgnoreCase("Y"))
						objMovementDto.setPrivateLabel(1);
					else
						objMovementDto.setPrivateLabel(0);
					
					if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
						objMovementDto.setRegularQuantity(rst
								.getDouble("QUANTITY")
								+ rst.getDouble("WEIGHT"));
						objMovementDto.setActualWeight(rst
								.getDouble("WEIGHT_ACTUAL"));
						objMovementDto.setActualQuantity(rst
								.getDouble("QUANTITY"));
						objMovementDto.setregMovementVolume(rst
								.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
						
						objMovementDto.setRevenueRegular(
												rst.getDouble("REVENUE"));

						objMovementDto.setregGrossRevenue(rst
												.getDouble("GROSSREVENUE"));
					}
					if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
						objMovementDto.setSaleQuantity(rst
								.getDouble("QUANTITY")
								+ rst.getDouble("WEIGHT"));
						objMovementDto.setActualWeight(rst
								.getDouble("WEIGHT_ACTUAL"));
						objMovementDto.setActualQuantity(rst
								.getDouble("QUANTITY"));
						objMovementDto.setsaleMovementVolume(rst
								.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
						
						objMovementDto.setRevenueSale(
													rst.getDouble("REVENUE"));
						
						objMovementDto.setsaleGrossRevenue(rst
										.getDouble("GROSSREVENUE"));
					}
					movementDataList.add(objMovementDto);
				}
			}
			else
			{
				logger.warn("There is no movement data");
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
	
	public List<MovementDailyAggregateDTO> getTransationDailyData(
			Connection _Conn, int storeId, int calendar_id, 
		String categoryIdStr, ArrayList<Integer> leastProductLevel, 
	HashMap<Integer, Integer> gasItems) throws GeneralException {

		List<MovementDailyAggregateDTO> movementDataList = 
			new ArrayList<MovementDailyAggregateDTO>();
		
		StringBuffer sql = new StringBuffer();
		StringBuffer selectSB = new StringBuffer();
		StringBuffer joinSB = new StringBuffer();
		StringBuffer whereSB = new StringBuffer();
		StringBuffer groupSB = new StringBuffer();
		StringBuffer orderSB = new StringBuffer();
		
		for (int i=0; i < leastProductLevel.size(); i++ ){
		selectSB.append(" PR" + i +".PRODUCT_ID as PRODUCT_ID" + i + ", ");
		joinSB.append(" LEFT OUTER JOIN PRODUCT_GROUP_RELATION PR");
		joinSB.append(i).append(" ON TL.ITEM_ID = PR"+i+ ".CHILD_PRODUCT_ID");
		joinSB.append(" AND PR" + i +".PRODUCT_LEVEL_ID=" + leastProductLevel.get(i));
		groupSB.append("PR" + i + ".PRODUCT_ID, ");
		orderSB.append("PR" + i + ".PRODUCT_ID, ");
		}
		
		sql.append("select").append(selectSB);
		sql.append(" TL.ITEM_ID AS ITEM_CODE, TL.SALE_TYPE as FLAG");
		sql.append(", I.UOM_ID, NVL(I.PRIVATE_LABEL_IND, 'N') AS PRIVATE_LABEL_IND, TL.POS_DEPARTMENT_ID AS POS_DEPARTMENT");
		sql.append(", SUM(CASE WHEN TTL.EXCLUDE_UNIT_CALC = 'Y' THEN 0 ELSE TL.QUANTITY * I.ITEM_SIZE END) AS VOLUME");
		sql.append(", SUM(CASE WHEN TTL.EXCLUDE_UNIT_CALC = 'Y' THEN 0 ELSE TL.QUANTITY END) AS QUANTITY");
		sql.append(", SUM(CASE WHEN TTL.EXCLUDE_UNIT_CALC = 'Y' THEN 0 ELSE TL.WEIGHT END) AS WEIGHT_ACTUAL");
		sql.append(", SUM(CASE WHEN TTL.EXCLUDE_UNIT_CALC = 'Y' THEN 0 ELSE (CASE WHEN TL.WEIGHT=0 THEN NULL ELSE 1 END) END) AS WEIGHT");
		sql.append(", SUM(CASE WHEN TTL.EXCLUDE_SALES_CALC = 'Y' THEN 0 ELSE TL.NET_AMT END) AS REVENUE");
		sql.append(", SUM(CASE WHEN TTL.EXCLUDE_SALES_CALC = 'Y' THEN 0 ELSE TL.REGULAR_AMT END) AS GROSSREVENUE");
		sql.append(" from TRANSACTION_LOG TL");
		sql.append(" LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TL.TRX_TYPE = TTL.TRX_TYPE");
		sql.append(" Left Join ITEM_LOOKUP I On TL.ITEM_ID=I.ITEM_CODE");
		sql.append(joinSB);
		sql.append(" where  TL.CALENDAR_ID =").append(calendar_id); 
		sql.append(" and STORE_ID=").append(storeId);
		
		//New code to support any level of POS based on configuration
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;
		
		if (Integer.parseInt(maxPOS) > 0){
			sql.append(" and TL.POS_DEPARTMENT_ID >0 ");
			sql.append(" and TL.POS_DEPARTMENT_ID <= ").append(maxPOS);
		}
		
		sql.append(whereSB);
		//sql.append(" and rownum < 100");
		// Exclude categories for revenue calculation
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" and (I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(") OR I.CATEGORY_ID IS NULL)");
		
		sql.append(" group by ").append(groupSB);
		sql.append(" TL.ITEM_ID, I.PRIVATE_LABEL_IND, TL.SALE_TYPE, I.UOM_ID, TL.POS_DEPARTMENT_ID");
		sql.append(" order by ").append(orderSB);
		sql.append(" TL.ITEM_ID, TL.SALE_TYPE");
			
		logger.debug("GetTransationDailyData SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"GetTransationDailyData");
		
		if (rst.size() > 0 ) {
			while (rst.next()) {
		
				if (rst.getInt("ITEM_CODE") == -1)
					continue;
				
				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
				
				HashMap<Integer, Integer> parentProductData = new HashMap<Integer, Integer>();
				for (int i=0; i < leastProductLevel.size(); i++ ){
					parentProductData.put(leastProductLevel.get(i) , rst.getInt("PRODUCT_ID" + i));
				}
		
				objMovementDto.setProductData(parentProductData);
				
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));
				
				objMovementDto.setPosDepartment(
											rst.getInt("POS_DEPARTMENT"));

				if (rst.getString("PRIVATE_LABEL_IND").trim().equalsIgnoreCase("Y"))
					objMovementDto.setPrivateLabel(1);
				else
					objMovementDto.setPrivateLabel(0);				
				
				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					objMovementDto.setRegularQuantity(rst
							.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst
							.getDouble("QUANTITY"));
					objMovementDto.setregMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					
					objMovementDto.setRevenueRegular(
											rst.getDouble("REVENUE"));
		
					objMovementDto.setregGrossRevenue(rst
											.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					objMovementDto.setSaleQuantity(rst
							.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst
							.getDouble("QUANTITY"));
					objMovementDto.setsaleMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					
					objMovementDto.setRevenueSale(
												rst.getDouble("REVENUE"));
					
					objMovementDto.setsaleGrossRevenue(rst
									.getDouble("GROSSREVENUE"));
				}
				movementDataList.add(objMovementDto);
			}
		}
		else
		{
			logger.warn("There is no movement data");
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

	public double getStoreVisitorSummary(Connection _Conn, String storeNumber, 
			int calendar_id, String categoryIdStr, int plFlag) throws GeneralException {
		
		double storeVisitCount =0;
		
		try {
		
			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
			
			StringBuffer sql = new StringBuffer();
		
		sql.append(" select COUNT(DISTINCT M.TRANSACTION_NO) ");
		sql.append(" AS VISIT_COUNT from MOVEMENT_DAILY M");

		if (plFlag == 1){
			sql.append(" JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_CODE");
			sql.append(" AND I.PRIVATE_LABEL_IND = 'Y'");
		}
		else{
			sql.append(" LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_CODE");
		}
		
		sql.append(" where  CALENDAR_ID=").append(calendar_id); 
		sql.append(" and  M.COMP_STR_NO='").append(storeNumber).append("'"); 
		sql.append(" and ( M.POS_DEPARTMENT <=" + maxPOS + " and M.POS_DEPARTMENT <> "+ Constants.POS_GASDEPARTMENT + " )");
		
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(")");
		
		logger.debug("getStoreVisitorSummary SQL: " + sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,	"getStoreVisitorSummary");
		
		while (result.next()) {
		storeVisitCount = result.getInt("VISIT_COUNT");
		}
		
		result.close();
		
		} catch (SQLException sql) {
		logger.error("Error While Fetching Store Visitor Summary " , sql);
		throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
		logger.error("Error While Fetching VisitSummary " , e);
		throw new GeneralException("Error While Fetching VisitSummary" ,e);
		}
		
		return storeVisitCount;
	}
	
	public double getStoreVisitorSummaryTL(Connection _Conn, int storeId, 
			int calendar_id, String categoryIdStr, int plFlag) 
												throws GeneralException {
		
		double storeVisitCount =0;
		
		try {
		
			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
			
			StringBuffer sql = new StringBuffer();
		
		sql.append(" select COUNT(DISTINCT M.TRX_NO) ");
		sql.append(" AS VISIT_COUNT from TRANSACTION_LOG M");

		if (plFlag == 1){
			sql.append(" JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_ID");
			sql.append(" AND I.PRIVATE_LABEL_IND = 'Y'");
		}
		else
			sql.append(" LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_ID");
			
		sql.append(" where  CALENDAR_ID=").append(calendar_id); 
		sql.append(" and  M.STORE_ID=").append(storeId); 
		sql.append(" and ( M.POS_DEPARTMENT_ID <=" + maxPOS + " and M.POS_DEPARTMENT_ID <> "+ Constants.POS_GASDEPARTMENT + " )");
		
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(")");
		
		logger.debug("getStoreVisitorSummaryTL SQL: " + sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,	"getStoreVisitorSummaryTL");
		
		while (result.next()) {
		storeVisitCount = result.getInt("VISIT_COUNT");
		}
		
		result.close();
		
		} catch (SQLException sql) {
		logger.error("Error While Fetching Store Visitor Summary " , sql);
		throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
		logger.error("Error While Fetching VisitSummary " , e);
		throw new GeneralException("Error While Fetching VisitSummary" ,e);
		}
		
		return storeVisitCount;
	}	
	

	public HouseholdDTO getLocationHouseholdSummary(Connection _Conn, 
		int locationLevelId, int locationId, int calendarId, String categoryIdStr, String calendarType) 
													throws GeneralException {
		
		HouseholdDTO householdDTO = new HouseholdDTO(); 
		
		try {
		
			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
			
			StringBuffer sql = new StringBuffer();
			
			sql.append("SELECT");
			sql.append(" COUNT(DISTINCT (CASE WHEN CL.HOUSEHOLD_NUMBER IS NOT NULL THEN 'H' || CL.HOUSEHOLD_NUMBER ELSE 'C' || CL.LOYALTY_CARD_NO END)) AS HOUSEHOLD_COUNT,");
			sql.append(" SUM(TL.NET_AMT) AS HOUSEHOLD_REVENUE,");
			sql.append(" SUM(TL.QUANTITY) AS HOUSEHOLD_UNITS");
			sql.append(" FROM TRANSACTION_LOG TL"); 
			sql.append(" JOIN CUSTOMER_LOYALTY_INFO CL");
			sql.append(" ON CL.CUSTOMER_ID = TL.CUSTOMER_ID");
			sql.append(" AND CL.IS_STORE_CARD = 'N'");
			sql.append(" LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = TL.ITEM_ID");		
			
			if (calendarType.equalsIgnoreCase(Constants.CALENDAR_DAY))
				sql.append(" WHERE CALENDAR_ID = ").append(calendarId);
			else
			{
				sql.append(" WHERE CALENDAR_ID IN (");
				sql.append(" SELECT D.CALENDAR_ID FROM RETAIL_CALENDAR W");
				sql.append(" JOIN RETAIL_CALENDAR D ON D.ROW_TYPE = '").append(Constants.CALENDAR_DAY).append("' AND D.START_DATE BETWEEN W.START_DATE AND W.END_DATE");
				sql.append(" WHERE W.CALENDAR_ID =").append(calendarId).append(")");
			}
			
			if (locationLevelId == Constants.STORE_LEVEL_ID)
				sql.append(" AND TL.STORE_ID=").append(locationId); 
			else if (locationLevelId == Constants.DISTRICT_LEVEL_ID)
				sql.append(" AND TL.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DISTRICT_ID = ").append(locationId).append(")");
			else if (locationLevelId == Constants.REGION_LEVEL_ID)
				sql.append(" AND TL.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE REGION_ID = ").append(locationId).append(")");
			else if (locationLevelId == Constants.DIVISION_LEVEL_ID)
				sql.append(" AND TL.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DIVISION_ID = ").append(locationId).append(")");
			else if (locationLevelId == Constants.CHAIN_LEVEL_ID)
				sql.append(" AND TL.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ").append(locationId).append(")");
			
			sql.append(" AND ( TL.POS_DEPARTMENT_ID <=" + maxPOS + " and TL.POS_DEPARTMENT_ID <> "+ Constants.POS_GASDEPARTMENT + " )");

			if(categoryIdStr != null && !categoryIdStr.isEmpty())
				sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(")");
		
		logger.debug("getLocationHouseholdSummary SQL: " + sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,	"getLocationHouseholdSummary");
		
		while (result.next()) {
			if (result.getObject("HOUSEHOLD_COUNT") != null && result.getDouble("HOUSEHOLD_COUNT") > 0){
				householdDTO.setProductLevelId(0);
				householdDTO.setProductId(0);
				householdDTO.setHouseholdCount(result.getDouble("HOUSEHOLD_COUNT"));
				householdDTO.setTotalRevenue(result.getDouble("HOUSEHOLD_REVENUE"));
				householdDTO.setTotalMovement(result.getDouble("HOUSEHOLD_UNITS"));
				householdDTO.setAverageSpend(householdDTO.getTotalRevenue() / householdDTO.getHouseholdCount());
				householdDTO.setAverageMovement(householdDTO.getTotalMovement() / householdDTO.getHouseholdCount());
			}
		}
		
		result.close();
		
		} catch (SQLException sql) {
		logger.error("Error While Fetching Store Household count " , sql);
		throw new GeneralException("Error While Fetching Household count" ,sql);
		} catch (GeneralException e) {
		logger.error("Error While Fetching Household count " , e);
		throw new GeneralException("Error While Fetching Household count" ,e);
		}
		
		return householdDTO;
	}
	
	public HashMap<String, Double> getCouponBasedAggregationV2(Connection _conn,
			String storeNumber, int calendarid) throws GeneralException {

		HashMap<String, Double> returnMap  = new HashMap<String, Double>();
		
		try {
			StringBuffer sql = new StringBuffer();
			sql.append(" select P.PRODUCT_ID, C.COUPONPRICE  FROM "); 
			sql.append(" (SELECT MV.POS_DEPT, ");
			sql.append(" SUM(DECODE(mv.CPN_WEIGHT, 0, DECODE(mv.CPN_QTY, 0, 1, mv.CPN_QTY ), mv.CPN_WEIGHT) * mv.PRICE) AS COUPONPRICE ");
			sql.append(" FROM MOV_DAILY_COUPON_INFO MV ");
			sql.append(" WHERE MV.CALENDAR_ID =").append(calendarid) ;
			sql.append(" AND MV.COMP_STR_NO   ='").append(storeNumber).append("'") ;
			sql.append(" AND MV.POS_DEPT NOT IN(37)");
			sql.append(" AND MV.CPN_TYPE     <> 6");
			sql.append(" AND MV.UPC = '000000000000'");
			sql.append(" GROUP BY MV.POS_DEPT");
			sql.append(" ORDER BY MV.POS_DEPT) C");
			sql.append(" join PRODUCT_GROUP P ON C.POS_DEPT = P.CODE ");
			sql.append(" where P.PRODUCT_LEVEL_ID=").append(Constants.POSDEPARTMENT);
				
			logger.debug(" Coupon query..... " + sql.toString());
				
				CachedRowSet rst = PristineDBUtil.executeQuery(_conn, sql, "getCouponBasedAggregation");
				
			while (rst.next()) {

				String posId = rst.getString("PRODUCT_ID");
				
				double revenue = rst.getDouble("COUPONPRICE");
				
				// for pos department aggregation
				if (returnMap.containsKey(posId)) {
					sumCouponRevenue(returnMap,
							returnMap.get(posId), posId, revenue);
				} else {
					sumCouponRevenue(returnMap, 0, posId, revenue);
				}
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
			double oldRevenue, String productId, double newrevenue) {

		double totRevenue = oldRevenue + newrevenue;

		returnMap.put(productId, totRevenue);

	}

	public HashMap<String, Integer> getProductVisit(Connection _Conn, 
				String storeNumber, int calendarId, int productLevelId, 
	ArrayList<ProductGroupDTO> childProdutLevelData, int plFlag) throws GeneralException{
	
	HashMap<String, Integer> visitMap = new HashMap<String, Integer>();
	
	try {		
	
	StringBuffer sbjoin = new StringBuffer();
	
	int childLevel = Constants.ITEMLEVELID;
	int previousProductLevel = 0;
	
	if (childProdutLevelData !=null && childProdutLevelData.size() > 0){
	
		for(int i=0; i < childProdutLevelData.size(); i++){
			
			ProductGroupDTO childProductDTO = childProdutLevelData.get(i);
			
			//if (childProductDTO.getProductLevelId() != productLevelId){
	
				if (childProductDTO.getProductLevelId() > Constants.ITEMLEVELID) {
				
					String tableAlias0 = "P" + i;
					String tableAlias1 = "P" + (i + 1);
	
					if (childProductDTO.getChildLevelId() == Constants.ITEMLEVELID) {
						String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
						tableAlias0 + " ON M.ITEM_CODE = "+ tableAlias0 + 
						".CHILD_PRODUCT_ID AND " + tableAlias0 + 
						".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
						
						//logger.debug("JOIN string:"+ joinStr);
						sbjoin.insert(0, joinStr);
					}
					else {
						String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
						tableAlias0 + " ON "+ tableAlias1+ ".PRODUCT_ID = "+ tableAlias0 + 
						".CHILD_PRODUCT_ID AND " + tableAlias0 + 
						".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
						
						//logger.debug("JOIN string:"+ joinStr);
						sbjoin.insert(0, joinStr);
					}
					
					previousProductLevel = childProductDTO.getProductLevelId();
					
				}
		}
	}
	String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");

	StringBuffer sb = new StringBuffer();
	sb.append("select P0.PRODUCT_ID as PRODUCT_ID, COUNT(DISTINCT M.TRANSACTION_NO)");
	sb.append(" as VISIT_COUNT from MOVEMENT_DAILY M");
	
	if (plFlag == 1)
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = M.ITEM_CODE AND IL.PRIVATE_LABEL_IND='Y'");
	
	
	if (childProdutLevelData != null)
		sb.append(sbjoin.toString());
	else {	
		if (childLevel == Constants.ITEMLEVELID){
			sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
			sb.append(" ON M.ITEM_CODE = P0.CHILD_PRODUCT_ID ");
			sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
		}
		else{
			sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
			sb.append(" ON P").append(previousProductLevel);
			sb.append(".PRODUCT_ID = P0.CHILD_PRODUCT_ID ");
			sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
		}
	}
	
	sb.append("	where  M.CALENDAR_ID =").append(calendarId);
	sb.append("	and COMP_STR_NO='").append(storeNumber);
	sb.append("' and M.POS_DEPARTMENT <=" + maxPOS);
	sb.append("	group by P0.PRODUCT_ID order by P0.PRODUCT_ID ");
	
	logger.debug("getProductVisit SQL:" + sb.toString());
	CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
											"GetLeastProductVisit");
	
	if (result.size() >0){
		while (result.next()) {
			visitMap.put(result.getString("PRODUCT_ID"), 
										result.getInt("VISIT_COUNT"));
		}
	}
	
	result.close();
	
	} catch (SQLException sql) {
	logger.error("Error While Fetching Product Visit Info" , sql);
	throw new GeneralException("Error While Fetching VisitSummary" ,sql);
	} catch (GeneralException e) {
	logger.error("Error While Fetching Product Visit Info" , e);
	throw new GeneralException("Error While Fetching Product Visit" ,e);
	}
	
	return visitMap;
	}
	
	public HashMap<String, Integer> getProductVisitTL(Connection _Conn, 
		int storeId, int calendarId, int productLevelId, 
		ArrayList<ProductGroupDTO> childProdutLevelData, int plFlag) 
											throws GeneralException{

		HashMap<String, Integer> visitMap = new HashMap<String, Integer>();

		try {		
		
			StringBuffer sbjoin = new StringBuffer();
			
			int childLevel = Constants.ITEMLEVELID;
			int previousProductLevel = 0;

			if (childProdutLevelData !=null && childProdutLevelData.size() > 0){

				for(int i=0; i < childProdutLevelData.size(); i++){
		
					ProductGroupDTO childProductDTO = childProdutLevelData.get(i);
					
					//if (childProductDTO.getProductLevelId() != productLevelId){
			
						if (childProductDTO.getProductLevelId() > Constants.ITEMLEVELID) {
						
							String tableAlias0 = "P" + i;
							String tableAlias1 = "P" + (i + 1);

							if (childProductDTO.getChildLevelId() == Constants.ITEMLEVELID) {
								String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
								tableAlias0 + " ON M.ITEM_ID = "+ tableAlias0 + 
								".CHILD_PRODUCT_ID AND " + tableAlias0 + 
								".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
								
								//logger.debug("JOIN string:"+ joinStr);
								sbjoin.insert(0, joinStr);
							}
							else {
								String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
								tableAlias0 + " ON "+ tableAlias1+ ".PRODUCT_ID = "+ tableAlias0 + 
								".CHILD_PRODUCT_ID AND " + tableAlias0 + 
								".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
								
								//logger.debug("JOIN string:"+ joinStr);
								sbjoin.insert(0, joinStr);
							}
				
							previousProductLevel = childProductDTO.getProductLevelId();
						}
					}
				}
				String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");

				StringBuffer sb = new StringBuffer();
				sb.append("select P0.PRODUCT_ID as PRODUCT_ID, COUNT(DISTINCT M.TRX_NO)");
				sb.append(" AS VISIT_COUNT FROM TRANSACTION_LOG M");

				if (plFlag == 1)
					sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = M.ITEM_ID AND IL.PRIVATE_LABEL_IND='Y'");
				
				if (childProdutLevelData != null)
					sb.append(sbjoin.toString());
				else {	
					if (childLevel == Constants.ITEMLEVELID){
						sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
						sb.append(" ON M.ITEM_ID = P0.CHILD_PRODUCT_ID ");
						sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
					}
					else{
						sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
						sb.append(" ON P").append(previousProductLevel);
						sb.append(".PRODUCT_ID = P0.CHILD_PRODUCT_ID ");
						sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
					}
				}

				sb.append("	WHERE  M.CALENDAR_ID =").append(calendarId);
				sb.append("	AND STORE_ID=").append(storeId);
				sb.append(" AND M.POS_DEPARTMENT_ID <=" + maxPOS);
				sb.append("	group by P0.PRODUCT_ID order by P0.PRODUCT_ID ");

				logger.debug("getProductVisit SQL:" + sb.toString());
				CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
														"GetLeastProductVisit");

				if (result.size() >0){
					while (result.next()) {
						visitMap.put(result.getString("PRODUCT_ID"), 
													result.getInt("VISIT_COUNT"));
					}
				}

				result.close();

			} catch (SQLException sql) {
				logger.error("Error While Fetching Product Visit Info" , sql);
				throw new GeneralException("Error While Fetching VisitSummary" ,sql);
			} catch (GeneralException e) {
				logger.error("Error While Fetching Product Visit Info" , e);
				throw new GeneralException("Error While Fetching Product Visit" ,e);
			}

		return visitMap;
	}
	
	
	
	
	
	
	
	
	
	
	public HashMap<Integer, HouseholdDTO> getProductHouseholdCount(Connection _Conn, 
				int locationLevelId, int locationId, int calendarId, int productLevelId, 
	ArrayList<ProductGroupDTO> childProdutLevelData, String calendarType) throws GeneralException{
	
	HashMap<Integer, HouseholdDTO> householdMap = new HashMap<Integer, HouseholdDTO>();
	
	try {		
	
	StringBuffer sbjoin = new StringBuffer();
	
	int childLevel = Constants.ITEMLEVELID;
	int previousProductLevel = 0;
	
	if (childProdutLevelData !=null && childProdutLevelData.size() > 0){
	
		for(int i=0; i < childProdutLevelData.size(); i++){
			
			ProductGroupDTO childProductDTO = childProdutLevelData.get(i);
			
			//if (childProductDTO.getProductLevelId() != productLevelId){
	
				if (childProductDTO.getProductLevelId() > Constants.ITEMLEVELID) {
				
					String tableAlias0 = "P" + i;
					String tableAlias1 = "P" + (i + 1);
	
					if (childProductDTO.getChildLevelId() == Constants.ITEMLEVELID) {
						String joinStr = "  JOIN PRODUCT_GROUP_RELATION "+ 
						tableAlias0 + " ON M.ITEM_ID = "+ tableAlias0 + 
						".CHILD_PRODUCT_ID AND " + tableAlias0 + 
						".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
						
						//logger.debug("JOIN string:"+ joinStr);
						sbjoin.insert(0, joinStr);
					}
					else {
						String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
						tableAlias0 + " ON "+ tableAlias1+ ".PRODUCT_ID = "+ tableAlias0 + 
						".CHILD_PRODUCT_ID AND " + tableAlias0 + 
						".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
						
						//logger.debug("JOIN string:"+ joinStr);
						sbjoin.insert(0, joinStr);
					}
					
					previousProductLevel = childProductDTO.getProductLevelId();
					
				}
		}
	}
	
	String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
	
	StringBuffer sb = new StringBuffer();
	sb.append("SELECT P0.PRODUCT_ID AS PRODUCT_ID,");
	sb.append(" COUNT(DISTINCT (CASE WHEN CL.HOUSEHOLD_NUMBER IS NOT NULL THEN 'H' || CL.HOUSEHOLD_NUMBER ELSE 'C' || CL.LOYALTY_CARD_NO END))");
	sb.append(" as HOUSEHOLD_COUNT,");
	sb.append(" SUM(M.NET_AMT) AS HOUSEHOLD_REVENUE,");
	sb.append(" SUM(M.QUANTITY) AS HOUSEHOLD_UNITS");
	sb.append(" FROM TRANSACTION_LOG M");
	sb.append(" JOIN CUSTOMER_LOYALTY_INFO CL ON CL.CUSTOMER_ID = M.CUSTOMER_ID AND CL.IS_STORE_CARD = 'N'");
	
	if (childProdutLevelData != null)
		sb.append(sbjoin.toString());
	else {	
		if (childLevel == Constants.ITEMLEVELID){
			sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
			sb.append(" ON M.ITEM_ID = P0.CHILD_PRODUCT_ID ");
			sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
		}
		else{
			sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
			sb.append(" ON P").append(previousProductLevel);
			sb.append(".PRODUCT_ID = P0.CHILD_PRODUCT_ID ");
			sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
		}
	}
	
	if (calendarType.equalsIgnoreCase(Constants.CALENDAR_DAY) )
		sb.append(" WHERE CALENDAR_ID = ").append(calendarId);
	else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
		sb.append(" WHERE CALENDAR_ID IN (");
		sb.append(" SELECT D.CALENDAR_ID FROM RETAIL_CALENDAR W");
		sb.append(" JOIN RETAIL_CALENDAR D ON D.ROW_TYPE = '").append(Constants.CALENDAR_DAY).append("' AND D.START_DATE BETWEEN W.START_DATE AND W.END_DATE");
		sb.append(" WHERE W.CALENDAR_ID =").append(calendarId).append(")");
	}
	
	if (locationLevelId == Constants.STORE_LEVEL_ID)
		sb.append("	AND M.STORE_ID=").append(locationId);
	else if (locationLevelId == Constants.DISTRICT_LEVEL_ID)
		sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DISTRICT_ID = ").append(locationId).append(")");
	else if (locationLevelId == Constants.REGION_LEVEL_ID)
		sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE REGION_ID = ").append(locationId).append(")");
	else if (locationLevelId == Constants.DIVISION_LEVEL_ID)
		sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DIVISION_ID = ").append(locationId).append(")");
	else if (locationLevelId == Constants.CHAIN_LEVEL_ID)
		sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ").append(locationId).append(")");
	
	sb.append(" AND (M.POS_DEPARTMENT_ID <=").append(maxPOS);
	sb.append(" AND M.POS_DEPARTMENT_ID <> ").append(Constants.POS_GASDEPARTMENT).append(")");
	sb.append("	GROUP BY P0.PRODUCT_ID order by P0.PRODUCT_ID ");
	
	logger.debug("getProductHouseholdCount SQL:" + sb.toString());
	CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
											"getProductHouseholdCount");
	
	if (result.size() >0){
		HouseholdDTO householdObj;
		while (result.next()) {
			if (result.getObject("HOUSEHOLD_COUNT") != null && result.getDouble("HOUSEHOLD_COUNT") > 0){

				householdObj = new HouseholdDTO();
				householdObj.setProductLevelId(productLevelId);
				householdObj.setProductId(result.getInt("PRODUCT_ID"));
				householdObj.setHouseholdCount(result.getDouble("HOUSEHOLD_COUNT"));
				householdObj.setTotalRevenue(result.getDouble("HOUSEHOLD_REVENUE"));
				householdObj.setTotalMovement(result.getDouble("HOUSEHOLD_UNITS"));
				householdObj.setAverageSpend(householdObj.getTotalRevenue() / householdObj.getHouseholdCount());
				householdObj.setAverageMovement(householdObj.getTotalMovement() / householdObj.getHouseholdCount());
				
				householdMap.put(result.getInt("PRODUCT_ID"), householdObj);
			}
		}
	}
	
	result.close();
	
	} catch (SQLException sql) {
	logger.error("Error while fetching product household count data" , sql);
	throw new GeneralException("Error while fetching product household count" ,sql);
	} catch (GeneralException e) {
	logger.error("Error while fetching product household count" , e);
	throw new GeneralException("Error while fetching product household count" ,e);
	}
	
	return householdMap;
	}

	
	
	
	public HashMap<Integer, HouseholdDTO> getHouseholdCount(Connection _Conn, 
				String storeNumber, int calendarId, int productLevelId, 
	ArrayList<ProductGroupDTO> childProdutLevelData, String calendarType) throws GeneralException{
	
	HashMap<Integer, HouseholdDTO> householdMap = new HashMap<Integer, HouseholdDTO>();
	
	try {		
	
	StringBuffer sbjoin = new StringBuffer();
	
	int childLevel = Constants.ITEMLEVELID;
	int previousProductLevel = 0;
	
	if (childProdutLevelData !=null && childProdutLevelData.size() > 0){
	
		for(int i=0; i < childProdutLevelData.size(); i++){
			
			ProductGroupDTO childProductDTO = childProdutLevelData.get(i);
			
			//if (childProductDTO.getProductLevelId() != productLevelId){
	
				if (childProductDTO.getProductLevelId() > Constants.ITEMLEVELID) {
				
					String tableAlias0 = "P" + i;
					String tableAlias1 = "P" + (i + 1);
	
					if (childProductDTO.getChildLevelId() == Constants.ITEMLEVELID) {
						String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
						tableAlias0 + " ON M.ITEM_CODE = "+ tableAlias0 + 
						".CHILD_PRODUCT_ID AND " + tableAlias0 + 
						".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
						
						//logger.debug("JOIN string:"+ joinStr);
						sbjoin.insert(0, joinStr);
					}
					else {
						String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
						tableAlias0 + " ON "+ tableAlias1+ ".PRODUCT_ID = "+ tableAlias0 + 
						".CHILD_PRODUCT_ID AND " + tableAlias0 + 
						".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
						
						//logger.debug("JOIN string:"+ joinStr);
						sbjoin.insert(0, joinStr);
					}
					
					previousProductLevel = childProductDTO.getProductLevelId();
					
				}
			
			//}
			//else{
			//	childLevel = childProductDTO.getChildLevelId();
			//}
		}
	}
	String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
	
	StringBuffer sb = new StringBuffer();
	sb.append("select P0.PRODUCT_ID as PRODUCT_ID, COUNT(DISTINCT CL.HOUSEHOLD_NUMBER)");
	sb.append(" AS HOUSEHOLD_COUNT,");
	sb.append(" SUM(M.NET_AMT) AS HOUSEHOLD_REVENUE,");
	sb.append(" SUM(M.QUANTITY) AS HOUSEHOLD_UNITS");
	sb.append("	FROM TRANSACTION_LOG M");
	sb.append(" JOIN CUSTOMER_LOYALTY_INFO CL ON CL.CUSTOMER_ID = M.CUSTOMER_ID AND CL.IS_STORE_CARD = 'N'");
	
	if (childProdutLevelData != null)
		sb.append(sbjoin.toString());
	else {	
	//if (childProdutLevelData !=null && childProdutLevelData.size() > 0)
	//if (childProdutLevelData == null)
	//	sb.append(sbjoin.toString());
	
		if (childLevel == Constants.ITEMLEVELID){
			sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
			sb.append(" ON M.ITEM_CODE = P0.CHILD_PRODUCT_ID ");
			sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
		}
		else{
			sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
			sb.append(" ON P").append(previousProductLevel);
			sb.append(".PRODUCT_ID = P0.CHILD_PRODUCT_ID ");
			sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
		}
	}
	
	if (calendarType.equalsIgnoreCase(Constants.CALENDAR_DAY) )
		sb.append(" WHERE CALENDAR_ID = ").append(calendarId);
	else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
		sb.append(" WHERE CALENDAR_ID IN (");
		sb.append(" SELECT D.CALENDAR_ID FROM RETAIL_CALENDAR W");
		sb.append(" JOIN RETAIL_CALENDAR D ON D.ROW_TYPE = '").append(Constants.CALENDAR_DAY).append("' AND D.START_DATE BETWEEN W.START_DATE AND W.END_DATE");
		sb.append(" WHERE W.CALENDAR_ID =").append(calendarId).append(")");
	}

	sb.append("	and COMP_STR_NO='").append(storeNumber);
	sb.append("' and M.POS_DEPARTMENT <=" + maxPOS);
	sb.append("	group by P0.PRODUCT_ID order by P0.PRODUCT_ID ");
	
	logger.debug("getProductVisit SQL:" + sb.toString());
	CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
											"GetLeastProductVisit");
	
	if (result.size() >0){
		HouseholdDTO householdObj;
		while (result.next()) {
			householdObj = new HouseholdDTO();
			householdObj.setProductLevelId(productLevelId);
			householdObj.setProductId(result.getInt("PRODUCT_ID"));			
			householdObj.setHouseholdCount(result.getDouble("HOUSEHOLD_COUNT"));
			householdObj.setTotalRevenue(result.getDouble("HOUSEHOLD_COUNT"));
			householdObj.setTotalMovement(result.getDouble("HOUSEHOLD_COUNT"));
			householdMap.put(result.getInt("PRODUCT_ID"), householdObj);
		}
	}
	
	result.close();
	
	} catch (SQLException sql) {
	logger.error("Error While Fetching Product Household Info" , sql);
	throw new GeneralException("Error While Fetching Household Count" ,sql);
	} catch (GeneralException e) {
	logger.error("Error While Fetching Product Visit Info" , e);
	throw new GeneralException("Error While Fetching Household Count" ,e);
	}
	
	return householdMap;
	}

	public HashMap<String, Integer> getProductVisitForFinDept(Connection _Conn, 
		String storeNumber, int calendarId, int productLevelId, int plFlag) throws GeneralException{

		HashMap<String, Integer> visitMap = new HashMap<String, Integer>();
		
		try {		
			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
			
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT PRODUCT_ID, COUNT(DISTINCT TRANSACTION_NO) as VISIT_COUNT FROM (");
			sb.append(" SELECT P.PRODUCT_ID, M.POS_DEPARTMENT, M.TRANSACTION_NO FROM MOVEMENT_DAILY M ");
			sb.append(" JOIN PRODUCT_GROUP_RELATION P ON P.CHILD_PRODUCT_ID = M.POS_DEPARTMENT ");
			sb.append(" AND P.PRODUCT_LEVEL_ID = ").append(productLevelId); 
			sb.append(" WHERE CALENDAR_ID = ").append(calendarId);
			sb.append(" AND M.COMP_STR_NO = '").append(storeNumber).append("'");
			sb.append("	AND M.POS_DEPARTMENT <").append(maxPOS).append(" AND TRANSACTION_NO > 0 )");
			sb.append(" GROUP BY PRODUCT_ID ");
	
			logger.debug("getProductVisitForFinDept SQL:" + sb.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
												"GetLeastProductVisit");
		
			if (result.size() >0){
				while (result.next()) {
					visitMap.put(result.getString("PRODUCT_ID"), 
											result.getInt("VISIT_COUNT"));
				}
			}
		
			result.close();
		
		} catch (SQLException sql) {
			logger.error("Error While Fetching Product Visit Info" , sql);
			throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching Product Visit Info" , e);
			throw new GeneralException("Error While Fetching Product Visit" ,e);
		}
		
	return visitMap;
	}
	
	public HashMap<String, Integer> getProductVisitForFinDeptTL(Connection _Conn, 
			int storeId, int calendarId, int productLevelId, int plFlag) throws GeneralException{

			HashMap<String, Integer> visitMap = new HashMap<String, Integer>();
			
			try {		
				String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
				
				StringBuffer sb = new StringBuffer();
				sb.append("SELECT PRODUCT_ID, COUNT(DISTINCT TRX_NO) as VISIT_COUNT FROM (");
				sb.append(" SELECT P.PRODUCT_ID, M.POS_DEPARTMENT_ID, M.TRX_NO FROM TRANSACTION_LOG M ");
				sb.append(" JOIN PRODUCT_GROUP_RELATION P ON P.CHILD_PRODUCT_ID = M.POS_DEPARTMENT_ID ");
				sb.append(" AND P.PRODUCT_LEVEL_ID = ").append(productLevelId); 
				sb.append(" WHERE CALENDAR_ID = ").append(calendarId);
				sb.append(" AND M.STORE_ID = '").append(storeId).append("'");
				sb.append("	AND M.POS_DEPARTMENT_ID <=").append(maxPOS).append(" AND TRX_NO > 0 )");
				sb.append(" GROUP BY PRODUCT_ID ");
		
				logger.debug("getProductVisitForFinDeptTL SQL:" + sb.toString());
				CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
													"getProductVisitForFinDeptTL");
			
				if (result.size() >0){
					while (result.next()) {
						visitMap.put(result.getString("PRODUCT_ID"), 
												result.getInt("VISIT_COUNT"));
					}
				}
			
				result.close();
			
			} catch (SQLException sql) {
				logger.error("Error While Fetching Product Visit Info" , sql);
				throw new GeneralException("Error While Fetching VisitSummary" ,sql);
			} catch (GeneralException e) {
				logger.error("Error While Fetching Product Visit Info" , e);
				throw new GeneralException("Error While Fetching Product Visit" ,e);
			}
			
		return visitMap;
		}
		
	
	
	public HashMap<Integer, HouseholdDTO> getProductHouseholdCountForFinDept(Connection _Conn, 
			int locationLevelId, int locationId, int calendarId, int productLevelId, String calendarType ) throws GeneralException{

			HashMap<Integer, HouseholdDTO> householdCountMap = new HashMap<Integer, HouseholdDTO>();
			
			try {		
				String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
				
				StringBuffer sb = new StringBuffer();
				sb.append("SELECT P0.PRODUCT_ID AS PRODUCT_ID,");
				sb.append(" COUNT(DISTINCT (CASE WHEN CL.HOUSEHOLD_NUMBER IS NOT NULL THEN 'H' || CL.HOUSEHOLD_NUMBER ELSE 'C' || CL.LOYALTY_CARD_NO END)) AS HOUSEHOLD_COUNT,");
				sb.append(" SUM(M.NET_AMT) AS HOUSEHOLD_REVENUE,");
				sb.append(" SUM(M.QUANTITY) AS HOUSEHOLD_UNITS");
				sb.append(" FROM TRANSACTION_LOG M");
				sb.append(" JOIN CUSTOMER_LOYALTY_INFO CL");
				sb.append(" ON CL.CUSTOMER_ID = M.CUSTOMER_ID");
				sb.append(" AND CL.IS_STORE_CARD = 'N'");
				sb.append(" JOIN PRODUCT_GROUP_RELATION P0");
				sb.append(" ON M.POS_DEPARTMENT_ID = P0.CHILD_PRODUCT_ID");
				sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
				
				if (calendarType.equalsIgnoreCase(Constants.CALENDAR_DAY) )
					sb.append(" WHERE CALENDAR_ID = ").append(calendarId);
				else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
					sb.append(" WHERE CALENDAR_ID IN (");
					sb.append(" SELECT D.CALENDAR_ID FROM RETAIL_CALENDAR W");
					sb.append(" JOIN RETAIL_CALENDAR D ON D.ROW_TYPE = '").append(Constants.CALENDAR_DAY).append("' AND D.START_DATE BETWEEN W.START_DATE AND W.END_DATE");
					sb.append(" WHERE W.CALENDAR_ID =").append(calendarId).append(")");
				}
				
				if (locationLevelId == Constants.STORE_LEVEL_ID)
					sb.append(" AND M.STORE_ID = ").append(locationId);
				else if (locationLevelId == Constants.DISTRICT_LEVEL_ID)
					sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DISTRICT_ID = ").append(locationId).append(")");
				else if (locationLevelId == Constants.REGION_LEVEL_ID)
					sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE REGION_ID = ").append(locationId).append(")");
				else if (locationLevelId == Constants.DIVISION_LEVEL_ID)
					sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DIVISION_ID = ").append(locationId).append(")");
				else if (locationLevelId == Constants.CHAIN_LEVEL_ID)
					sb.append("	AND M.STORE_ID IN (").append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ").append(locationId).append(")");

				sb.append("	AND M.POS_DEPARTMENT_ID <=").append(maxPOS);
				sb.append(" GROUP BY PRODUCT_ID ");
		
				logger.debug("getProductHouseholdCountForFinDept SQL:" + sb.toString());
				CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
													"getProductHouseholdCountForFinDept");
			
				if (result.size() >0){
					HouseholdDTO householdObj;
					while (result.next()) {
						if (result.getObject("HOUSEHOLD_COUNT") != null && result.getDouble("HOUSEHOLD_COUNT") > 0){

							householdObj = new HouseholdDTO();
							
							householdObj.setProductLevelId(productLevelId);
							householdObj.setProductId(result.getInt("PRODUCT_ID"));
							householdObj.setHouseholdCount(result.getDouble("HOUSEHOLD_COUNT"));
							householdObj.setTotalRevenue(result.getDouble("HOUSEHOLD_REVENUE"));
							householdObj.setTotalMovement(result.getDouble("HOUSEHOLD_UNITS"));
							householdObj.setAverageSpend(householdObj.getTotalRevenue() / householdObj.getHouseholdCount());
							householdObj.setAverageMovement(householdObj.getTotalMovement() / householdObj.getHouseholdCount());
							
							householdCountMap.put(result.getInt("PRODUCT_ID"), householdObj);
						}
					}
				}
				result.close();
			
			} catch (SQLException sql) {
				logger.error("Error While Fetching Fin Dept Household Count " , sql);
				throw new GeneralException("Error While Fetching Fin Dept Household Count " ,sql);
			} catch (GeneralException e) {
				logger.error("Error While Fetching Fin Dept Household Count " , e);
				throw new GeneralException("Error While Fetching Fin Dept Household Count " ,e);
			}
			
		return householdCountMap;
		}
	
	// Overloaded method for updating into Movement_Weekly for improved performance
	public void updateWeeklyMovement(Connection conn, List<MovementWeeklyDTO> movementWeeklyDTOList) throws GeneralException {
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY set REVENUE_REGULAR = ?, QUANTITY_REGULAR = ?, REVENUE_SALE = ?, QUANTITY_SALE = ?, ");
		sb.append("TOTAL_MARGIN = ?, MARGIN_PCT = ?, VISIT_COUNT = ?, QTY_REGULAR_13WK = ?, QTY_SALE_13WK = ? WHERE CHECK_DATA_ID = ? ");
		
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int recordCount = 0;
		try{
			statement = conn.prepareStatement(sb.toString());
			
			for(MovementWeeklyDTO movement : movementWeeklyDTOList){
				int counter = 0;
				statement.setDouble(++counter, movement.getRevenueRegular());
				statement.setDouble(++counter, movement.getQuantityRegular());
				statement.setDouble(++counter, movement.getRevenueSale());
				statement.setDouble(++counter, movement.getQuantitySale());
				
				double margin = movement.getMargin();
				if (margin > 99999.99)
					margin = 99999.99;
				else if (margin < -99999.99)
					margin = -99999.99;
				statement.setDouble(++counter, margin);
				
				double marginPct = movement.getMarginPercent();
				if (marginPct > 999.99)
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;
				statement.setDouble(++counter, marginPct);
				
				statement.setInt(++counter, movement.getVisitCount());
				statement.setDouble(++counter, movement.getQuantityRegular13Week());
				statement.setDouble(++counter, movement.getQuantitySale13Week());
				statement.setInt(++counter, movement.getCheckDataId());
				
				itemNoInBatch++;
				statement.addBatch();
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
				recordCount++;
				if(recordCount % 5000 == 0){
					logger.info("Updated " + recordCount + " records");
				}
			}
			if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
			logger.info("Updated " + recordCount + " records");
		}catch(SQLException sqlException){
			logger.error("Error in updateWeeklyMovement - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return;
	}
	

	public void insertWeeklyMovement(Connection conn, List<MovementWeeklyDTO> movementWeeklyDTOList) throws GeneralException {
		StringBuffer sb = new StringBuffer("insert into MOVEMENT_WEEKLY ( ");
		sb.append("COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID, ");
		sb.append("REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE, ");
		sb.append("TOTAL_MARGIN, MARGIN_PCT, VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK, PRICE_ZONE_ID, LOCATION_LEVEL_ID, LOCATION_ID) VALUES");
		sb.append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?");
		
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int recordCount = 0;
		try{
			statement = conn.prepareStatement(sb.toString());
			
			for(MovementWeeklyDTO movement : movementWeeklyDTOList){
				int counter = 0;
				statement.setInt(++counter, movement.getCompStoreId());
				statement.setInt(++counter, movement.getItemCode());
				statement.setInt(++counter, movement.getCheckDataId());
				statement.setDouble(++counter, movement.getRevenueRegular());
				statement.setDouble(++counter, movement.getQuantityRegular());
				statement.setDouble(++counter, movement.getRevenueSale());
				statement.setDouble(++counter, movement.getQuantitySale());
				
				double margin = movement.getMargin();
				if (margin > 99999.99)
					margin = 99999.99;
				else if (margin < -99999.99)
					margin = -99999.99;
				statement.setDouble(++counter, margin);
				
				double marginPct = movement.getMarginPercent();
				if (marginPct > 999.99)
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;
				statement.setDouble(++counter, marginPct);
				
				statement.setInt(++counter, movement.getVisitCount());
				statement.setDouble(++counter, movement.getQuantityRegular13Week());
				statement.setDouble(++counter, movement.getQuantitySale13Week());
				statement.setInt(++counter, movement.getPriceZoneId());
				statement.setInt(++counter, movement.getLocationLevelId());
				statement.setInt(++counter, movement.getLocationId());
				
				itemNoInBatch++;
				statement.addBatch();
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
				recordCount++;
				if(recordCount % 5000 == 0){
					logger.info("Inserted " + recordCount + " records");
				}
			}
			if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
			logger.info("Inserted " + recordCount + " records");
		}catch(SQLException sqlException){
			logger.error("Error in insertWeeklyMovement - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return;
	}
	
	public HashMap<Integer, MovementWeeklyDTO> getCostData(Connection conn, int scheduleId){
		long startTime = System.currentTimeMillis();
		StringBuffer sb = new StringBuffer("select cd.item_code, cd.check_data_id, mw.list_cost, mw.deal_cost, ");
		sb.append("unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack) reg_price, ");
		sb.append("unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack) sale_price from ");
		sb.append("competitive_data cd, movement_weekly mw where cd.schedule_id = ? ");
		sb.append("and cd.check_data_id = mw.check_data_id");
	
		PreparedStatement statement = null;
		ResultSet result = null;
		HashMap<Integer, MovementWeeklyDTO> costMap = new HashMap<Integer, MovementWeeklyDTO>();
		try{
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, scheduleId);
			result = statement.executeQuery();
			
			while(result.next()){
				MovementWeeklyDTO movementWeeklyDTO = new MovementWeeklyDTO();
				movementWeeklyDTO.setItemCode(result.getInt("ITEM_CODE"));
				movementWeeklyDTO.setCheckDataId(result.getInt("CHECK_DATA_ID"));
				movementWeeklyDTO.setListCost(result.getDouble("LIST_COST"));
				movementWeeklyDTO.setDealCost(result.getDouble("DEAL_COST"));
				movementWeeklyDTO.setRegUnitPrice(result.getDouble("REG_PRICE"));
				movementWeeklyDTO.setSaleUnitPrice(result.getDouble("SALE_PRICE"));
				
				costMap.put(movementWeeklyDTO.getItemCode(), movementWeeklyDTO);
			}
		}catch(SQLException sqlException){
			logger.error("Error in getCostData - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Time taken to retrieve cost data - " + (endTime - startTime) + " ms");
		return costMap;
	}
	
	public HashMap<Integer, Integer> getCompDataLIGForSch(Connection conn, int scheduleId){
		long startTime = System.currentTimeMillis();
		StringBuffer sb = new StringBuffer("select item_code, check_data_id from ");
		sb.append("competitive_data_lig where schedule_id = ? ");
		
		PreparedStatement statement = null;
		ResultSet result = null;
		HashMap<Integer, Integer> checkDataIdMap = new HashMap<Integer, Integer>();
		try{
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, scheduleId);
			result = statement.executeQuery();
			
			while(result.next()){
				checkDataIdMap.put(result.getInt("ITEM_CODE"), result.getInt("CHECK_DATA_ID"));
			}
		}catch(SQLException sqlException){
			logger.error("Error in getCompDataLIGForSch - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Time taken to retrieve checkDataId from Comp Data LIG table - " + (endTime - startTime) + " ms");
		return checkDataIdMap;
	}
	
	public List<Integer> getMovementLIGCheckDataId(Connection conn, int scheduleId){
		long startTime = System.currentTimeMillis();
		StringBuffer sb = new StringBuffer("select mw.check_data_id from ");
		sb.append("competitive_data_lig cd, movement_weekly_lig mw where cd.schedule_id = ? ");
		sb.append("and cd.check_data_id = mw.check_data_id");
	
		PreparedStatement statement = null;
		ResultSet result = null;
		List<Integer> checkDataIdMap = new ArrayList<Integer>();
		try{
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, scheduleId);
			result = statement.executeQuery();
			
			while(result.next()){
				checkDataIdMap.add(result.getInt("CHECK_DATA_ID"));
			}
		}catch(SQLException sqlException){
			logger.error("Error in getMovementLIGCheckDataId - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Number of Check Data Id retrieved from Movement LIG - " + checkDataIdMap.size());
		logger.debug("Time taken to retrieve checkDataId from Movement LIG table - " + (endTime - startTime) + " ms");
		return checkDataIdMap;
	}
	
	public void updateWeeklyLIGMovement(Connection conn, List<MovementWeeklyDTO> movementWeeklyDTOList){
		StringBuffer sb = new StringBuffer("update MOVEMENT_WEEKLY_LIG set REVENUE_REGULAR = ?, QUANTITY_REGULAR = ?, REVENUE_SALE = ?, QUANTITY_SALE = ?, ");
		sb.append("LIST_COST = ?, EFF_LIST_COST_DATE = TO_DATE(?, 'YYYYMMDD'),  DEAL_COST = ?, DEAL_START_DATE = TO_DATE(?, 'YYYYMMDD'), DEAL_END_DATE = TO_DATE(?, 'YYYYMMDD'), ");
		sb.append("COST_CHG_DIRECTION = ?, MARGIN_CHG_DIRECTION = ?, MARGIN_PCT = ?, TOTAL_MARGIN = ?, VISIT_COUNT = ? where CHECK_DATA_ID = ? ");
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int recordCount = 0;
		try{
			statement = conn.prepareStatement(sb.toString());
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			
			for(MovementWeeklyDTO movement : movementWeeklyDTOList){
				int counter = 0;
				statement.setDouble(++counter, movement.getRevenueRegular());
				statement.setDouble(++counter, movement.getQuantityRegular());
				statement.setDouble(++counter, movement.getRevenueSale());
				statement.setDouble(++counter, movement.getQuantitySale());
				statement.setDouble(++counter, movement.getListCost());
				
				String dateStr;
				if (movement.getEffListCostDate() != null) {
					dateStr = formatter.format(movement.getEffListCostDate());
					statement.setString(++counter, dateStr);
				}else{
					statement.setString(++counter, null);
				}
				
				statement.setDouble(++counter, movement.getDealCost());
				if (movement.getDealStartDate() != null) {
					dateStr = formatter.format(movement.getDealStartDate());
					statement.setString(++counter, dateStr);
				}else{
					statement.setString(++counter, null);
				}
				if (movement.getDealEndDate() != null) {
					dateStr = formatter.format(movement.getDealEndDate());
					statement.setString(++counter, dateStr);
				}else{
					statement.setString(++counter, null);
				}
				
				statement.setInt(++counter, movement.getCostChangeDirection());
				statement.setInt(++counter, movement.getMarginChangeDirection());
				
				double marginPct = movement.getMarginPercent();
				if (marginPct > 999.99)
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;
				statement.setDouble(++counter, marginPct);
				
				statement.setDouble(++counter, movement.getMargin());	
				statement.setInt(++counter, movement.getVisitCount());
				statement.setInt(++counter, movement.getCheckDataId());
				
				itemNoInBatch++;
				statement.addBatch();
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
				recordCount++;
				if(recordCount % 5000 == 0){
					logger.info("Updated " + recordCount + " records");
				}
			}
			if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
			logger.info("Updated " + recordCount + " records");
		}catch(SQLException sqlException){
			logger.error("Error in updateWeeklyLIGMovement - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return;
	}
	
	public void insertWeeklyLIGMovement(Connection conn, List<MovementWeeklyDTO> movementWeeklyDTOList){
		StringBuffer sb = new StringBuffer("insert into MOVEMENT_WEEKLY_LIG (COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID, REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE, ");
		sb.append("LIST_COST, EFF_LIST_COST_DATE,  DEAL_COST, DEAL_START_DATE, DEAL_END_DATE, ");
		sb.append("COST_CHG_DIRECTION, MARGIN_CHG_DIRECTION, MARGIN_PCT, TOTAL_MARGIN, VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK, PRICE_ZONE_ID, ");
		sb.append(" LOCATION_LEVEL_ID, LOCATION_ID) VALUES ");
		sb.append("(?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'YYYYMMDD'), ?, TO_DATE(?, 'YYYYMMDD'), TO_DATE(?, 'YYYYMMDD'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int recordCount = 0;
		try{
			statement = conn.prepareStatement(sb.toString());
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			
			for(MovementWeeklyDTO movement : movementWeeklyDTOList){
				int counter = 0;
				statement.setInt(++counter, movement.getCompStoreId());
				statement.setInt(++counter, movement.getItemCode());
				statement.setInt(++counter, movement.getCheckDataId());
				statement.setDouble(++counter, movement.getRevenueRegular());
				statement.setDouble(++counter, movement.getQuantityRegular());
				statement.setDouble(++counter, movement.getRevenueSale());
				statement.setDouble(++counter, movement.getQuantitySale());
				statement.setDouble(++counter, movement.getListCost());
				
				String dateStr;
				if (movement.getEffListCostDate() != null) {
					dateStr = formatter.format(movement.getEffListCostDate());
					statement.setString(++counter, dateStr);
				}else{
					statement.setString(++counter, null);
				}
				
				statement.setDouble(++counter, movement.getDealCost());
				if (movement.getDealStartDate() != null) {
					dateStr = formatter.format(movement.getDealStartDate());
					statement.setString(++counter, dateStr);
				}else{
					statement.setString(++counter, null);
				}
				if (movement.getDealEndDate() != null) {
					dateStr = formatter.format(movement.getDealEndDate());
					statement.setString(++counter, dateStr);
				}else{
					statement.setString(++counter, null);
				}
				
				statement.setInt(++counter, movement.getCostChangeDirection());
				statement.setInt(++counter, movement.getMarginChangeDirection());
				
				double marginPct = movement.getMarginPercent();
				if (marginPct > 999.99)
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;
				statement.setDouble(++counter, marginPct);
				
				statement.setDouble(++counter, movement.getMargin());	
				statement.setInt(++counter, movement.getVisitCount());
				statement.setDouble(++counter, movement.getQuantityRegular13Week());
				statement.setDouble(++counter, movement.getQuantitySale13Week());
				statement.setInt(++counter, movement.getPriceZoneId());
				statement.setInt(++counter, movement.getLocationLevelId());
				statement.setInt(++counter, movement.getLocationId());
				
				itemNoInBatch++;
				//logger.info(movement.getRevenueRegular() + "\t" + movement.getQuantityRegular() + "\t" + movement.getRevenueSale() + "\t" + movement.getQuantitySale() + "\t" + marginPct + "\t" + movement.getMargin() + "\t" + movement.getVisitCount());
				statement.addBatch();
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
				recordCount++;
				if(recordCount % 5000 == 0){
					logger.info("Inserted " + recordCount + " records");
				}
			}
			if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
			logger.info("Inserted " + recordCount + " records");
		}catch(SQLException sqlException){
			logger.error("Error in insertWeeklyLIGMovement - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return;
	}
	
	
	public HashMap<String, Integer> getPOSVisit(Connection _Conn, 
			String storeNumber, int calendarId, int plFlag) throws GeneralException{

		HashMap<String, Integer> visitMap = new HashMap<String, Integer>();
		
		try {		
			
			String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0");
		
			StringBuffer sb = new StringBuffer();
			sb.append("select M.POS_DEPARTMENT as PRODUCT_ID, COUNT(DISTINCT M.TRANSACTION_NO)");
			sb.append(" as VISIT_COUNT from MOVEMENT_DAILY M");
			sb.append("	where  M.CALENDAR_ID =").append(calendarId);
			sb.append("	and COMP_STR_NO='").append(storeNumber);
			sb.append("' and M.POS_DEPARTMENT <=" + maxPOS);
			sb.append("	group by POS_DEPARTMENT order by POS_DEPARTMENT ");
			
			logger.debug("getPOSVisit SQL:" + sb.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
												"GetLeastProductVisit");
		
			if (result.size() >0){
				while (result.next()) {
					visitMap.put(result.getString("PRODUCT_ID"), 
												result.getInt("VISIT_COUNT"));
				}
			}
		
			result.close();
	
		} catch (SQLException sql) {
			logger.error("Error While Fetching POS Visit Info" , sql);
			throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching POS Visit Info" , e);
			throw new GeneralException("Error While Fetching Product Visit" ,e);
		}
	
		return visitMap;
	}

	
	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */
	private String InsertTransactionLogSQL(String targetTable) {

		StringBuffer Sql = new StringBuffer();
		Sql.append("insert into " ).append(targetTable).append(" (");
		Sql.append(" CALENDAR_ID, STORE_ID, ITEM_ID, TRX_NO, "); 
		Sql.append(" UNIT_PRICE, QUANTITY, WEIGHT, NET_AMT,"); 
		Sql.append(" SALE_TYPE, POS_DEPARTMENT_ID, CUSTOMER_ID, "); 
		Sql.append(" REGULAR_AMT, STORE_COUPON_USED, MFR_COUPON_USED, ");
		Sql.append("  CARD_DISCOUNT_AMT, AD_DISCOUNT_AMT, ");
		Sql.append(" OTHER_DISCOUNT_AMT, TRX_TIME, TRACK_LOG_ID) ");
		Sql.append(" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,");
		Sql.append(" ?, ?, ?, ?, To_DATE(?, 'YYYYMMDDHH24MI'), ?)");
		logger.debug("InsertTransactionLofSQL " + Sql.toString());
		return Sql.toString();
	}
	
	public void insertTransactionLog(Connection _Conn,
		ArrayList<TransactionLogDTO> transList, String targetTable, int trackId) 
											throws GeneralException {
		try {

			PreparedStatement psmt = _Conn
					.prepareStatement(InsertTransactionLogSQL(targetTable)); 

			TransactionLogDTO objMDto;

			for (int recordCnt = 0; recordCnt < transList.size(); recordCnt++) {

				objMDto = transList.get(recordCnt);
				addTransactionLogData(objMDto, psmt, trackId);
			}
			psmt.executeBatch();
			psmt.close();
			_Conn.commit();
			transList = null;

		} catch (Exception se) {
			logger.error("insertTransactionLog() - Error while batch insert in TransactionLog " + se.getMessage());
			throw new GeneralException("insertTransactionLog", se);
		}
	}
	

	/*
	 * ****************************************************************
	 * Method to Create the insert script and add the script into batch List
	 * Argument 1 : transLogDto
	 * 
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */
	public void addTransactionLogData(TransactionLogDTO transLogDto, PreparedStatement psmt, int trackId)
			throws GeneralException {

		try {
			// CALENDAR_ID
			psmt.setObject(1, transLogDto.getCalendarId());
			
			// COMP_STR_NO
			psmt.setObject(2, transLogDto.getCompStoreId());

			// ITEM_CODE
			psmt.setObject(3, transLogDto.getItemCode());

			//TRX_NO, UNIT_PRICE, QUANTITY, WEIGHT, 
			// TRANSACTION_NO
			psmt.setObject(4, transLogDto.getTransactionNo());

			// UNIT_PRICE
			double unitPrice = transLogDto.getItemNetPrice();
			psmt.setObject(5, unitPrice);

			// QUANTITY
			psmt.setObject(6, transLogDto.getQuantity());		
			
			// WEIGHT
			psmt.setObject(7, transLogDto.getWeight());			
			
			// PRICE
			double price = transLogDto.getExtendedNetPrice();
			psmt.setObject(8, price);

			double regularPrice = transLogDto.getExtendedGrossPrice();
			
			// SALE_FLAG
			if ((0 <= price) && (price < regularPrice)) {
				psmt.setObject(9, "Y");
			} else {
				psmt.setObject(9, "N");
			}
			
			// pos Department
			psmt.setObject(10, transLogDto.getPOSDepartmentId());

			// CUSTOMER_Id
			if (transLogDto.getCustomerId() >0)
				psmt.setObject(11, transLogDto.getCustomerId());
			else
				psmt.setNull(11, Types.INTEGER);
			
			//REGULAR_AMT, STORE_COUPON_USED, MFR_COUPON_USED, PROCESSED_FLAG, TRX_TIME
			// Extended Gross Price
			psmt.setObject(12, regularPrice);

			// Store coupon used
			if (transLogDto.getStoreCouponUsed() == null)
				psmt.setNull(13, Types.CHAR); 
			else
				psmt.setObject(13, transLogDto.getStoreCouponUsed());

			// mfr coupon used
			if (transLogDto.getMfrCouponUsed() == null)
				psmt.setNull(14, Types.CHAR); 
			else				
				psmt.setObject(14, transLogDto.getMfrCouponUsed());

			// Card discount Amt
			if (transLogDto.getCardDiscountAmt() == 0)
				psmt.setNull(15, Types.INTEGER); 
			else				
				psmt.setObject(15, transLogDto.getCardDiscountAmt());

			if (transLogDto.getAdDiscountAmt() == 0)
				psmt.setNull(16, Types.INTEGER); 
			else				
				psmt.setObject(16, transLogDto.getAdDiscountAmt());

			if (transLogDto.getOtherDiscountAmt() == 0)
				psmt.setNull(17, Types.INTEGER); 
			else				
				psmt.setObject(17, transLogDto.getOtherDiscountAmt());
			
			// POS_TIMESTAMP
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
			String dateStr = formatter.format(transLogDto.getTransationTimeStamp());
			psmt.setObject(18, dateStr);

			//Track Id
			psmt.setObject(19, trackId);
			
			psmt.addBatch();

		} catch (Exception er) {
			logger.error("addTransactionLogData() - Exception=" + er);
			throw new GeneralException("addTransactionLogData", er);

		}
	}
	
	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */
	private String InsertExceptionTLogSQL() {

		StringBuffer Sql = new StringBuffer();
		Sql.append("insert into TLOG_EXCEPTION_TRX (");
		Sql.append(" COMP_STR_NO, ITEM_UPC, TRX_NO, UNIT_PRICE, QUANTITY, ");
		Sql.append(" WEIGHT, NET_AMT, SALE_TYPE, POS_DEPARTMENT_ID,  ");
		Sql.append(" CUSTOMER_CARD_NO, REGULAR_AMT, STORE_COUPON_USED, ");
		Sql.append(" MFR_COUPON_USED, CARD_DISCOUNT_AMT, AD_DISCOUNT_AMT, ");
		Sql.append(" OTHER_DISCOUNT_AMT, TRX_TIME, TRACK_LOG_ID," );
		Sql.append(" ROW_NUMBER, ERROR_DESC )");
		Sql.append(" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,");
		Sql.append(" ?, ?, ?, To_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?)");
		logger.debug("InsertExceptionTLogSQL " + Sql.toString());
		return Sql.toString();
	}
	
	public void insertErrorTLog(Connection _Conn,
			ArrayList<TransactionLogDTO> transList, String targetTable, int trackId) 
											throws GeneralException {
		try {

			PreparedStatement psmt = _Conn
					.prepareStatement(InsertExceptionTLogSQL());

			TransactionLogDTO objMDto = new TransactionLogDTO();

			for (int recordCnt = 0; recordCnt < transList.size(); recordCnt++) {

				objMDto = transList.get(recordCnt);
				addExceptionTLogData(objMDto, psmt , trackId);
			}
			psmt.executeBatch();
			psmt.close();
			_Conn.commit();
			transList = null;

		} catch (Exception se) {
			logger.debug(se);
			logger.error("readAndLoadMovements() - Failed to save error records!  Exception=", se);
			
			// Don't throw but continue operation in main method
			//throw new GeneralException("insertTransactionLog() - Exception=", se);
		}

	}
	

	/*
	 * ****************************************************************
	 * Method to Create the insert script and add the script into batch List
	 * Argument 1 : transLogDto
	 * 
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */
	public void addExceptionTLogData(TransactionLogDTO transLogDto, 
										PreparedStatement psmt, int trackId)
			throws GeneralException {

		try {
			// COMP_STR_NO
			psmt.setObject(1, transLogDto.getCompStoreNumber());
			
			// UPC
			String upc = PrestoUtil.castUPC(transLogDto.getItemUPC(), true);
			psmt.setObject(2, upc);

			//TRX_NO, UNIT_PRICE, QUANTITY, WEIGHT, 
			// TRANSACTION_NO
			psmt.setObject(3, transLogDto.getTransactionNo());

			// UNIT_PRICE
			double unitPrice = transLogDto.getItemNetPrice();
			psmt.setObject(4, unitPrice);

			// QUANTITY
			psmt.setObject(5, transLogDto.getQuantity());		
			
			// WEIGHT
			psmt.setObject(6, transLogDto.getWeight());			
			
			// PRICE
			double price = transLogDto.getExtendedNetPrice();
			psmt.setObject(7, price);

			double regularPrice = transLogDto.getExtendedGrossPrice();
			// SALE_FLAG
			if ((0 <= price) && (price < regularPrice)) {
				psmt.setObject(8, "Y");
			} else {
				psmt.setObject(8, "N");
			}
			
			// pos Department
			psmt.setObject(9, transLogDto.getPOSDepartmentId());

			// CUSTOMER_Card Number
			psmt.setObject(10, transLogDto.getCustomerCardNo());
			
			//REGULAR_AMT, STORE_COUPON_USED, MFR_COUPON_USED, PROCESSED_FLAG, TRX_TIME
			// Extended Gross Price
			psmt.setObject(11, regularPrice);

			// Store coupon used
			if (transLogDto.getStoreCouponUsed() == null)
				psmt.setNull(12, Types.CHAR); 
			else
				psmt.setObject(12, transLogDto.getStoreCouponUsed());

			// mfr coupon used
			if (transLogDto.getMfrCouponUsed() == null)
				psmt.setNull(13, Types.CHAR); 
			else				
				psmt.setObject(13, transLogDto.getMfrCouponUsed());

			// Card discount Amt
			if (transLogDto.getCardDiscountAmt() == 0)
				psmt.setNull(14, Types.INTEGER); 
			else				
				psmt.setObject(14, transLogDto.getCardDiscountAmt());

			if (transLogDto.getAdDiscountAmt() == 0)
				psmt.setNull(15, Types.INTEGER); 
			else				
				psmt.setObject(15, transLogDto.getAdDiscountAmt());

			if (transLogDto.getOtherDiscountAmt() == 0)
				psmt.setNull(16, Types.INTEGER); 
			else				
				psmt.setObject(16, transLogDto.getOtherDiscountAmt());
			
			// POS_TIMESTAMP
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
			String dateStr = formatter.format(transLogDto.getTransationTimeStamp());
			psmt.setObject(17, dateStr);

			//Track Id
			psmt.setObject(18, trackId);
			
			//Processed row number
			psmt.setObject(19, transLogDto.getProcessRow());

			//Error message
			psmt.setObject(20, transLogDto.getErrorMessage());
			
			
			psmt.addBatch();

		} catch (Exception sql) {
			logger.debug(sql);
			throw new GeneralException("addExceptionTLogData() - Exception=", sql);

		}
	}

	public List<MovementDailyAggregateDTO> getTransDataForHouseholdWeekly(
			Connection _Conn, int storeId, int calendar_id, 
		String categoryIdStr, RetailCalendarDTO objCalendarDto) throws GeneralException {

		//New code to support any level of POS based on configuration
		String maxPOS = PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0") ;		
		
		List<MovementDailyAggregateDTO> movementDataList = 
			new ArrayList<MovementDailyAggregateDTO>();
		
		StringBuffer sql = new StringBuffer();
		
		sql.append("SELECT TL.CUSTOMER_ID,");
		sql.append(" TL.TRX_NO,");
		sql.append(" TL.ITEM_ID AS ITEM_CODE,");
		sql.append(" TL.SALE_TYPE AS FLAG, I.UOM_ID,");
		sql.append(" NVL(I.PRIVATE_LABEL_IND, 'N')  AS PRIVATE_LABEL_IND,");
		sql.append(" TL.POS_DEPARTMENT_ID AS POS_DEPARTMENT,");
		sql.append(" TL.QUANTITY * I.ITEM_SIZE AS VOLUME,");
		sql.append(" TL.QUANTITY,");
		sql.append(" TL.WEIGHT AS WEIGHT_ACTUAL,");
		sql.append(" CASE WHEN TL.WEIGHT=0 THEN NULL ELSE 1 END AS WEIGHT,");
		sql.append(" TL.NET_AMT AS REVENUE,");
		sql.append(" TL.REGULAR_AMT AS GROSSREVENUE");		
		sql.append(" FROM TRANSACTION_LOG_TEMP TL");
		sql.append(" LEFT JOIN ITEM_LOOKUP I ON TL.ITEM_ID = I.ITEM_CODE");
		sql.append(" WHERE CALENDAR_ID IN");
		sql.append(" (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE");
		sql.append(" START_DATE BETWEEN To_Date('").append(objCalendarDto.getStartDate()).append("', 'dd-MM-yyyy')");
		sql.append(" AND to_date('").append(objCalendarDto.getEndDate()).append("', 'dd-MM-yyyy') AND ROW_TYPE = 'D')");
		sql.append(" AND TL.STORE_ID = ").append(storeId);
		sql.append(" AND TL.ITEM_ID > 0 AND TL.CUSTOMER_ID > 0");
		
		if (Integer.parseInt(maxPOS) > 0){
			sql.append(" and TL.POS_DEPARTMENT_ID >0 ");
			sql.append(" and TL.POS_DEPARTMENT_ID <= ").append(maxPOS);
		}
		
		// Exclude categories for revenue calculation
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" and (I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(") OR I.CATEGORY_ID IS NULL)");
		
		// TempORARY SQL
		sql.append(" AND ROWNUM < 101");
		
		sql.append(" order by TL.CUSTOMER_ID, TL.TRX_NO, TL.ITEM_ID");
			
		logger.debug("GetTransDataForHouseholdWeekly SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"getTransDataForHouseholdWeekly");
		
		if (rst.size() > 0 ) {
			while (rst.next()) {
		
				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
				
				HashMap<Integer, Integer> parentProductData = new HashMap<Integer, Integer>();
		
				objMovementDto.setProductData(parentProductData);
				
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));
				objMovementDto.setHouseholdNumber(rst.getInt("CUSTOMER_ID"));
				logger.debug("HH:" + rst.getString("CUSTOMER_ID"));
				
				objMovementDto.setPosDepartment(
											rst.getInt("POS_DEPARTMENT"));

				if (rst.getString("PRIVATE_LABEL_IND").trim().equalsIgnoreCase("Y"))
					objMovementDto.setPrivateLabel(1);
				else
					objMovementDto.setPrivateLabel(0);				
				
				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					objMovementDto.setRegularQuantity(rst
							.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst
							.getDouble("QUANTITY"));
					objMovementDto.setregMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					
					objMovementDto.setRevenueRegular(
											rst.getDouble("REVENUE"));
		
					objMovementDto.setregGrossRevenue(rst
											.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					objMovementDto.setSaleQuantity(rst
							.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst
							.getDouble("QUANTITY"));
					objMovementDto.setsaleMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					
					objMovementDto.setRevenueSale(
												rst.getDouble("REVENUE"));
					
					objMovementDto.setsaleGrossRevenue(rst
									.getDouble("GROSSREVENUE"));
				}
				movementDataList.add(objMovementDto);
			}
		}
		else
		{
			logger.warn("There is no movement data");
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
	

	
	
	public List<MovementDailyAggregateDTO> getTransDataForHouseholdDaily(
			Connection _Conn, HouseholdSummaryDailyInDTO inDto) throws GeneralException {

		//New code to support any level of POS based on configuration
		
		List<MovementDailyAggregateDTO> movementDataList = 
			new ArrayList<MovementDailyAggregateDTO>();
		
		StringBuffer sql = new StringBuffer();
		
		sql.append("SELECT TL.CUSTOMER_ID,");
		sql.append(" TL.TRX_NO,");
		sql.append(" TL.ITEM_ID AS ITEM_CODE,");
		sql.append(" TL.SALE_TYPE AS FLAG, I.UOM_ID,");
		sql.append(" NVL(I.PRIVATE_LABEL_IND, 'N')  AS PRIVATE_LABEL_IND,");
		sql.append(" TL.POS_DEPARTMENT_ID AS POS_DEPARTMENT,");
		sql.append(" TL.QUANTITY * I.ITEM_SIZE AS VOLUME,");
		sql.append(" TL.QUANTITY,");
		sql.append(" TL.WEIGHT AS WEIGHT_ACTUAL,");
		sql.append(" CASE WHEN TL.WEIGHT=0 THEN 0 ELSE 1 END AS WEIGHT,");
		sql.append(" TL.NET_AMT AS REVENUE,");
		sql.append(" TL.REGULAR_AMT AS GROSSREVENUE");		
		sql.append(" FROM transaction_log TL");
		sql.append(" LEFT JOIN ITEM_LOOKUP I ON TL.ITEM_ID = I.ITEM_CODE");
		sql.append(" WHERE CALENDAR_ID =").append(inDto.getcalendarId()) ;
		sql.append(" AND TL.STORE_ID = ").append(inDto.getStoreId());
		sql.append(" AND TL.ITEM_ID > 0 AND TL.NET_AMT !=0");
		
		if (inDto.getMaxPos() > 0){
			sql.append(" AND TL.POS_DEPARTMENT_ID >0 ");
			sql.append(" AND TL.POS_DEPARTMENT_ID <= ").append(inDto.getMaxPos());
		}

		// Exclude POS for revenue calculation		
		if(inDto.getExcludePOS() != null && !inDto.getExcludePOS().isEmpty())
			sql.append(" AND TL.POS_DEPARTMENT_ID NOT IN (").append(inDto.getExcludePOS()).append(")");
		
		// Exclude categories for revenue calculation
		if(inDto.getExcludeCategory() != null && !inDto.getExcludeCategory().isEmpty())
			sql.append(" AND (I.CATEGORY_ID NOT IN (").append(inDto.getExcludeCategory()).append(") OR I.CATEGORY_ID IS NULL)");
		
		sql.append(" ORDER BY TL.CUSTOMER_ID, TL.TRX_NO, TL.ITEM_ID");
			
		logger.debug("getTransDataForHouseholdDaily SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"getTransDataForHouseholdDaily");
		
		if (rst.size() > 0 ) {
			while (rst.next()) {
		
				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
				
				HashMap<Integer, Integer> parentProductData = new HashMap<Integer, Integer>();
		
				objMovementDto.setProductData(parentProductData);
				
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));
				objMovementDto.setHouseholdNumber(rst.getInt("CUSTOMER_ID"));
				objMovementDto.setPosDepartment(rst.getInt("POS_DEPARTMENT"));
				objMovementDto.settransactionNo(rst.getString("TRX_NO"));
				
				if (rst.getString("PRIVATE_LABEL_IND").trim().equalsIgnoreCase("Y"))
					objMovementDto.setPrivateLabel(1);
				else
					objMovementDto.setPrivateLabel(0);				
				
				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					
					if (rst.getDouble("WEIGHT") > 0){
						objMovementDto.setRegularQuantity(rst.getDouble("WEIGHT"));
						objMovementDto.setregMovementVolume(rst.getDouble("WEIGHT"));
					}
					else{
						objMovementDto.setRegularQuantity(rst.getDouble("QUANTITY"));
						objMovementDto.setregMovementVolume(rst.getDouble("VOLUME"));
					}
					
					objMovementDto.setActualWeight(rst.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					
					objMovementDto.setRevenueRegular(rst.getDouble("REVENUE"));
					objMovementDto.setregGrossRevenue(rst.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {

					if (rst.getDouble("WEIGHT") > 0){
						objMovementDto.setSaleQuantity(rst.getDouble("WEIGHT"));
						objMovementDto.setsaleMovementVolume(rst.getDouble("WEIGHT"));
					}
					else
					{
						objMovementDto.setSaleQuantity(rst.getDouble("QUANTITY"));
						objMovementDto.setsaleMovementVolume(rst.getDouble("VOLUME"));
					}						

					objMovementDto.setActualWeight(rst.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setRevenueSale(rst.getDouble("REVENUE"));
					objMovementDto.setsaleGrossRevenue(rst.getDouble("GROSSREVENUE"));
				}
				
				movementDataList.add(objMovementDto);
			}
		}
		else
		{
			logger.warn("There is no movement data");
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
	
	/**
	 * To get Coupon type and it's id's
	 * @param conn
	 * @return
	 */
	public HashMap<String, String> getCouponId(Connection  conn){
		HashMap<String, String> couponIdMap = new HashMap<String, String>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
	    String query = "SELECT COUPON_ID,COUPON_TYPE FROM COUPON_TYPE_LOOKUP";
		try{
			statement = conn.prepareStatement(query);
			rs = statement.executeQuery();
			while(rs.next()){
				couponIdMap.put(rs.getString("COUPON_TYPE"), rs.getString("COUPON_ID"));
			}
		}
		catch(Exception e){
			logger.error("getCouponId() - Error while getting coupon id mapping - " + e.toString());	
		}
		finally{
			PristineDBUtil.close(statement);
			}
		return couponIdMap;
	}
	
	private void getSalePriceFromTLog(Connection conn, String startDate, String endDate, String itemCodes, 
			int zoneNum, HashMap<String, RetailPriceDTO> itemAndSalePriceMap) throws Exception {

		String tableName = PropertyManager.getProperty("TABLE_NAME_TO_GET_SALE_PRICE", "TRANSACTION_LOG");
		boolean useProductLocationMapforZoneIMSAggr = Boolean.parseBoolean(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		String query = "";
		if(useProductLocationMapforZoneIMSAggr)
		{
			query = new String(GET_SALE_PRICE_FROM_TLOG_PLM);	
		}
		else
		{
			query = new String(GET_SALE_PRICE_FROM_TLOG);	
		}
		
		query = query.replace("%TABLE_NAME%", tableName);
		query = query.replace("%ITEM_CODES%", itemCodes);
		
		logger.debug ("getSalePriceFromTLog SQL: " + query);
		
//		logger.debug(" Get Sale Price query: "+query);
//		logger.debug("Parm: Zone Id: "+zoneNum+" startDate: "+startDate+
//				" End Date: "+endDate);
		try {
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, zoneNum);
			statement.setString(++counter, startDate);
			statement.setString(++counter, endDate);
			statement.setString(++counter, Constants.CALENDAR_DAY);
			rs = statement.executeQuery();
			while (rs.next()) {
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				retailPriceDTO.setItemcode(rs.getString("ITEM_ID"));
				retailPriceDTO.setSalePrice(rs.getFloat("UNIT_PRICE"));
				retailPriceDTO.setPromotionFlag(rs.getString("SALE_TYPE"));
				retailPriceDTO.setSaleQty(1);
				itemAndSalePriceMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
			}
		} catch (Exception e) {
			throw new Exception("getSalePriceFromTLog() - Error while getting sale price from tlog - " + e.toString());
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	//Added by RB
	private void getRegPriceFromTLog(Connection conn, String startDate, String endDate, String itemCodes, 
			int zoneNum, HashMap<String, RetailPriceDTO> itemAndRegPriceMap) throws Exception {

		//String tableName = PropertyManager.getProperty("TABLE_NAME_TO_GET_SALE_PRICE", "TRANSACTION_LOG");
		boolean useProductLocationMapforZoneIMSAggr = Boolean.parseBoolean(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		String query = "";
		if(useProductLocationMapforZoneIMSAggr)
		{
			query = new String(GET_REG_PRICE_FROM_TLOG_PLM);	
		}
		else
		{
			query = new String(GET_REG_PRICE_FROM_TLOG);	
		}
		
		query = query.replace("%TABLE_NAME%", "TRANSACTION_LOG");
		query = query.replace("%ITEM_CODES%", itemCodes);
		
		logger.debug ("getRegPriceFromTLog SQL: " + query);
		
		try {
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, zoneNum);
			statement.setString(++counter, startDate);
			statement.setString(++counter, endDate);
			statement.setString(++counter, Constants.CALENDAR_DAY);
			rs = statement.executeQuery();
			while (rs.next()) {
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				retailPriceDTO.setItemcode(rs.getString("ITEM_ID"));
				retailPriceDTO.setRegPrice(rs.getFloat("UNIT_PRICE"));
				retailPriceDTO.setRegQty(1);
				itemAndRegPriceMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
			}
		} catch (Exception e) {
			throw new Exception("getSalePriceFromTLog() - Error while getting sale price from tlog - " + e.toString());
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	//Added by RB
public HashMap<String, RetailPriceDTO> getRegPriceDetailsFromTLog(Connection conn, String startDate, String endDate, List<String> itemCodeList, 
			int zoneNum) throws Exception {
		HashMap<String, RetailPriceDTO> itemAndRegPriceMap = new HashMap<>();
		List<String> itemCodes = new ArrayList<>();
		int limitCount = 0;
		for (String itemCode : itemCodeList) {
			itemCodes.add(itemCode);
			limitCount++;
			if (limitCount > 0 && (limitCount % Constants.BATCH_UPDATE_COUNT == 0)) {
				String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
				getRegPriceFromTLog(conn, startDate, endDate, itemCodeValue, zoneNum, itemAndRegPriceMap);
				itemCodes.clear();
			}
		}
		if (itemCodes.size() > 0) {
			String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
			getRegPriceFromTLog(conn, startDate, endDate, itemCodeValue, zoneNum, itemAndRegPriceMap);
			itemCodes.clear();
		}
		return itemAndRegPriceMap;
	}
	
	public HashMap<String, RetailPriceDTO> getSaleDetailsFromTLog(Connection conn, String startDate, String endDate, Set<String> itemCodeList, 
			int zoneNum) throws Exception {
		HashMap<String, RetailPriceDTO> itemAndSalePriceMap = new HashMap<>();
		List<String> itemCodes = new ArrayList<>();
		int limitCount = 0;
		for (String itemCode : itemCodeList) {
			itemCodes.add(itemCode);
			limitCount++;
			if (limitCount > 0 && (limitCount % Constants.BATCH_UPDATE_COUNT == 0)) {
				String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
				getSalePriceFromTLog(conn, startDate, endDate, itemCodeValue, zoneNum, itemAndSalePriceMap);
				itemCodes.clear();
			}
		}
		if (itemCodes.size() > 0) {
			String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
			getSalePriceFromTLog(conn, startDate, endDate, itemCodeValue, zoneNum, itemAndSalePriceMap);
			itemCodes.clear();
		}
		return itemAndSalePriceMap;
	}
	
	
	
	
	
	/**
	 * 
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param itemCodeList
	 * @param storeId
	 * @return regular price map from T-log
	 * @throws Exception
	 */
	public HashMap<String, RetailPriceDTO> getRegPriceDetailsFromTLogForStore(Connection conn, String startDate,
			String endDate, List<String> itemCodeList, int storeId) throws Exception {

		HashMap<String, RetailPriceDTO> itemAndRegPriceMap = new HashMap<>();
		List<String> itemCodes = new ArrayList<>();
		int limitCount = 0;
		for (String itemCode : itemCodeList) {
			itemCodes.add(itemCode);
			limitCount++;
			if (limitCount > 0 && (limitCount % Constants.BATCH_UPDATE_COUNT == 0)) {
				String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
				getRegPriceFromTLogForStore(conn, startDate, endDate, itemCodeValue, storeId, itemAndRegPriceMap);
				itemCodes.clear();
			}
		}
		if (itemCodes.size() > 0) {
			String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
			getRegPriceFromTLogForStore(conn, startDate, endDate, itemCodeValue, storeId, itemAndRegPriceMap);
			itemCodes.clear();
		}
		return itemAndRegPriceMap;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param itemCodes
	 * @param storeId
	 * @param itemAndRegPriceMap
	 * @throws Exception
	 */
	private void getRegPriceFromTLogForStore(Connection conn, String startDate, String endDate, String itemCodes,
			int storeId, HashMap<String, RetailPriceDTO> itemAndRegPriceMap) throws Exception {

		String tableName = PropertyManager.getProperty("T_LOG_SOURCE_TABLE", "TRANSACTION_LOG");
		PreparedStatement statement = null;
		ResultSet rs = null;

		String query = new String(GET_REG_PRICE_FROM_TLOG_FOR_STORE);
		query = query.replace("%TABLE_NAME%", tableName);
		query = query.replace("%ITEM_CODES%", itemCodes);

		logger.debug("getRegPriceFromTLog SQL: " + query);

		try {
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, storeId);
			statement.setString(++counter, startDate);
			statement.setString(++counter, endDate);
			statement.setString(++counter, Constants.CALENDAR_DAY);
			rs = statement.executeQuery();
			while (rs.next()) {
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				retailPriceDTO.setItemcode(rs.getString("ITEM_ID"));
				retailPriceDTO.setRegPrice(rs.getFloat("UNIT_PRICE"));
				retailPriceDTO.setRegQty(1);
				itemAndRegPriceMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
			}
		} catch (Exception e) {
			throw new Exception("getSalePriceFromTLog() - Error while getting sale price from tlog - " + e.toString());
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	
	
	/**
	 * 
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param itemCodeList
	 * @param storeId
	 * @return sale prices from T-log
	 * @throws Exception
	 */
	public HashMap<String, RetailPriceDTO> getSaleDetailsFromTLogForStore(Connection conn, String startDate, String endDate, Set<String> itemCodeList, 
			int storeId) throws Exception {
		HashMap<String, RetailPriceDTO> itemAndSalePriceMap = new HashMap<>();
		List<String> itemCodes = new ArrayList<>();
		int limitCount = 0;
		for (String itemCode : itemCodeList) {
			itemCodes.add(itemCode);
			limitCount++;
			if (limitCount > 0 && (limitCount % Constants.BATCH_UPDATE_COUNT == 0)) {
				String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
				getSalePriceFromTLogForStore(conn, startDate, endDate, itemCodeValue, storeId, itemAndSalePriceMap);
				itemCodes.clear();
			}
		}
		if (itemCodes.size() > 0) {
			String itemCodeValue = itemCodes.stream().collect(Collectors.joining(","));
			getSalePriceFromTLogForStore(conn, startDate, endDate, itemCodeValue, storeId, itemAndSalePriceMap);
			itemCodes.clear();
		}
		return itemAndSalePriceMap;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param itemCodes
	 * @param zoneNum
	 * @param itemAndSalePriceMap
	 * @throws Exception
	 */
	private void getSalePriceFromTLogForStore(Connection conn, String startDate, String endDate, String itemCodes, 
			int zoneNum, HashMap<String, RetailPriceDTO> itemAndSalePriceMap) throws Exception {

		String tableName = PropertyManager.getProperty("T_LOG_SOURCE_TABLE", "TRANSACTION_LOG");
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		String query = new String(GET_SALE_PRICE_FROM_TLOG_FOR_STORE);	
		
		query = query.replace("%TABLE_NAME%", tableName);
		query = query.replace("%ITEM_CODES%", itemCodes);
		
		logger.debug ("getSalePriceFromTLog SQL: " + query);
		
//		logger.debug(" Get Sale Price query: "+query);
//		logger.debug("Parm: Zone Id: "+zoneNum+" startDate: "+startDate+
//				" End Date: "+endDate);
		try {
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, zoneNum);
			statement.setString(++counter, startDate);
			statement.setString(++counter, endDate);
			statement.setString(++counter, Constants.CALENDAR_DAY);
			rs = statement.executeQuery();
			while (rs.next()) {
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				retailPriceDTO.setItemcode(rs.getString("ITEM_ID"));
				retailPriceDTO.setSalePrice(rs.getFloat("UNIT_PRICE"));
				retailPriceDTO.setSaleQty(1);
				itemAndSalePriceMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
			}
		} catch (Exception e) {
			throw new Exception("getSalePriceFromTLog() - Error while getting sale price from tlog - " + e.toString());
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public List<ProductMetricsDataDTO> getWeeklyMovementAtTestZoneLevel(
			Connection _Conn, String weekCalIds,PriceZoneDTO priceZoneDTO,List<Integer>storeList,List<Integer>itemList, List<ProductMetricsDataDTO> movementDataList)
			throws GeneralException {


	 {

			List<Integer> storesList = new ArrayList<Integer>();
			List<Integer> itemsList = new ArrayList<Integer>();
			int limitcount = 0;
			int itemslimiCount = 0;
			for (Integer store : storeList) {
				storesList.add(store);
				limitcount++;
			
				if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
					Object[] values = storesList.toArray();
				
					for (Integer item : itemList) {
						itemsList.add(item);
						itemslimiCount++;
						if (itemslimiCount > 0 && (itemslimiCount % this.commitCount == 0)) {
							Object[] itemValues = itemsList.toArray();
							retrieveMovementInfo(_Conn, weekCalIds,values,itemValues,movementDataList);
							itemsList.clear();
						}

					}
					
					if (itemsList.size() > 0) {
						Object[] itemValues = itemsList.toArray();
						retrieveMovementInfo(_Conn, weekCalIds,values,itemValues,movementDataList);
						itemsList.clear();
					}
					storesList.clear();
				}

			}
			if (storesList.size() > 0) {
				Object[] values = storesList.toArray();

				for (Integer item : itemList) {
					itemsList.add(item);
					itemslimiCount++;
					if (itemslimiCount > 0 && (itemslimiCount % this.commitCount == 0)) {
						Object[] itemValues = itemsList.toArray();
						 retrieveMovementInfo(_Conn, weekCalIds, values, itemValues,movementDataList);
						itemsList.clear();
					}

				}

				if (itemsList.size() > 0) {
					Object[] itemValues = itemsList.toArray();
				retrieveMovementInfo(_Conn, weekCalIds, values, itemValues,movementDataList);
					itemsList.clear();

				}

			}

		}
		
	

		return movementDataList;
	}

	private void  retrieveMovementInfo(Connection _Conn, String weekCalIds, Object[] values,
			Object[] itemValues, List<ProductMetricsDataDTO> movementDataList) throws GeneralException {

		PreparedStatement statement = null;
		ResultSet rst = null;

		try {

			StringBuffer sql = new StringBuffer();

			String excludeGrossUnitsTrxTypes = PropertyManager.getProperty("EXCLUDE_TRX_TYPES_FOR_GROSS_UNITS", "0");
			String excludeGrossSalesTrxTypes = PropertyManager.getProperty("EXCLUDE_TRX_TYPES_FOR_GROSS_SALES", "0");

			sql.append("select count(distinct trx_no) TRX_COUNT, MV.ITEM_ID ITEM_CODE			, ");
			sql.append("count(case when MV.SALE_TYPE = 'Y' then 1 else null end) SALE_FLAG_COUNT, ");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_unit_calc = 'Y' then 0 else MV.QUANTITY end) as REG_QUANTITY,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_unit_calc = 'Y' then 0 else MV.QUANTITY end) as SALE_QUANTITY,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_unit_calc = 'Y' then 0 else MV.WEIGHT end) as REG_WEIGHT_ACTUAL,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_unit_calc = 'Y' then 0 else MV.WEIGHT end) as SALE_WEIGHT_ACTUAL,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_unit_calc = 'Y' and MV.WEIGHT <> 0  then NULL else MV.WEIGHT end) as REG_WEIGHT,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_unit_calc = 'Y' and MV.WEIGHT <> 0  then NULL else MV.WEIGHT end) as SALE_WEIGHT,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'Y' OR mv.exclude_sales_calc = 'Y'  then 0 else MV.net_amt end) as REG_REVENUE,");
			sql.append(
					"sum(case when MV.SALE_TYPE = 'N' OR mv.exclude_sales_calc = 'Y' then 0 else MV.net_amt end) as SALE_REVENUE, ");
			sql.append("sum(case when MV.TRX_TYPE IN (");
			sql.append(excludeGrossUnitsTrxTypes).append(") then 0 else MV.QUANTITY end) as GROSS_UNITS,");
			sql.append("sum(case when MV.TRX_TYPE IN (");
			sql.append(excludeGrossSalesTrxTypes).append(") then 0 else MV.NET_AMT end) as GROSS_SALES ");
			sql.append(
					" FROM (SELECT ITEM_ID, TRX_NO, SALE_TYPE, QUANTITY, WEIGHT, NET_AMT, CALENDAR_ID, STORE_ID, POS_DEPARTMENT_ID,  ");
			sql.append(
					" CASE WHEN TTL.EXCLUDE_UNIT_CALC IS NULL THEN 'N' ELSE TTL.EXCLUDE_UNIT_CALC END AS EXCLUDE_UNIT_CALC,  ");
			sql.append(
					" CASE WHEN TTL.EXCLUDE_SALES_CALC IS NULL THEN 'N' ELSE TTL.EXCLUDE_SALES_CALC END AS EXCLUDE_SALES_CALC,  ");
			sql.append(
					" TTL.TRX_TYPE FROM TRANSACTION_LOG TL LEFT JOIN TRANSACTION_TYPE_LOOKUP TTL ON TTL.TRX_TYPE = TL.TRX_TYPE) MV ");
			sql.append("where  MV.CALENDAR_ID IN ( " + weekCalIds + ")");
			sql.append(" AND STORE_ID IN( ");

			for (int i = 0; i < values.length; i++) {
				if (i != values.length - 1)
					sql.append(values[i] + ",");
				else
					sql.append(values[i] + ")");
			}
			sql.append(" AND ITEM_ID IN( ");

			for (int i = 0; i < itemValues.length; i++) {
				if (i != itemValues.length - 1)
					sql.append(itemValues[i] + ",");
				else
					sql.append(itemValues[i] + ")");
			}
			sql.append(" group by MV.ITEM_ID ");
			statement = _Conn.prepareStatement(sql.toString());

			logger.debug("getMovementForPriceTestData SQL:" + sql.toString());

			rst = statement.executeQuery();

			while (rst.next()) {
				ProductMetricsDataDTO objMovementDto = new ProductMetricsDataDTO();
				objMovementDto.setProductId((int) rst.getDouble("ITEM_CODE"));
				objMovementDto.setRegularMovement(rst.getDouble("REG_QUANTITY") + rst.getDouble("REG_WEIGHT"));
				objMovementDto.setRegularWeight(rst.getDouble("REG_WEIGHT_ACTUAL"));
				objMovementDto.setRegularQuantity(rst.getDouble("REG_QUANTITY"));
				objMovementDto.setRegularRevenue(rst.getDouble("REG_REVENUE"));

				// Setting Sale Data
				objMovementDto.setSaleMovement(rst.getDouble("SALE_QUANTITY") + rst.getDouble("SALE_WEIGHT"));
				objMovementDto.setSaleWeight(rst.getDouble("SALE_WEIGHT_ACTUAL"));
				objMovementDto.setSaleQuantity(rst.getDouble("SALE_QUANTITY"));
				objMovementDto.setSaleRevenue(rst.getDouble("SALE_REVENUE"));
				objMovementDto.setTotalVisits(rst.getDouble("TRX_COUNT"));
				objMovementDto.setAvgOrderSize((objMovementDto.getRegularRevenue() + objMovementDto.getSaleRevenue())
						/ objMovementDto.getTotalVisits());

				objMovementDto.setGrossUnits(rst.getDouble("GROSS_UNITS"));
				objMovementDto.setGrossSales(rst.getDouble("GROSS_SALES"));
				movementDataList.add(objMovementDto);
			}

			logger.debug("Total Items:" + movementDataList.size());

			if (movementDataList.size() == 0)
				logger.warn("No movement data found for: "+ weekCalIds);

		} catch (Exception exe) {
			logger.error("retrieveMovementInfo()- Error while fetching the results.... ", exe);
			throw new GeneralException(" Error while fetching the results.... ", exe);
		} finally {
			PristineDBUtil.close(rst);
			PristineDBUtil.close(statement);
		}

		
	}

}
/*
 * CREATE TABLE MOVEMENT_DAILY ( COMP_STR_NO VARCHAR2(10 BYTE) NOT NULL, UPC
 * VARCHAR2(20 BYTE) NOT NULL, POS_TIMESTAMP DATE NOT NULL, UNIT_PRICE
 * NUMBER(5,2) NOT NULL, SALE_FLAG CHAR(1 BYTE) NOT NULL, QUANTITY NUMBER(6,0)
 * DEFAULT 0, WEIGHT NUMBER(8,4) DEFAULT 0, PRICE NUMBER(6,2) NOT NULL, WEEKDAY
 * VARCHAR2(10 BYTE) NOT NULL, TRANSACTION_NO NUMBER(8,0) NOT NULL,
 * CUSTOMER_CARD_NO VARCHAR2(30 BYTE) ) TABLESPACE "USERS";
 * 
 * ALTER TABLE MOVEMENT_DAILY RENAME COLUMN DAY_OFF_WEEK to WEEKDAY; ALTER TABLE
 * MOVEMENT_DAILY ADD TRANSACTION_NO NUMBER(8,0) DEFAULT 0;
 * 
 * ALTER TABLE MOVEMENT_DAILY MODIFY UNIT_PRICE NUMBER(7,2); ALTER TABLE
 * MOVEMENT_DAILY MODIFY PRICE NUMBER(8,2);
 * 
 * ALTER TABLE MOVEMENT_DAILY ADD ITEM_CODE NUMBER(20,0); ALTER TABLE
 * MOVEMENT_DAILY ADD CALENDAR_ID NUMBER(8,0);
 * 
 * CREATE TABLE MOVEMENT_WEEKLY ( COMP_STR_ID NUMBER(5,0) NOT NULL, ITEM_CODE
 * NUMBER(6,0) NOT NULL, CHECK_DATA_ID NUMBER(8,0) NOT NULL PRIMARY KEY,
 * REVENUE_REGULAR NUMBER(8,2) DEFAULT 0, QUANTITY_REGULAR NUMBER(10,4) DEFAULT
 * 0, REVENUE_SALE NUMBER(8,2) DEFAULT 0, QUANTITY_SALE NUMBER(10,4) DEFAULT 0,
 * LIST_COST NUMBER(7,2), EFF_LIST_COST_DATE DATE, DEAL_COST NUMBER(7,2),
 * DEAL_START_DATE DATE, DEAL_END_DATE DATE, COST_CHG_DIRECTION NUMBER(1),
 * MARGIN_CHG_DIRECTION NUMBER(1), MARGIN_PCT NUMBER(5,2) ) TABLESPACE "USERS";
 * 
 * Alter table movement_weekly add List_cost number (7,2); Alter table
 * movement_weekly add Eff_List_Cost_Date date; Alter table movement_weekly add
 * Deal_cost number (7,2); Alter table movement_weekly add Deal_Start_date Date;
 * Alter table movement_weekly add Deal_End_date Date; Alter table
 * movement_weekly add Cost_Chg_Direction Number (1);
 * 
 * Alter table movement_weekly add Margin_Chg_Direction Number (1); Alter table
 * movement_weekly add Margin_Pct Number (5,2);
 * 
 * CREATE INDEX Movement_Weekly_Index ON MOVEMENT_WEEKLY (COMP_STR_ID,
 * ITEM_CODE) TABLESPACE "USERS"; ALTER TABLE MOVEMENT_WEEKLY ADD ( CONSTRAINT
 * FK_MOVEMENT_WEEKLY_COMP_STORE FOREIGN KEY (COMP_STR_ID) REFERENCES
 * COMPETITOR_STORE(COMP_STR_ID)); ALTER TABLE MOVEMENT_WEEKLY ADD ( CONSTRAINT
 * FK_MOVEMENT_WEEKLY_ITEMLOOKUP FOREIGN KEY (ITEM_CODE) REFERENCES ITEM_LOOKUP
 * (ITEM_CODE)); ALTER TABLE MOVEMENT_WEEKLY ADD ( CONSTRAINT
 * FK_MOVEMENT_WEEKLY_COMP_DATA FOREIGN KEY (CHECK_DATA_ID) REFERENCES
 * COMPETITIVE_DATA (CHECK_DATA_ID));
 * 
 * CREATE TABLE MOVEMENT_WEEKLY_LIG ( COMP_STR_ID NUMBER(5,0) NOT NULL,
 * ITEM_CODE NUMBER(6,0) NOT NULL, CHECK_DATA_ID NUMBER(8,0) NOT NULL PRIMARY
 * KEY, REVENUE_REGULAR NUMBER(8,2) DEFAULT 0, QUANTITY_REGULAR NUMBER(10,4)
 * DEFAULT 0, REVENUE_SALE NUMBER(8,2) DEFAULT 0, QUANTITY_SALE NUMBER(10,4)
 * DEFAULT 0, LIST_COST NUMBER(7,2), EFF_LIST_COST_DATE DATE, DEAL_COST
 * NUMBER(7,2), DEAL_START_DATE DATE, DEAL_END_DATE DATE, COST_CHG_DIRECTION
 * NUMBER(1), MARGIN_CHG_DIRECTION NUMBER(1), MARGIN_PCT NUMBER(5,2) )
 * TABLESPACE "USERS";
 * 
 * CREATE INDEX Movement_Weekly_Lig_Index ON MOVEMENT_WEEKLY_LIG (COMP_STR_ID,
 * ITEM_CODE) TABLESPACE "USERS"; ALTER TABLE MOVEMENT_WEEKLY_LIG ADD (
 * CONSTRAINT FK_MOVEMENT_WKLY_LIG_COMP_STR FOREIGN KEY (COMP_STR_ID) REFERENCES
 * COMPETITOR_STORE(COMP_STR_ID)); ALTER TABLE MOVEMENT_WEEKLY_LIG ADD (
 * CONSTRAINT FK_MOVEMENT_WKLY_LIG_ITEMLKUP FOREIGN KEY (ITEM_CODE) REFERENCES
 * ITEM_LOOKUP (ITEM_CODE)); ALTER TABLE MOVEMENT_WEEKLY_LIG ADD ( CONSTRAINT
 * FK_MOVEMENT_WKLY_LIG_COMP_DAT FOREIGN KEY (CHECK_DATA_ID) REFERENCES
 * COMPETITIVE_DATA (CHECK_DATA_ID));
 * 
 * 
 * 
 * update MOVEMENT_DAILY set TRANSACTION_NO = 830175 where COMP_STR_NO = '0209'
 * and UPC = '00000000741' and POS_TIMESTAMP = To_DATE('201103132339',
 * 'YYYYMMDDHH24MI') and UNIT_PRICE = 3.99 and SALE_FLAG = 'N' and QUANTITY =
 * 0.0 and WEIGHT = 2.5789 and PRICE = 10.29
 * 
 * -- upc level weekly revenue and scanned units select sum(price) as revenue,
 * sum(quantity) as scans, m.upc, c.name as category, d.name as dept from
 * (select m.upc, price, (case when quantity > 0 then quantity else weight end)
 * as quantity from movement_daily where pos_timestamp >=
 * To_DATE('201107170000', 'YYYYMMDDHH24MI') and pos_timestamp <=
 * To_DATE('201107232359', 'YYYYMMDDHH24MI') and comp_str_no = '0363') m,
 * item_lookup i, category c, department d where decode(length(m.upc), 14,
 * m.upc, '0' || m.upc) = i.upc and i.category_id = c.id and i.dept_id = d.id
 * group by d.name, c.name, m.upc order by upper(d.name), upper(c.name),
 * upper(m.upc)
 * 
 * -- Category level weekly revenue and scanned units select sum(price) as
 * revenue, sum(quantity) as scans, c.name as category, d.name as dept from
 * (select upc, price, (case when quantity > 0 then quantity else weight end) as
 * quantity from movement_daily where pos_timestamp >= To_DATE('201107170000',
 * 'YYYYMMDDHH24MI') and pos_timestamp <= To_DATE('201107232359',
 * 'YYYYMMDDHH24MI') and comp_str_no = '0363') m, item_lookup i, category c,
 * department d where decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and
 * i.category_id = c.id and i.dept_id = d.id group by d.name, c.name order by
 * upper(d.name), upper(c.name)
 * 
 * -- Dept level weekly revenue select d.name as dept, sum(price) as revenue
 * from (select upc, price, (case when quantity > 0 then quantity else weight
 * end) as quantity from movement_daily where pos_timestamp >=
 * To_DATE('201107240000', 'YYYYMMDDHH24MI') and pos_timestamp <=
 * To_DATE('201108302359', 'YYYYMMDDHH24MI') and comp_str_no = '0363') m,
 * item_lookup i, department d where decode(length(m.upc), 14, m.upc, '0' ||
 * m.upc) = i.upc and i.dept_id = d.id and d.id != 37 group by d.name order by
 * upper(d.name)
 * 
 * -- RX items select upc, sum(price) as revenue from movement_daily where
 * pos_timestamp >= To_DATE('201107310000', 'YYYYMMDDHH24MI') and pos_timestamp
 * <= To_DATE('201108062359', 'YYYYMMDDHH24MI') and comp_str_no = '0363' and
 * (upc like '%00000000101' or upc like '%0000000102') group by upc order by upc
 * 
 * -- TOPS weekly revenue select sum(price) as revenue, sum(quantity) as scans,
 * c.name as category, d.name as dept from (select upc, price, (case when
 * quantity > 0 then quantity else weight end) as quantity from movement_daily
 * where pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI') and
 * pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI') and comp_str_no =
 * '0363') m, item_lookup i, category c, department d where
 * decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.category_id =
 * c.id and i.dept_id = d.id group by d.name, c.name order by upper(d.name),
 * upper(c.name) 440 669 439 441 645 666
 * 
 * // Retrieve count for 15 Mar 11 select count(*) from movement_daily where
 * pos_timestamp > To_DATE('20110314235959', 'YYYYMMDDHH24MISS') and
 * pos_timestamp < To_DATE('20110316000000', 'YYYYMMDDHH24MISS')
 * 
 * // Total margin, visit count - weekly movement select * from MOVEMENT_WEEKLY
 * m where m.COMP_STR_ID = 5670 and m.CHECK_DATA_ID = 22250047 and m.ITEM_CODE =
 * 145256 update MOVEMENT_WEEKLY set total_margin = null, margin_pct = null,
 * visit_count = null, qty_regular_13wk = null, qty_sale_13wk = null where
 * COMP_STR_ID = 5670 and CHECK_DATA_ID = 22250047 and ITEM_CODE = 145256
 * 
 * // LIG select m.COMP_STR_NO, i.RET_LIR_ID, l.ret_lir_item_code,
 * count(distinct m.transaction_no) as VISIT_COUNT from MOVEMENT_DAILY m,
 * ITEM_LOOKUP i, retailer_like_item_group l where decode(length(m.upc), 14,
 * m.upc, '0' || m.upc) = i.upc and i.ret_lir_id = l.ret_lir_id and
 * m.pos_timestamp >= To_DATE('201107030000', 'YYYYMMDDHH24MI') and
 * m.pos_timestamp <= To_DATE('201107092359', 'YYYYMMDDHH24MI') and
 * m.transaction_no > 0 group by m.COMP_STR_NO, i.RET_LIR_ID,
 * l.ret_lir_item_code order by m.COMP_STR_NO, i.RET_LIR_ID
 * 
 * // LIG quantity select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR,
 * sum(m.QUANTITY_SALE) as QUANTITY_SALE from MOVEMENT_WEEKLY_LIG m,
 * COMPETITIVE_DATA_VIEW d, item_lookup i, retailer_like_item_group l where
 * m.COMP_STR_ID = d.COMP_STR_ID and i.ret_lir_id = l.ret_lir_id and m.ITEM_CODE
 * = l.RET_LIR_ITEM_CODE and i.ret_lir_id is not null and m.COMP_STR_ID = 5670
 * and m.ITEM_CODE = 420841 and i.ret_lir_id = 8795 and TO_CHAR(d.START_DATE,
 * 'YYYY/MM/DD') >= '2011/03/01' and TO_CHAR(d.START_DATE, 'YYYY/MM/DD') <=
 * '2011/07/03'
 * 
 * // 13 week data select m.ITEM_CODE, sum(m.QUANTITY_REGULAR) as
 * QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE from MOVEMENT_WEEKLY
 * m, COMPETITIVE_DATA_VIEW d where m.COMP_STR_ID = d.COMP_STR_ID and
 * m.CHECK_DATA_ID = d.CHECK_DATA_ID and m.COMP_STR_ID = 5704 and m.ITEM_CODE in
 * (11804,11805,11807,204999,204997,12081,12082) and d.START_DATE >= '24-Apr-11'
 * and d.START_DATE <= '17-Jul-11' group by m.ITEM_CODE
 * 
 * select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as
 * QUANTITY_SALE from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d where m.ITEM_CODE =
 * sb.append(" and d.SCHEDULE_ID in (").append(schCSV).append(")"); }
 * sb.append(" and m.CHECK_DATA_ID = d.CHECK_DATA_ID");
 * 
 * select comp_str_id, item_code, total_margin, margin_pct, visit_count,
 * qty_regular_13wk, qty_sale_13wk from movement_weekly where comp_str_id = 5704
 * and qty_regular_13wk > 0 and qty_sale_13wk > 0 and item_code in
 * (11804,11805,11807,204999,204997,12081,12082)
 * 
 * -- getVisitWeekly select COMP_STR_NO, UPC, sum(VISIT_COUNT) as VISIT_COUNT
 * from ( select COMP_STR_NO, WEEKDAY, UPC, count(distinct transaction_no) as
 * VISIT_COUNT from MOVEMENT_DAILY where pos_timestamp >=
 * To_DATE('201107170000', 'YYYYMMDDHH24MI') and pos_timestamp <=
 * To_DATE('201107232359', 'YYYYMMDDHH24MI') and transaction_no > 0 and
 * COMP_STR_NO ='0363' group by COMP_STR_NO, WEEKDAY, UPC ) group by
 * COMP_STR_NO, UPC
 * 
 * -- getLIGVisitWeekly select COMP_STR_NO, RET_LIR_ID, RET_LIR_ITEM_CODE,
 * sum(VISIT_COUNT) as VISIT_COUNT from ( select m.COMP_STR_NO, i.RET_LIR_ID,
 * l.RET_LIR_ITEM_CODE, WEEKDAY, count(distinct m.transaction_no) as VISIT_COUNT
 * from MOVEMENT_DAILY m, ITEM_LOOKUP i, retailer_like_item_group l where
 * decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc and i.ret_lir_id =
 * l.ret_lir_id and m.pos_timestamp >= To_DATE('201107170000', 'YYYYMMDDHH24MI')
 * and pos_timestamp <= To_DATE('201107232359', 'YYYYMMDDHH24MI') and
 * m.transaction_no > 0and m.COMP_STR_NO ='0363' group by m.COMP_STR_NO,
 * i.RET_LIR_ID, l.RET_LIR_ITEM_CODE, WEEKDAY ) group by COMP_STR_NO,
 * RET_LIR_ID, RET_LIR_ITEM_CODE order by COMP_STR_NO, RET_LIR_ID
 * 
 * select (case when m.DEAL_COST is null then m.LIST_COST when m.DEAL_COST = 0
 * then m.LIST_COST else m.DEAL_COST end) COST from MOVEMENT_WEEKLY m where
 * m.COMP_STR_ID = 5704 and m.CHECK_DATA_ID = 44330640 and m.ITEM_CODE = 43413
 * 
 * and m.CHECK_DATA_ID = 37769057 and m.ITEM_CODE = 1192
 */
