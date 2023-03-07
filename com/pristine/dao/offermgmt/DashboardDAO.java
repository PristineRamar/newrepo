package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dataload.offermgmt.Dashboard;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.DashboardDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class DashboardDAO {
	static Logger logger = Logger.getLogger("DashboardDAO");
	
	private Connection conn = null;
	//private RetailCostService costService = null;
	private static final String GET_DASHBOARD_DATA = 
			"SELECT DASHBOARD_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, FUTURE_PI, VALUE, NO_COMP_CHANGE, " +													
					"NO_COST_CHANGE, NO_NEW_ITEM, TO_CHAR(RESET_DATE, 'MM/dd/yyyy') AS RESET_DATE, EXPORT_DATE, STRATEGY_ID, UPDATED, " +
					"UPDATED_BY FROM PR_DASHBOARD";
	
	/*private static final String GET_COMP_PRICE_CHANGE_ITEMS = "SELECT PREVIOUS_COMP.ITEM_CODE AS ITEM_CODE, PREVIOUS_COMP.REG_PRICE, PREVIOUS_COMP.CHECK_DATETIME, " + 
																" CURRENT_COMP.ITEM_CODE, CURRENT_COMP.REG_PRICE, CURRENT_COMP.CHECK_DATETIME FROM " + 
																" ( " + 
																" (SELECT ITEM_CODE, REG_PRICE, CHECK_DATETIME FROM " +
																" (SELECT DISTINCT IL2.ITEM_CODE, REG_PRICE, CHECK_DATETIME, " + 
																" ROW_NUMBER() OVER ( PARTITION BY IL2.ITEM_CODE ORDER BY CHECK_DATETIME DESC) ROW_NUMBER FROM " +   
																" ( " + 
																" SELECT A.ITEM_CODE, " + 
																"   UNITPRICE( A.REG_PRICE,A.REG_M_PRICE, A.REG_M_PACK ) REG_PRICE, " + 
																"   TO_CHAR(A.CHECK_DATETIME,'MM/DD/YY') AS CHECK_DATETIME " + 
																" FROM SYNONYM_COMPETITIVE_DATA A " + 
																" WHERE A.SCHEDULE_ID IN " + 
																"   (SELECT SCHEDULE_ID " + 
																"   FROM SCHEDULE " + 
																"   WHERE COMP_STR_ID = ? " + 
																"   AND START_DATE    >= (TO_DATE(?,'MM/DD/YYYY') - (?)) " + 
																"   AND START_DATE    <= TO_DATE(?,'MM/DD/YYYY') " + 
																"   ) " + 
																" AND A.ITEM_NOT_FOUND_FLG = 'N' " + 
																" AND A.ITEM_CODE         IN " + 
																"   (SELECT CHILD_PRODUCT_ID " + 
																"   FROM " + 
																"     (SELECT CHILD_PRODUCT_ID, " + 
																"       CHILD_PRODUCT_LEVEL_ID " + 
																"     FROM product_group_relation pgr " + 
																"       START WITH product_level_id       = ? " + 
																"     AND product_id                      = ? " + 
																"       CONNECT BY prior child_product_id = product_id " + 
																"     AND prior child_product_level_id    = product_level_id " + 
																"     ) " + 
																"   WHERE child_product_level_id = 1 " + 
																"   ) " + 
																" ) DATA " +
																" LEFT JOIN ITEM_LOOKUP IL1 ON DATA.ITEM_CODE = IL1.ITEM_CODE AND IL1.LIR_IND <> 'Y' AND IL1.ACTIVE_INDICATOR = 'Y' " +  
																" LEFT JOIN ITEM_LOOKUP IL2 ON IL1.RET_LIR_ID = IL2.RET_LIR_ID AND IL1.CATEGORY_ID = IL2.CATEGORY_ID AND IL2.LIR_IND <> 'Y' AND IL2.ACTIVE_INDICATOR = 'Y' " +
																" ) " +
																" WHERE ROW_NUMBER = 1 " + 
																" ) PREVIOUS_COMP " + 
																" JOIN " + 
																" (SELECT ITEM_CODE, REG_PRICE, CHECK_DATETIME FROM " +
																" (SELECT DISTINCT IL2.ITEM_CODE, REG_PRICE, CHECK_DATETIME, " + 
																" ROW_NUMBER() OVER ( PARTITION BY IL2.ITEM_CODE ORDER BY CHECK_DATETIME DESC) ROW_NUMBER FROM " + 
																" ( " + 
																" SELECT A.ITEM_CODE, " + 
																"   UNITPRICE( A.REG_PRICE,A.REG_M_PRICE, A.REG_M_PACK ) REG_PRICE, " + 
																"   TO_CHAR(A.CHECK_DATETIME,'MM/DD/YY') AS CHECK_DATETIME " + 
																" FROM SYNONYM_COMPETITIVE_DATA A " + 
																" WHERE A.SCHEDULE_ID IN " + 
																"   (SELECT SCHEDULE_ID " + 
																"   FROM SCHEDULE " + 
																"   WHERE COMP_STR_ID = ? " + 
																"   AND START_DATE    >= (TO_DATE(?,'MM/DD/YYYY') - (?)) " + 
																"   AND START_DATE    <= TO_DATE(?,'MM/DD/YYYY') " + 
																"   ) " + 
																" AND A.ITEM_NOT_FOUND_FLG = 'N' " + 
																" AND A.ITEM_CODE         IN " + 
																"   (SELECT CHILD_PRODUCT_ID " + 
																"   FROM " + 
																"     (SELECT CHILD_PRODUCT_ID, " + 
																"       CHILD_PRODUCT_LEVEL_ID " + 
																"     FROM product_group_relation pgr " + 
																"       START WITH product_level_id       = ? " + 
																"     AND product_id                      = ? " + 
																"       CONNECT BY prior child_product_id = product_id " + 
																"     AND prior child_product_level_id    = product_level_id " + 
																"     ) " + 
																"   WHERE child_product_level_id = 1 " + 
																"   ) " + 
																" )DATA " +
																" LEFT JOIN ITEM_LOOKUP IL1 ON DATA.ITEM_CODE = IL1.ITEM_CODE AND IL1.LIR_IND <> 'Y' AND IL1.ACTIVE_INDICATOR = 'Y' " +  
																" LEFT JOIN ITEM_LOOKUP IL2 ON IL1.RET_LIR_ID = IL2.RET_LIR_ID AND IL1.CATEGORY_ID = IL2.CATEGORY_ID AND IL2.LIR_IND <> 'Y' AND IL2.ACTIVE_INDICATOR = 'Y' " +
																" ) " +
																"WHERE ROW_NUMBER = 1 " + 
																" ) CURRENT_COMP ON PREVIOUS_COMP.ITEM_CODE = CURRENT_COMP.ITEM_CODE " +  
																" ) " + 
																" WHERE PREVIOUS_COMP.REG_PRICE <> CURRENT_COMP.REG_PRICE ";*/
	
	/*private static final String GET_COMP_PRICE_CHANGE_ITEMS = "SELECT PREVIOUS_COMP.ITEM_CODE AS ITEM_CODE, PREVIOUS_COMP.REG_PRICE, "
			+ " PREVIOUS_COMP.CHECK_DATETIME, "
			+ " CURRENT_COMP.ITEM_CODE, CURRENT_COMP.REG_PRICE, CURRENT_COMP.CHECK_DATETIME FROM "
			+ " ( "
			+ " (SELECT ITEM_CODE, REG_PRICE, CHECK_DATETIME FROM "
			+ " (SELECT DISTINCT IL2.ITEM_CODE, REG_PRICE, CHECK_DATETIME, "
			+ " ROW_NUMBER() OVER ( PARTITION BY IL2.ITEM_CODE ORDER BY CHECK_DATETIME DESC) ROW_NUMBER FROM "
			+ " ( "
			+ " SELECT A.ITEM_CODE, "
			+ "   UNITPRICE( A.REG_PRICE,A.REG_M_PRICE, A.REG_M_PACK ) REG_PRICE, "
			+ "   TO_CHAR(A.CHECK_DATETIME,'MM/DD/YY') AS CHECK_DATETIME "
			+ " FROM SYNONYM_COMPETITIVE_DATA A "
			+ " WHERE A.SCHEDULE_ID IN "
			+ "   (SELECT SCHEDULE_ID "
			+ "   FROM SCHEDULE "
			+ "   WHERE COMP_STR_ID = ? "
			+ "   AND START_DATE    >= (TO_DATE(?,'MM/DD/YYYY') - (?)) "
			+ "   AND START_DATE    <= TO_DATE(?,'MM/DD/YYYY') "
			+ "   ) "
			+ " AND A.ITEM_NOT_FOUND_FLG = 'N' "
			+ " AND A.ITEM_CODE         IN "
			+ "   ( "
			+ "%AUTOHROIZED_ITEMS_QUERY%" 		
			+ "   ) "
			+ " ) DATA "
			+ " LEFT JOIN ITEM_LOOKUP IL1 ON DATA.ITEM_CODE = IL1.ITEM_CODE AND IL1.LIR_IND <> 'Y' " 
			+ " AND IL1.ACTIVE_INDICATOR = 'Y' "
			+ " LEFT JOIN ITEM_LOOKUP IL2 ON IL1.RET_LIR_ID = IL2.RET_LIR_ID AND IL1.CATEGORY_ID = IL2.CATEGORY_ID AND IL2.LIR_IND <> 'Y' "
			+ " AND IL2.ACTIVE_INDICATOR = 'Y' "
			+ " ) "
			+ " WHERE ROW_NUMBER = 1 "
			+ " ) PREVIOUS_COMP "
			+ " JOIN "
			+ " (SELECT ITEM_CODE, REG_PRICE, CHECK_DATETIME FROM "
			+ " (SELECT DISTINCT IL2.ITEM_CODE, REG_PRICE, CHECK_DATETIME, "
			+ " ROW_NUMBER() OVER ( PARTITION BY IL2.ITEM_CODE ORDER BY CHECK_DATETIME DESC) ROW_NUMBER FROM "
			+ " ( "
			+ " SELECT A.ITEM_CODE, "
			+ "   UNITPRICE( A.REG_PRICE,A.REG_M_PRICE, A.REG_M_PACK ) REG_PRICE, "
			+ "   TO_CHAR(A.CHECK_DATETIME,'MM/DD/YY') AS CHECK_DATETIME "
			+ " FROM SYNONYM_COMPETITIVE_DATA A "
			+ " WHERE A.SCHEDULE_ID IN "
			+ "   (SELECT SCHEDULE_ID "
			+ "   FROM SCHEDULE "
			+ "   WHERE COMP_STR_ID = ? "
			+ "   AND START_DATE    >= (TO_DATE(?,'MM/DD/YYYY') - (?)) "
			+ "   AND START_DATE    <= TO_DATE(?,'MM/DD/YYYY') "
			+ "   ) "
			+ " AND A.ITEM_NOT_FOUND_FLG = 'N' "
			+ " AND A.ITEM_CODE         IN "
			+ "   ( "
			+ "%AUTOHROIZED_ITEMS_QUERY%" 		
			+ "   ) "
			+ " )DATA "
			+ " LEFT JOIN ITEM_LOOKUP IL1 ON DATA.ITEM_CODE = IL1.ITEM_CODE AND IL1.LIR_IND <> 'Y' " 
			+ " AND IL1.ACTIVE_INDICATOR = 'Y' "
			+ " LEFT JOIN ITEM_LOOKUP IL2 ON IL1.RET_LIR_ID = IL2.RET_LIR_ID AND IL1.CATEGORY_ID = IL2.CATEGORY_ID AND IL2.LIR_IND <> 'Y' "
			+ " AND IL2.ACTIVE_INDICATOR = 'Y' "
			+ " ) "
			+ "WHERE ROW_NUMBER = 1 "
			+ " ) CURRENT_COMP ON PREVIOUS_COMP.ITEM_CODE = CURRENT_COMP.ITEM_CODE "
			+ " ) "
			+ " WHERE PREVIOUS_COMP.REG_PRICE <> CURRENT_COMP.REG_PRICE ";*/
	
	/*private static final String GET_NEW_ITEM_CNT = 
			"SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE IN " +			
					" ( " +													
					" SELECT CHILD_PRODUCT_ID " +					
					" FROM " +					
					"   (SELECT CHILD_PRODUCT_ID, " +					
					"     CHILD_PRODUCT_LEVEL_ID " +					
					"   FROM product_group_relation pgr " +					
					"     START WITH product_level_id       = ? " +					
					"   AND product_id                      = ? " +					
					"     CONNECT BY prior child_product_id = product_id " +					
					"   AND prior child_product_level_id    = product_level_id " +					
					"   ) " +					
					" WHERE child_product_level_id = 1  " +					
					" ) AND ACTIVE_INDICATOR = 'Y' AND CREATE_TIMESTAMP >= TO_DATE(?, 'MM/dd/yyyy') - (?) AND LIR_IND <> 'Y'";*/
	
	private static final String GET_NEW_ITEM_CNT = 
			"SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE IN " +			
					" ( " +													
					"%AUTOHROIZED_ITEMS_QUERY%" +							
					" ) " +
					" AND ACTIVE_INDICATOR = 'Y' " +
					" AND CREATE_TIMESTAMP >= TO_DATE(?, 'MM/dd/yyyy') - (?) AND LIR_IND <> 'Y'";

	/*private static final String GET_ITEM_LIST = "SELECT ITEM_CODE, RET_LIR_ID FROM ITEM_LOOKUP WHERE ITEM_CODE IN " +
			" ( " +
			" SELECT CHILD_PRODUCT_ID " +
			" FROM " +
			"   (SELECT CHILD_PRODUCT_ID, " +
			"     CHILD_PRODUCT_LEVEL_ID " +
			"   FROM product_group_relation pgr " +
			"     START WITH product_level_id       = ? " +
			"   AND product_id                      = ? " +
			"     CONNECT BY prior child_product_id = product_id " +
			"   AND prior child_product_level_id    = product_level_id " +
			"   ) " +
			" WHERE child_product_level_id = 1  " +
			" ) AND ACTIVE_INDICATOR = 'Y' AND LIR_IND <> 'Y'";*/
	
	private static final String GET_ITEM_LIST = "SELECT ITEM_CODE, RET_LIR_ID FROM ITEM_LOOKUP WHERE ITEM_CODE IN " +
			"( " +
			"%AUTOHROIZED_ITEMS_QUERY%" +
			") "+
			"AND ACTIVE_INDICATOR = 'Y' " +
			"AND LIR_IND <> 'Y'";

	/*private static final String GET_REVENUE_DATA = "SELECT PRODUCT_ID, SUM(TOT_REVENUE) AS TOT_REVENUE FROM ITEM_METRIC_SUMMARY_WEEKLY "
			+ " WHERE CALENDAR_ID =  "
			+ " (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE = TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE = 'W')  "
			+ " AND LOCATION_LEVEL_ID = 5 "
			+ " AND LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) "
			+ " AND PRODUCT_LEVEL_ID = 1 "
			+ " AND PRODUCT_ID IN "
			+ " ( "
			+ " SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR  "
			+ " START WITH PRODUCT_LEVEL_ID = 4 "
			+ " AND PRODUCT_ID = ? "
			+ " CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID  AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID "
			+ " )  "
			+ " WHERE CHILD_PRODUCT_LEVEL_ID = 1 "
			+ " ) "
			+ " GROUP BY PRODUCT_ID";*/
	
	private static final String GET_REVENUE_DATA = 
			//"SELECT PRODUCT_ID, SUM(TOT_REVENUE) AS TOT_REVENUE FROM ITEM_METRIC_SUMMARY_WEEKLY "
			//"SELECT PRODUCT_ID, SUM(TOT_REVENUE) AS TOT_REVENUE FROM SYNONYM_IMS_WEEKLY "
			"SELECT PRODUCT_ID, SUM(TOT_REVENUE) AS TOT_REVENUE FROM SYN_ITEM_METRIC_SUMMARY_WEEKLY "
			+ " WHERE CALENDAR_ID =  "
			+ " (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE = TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE = 'W')  "
			+ " AND LOCATION_LEVEL_ID = 5 "
			+ " AND LOCATION_ID IN (%PRICE_ZONE_STORES%) "
			+ " AND PRODUCT_LEVEL_ID = 1 "
			+ " AND PRODUCT_ID IN "
			+ " ( "
			+ "%AUTOHROIZED_ITEMS_QUERY%"			
			+ " ) "
			+ " GROUP BY PRODUCT_ID";
	
	private static final String GET_ACTIVE_STRATEGIES = "SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, APPLY_TO, STRATEGY_ID, START_DATE, END_DATE,  " +
														" (END_DATE - START_DATE) AS DAYS FROM  " +
														" ( " +
														" SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, APPLY_TO, STRATEGY_ID,  " +
														" START_CALENDAR_ID, END_CALENDAR_ID, RCS.START_DATE, (CASE WHEN RCE.END_DATE IS NULL THEN RCE.START_DATE ELSE RCE.END_DATE END) AS END_DATE " +
														" FROM PR_STRATEGY S " +
														" LEFT JOIN RETAIL_CALENDAR RCS ON S.START_CALENDAR_ID = RCS.CALENDAR_ID " +
														" LEFT JOIN RETAIL_CALENDAR RCE ON S.END_CALENDAR_ID = RCE.CALENDAR_ID " +
														" WHERE TEMP <> 'Y' " +
														" ) WHERE (START_DATE <= TO_DATE(?, 'MM/dd/yyyy') AND END_DATE >= TO_DATE(?, 'MM/dd/yyyy')) OR (START_DATE <= TO_DATE(?, 'MM/dd/yyyy') AND END_DATE IS NULL) " +
														" ORDER BY LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, DAYS";
	
	private static final String GET_LOCATION_LIST_ID = "SELECT LOCATION_ID FROM  " + 
														" ( " + 
														" SELECT CHILD_LOCATION_ID, CHILD_LOCATION_LEVEL_ID, LOCATION_LEVEL_ID, LOCATION_ID " + 
														" FROM LOCATION_GROUP_RELATION PGR WHERE LOCATION_LEVEL_ID  = 21 " + 
														" ) WHERE CHILD_LOCATION_LEVEL_ID = ? AND CHILD_LOCATION_ID = ? "; 

	private static final String GET_CATEGORY_HIERARCHY = "SELECT A1.CHILD_PRODUCT_ID CATEGORY_ID, " +
														" A1.CHILD_PRODUCT_LEVEL_ID CATEGORY_LEVEL_ID, " +
														" A1.PRODUCT_LEVEL_ID P_L_ID_1, " +
														" A1.PRODUCT_ID P_ID_1, " +
														" A2.PRODUCT_LEVEL_ID P_L_ID_2, " +
														" A2.PRODUCT_ID P_ID_2 " +
														" FROM ( ( PRODUCT_GROUP_RELATION A1 " +
														" LEFT JOIN PRODUCT_GROUP_RELATION A2 " +
														" ON A1.PRODUCT_ID  = A2.CHILD_PRODUCT_ID " +
														" AND A1.PRODUCT_LEVEL_ID = A2.CHILD_PRODUCT_LEVEL_ID) " +
														" LEFT JOIN PRODUCT_GROUP_RELATION A3 " +
														" ON A2.PRODUCT_ID  = A3.CHILD_PRODUCT_ID " +
														" AND A2.PRODUCT_LEVEL_ID = A3.CHILD_PRODUCT_LEVEL_ID ) " +
														" WHERE A1.CHILD_PRODUCT_LEVEL_ID = 4 %CHILD_PRODUCT_ID_CONDITION%";
	
	private static final String GET_DASHBOARD_RESET_DATE = 
			"SELECT TO_CHAR(RESET_DATE, 'MM/DD/YYYY') RESET_DATE FROM PR_DASHBOARD WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? " +	
					"AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	
	private static final String GET_DASHBOARD_IS_AUTOMATIC_RECOMMENDATION = 
			"SELECT IS_AUTOMATIC_RECOMMENDATION FROM PR_DASHBOARD WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? " +	
					"AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	
	private static final String INSERT_PR_DASHBOARD =			
			"INSERT INTO PR_DASHBOARD (DASHBOARD_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, NO_COMP_CHANGE, " +
					"NO_COST_CHANGE, NO_NEW_ITEM, KVI_NO_COMP_CHANGE, KVI_NO_COST_CHANGE, KVI_NO_NEW_ITEM, STRATEGY_ID, UPDATED, UPDATED_BY, " +													  
					"KVI_STRATEGY_ID, TOT_ITEMS, KVI_TOT_ITEMS, TOT_REVENUE, KVI_TOT_REVENUE, " +													  
					"DIST_NO_COMP_CHANGE, DIST_KVI_NO_COMP_CHANGE, DIST_NO_COST_CHANGE, DIST_KVI_NO_COST_CHANGE, " +													  
					"DIST_NO_NEW_ITEM, DIST_KVI_NO_NEW_ITEM, DIST_TOT_ITEMS, DIST_KVI_TOT_ITEMS, PRIMARY_COMP_RECENT_CHECK_DATE) VALUES " +													  
					"(PR_DASHBOARD_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'MM/dd/yyyy')) ";
	
	private static final String UPDATE_PR_DASHBOARD = "UPDATE PR_DASHBOARD SET %UPDATE_PARAMETERS% WHERE DASHBOARD_ID = ?";
	
	private static final String UPDATE_FUTURE_PI_VALUE = 
			" UPDATE PR_DASHBOARD SET FUTURE_PI = ?, VALUE = ? WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? " +	
					"AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	
	private static final String UPDATE_ACTIVE = 
			" UPDATE PR_DASHBOARD SET ACTIVE = ? WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? " +	
					"AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	
	private static final String GET_RECENT_CHECK_DATE = "SELECT TO_CHAR(MAX(CHECK_DATETIME),'MM/dd/yyyy') AS RECENT_CHECK_DATETIME "
			+ "FROM SYN_COMPETITIVE_DATA WHERE SCHEDULE_ID IN (SELECT SCHEDULE_ID FROM SCHEDULE WHERE "
			+ "(START_DATE <= SYSDATE AND START_DATE >= SYSDATE - 90) AND COMP_STR_ID = ?) "
			+ " AND ITEM_CODE IN "
			+ "(SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE IN " 
			+ "(%AUTOHROIZED_ITEMS_QUERY% ) "
			+ "AND ACTIVE_INDICATOR = 'Y' " 
			+ "AND LIR_IND <> 'Y') "
			+ "AND CHECK_DATETIME IS NOT NULL";
	
	public DashboardDAO(Connection conn){
		this.conn = conn;
	}
	
	/**
	 * Retrieves records from pr_dashboard table
	 * @param conn	Connection
	 * @return
	 */
	public HashMap<String, DashboardDTO> getDashboardData() throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, DashboardDTO> dashboardMap = new HashMap<String, DashboardDTO>();
		try{
			stmt = conn.prepareStatement(GET_DASHBOARD_DATA);
			rs = stmt.executeQuery();
			while(rs.next()){
				DashboardDTO dto = new DashboardDTO();
				dto.setDashboardId(rs.getInt("DASHBOARD_ID"));
				dto.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				dto.setLocationId(rs.getInt("LOCATION_ID"));
				dto.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				dto.setProductId(rs.getInt("PRODUCT_ID"));
				if(rs.getObject("FUTURE_PI") != null){
					dto.setFuturePI(rs.getDouble("FUTURE_PI"));
				}else{
					dto.setFuturePI(null);
				}
				if(rs.getObject("VALUE") != null){
					dto.setValue(rs.getDouble("VALUE"));
				}else{
					dto.setValue(null);
				}
				dto.setCompChange(rs.getInt("NO_COMP_CHANGE"));
				dto.setCostChange(rs.getInt("NO_COST_CHANGE"));
				dto.setNewItem(rs.getInt("NO_NEW_ITEM"));
				dto.setResetDate(rs.getString("RESET_DATE"));
				dto.setExportDate(rs.getString("EXPORT_DATE"));
				dto.setStrategyId(rs.getInt("STRATEGY_ID"));
				dto.setUpdatedDate(rs.getString("UPDATED"));
				dto.setUpdatedBy(rs.getString("UPDATED_BY"));
				String key = dto.getLocationLevelId() + "-" + dto.getLocationId() + "-" + dto.getProductLevelId() + "-" + dto.getProductId();
				dashboardMap.put(key, dto);
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving dashboard data - " + ex.getMessage());
			throw new GeneralException("Error when retrieving dashboard data - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return dashboardMap;
	}
	
	/**
	 * Returns number of items with competition price change between current date and reset date
	 * @param compLocationId	Comp Str Id
	 * @param productLevelId	Product Level Id
	 * @param productId			Product Id
	 * @param resetDate			Reset Date
	 * @param currentDate		Current Date
	 * @return
	 */
	/*public ArrayList<Integer> getCompPriceChangeCountOld(int zoneId, int compLocationId, int productLevelId, int productId, String resetDate, String currentDate){
		int compHistory = Integer.parseInt(PropertyManager.getProperty("PR_COMP_HISTORY", "10"));
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> compChangeItems = new ArrayList<Integer>();
		int counter = 0;
		try{
			String sql = null;
			sql = GET_COMP_PRICE_CHANGE_ITEMS;
			sql = sql.replaceAll("%AUTOHROIZED_ITEMS_QUERY%", queryToGetAuthorizedItems(productLevelId, productId));
			
			//stmt = conn.prepareStatement(GET_COMP_PRICE_CHANGE_ITEMS);
			stmt = conn.prepareStatement(sql);
			logger.debug("Comp Price Change Count Query - " + GET_COMP_PRICE_CHANGE_ITEMS);
			stmt.setInt(++counter, compLocationId);
			stmt.setString(++counter, resetDate);
			stmt.setInt(++counter, compHistory * 7);
			stmt.setString(++counter, resetDate);
			
			stmt.setInt(++counter, zoneId);
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			stmt.setInt(++counter, compLocationId);
			stmt.setString(++counter, currentDate);
			stmt.setInt(++counter, compHistory * 7);
			stmt.setString(++counter, currentDate);
			
			stmt.setInt(++counter, zoneId);
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			rs = stmt.executeQuery();
			while(rs.next()){
				compChangeItems.add(rs.getInt("ITEM_CODE"));
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving comp change count - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return compChangeItems;
	}*/
	
	/**
	 * Returns number of items with competition price change between current date and reset date
	 * @param zoneId
	 * @param compLocationId
	 * @param productLevelId
	 * @param productId
	 * @param resetDate
	 * @param currentDate
	 * @param itemList			List of authorized items
	 * @return
	 */
	public ArrayList<Integer> getCompPriceChangeCount(int zoneId, LocationKey compDetail, int productLevelId, int productId, String resetDate,
			String currentDate, Collection<ItemDTO> authorizedItems, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		PricingEngineDAO pDAO = new PricingEngineDAO();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		inputDTO.setLocationLevelId(compDetail.getLocationLevelId());
		inputDTO.setLocationId(compDetail.getLocationId());
		inputDTO.setProductLevelId(productLevelId);
		inputDTO.setProductId(productId);

		int compHistory = Integer.parseInt(PropertyManager.getProperty("PR_COMP_HISTORY", "10"));
		ArrayList<Integer> compChangeItems = new ArrayList<Integer>();
		try {
//			HashMap<Integer, CompetitiveDataDTO> priceData = pDAO.getCompPriceData(conn, inputDTO, currentDate, compHistory * 7);
			HashMap<Integer, CompetitiveDataDTO> priceData = pDAO.getLatestCompPriceData(conn, inputDTO, currentDate, compHistory * 7);
			HashMap<Integer, CompetitiveDataDTO> ligPriceData = pDAO.getMostOccuringCompPriceForLIG(priceData.values());

//			HashMap<Integer, CompetitiveDataDTO> prevPriceData = pDAO.getCompPriceData(conn, inputDTO, resetDate, compHistory * 7);
			HashMap<Integer, CompetitiveDataDTO> prevPriceData = pDAO.getPreviousCompPriceData(conn, inputDTO, currentDate, compHistory * 7);
			HashMap<Integer, CompetitiveDataDTO> ligPrevPriceData = pDAO.getMostOccuringCompPriceForLIG(prevPriceData.values());

			for (ItemDTO item : authorizedItems) {
				double currentPrice = 0;
				double previousPrice = 0;
				MultiplePrice curMultiplePrice = null, preMultiplePrice = null;
				String latestCompPriceCheckDate = null;
				
				int itemCode = item.itemCode;
				int retLirId = item.likeItemId;
				if (priceData.get(itemCode) != null) {
					CompetitiveDataDTO compDataDTO = priceData.get(itemCode);
					double curPrice = compDataDTO.regPrice;
					double regMPrice = compDataDTO.regMPrice;
					curMultiplePrice = PRCommonUtil.getMultiplePrice(compDataDTO.regMPack, curPrice, regMPrice);
					latestCompPriceCheckDate = compDataDTO.checkDate;
					//currentPrice = compDataDTO.regPrice;
				} else if (ligPriceData.get(retLirId) != null) {
					CompetitiveDataDTO compDataDTO = ligPriceData.get(retLirId);
					logger.debug("Copying LIG level current comp price for item " + itemCode);
					double curPrice = compDataDTO.regPrice;
					double regMPrice = compDataDTO.regMPrice;
					curMultiplePrice = PRCommonUtil.getMultiplePrice(compDataDTO.regMPack, curPrice, regMPrice);
					//currentPrice = compDataDTO.regPrice;
					latestCompPriceCheckDate = compDataDTO.checkDate;
				}

				if (prevPriceData.get(itemCode) != null) {
					CompetitiveDataDTO compDataDTO = prevPriceData.get(itemCode);
					double curPrice = compDataDTO.regPrice;
					double regMPrice = compDataDTO.regMPrice;
					preMultiplePrice = PRCommonUtil.getMultiplePrice(compDataDTO.regMPack, curPrice, regMPrice);
					//previousPrice = compDataDTO.regPrice;
				} else if (ligPrevPriceData.get(retLirId) != null) {
					CompetitiveDataDTO compDataDTO = ligPrevPriceData.get(retLirId);
					logger.debug("Copying LIG level previous comp price for item " + itemCode);
					//previousPrice = compDataDTO.regPrice;
					double curPrice = compDataDTO.regPrice;
					double regMPrice = compDataDTO.regMPrice;
					preMultiplePrice = PRCommonUtil.getMultiplePrice(compDataDTO.regMPack, curPrice, regMPrice);
				}
				//Check against unit price
				currentPrice = PRCommonUtil.getUnitPrice(curMultiplePrice, true);
				previousPrice = PRCommonUtil.getUnitPrice(preMultiplePrice, true);
//				if (currentPrice > 0 && previousPrice > 0) {
//					if (currentPrice != previousPrice)
//						compChangeItems.add(itemCode);
//				}
				ItemKey itemKey = new ItemKey(itemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
				
				if (itemDataMap.get(itemKey) != null) {
					Integer compChgIndicator = pDAO.getCompChgIndicator(itemDataMap.get(itemKey), currentPrice, previousPrice,
							latestCompPriceCheckDate, currentDate);
					if (compChgIndicator != null  && compChgIndicator != 0) {
						compChangeItems.add(itemCode);
					}
				}

			}
		} catch (GeneralException ge) {
			logger.error("Error when retrieving comp price change count for zoneId " + zoneId);
		}
		return compChangeItems;
	}
	
	/**
	 * Returns number of items with cost change between current date and reset date
	 * @param locationId			Zone Id
	 * @param productLevelId		Product Level Id
	 * @param productId				Product Id
	 * @param resetCalendarId		Calendar Id for the date when count was reset
	 * @param currentCalendarId		Calendar Id for current date
	 * @return
	 */
	/*public ArrayList<Integer> getCostChangeCount(int locationId, int productLevelId, int productId, int resetCalendarId, int currentCalendarId){
		RetailCostService costService = new RetailCostService();
		ArrayList<Integer> costChangeItems = new ArrayList<Integer>();
		double costChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PR_DASHBOARD.COST_CHANGE_THRESHOLD", "0.10"));
		int costHistory = Integer.parseInt(PropertyManager.getProperty("PR_COST_HISTORY","8"));
		try{
			DecimalFormat df = new DecimalFormat("######.##");
			HashMap<Integer, TreeMap<Date, RetailCostDTO>>  previousCostMap = costService.getRetailCost(conn, resetCalendarId, costHistory, 
					PRConstants.ZONE_LEVEL_TYPE_ID, locationId, productLevelId, productId);
			HashMap<Integer, Double> pMap = new HashMap<Integer, Double>();
			for(Map.Entry<Integer, TreeMap<Date, RetailCostDTO>> entry : previousCostMap.entrySet()){
				for(Map.Entry<Date, RetailCostDTO> inEntry : entry.getValue().entrySet()){
					pMap.put(entry.getKey(), new Double(df.format(inEntry.getValue().getListCost())));
					break;
				}
			}
			HashMap<Integer, TreeMap<Date, RetailCostDTO>>  currentCostMap = costService.getRetailCost(conn, currentCalendarId, costHistory, 
					PRConstants.ZONE_LEVEL_TYPE_ID, locationId, productLevelId, productId);
			HashMap<Integer, Double> cMap = new HashMap<Integer, Double>();
			for(Map.Entry<Integer, TreeMap<Date, RetailCostDTO>> entry : currentCostMap.entrySet()){
				for(Map.Entry<Date, RetailCostDTO> inEntry : entry.getValue().entrySet()){
					cMap.put(entry.getKey(), new Double(df.format(inEntry.getValue().getListCost())));
					break;
				}
			}
			
			for(Map.Entry<Integer, Double> entry : cMap.entrySet()){
				int itemCode = entry.getKey();
				double currentCost = entry.getValue();
				if(pMap.get(itemCode) != null){
					double previousCost = pMap.get(itemCode);
					double costChange = new Double(df.format(Math.abs(previousCost - currentCost)));
					if(currentCost != previousCost && costChange > costChangeThreshold && (currentCost > 0 && previousCost > 0)){
						logger.debug("Cost Change - " + itemCode + "\t" + previousCost + "\t" + currentCost);
						costChangeItems.add(entry.getKey());
					}
				}
			}
			
			previousCostMap = null;
			currentCostMap = null;
			pMap = null;
			cMap = null;
		}catch(GeneralException gex){
			logger.error("Error when parsing date - " + gex.getMessage());
		}
		return costChangeItems;
	}*/
	
	public ArrayList<Integer> getCostChangeCountOptimized(int chainId, int locationId, int productLevelId, int productId, int resetCalendarId,
			int currentCalendarId, Set<Integer> itemCodeSet, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores) {
		RetailCostServiceOptimized costService = new RetailCostServiceOptimized(conn);
		ArrayList<Integer> costChangeItems = new ArrayList<Integer>();
		//double costChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PR_DASHBOARD.COST_CHANGE_THRESHOLD", "0.10"));
		double costChangeThreshold = 0;
		int costHistory = Integer.parseInt(PropertyManager.getProperty("PR_COST_HISTORY", "8"));
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, locationId);
		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);

		try {
			DecimalFormat df = new DecimalFormat("######.##");

			RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
			List<RetailCalendarDTO> retailCalendarList = calendarDAO.getCalendarList(conn, resetCalendarId, costHistory);

			Set<Integer> itemCodeSetT = new HashSet<Integer>();
			Set<Integer> itemCodeSetTCopy = new HashSet<Integer>();
			for (Integer item : itemCodeSet) {
				itemCodeSetT.add(item);
				itemCodeSetTCopy.add(item);
			}

			HashMap<Integer, Double> pMap = new HashMap<Integer, Double>();
			for (RetailCalendarDTO calDTO : retailCalendarList) {
				logger.debug("Running for calendar id " + calDTO.getCalendarId() + " for items " + itemCodeSetT.size());
				if (itemCodeSetT.size() > 0) {
					HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = costService.getRetailCost(calDTO.getCalendarId(),
							chainId, priceAndStrategyZoneNos, priceZoneStores, false, itemCodeSetT);
					for (Integer itemCode : itemCodeSetT) {
						if (costDataMap.get(itemCode) != null) {
							HashMap<RetailPriceCostKey, RetailCostDTO> costMap = costDataMap.get(itemCode);
							RetailCostDTO costDTO = costService.findCostForZone(costMap, zoneKey, chainKey);
							pMap.put(itemCode, new Double(costDTO.getListCost()));
							itemCodeSetTCopy.remove(itemCode);
						}
					}
					itemCodeSetT = new HashSet<Integer>();
					for (Integer item : itemCodeSetTCopy) {
						itemCodeSetT.add(item);
					}
				}
			}

			retailCalendarList = calendarDAO.getCalendarList(conn, currentCalendarId, costHistory);

			itemCodeSetT = new HashSet<Integer>();
			itemCodeSetTCopy = new HashSet<Integer>();
			for (Integer item : itemCodeSet) {
				itemCodeSetT.add(item);
				itemCodeSetTCopy.add(item);
			}

			HashMap<Integer, Double> cMap = new HashMap<Integer, Double>();
			for (RetailCalendarDTO calDTO : retailCalendarList) {
				logger.debug("Running for calendar id " + calDTO.getCalendarId() + " for items " + itemCodeSetT.size());
				if (itemCodeSetT.size() > 0) {
					HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = costService.getRetailCost(calDTO.getCalendarId(),
							chainId, priceAndStrategyZoneNos, priceZoneStores, false, itemCodeSetT);
					for (Integer itemCode : itemCodeSetT) {
						if (costDataMap.get(itemCode) != null) {
							HashMap<RetailPriceCostKey, RetailCostDTO> costMap = costDataMap.get(itemCode);
							RetailCostDTO costDTO = costService.findCostForZone(costMap, zoneKey, chainKey);
							cMap.put(itemCode, new Double(costDTO.getListCost()));
							itemCodeSetTCopy.remove(itemCode);
						}
					}
					itemCodeSetT = new HashSet<Integer>();
					for (Integer item : itemCodeSetTCopy) {
						itemCodeSetT.add(item);
					}
				}
			}

			for (Map.Entry<Integer, Double> entry : cMap.entrySet()) {
				int itemCode = entry.getKey();
				double currentCost = entry.getValue();
				if (pMap.get(itemCode) != null) {
					double previousCost = pMap.get(itemCode);
					double costChange = new Double(df.format(Math.abs(previousCost - currentCost)));
					if (currentCost != previousCost && costChange > costChangeThreshold && (currentCost > 0 && previousCost > 0)) {
						logger.debug("Cost Change - " + itemCode + "\t" + previousCost + "\t" + currentCost);
						costChangeItems.add(entry.getKey());
					}
				}
			}

			pMap = null;
			cMap = null;
		} catch (GeneralException gex) {
			logger.error("Error when parsing date - " + gex.getMessage());
		}
		return costChangeItems;
	}
	/**
	 * Returns no of new items between current date and reset date
	 * @param productLevelId	Product Level Id
	 * @param productId			Product Id
	 * @param resetDate			Reset Date
	 * @param currentDate		Current Date
	 * @return
	 */
	public ArrayList<Integer> getNewItemCount(int zoneId, int productLevelId, int productId, String resetDate,
			String currentDate, List<Integer> priceZoneStores, int locationLevelId) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> newItems = new ArrayList<Integer>();
		int counter = 0;
		try{
			String sql = null;
			sql = GET_NEW_ITEM_CNT;
			sql = sql.replaceAll("%AUTOHROIZED_ITEMS_QUERY%", queryToGetAuthorizedItems(productLevelId, productId, priceZoneStores, zoneId,locationLevelId));
						
			//stmt = conn.prepareStatement(GET_NEW_ITEM_CNT);
			stmt = conn.prepareStatement(sql);
			logger.debug("New Item Count Query - " + sql);
			//stmt.setInt(++counter, zoneId);
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			stmt.setString(++counter, currentDate);
			int noOfDays = new Long(DateUtil.getDateDiff(DateUtil.toDate(currentDate), DateUtil.toDate(resetDate))).intValue();
			stmt.setInt(++counter, noOfDays);
			rs = stmt.executeQuery();
			while(rs.next()){
				newItems.add(rs.getInt("ITEM_CODE"));
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving new item count - " + ex.getMessage());
		}catch(GeneralException gex){
			logger.error("Error when parsing date - " + gex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return newItems;
	}
	
	/**
	 * Returns all items under product
	 * @param productLevelId	Product Level Id
	 * @param productId			Product Id
	 * @return
	 * @throws GeneralException 
	 */
	public HashMap<Integer, ItemDTO> getAllItems(int zoneId, int productLevelId, int productId, List<Integer> priceZoneStores, int locationLevelId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, ItemDTO> itemMap = new HashMap<Integer, ItemDTO>();
		int counter = 0;
		try{
			String sql = null;
			sql = GET_ITEM_LIST;
			sql = sql.replaceAll("%AUTOHROIZED_ITEMS_QUERY%", queryToGetAuthorizedItems(productLevelId, productId, priceZoneStores, zoneId, locationLevelId));
			
			stmt = conn.prepareStatement(sql);
			logger.debug("Item Count Query - " + sql);			
//			stmt.setInt(++counter, zoneId);
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			rs = stmt.executeQuery();
			while(rs.next()){
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.itemCode = rs.getInt("ITEM_CODE");
				itemDTO.likeItemId = rs.getInt("RET_LIR_ID");
				itemMap.put(itemDTO.itemCode, itemDTO);
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving items - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemMap;
	}
	
	/**
	 * Inserts into PR_DASHBOARD
	 * @param insertList
	 */
	public void insertDashboardData(ArrayList<DashboardDTO> insertList) throws GeneralException{
		PreparedStatement statement = null;
	    try{
	    	statement = conn.prepareStatement(INSERT_PR_DASHBOARD);
	        DecimalFormat format = new DecimalFormat("###.###");
			int recordCnt = 0;
			int itemNoInBatch = 0;
			logger.debug("INSERT_PR_DASHBOARD" + INSERT_PR_DASHBOARD);
	        for(DashboardDTO dto:insertList){
	        	int counter = 0;
	        	statement.setInt(++counter, dto.getLocationLevelId());
	        	statement.setInt(++counter, dto.getLocationId());
	        	statement.setInt(++counter, dto.getProductLevelId());
	        	statement.setInt(++counter, dto.getProductId());
	        	statement.setInt(++counter, dto.getCompChange());
	        	statement.setInt(++counter, dto.getCostChange());
	        	statement.setInt(++counter, dto.getNewItem());
	        	statement.setInt(++counter, dto.getKviCompChange());
	        	statement.setInt(++counter, dto.getKviCostChange());
	        	statement.setInt(++counter, dto.getKviNewItem());
	        	if(dto.getStrategyId() != null)
	        		statement.setInt(++counter, dto.getStrategyId());
	        	else
	        		statement.setNull(++counter, Types.INTEGER);
	        	statement.setString(++counter, "BATCH");
	        	if(dto.getKviStrategyId() != null)
	        		statement.setInt(++counter, dto.getKviStrategyId());
	        	else
	        		statement.setNull(++counter, Types.INTEGER);
	        	statement.setInt(++counter, dto.getNoOfItems());
	        	statement.setInt(++counter, dto.getNoOfKviItems());
	        	statement.setDouble(++counter, dto.getTotRevenue());
	        	statement.setDouble(++counter, dto.getKviTotRevenue());
	        	
	        	statement.setInt(++counter, dto.getDistinctCompChange());
	        	statement.setInt(++counter, dto.getDistinctKviCompChange());
	        	statement.setInt(++counter, dto.getDistinctCostChange());
	        	statement.setInt(++counter, dto.getDistinctKviCostChange());
	        	statement.setInt(++counter, dto.getDistinctNewItem());
	        	statement.setInt(++counter, dto.getDistinctKviNewItem());
	        	statement.setInt(++counter, dto.getDistinctNoOfItems());
	        	statement.setInt(++counter, dto.getDistinctNoOfKviItems());
	        	
	        	statement.setString(++counter, dto.getPrimaryCompRecentCheckDate());
	        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while inserting dashboard data " + e.getMessage());
			throw new GeneralException("Error while inserting dashboard data " + e.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Updates into PR_DASHBOARD
	 * @param updateList
	 * @param dashboard 
	 */
	public void updateDashboardData(ArrayList<DashboardDTO> updateList, Dashboard dashboard) throws GeneralException{
		StringBuffer updateParameterQuery = new StringBuffer();
		if(dashboard.isCompChangeCntReqd()){
			updateParameterQuery.append(" NO_COMP_CHANGE = ?,");
			updateParameterQuery.append(" KVI_NO_COMP_CHANGE = ?,");
			updateParameterQuery.append(" DIST_NO_COMP_CHANGE = ?,");
			updateParameterQuery.append(" DIST_KVI_NO_COMP_CHANGE = ?,");
		}
		if(dashboard.isCostChangeCntReqd()){
			updateParameterQuery.append(" NO_COST_CHANGE = ?,");
			updateParameterQuery.append(" KVI_NO_COST_CHANGE = ?,");
			updateParameterQuery.append(" DIST_NO_COST_CHANGE = ?,");
			updateParameterQuery.append(" DIST_KVI_NO_COST_CHANGE = ?,");
		}
		if(dashboard.isNewItemCntReqd()){
			updateParameterQuery.append(" NO_NEW_ITEM = ?,");
			updateParameterQuery.append(" KVI_NO_NEW_ITEM = ?,");
			updateParameterQuery.append(" DIST_NO_NEW_ITEM = ?,");
			updateParameterQuery.append(" DIST_KVI_NO_NEW_ITEM = ?,");
		}
		if(dashboard.isStrategyIdReqd()){
			updateParameterQuery.append(" STRATEGY_ID = ?,");
			updateParameterQuery.append(" KVI_STRATEGY_ID = ?,");
		}
		if(dashboard.isItemCntReqd()){
			updateParameterQuery.append(" TOT_ITEMS = ?,");
			updateParameterQuery.append(" KVI_TOT_ITEMS = ?,");
			updateParameterQuery.append(" DIST_TOT_ITEMS = ?,");
			updateParameterQuery.append(" DIST_KVI_TOT_ITEMS = ?,");
		}
		if(dashboard.isRevenueReqd()){
			updateParameterQuery.append(" TOT_REVENUE = ?,");
			updateParameterQuery.append(" KVI_TOT_REVENUE = ?,");
		}
		updateParameterQuery.append(" PRIMARY_COMP_RECENT_CHECK_DATE = TO_DATE(?, 'MM/dd/yyyy'),");
		
		updateParameterQuery.append(" UPDATED_BY = ?,");
		updateParameterQuery.append(" UPDATED = SYSDATE");
		
		PreparedStatement statement = null;
	    try{
	    	String sql = UPDATE_PR_DASHBOARD.replaceAll("%UPDATE_PARAMETERS%", updateParameterQuery.toString());
	    	logger.debug("SQL is " + sql);
	    	statement = conn.prepareStatement(sql);
			int recordCnt = 0;
			int itemNoInBatch = 0;
	        for(DashboardDTO dto:updateList){
	        	int counter = 0;
	        	if(dashboard.isCompChangeCntReqd()){
	        		statement.setInt(++counter, dto.getCompChange());
	        		statement.setInt(++counter, dto.getKviCompChange());
	        		statement.setInt(++counter, dto.getDistinctCompChange());
	        		statement.setInt(++counter, dto.getDistinctKviCompChange());
	        	}
	        	if(dashboard.isCostChangeCntReqd()){
	        		statement.setInt(++counter, dto.getCostChange());
	        		statement.setInt(++counter, dto.getKviCostChange());
	        		statement.setInt(++counter, dto.getDistinctCostChange());
	        		statement.setInt(++counter, dto.getDistinctKviCostChange());
	        	}
	        	if(dashboard.isNewItemCntReqd()){
	        		statement.setInt(++counter, dto.getNewItem());
	        		statement.setInt(++counter, dto.getKviNewItem());
	        		statement.setInt(++counter, dto.getDistinctNewItem());
	        		statement.setInt(++counter, dto.getDistinctKviNewItem());
	        	}
	        	if(dashboard.isStrategyIdReqd()){
	        		if(dto.getStrategyId() != null)
		        		statement.setInt(++counter, dto.getStrategyId());
		        	else
		        		statement.setNull(++counter, Types.INTEGER);
	        		if(dto.getKviStrategyId() != null)
		        		statement.setInt(++counter, dto.getKviStrategyId());
		        	else
		        		statement.setNull(++counter, Types.INTEGER);
	        	}
	        	if(dashboard.isItemCntReqd()){
	        		statement.setInt(++counter, dto.getNoOfItems());
		        	statement.setInt(++counter, dto.getNoOfKviItems());
		        	statement.setInt(++counter, dto.getDistinctNoOfItems());
		        	statement.setInt(++counter, dto.getDistinctNoOfKviItems());
	        	}
	        	if(dashboard.isRevenueReqd()){
	        		statement.setDouble(++counter, dto.getTotRevenue());
		        	statement.setDouble(++counter, dto.getKviTotRevenue());
	        	}
	        	
	        	statement.setString(++counter, dto.getPrimaryCompRecentCheckDate());
	        	
	        	statement.setString(++counter, "BATCH");
	        	statement.setInt(++counter, dto.getDashboardId());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while updating dashboard data " + e.getMessage());
			throw new GeneralException("Error while updating dashboard data " + e.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Retrieves strategies active as on current date
	 */
	public HashMap<String, HashMap<String, Integer>> getStrategies(String currentDate) throws GeneralException{
		HashMap<String, HashMap<String, Integer>> strategyMap = new HashMap<String, HashMap<String,Integer>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int counter = 0;
	    try{
	    	stmt = conn.prepareStatement(GET_ACTIVE_STRATEGIES);
	    	stmt.setString(++counter, currentDate);
	    	stmt.setString(++counter, currentDate);
	    	stmt.setString(++counter, currentDate);
	    	rs = stmt.executeQuery();
	    	while(rs.next()){
	    		int locationLevelId = rs.getInt("LOCATION_LEVEL_ID");
	    		int locationId = rs.getInt("LOCATION_ID");
	    		int productLevelId = rs.getInt("PRODUCT_LEVEL_ID");
	    		int productId = rs.getInt("PRODUCT_ID");
	    		int strategyId = rs.getInt("STRATEGY_ID");
	    		int priceCheckListId = rs.getInt("APPLY_TO");
	    		
	    		String locKey = locationLevelId + "-" + locationId;
	    		String proKey = productLevelId + "-" + productId;
	    		
	    		if(priceCheckListId > 0) proKey = proKey + "-" + priceCheckListId;
	    		
	    		if(strategyMap.get(locKey) != null){
	    			HashMap<String, Integer> tempProductMap = strategyMap.get(locKey);
	    			if(tempProductMap.get(proKey) == null){
	    				tempProductMap.put(proKey, strategyId);
		    			strategyMap.put(locKey, tempProductMap);
	    			}
	    		}else{
	    			HashMap<String, Integer> tempProductMap = new HashMap<String, Integer>();
	    			tempProductMap.put(proKey, strategyId);
	    			strategyMap.put(locKey, tempProductMap);
	    		}
	    	}
	    }catch (SQLException e){
			logger.error("Error while retrieving strategies " + e.getMessage());
			throw new GeneralException("Error while retrieving strategies " + e.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}	
	    return strategyMap;
	}
	
	/**
	 * Returns location list ids that has input product id as its child
	 * @param locationLevelId
	 * @param locationId
	 * @return
	 */
	public ArrayList<Integer> getLocationListId(int locationLevelId, int locationId){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> locationListId = new ArrayList<Integer>();
		int counter = 0;
	    try{
	    	stmt = conn.prepareStatement(GET_LOCATION_LIST_ID);
	    	stmt.setInt(++counter, locationLevelId);
	    	stmt.setInt(++counter, locationId);
	    	rs = stmt.executeQuery();
	    	while(rs.next()){
	    		locationListId.add(rs.getInt("LOCATION_ID"));
	    	}
	    }catch (SQLException e){
			logger.error("Error while retrieving location list id " + e.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
	    return locationListId;
	}
	
	/**
	 * Returns category hierarchy for all categories
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, ProductDTO> getCategoryHierarchy(int productId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, ProductDTO> categoryDetails = new HashMap<Integer, ProductDTO>();
		try{
			String sql = GET_CATEGORY_HIERARCHY;
			if(productId > 0)
				sql = sql.replaceAll("%CHILD_PRODUCT_ID_CONDITION%", "AND A1.CHILD_PRODUCT_ID = ?");
			else
				sql = sql.replaceAll("%CHILD_PRODUCT_ID_CONDITION%", "");
				
			stmt = conn.prepareStatement(sql);
			if(productId > 0)
				stmt.setInt(1, productId);
	    	stmt.setFetchSize(1000);
	    	rs = stmt.executeQuery();
	    	while(rs.next()){
	    		if(rs.getInt("P_L_ID_1") != PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID && rs.getInt("P_L_ID_2") != PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID){
		    		int categoryId = rs.getInt("CATEGORY_ID");
		    		ProductDTO pDto = new ProductDTO();
		    		pDto.setCategoryId(categoryId);
		    		populateHierarchy(pDto, rs.getInt("P_L_ID_1"), rs.getInt("P_ID_1"));
		    		populateHierarchy(pDto, rs.getInt("P_L_ID_2"), rs.getInt("P_ID_2"));
		    		categoryDetails.put(categoryId, pDto);
	    		}
	    	}
	    }catch (SQLException e){
			logger.error("Error while retrieving category hierarchy " + e.getMessage());
			throw new GeneralException("Error while retrieving category hierarchy " + e.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return categoryDetails;
	}
	
	public void populateHierarchy(ProductDTO pDTO, int productLevelId, int productId){
		if(productLevelId == Constants.DEPARTMENTLEVELID){
			pDTO.setDeptId(productId);
		}else if(productLevelId == Constants.PORTFOLIO){
			pDTO.setPortfolioId(productId);
		}
	}
	
	/**
	 * Gets reset date from dashboard table
	 * @param locationLevelId	Location Level Id
	 * @param locationId		Location Id
	 * @param productLevelId	Product Level Id
	 * @param productId			Product Id
	 * @return
	 * @throws GeneralException
	 */
	public String getDashboardResetDate(int locationLevelId, int locationId, int productLevelId, int productId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String resetDate = null;
		try{
			stmt = conn.prepareStatement(GET_DASHBOARD_RESET_DATE);
			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			stmt.setInt(3, productLevelId);
			stmt.setInt(4, productId);
			rs = stmt.executeQuery();
			if(rs.next()){
				resetDate = rs.getString("RESET_DATE");
			}
		}catch (SQLException e){
			logger.error("Error while retrieving reset date " + e.getMessage());
			throw new GeneralException("Error while retrieving reset date " + e.getMessage());
		}finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
		}
		return resetDate;
	}
	
	public Boolean getIsAutomaticRecommendation(int locationLevelId, int locationId, int productLevelId, int productId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		//String resetDate = null;
		Boolean isAutomaticRecommendation = true;
		try{
			stmt = conn.prepareStatement(GET_DASHBOARD_IS_AUTOMATIC_RECOMMENDATION);
			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			stmt.setInt(3, productLevelId);
			stmt.setInt(4, productId);
			rs = stmt.executeQuery();
			if(rs.next()){
				isAutomaticRecommendation = rs.getString("IS_AUTOMATIC_RECOMMENDATION").charAt(0) == Constants.NO ?
						false : true;				
			}
		}catch (SQLException e){
			logger.error("Error while retrieving reset date " + e.getMessage());
			throw new GeneralException("Error while retrieving reset date " + e.getMessage());
		}finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
		}
		return isAutomaticRecommendation;
	}
	
	/**
	 * Retrieves Revenue data
	 * @param locationId	Location Id
	 * @param productId		Product Id
	 * @param date			Date
	 * @return
	 * @throws GeneralException 
	 */
	public HashMap<Integer, Double> getRevenueData(int locationId, int productLevelId, int productId, String date,
			List<Integer> priceZoneStores, int locationLevelId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, Double> revenueMap = new HashMap<Integer, Double>();
		try{
			String sql = null;
			sql = GET_REVENUE_DATA;
			String psStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores); 
			sql = sql.replaceAll("%PRICE_ZONE_STORES%", psStores);
			sql = sql.replaceAll("%AUTOHROIZED_ITEMS_QUERY%", queryToGetAuthorizedItems(productLevelId, productId, priceZoneStores, locationId, locationLevelId));
			
			stmt = conn.prepareStatement(sql);
			//stmt = conn.prepareStatement(GET_REVENUE_DATA);
			stmt.setString(1, date);
			//stmt.setInt(2, locationId);
			//stmt.setInt(3, locationId);
			stmt.setInt(2, productLevelId);
			stmt.setInt(3, productId);
			rs = stmt.executeQuery();
			while(rs.next()){
				revenueMap.put(rs.getInt("PRODUCT_ID"), rs.getDouble("TOT_REVENUE"));
			}
			
			logger.debug("Get Revenue Query:" + sql + ",Parameters--Date:" + date + ",productLevelId:" + productLevelId + ",productId:" + productId);
			
		}catch (SQLException e){
			logger.error("Error while retrieving revenue data " + e.getMessage());
		}finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
		}
		return revenueMap;
	}

	/*private String queryToGetAuthorizedItems(int productLevelId, int productId){
		
		StringBuffer subQuery = new StringBuffer("");
		subQuery.append(" SELECT ITEM_CODE FROM STORE_ITEM_MAP WHERE LEVEL_TYPE_ID = 2 AND LEVEL_ID IN ( ");
		subQuery.append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ? ) ");
		//subQuery.append(" AND COST_INDICATOR = 'Y' ");
		subQuery.append(" AND IS_AUTHORIZED = 'Y' " );
		subQuery.append(" AND ITEM_CODE IN ");
		subQuery.append("(SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR ");	
		
		if(productLevelId > 1){
			subQuery.append("start with product_level_id = ? ");
			if(productId > 0){
				subQuery.append(" and product_id = ? ");
			}
			subQuery.append("connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id");	
		}else if(productLevelId == 1){
			subQuery.append("where child_product_level_id = 1 and child_product_id = ?");
		}
		subQuery.append(") WHERE CHILD_PRODUCT_LEVEL_ID = 1 )");
		
		return subQuery.toString();
	}*/
	
	private String queryToGetAuthorizedItems(int productLevelId, int productId, List<Integer> priceZoneStores, int locationId, 
			int locationLevelId) throws GeneralException {
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		String storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
		StringBuffer subQuery = new StringBuffer("");
		subQuery.append(" SELECT ITEM_CODE FROM ( ");
		subQuery.append(itemDAO.queryToGetAuthorizedItems(conn, productLevelId, productId, storeIds, locationId, locationLevelId));
		subQuery.append(" ) ");
		return subQuery.toString();
	}
	
	public void updateFuturePIAndValue(int locationLevelId, int locationId, int productLevelId, int productId,
			Double futurePI, Double value) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_FUTURE_PI_VALUE);
			if (futurePI != null)
				stmt.setDouble(1, futurePI);
			else
				stmt.setNull(1, java.sql.Types.DOUBLE);
	
			if (value != null)
				stmt.setDouble(2, value);
			else
				stmt.setNull(2, java.sql.Types.DOUBLE);
			stmt.setInt(3, locationLevelId);
			stmt.setInt(4, locationId);
			stmt.setInt(5, productLevelId);
			stmt.setInt(6, productId);
			 
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateFuturePIAndValue() - " + exception.toString());
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void updateActiveStatus(int locationLevelId, int locationId, int productLevelId, int productId,
			String activeStatus) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_ACTIVE);
			stmt.setString(1, String.valueOf(activeStatus));
			stmt.setInt(2, locationLevelId);
			stmt.setInt(3, locationId);
			stmt.setInt(4, productLevelId);
			stmt.setInt(5, productId);
			 
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateActiveStatus() - " + exception.toString());
			throw new GeneralException(exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public String getRecentCompCheckDate(int productLevelId, int productId, int compStrId, List<Integer> priceZoneStores, int locationId,
			int locationLevelId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String recentCompCheckDate = null;
		try{
			stmt = conn.prepareStatement(GET_RECENT_CHECK_DATE);
			
			String sql = GET_RECENT_CHECK_DATE;
			sql = sql.replaceAll("%AUTOHROIZED_ITEMS_QUERY%", queryToGetAuthorizedItems(productLevelId, productId, priceZoneStores, locationId,
					locationLevelId));
			
			logger.debug("Authorzation Qry: " + sql);
			logger.debug("Params: 1 = " + compStrId + ", 2 = " + productLevelId + ", 3 = " + productId);
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, compStrId);
			stmt.setInt(2, productLevelId);
			stmt.setInt(3, productId);
			rs = stmt.executeQuery();
			if(rs.next()){
				recentCompCheckDate = rs.getString("RECENT_CHECK_DATETIME");
			}
		}catch (SQLException e){
			logger.error("Error in getRecentCompCheckDate() " + e.getMessage());
			throw new GeneralException("Error in getRecentCompCheckDate() " + e.getMessage());
		}finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
		}
		return recentCompCheckDate;
	}
}
