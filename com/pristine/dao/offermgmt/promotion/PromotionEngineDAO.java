package com.pristine.dao.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.itemClassification.ItemClassificationDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoAdSaleInfo;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.ProductService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * 
 * @author Nagarajan
 *
 */
public class PromotionEngineDAO {

	private static Logger logger = Logger.getLogger("PromotionEngineDAO");

	private final String GET_HIGH_HH_IMPACT_ITEMS = "SELECT ROWNUM, PRODUCT_ID, PRODUCT_LEVEL_ID, "
			+ " VISIT_CNT, UNIQUE_HOUSEHOLD_CNT, ANNUAL_SPEND_DOLLAR FROM ( SELECT PRODUCT_ID,"
			+ " PRODUCT_LEVEL_ID, VISIT_CNT, UNIQUE_HOUSEHOLD_CNT, ANNUAL_SPEND_DOLLAR " + " FROM ITEM_CLASSIFICATION IC WHERE "
			+ " RUN_ID IN (    SELECT RUN_ID FROM ( SELECT RUN_ID, RANKING_PRODUCT_ID, RANK() OVER "
			+ " (PARTITION BY LOCATION_LEVEL_ID, LOCATION_ID, RANKING_PRODUCT_ID, RANKING_PRODUCT_LEVEL_ID,"
			+ " ANALYSIS_LEVEL_ID ORDER BY UPDATED DESC) AS RANK FROM ITEM_CLASSIFICATION_RUN_HEADER "
			+ " WHERE (RANKING_PRODUCT_ID, RANKING_PRODUCT_LEVEL_ID) IN (SELECT CHILD_PRODUCT_ID ,"
			+ " CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_ID = ? "
			+ " AND PRODUCT_LEVEL_ID = ?) AND LOCATION_ID = ? AND LOCATION_LEVEL_ID = ?  AND ANALYSIS_LEVEL_ID =11 "
			+ " AND ACTIVE = 'Y' ) WHERE RANK = 1) ORDER BY UNIQUE_HOUSEHOLD_RANK ASC ) " + " WHERE VISIT_CNT IS NOT NULL";

	private final String GET_HH_REC_ITEMS = "SELECT ROWNUM,PRODUCT_LEVEL_ID, PRODUCT_ID, NO_OF_HOUSEHOLD FROM ( "
			+ " SELECT OFFERED_PRODUCT_LEVEL AS PRODUCT_LEVEL_ID, OFFERED_PRODUCT_ID AS PRODUCT_ID, "
			+ " COUNT(DISTINCT T.CUSTOMER_ID) AS NO_OF_HOUSEHOLD FROM CST_TARGETED_OFFER T WHERE PREDICTION_PERIOD_START  = TO_DATE(?,'MM/dd/yyyy') "
			+ "	AND "
			// + "TARGETED_FOR = 'Increasing Breadth' "
			+ " UPPER(TARGETED_FOR) IN ('INCREASING BREADTH','INCREASING VISITS') "
			+ " AND (OFFERED_PRODUCT_LEVEL, OFFERED_PRODUCT_ID) IN (SELECT PRODUCT_LEVEL_ID, " + " PRODUCT_ID FROM "
			+ " (SELECT	CASE NVL(IL.RET_LIR_ID,0) WHEN 0 THEN " + Constants.ITEMLEVELID + " ELSE " + Constants.PRODUCT_LEVEL_ID_LIG
			+ " END AS PRODUCT_LEVEL_ID, " + " CASE NVL(IL.RET_LIR_ID,0) WHEN 0 THEN IL.ITEM_CODE ELSE IL.RET_LIR_ID END AS PRODUCT_ID" + " FROM "
			+ " (SELECT CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR"
			+ " START WITH product_level_id = ?	AND product_id IN ( %PRODUCT_LIST% ) CONNECT BY prior child_product_id = product_id  AND "
			+ " prior child_product_level_id = product_level_id) WHERE CHILD_PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID + ") ITEMS"
			+ " LEFT JOIN ITEM_LOOKUP IL ON ITEMS.CHILD_PRODUCT_ID = IL.ITEM_CODE)) "
			+ "	GROUP BY OFFERED_PRODUCT_LEVEL,OFFERED_PRODUCT_ID ORDER BY COUNT(DISTINCT T.CUSTOMER_ID) DESC)";

	private final String ITEM_STORE_MAP = "SELECT SIM.ITEM_CODE AS ITEM_CODE, SIM.LEVEL_ID AS LEVEL_ID,  "
			+ " SIM.PRICE_ZONE_ID AS ALT_PRICE_ZONE_ID, RPZ.PRICE_ZONE_ID AS PRICE_ZONE_ID FROM STORE_ITEM_MAP SIM "
			+ "	LEFT JOIN COMPETITOR_STORE CS ON SIM.LEVEL_ID = CS.COMP_STR_ID "
			+ " LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID =  RPZ.PRICE_ZONE_ID " + " WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN ( "
			// + "5672,5709,5681,5736,5894,5849,5767,5741,5673 ) "
			+ " %STORES_LIST% ) " + " AND SIM.ITEM_CODE IN (" + " SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM "
			+ " PRODUCT_GROUP_RELATION PGR START WITH PRODUCT_LEVEL_ID = ? AND PRODUCT_ID IN (%DEPT_LIST%) CONNECT BY "
			+ " PRIOR CHILD_PRODUCT_ID = PRODUCT_ID AND PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID "
			+ " ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ";//AND SIM.IS_AUTHORIZED = 'Y' ";

	private static final String PPG_GROUP_DETAILS = "SELECT * FROM (SELECT PPG_FINAL.*, "
			+ " PR.NAME  AS CATEGORY_NAME, PR.PRODUCT_ID AS CATEGORY_ID, PR1.NAME AS DEPARTMENT_NAME, "
			+ " PR1.PRODUCT_ID AS DEPARTMENT_ID FROM (SELECT PPG_1.GROUP_ID, PPG_1.ITEM_LEVEL_ID, "
			+ " PPG_1.ITEM_ID, CASE PPG_1.IS_LEAD_ITEM WHEN 1 THEN 'Y' ELSE 'N' END AS IS_LEAD_ITEM, "
			+ " PPG_1.TOTAL_MOVEMENT, PPG_1.LIG_REP_OR_ITEM_CODE, "
			+ " CASE ITEM_LEVEL_ID WHEN 11 THEN RET_LIR_NAME ELSE ITEM_NAME END AS LIG_OR_ITEM_NAME , "
			+ " CASE ITEM_LEVEL_ID WHEN 11 THEN BL.BRAND_NAME ELSE BL1.BRAND_NAME END AS BRAND_NAME, "
			+ " CASE ITEM_LEVEL_ID WHEN 11 THEN BL.BRAND_ID ELSE BL1.BRAND_ID END AS BRAND_ID "
			+ " FROM (SELECT PPG.GROUP_ID, PPG.ITEM_LEVEL_ID, PPG.ITEM_ID, PPG.IS_LEAD_ITEM, "
			+ " PPG.TOTAL_MOVEMENT, BRAND_ID, CASE ITEM_LEVEL_ID WHEN 11 THEN ITEM_CODE "
			+ " ELSE ITEM_ID END AS LIG_REP_OR_ITEM_CODE, RET_LIR_NAME FROM (SELECT * FROM " + " PM_PPG_ITEMS WHERE  GROUP_ID IN "
			+ " (SELECT GROUP_ID FROM PM_PPG_HEADER WHERE RUN_ID IN "
			+ " (SELECT RUN_ID FROM (select MAX(RUN_ID) AS RUN_ID, DEPARTMENT_ID FROM PM_PPG_HEADER "
			+ " GROUP BY DEPARTMENT_ID ) WHERE DEPARTMENT_ID IN (%DEPARTMENT_LIST%) ))) PPG "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON LIG.RET_LIR_ID = PPG.ITEM_ID "
			+ " LEFT JOIN ITEM_LOOKUP IL ON IL.RET_LIR_ID = LIG.RET_LIR_ID AND " + " IL.ITEM_CODE = LIG.RET_LIR_ITEM_CODE AND IL.LIR_IND   = 'Y' "
			+ " ) PPG_1 LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PPG_1.ITEM_ID " + " LEFT JOIN BRAND_LOOKUP BL ON PPG_1.BRAND_ID = BL.BRAND_ID "
			+ " LEFT JOIN BRAND_LOOKUP BL1 ON IL.BRAND_ID = BL1.BRAND_ID ) PPG_FINAL "
			+ " LEFT JOIN (SELECT ITEM_CODE AS ITEM_CODE_1, P_L_ID_1, P_ID_1, P_L_ID_2, " + " P_ID_2, P_L_ID_3, P_ID_3, P_L_ID_4, P_ID_4 FROM "
			+ " (SELECT A1.PRODUCT_LEVEL_ID P_L_ID_1, A1.PRODUCT_ID P_ID_1, A1.CHILD_PRODUCT_ID ITEM_CODE , "
			+ " A2.PRODUCT_LEVEL_ID P_L_ID_2, A2.PRODUCT_ID P_ID_2, A2.CHILD_PRODUCT_ID CHILD_P_ID_2, "
			+ " A3.PRODUCT_LEVEL_ID P_L_ID_3, A3.PRODUCT_ID P_ID_3, A3.CHILD_PRODUCT_ID CHILD_P_ID_3, "
			+ " A4.PRODUCT_LEVEL_ID P_L_ID_4, A4.PRODUCT_ID P_ID_4, A4.CHILD_PRODUCT_ID CHILD_P_ID_4 " + " FROM ( ( ( ( PRODUCT_GROUP_RELATION A1 "
			+ " LEFT JOIN PRODUCT_GROUP_RELATION A2 " + " ON A1.PRODUCT_ID        = A2.CHILD_PRODUCT_ID"
			+ " AND A1.PRODUCT_LEVEL_ID = A2.CHILD_PRODUCT_LEVEL_ID)" + " LEFT JOIN PRODUCT_GROUP_RELATION A3"
			+ " ON A2.PRODUCT_ID        = A3.CHILD_PRODUCT_ID" + " AND A2.PRODUCT_LEVEL_ID = A3.CHILD_PRODUCT_LEVEL_ID )"
			+ " LEFT JOIN PRODUCT_GROUP_RELATION A4" + " ON A3.PRODUCT_ID        = A4.CHILD_PRODUCT_ID"
			+ " AND A3.PRODUCT_LEVEL_ID = A4.CHILD_PRODUCT_LEVEL_ID )" + " LEFT JOIN PRODUCT_GROUP_RELATION A5"
			+ " ON A4.PRODUCT_ID                = A5.CHILD_PRODUCT_ID" + " AND A4.PRODUCT_LEVEL_ID         = A5.CHILD_PRODUCT_LEVEL_ID )"
			+ " WHERE A1.CHILD_PRODUCT_LEVEL_ID = 1" + " ) WHERE P_L_ID_4 = 5 ) PRODUCT_HIERARCHY"
			+ " ON PRODUCT_HIERARCHY.ITEM_CODE_1 = PPG_FINAL.LIG_REP_OR_ITEM_CODE" + " LEFT JOIN PRODUCT_GROUP PR"
			+ " ON PR.PRODUCT_ID        = PRODUCT_HIERARCHY.P_ID_3" + " AND PR.PRODUCT_LEVEL_ID = 4" + " LEFT JOIN PRODUCT_GROUP PR1"
			+ " ON PR1.PRODUCT_ID        = PRODUCT_HIERARCHY.P_ID_4" + " AND PR1.PRODUCT_LEVEL_ID = 5 " + " ) " + " ORDER BY GROUP_ID ASC ";

	private static final String GET_PAST_AD_DETAILS = " SELECT ITEMS.ITEM_CODE, ITEMS.PAGE_NUMBER, "
			+ " ITEMS.BLOCK_NUMBER, ITEMS.SALE_QTY, ITEMS.SALE_PRICE, ITEMS.SALE_M_PRICE, ITEMS.REG_QTY,"
			+ " ITEMS.REG_PRICE, ITEMS.REG_M_PRICE, TO_CHAR(RC.START_DATE,'MM/dd/YYYY') AS AD_DATE, ITEMS.LIST_COST, ITEMS.DEAL_COST,"
			+ " ITEMS.PROMO_TYPE_ID FROM"
			+ "	(SELECT PBI.PROMO_DEFINITION_ID, PBI.ITEM_CODE,PAGE_NUMBER, BLOCK_NUMBER, WEEKLY_AD_ID, PBI.SALE_QTY, "
			+ " PBI.SALE_PRICE, PBI.SALE_M_PRICE, PBI.REG_QTY, PBI.REG_PRICE, PBI.REG_M_PRICE, PBI.LIST_COST, PBI.DEAL_COST, PD.PROMO_TYPE_ID FROM "
			+ " PM_WEEKLY_AD_PROMO WAP " + " LEFT JOIN PM_PROMO_BUY_ITEM PBI ON PBI.PROMO_DEFINITION_ID = WAP.PROMO_DEFINITION_ID "
			+ " LEFT JOIN PM_PROMO_DEFINITION PD ON PBI.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID "
			+ " LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON WAB.BLOCK_ID = WAP.BLOCK_ID " + " LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON WAB.PAGE_ID = WAP.PAGE_ID "
			+ " WHERE PD.PROMO_TYPE_ID IN (" + PromoTypeLookup.BOGO.getPromoTypeId() + "," + PromoTypeLookup.STANDARD.getPromoTypeId() + ")"
			+ " AND WAP.PAGE_ID IN "
			+ " (SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID IN (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION "
			+ " WHERE (%PROMO_LOCATIONS%) AND CALENDAR_ID IN ( "
			+ " SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE='W' AND START_DATE <= TO_DATE(?,'MM/dd/yyyy') "
			+ " AND START_DATE >= TO_DATE(?,'MM/dd/yyyy') - ((?-1)*7))))" + " AND PD.PROMO_DEFINITION_ID IN ("
			+ " SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_LOCATION PL"
			+ " WHERE PL.PROMO_DEFINITION_ID IN (SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_DEFINITION PD "
			+ " LEFT JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PD.START_CALENDAR_ID"
			+ " LEFT JOIN RETAIL_CALENDAR RC2 ON RC2.CALENDAR_ID = PD.END_CALENDAR_ID"
			+ " WHERE RC1.START_DATE <= TO_DATE(?,'MM/dd/yyyy') AND RC2.START_DATE >= TO_DATE(?,'MM/dd/yyyy') - ((?-1)*7)) AND"
			+ " (%PROMO_LOCATIONS%))" + ") ITEMS " + " LEFT JOIN PM_WEEKLY_AD_DEFINITION WAD   ON WAD.WEEKLY_AD_ID = ITEMS.WEEKLY_AD_ID"
			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = WAD.CALENDAR_ID" + "	WHERE ITEM_CODE IN (%ITEMS_IN_CATEGORY%)";
	// + " WHERE ITEM_CODE IN (10832, 10952) ";
	// + " WHERE ITEM_CODE IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE RET_LIR_ID = 27313) ";//

	private static final String GET_RETAIL_COST = "SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, "
			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE " + " CAST_DATA(COMP_STR_NO,"
			+ Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) + " )= " + " CAST_DATA(LEVEL_ID,"
			+ Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) + ")" + " AND COMP_CHAIN_ID = ?) "
			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE " + "CAST_DATA(ZONE_NUM,"
			+ Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) + " )= " + " CAST_DATA(LEVEL_ID,"
			+ Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) + "))"
			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, " + "LIST_COST, DEAL_COST, VIP_COST, "
			+ " TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, "
			+ "DEAL_COST, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE " + ", LIST_COST_2"
			+ "  FROM SYN_RETAIL_COST_INFO " + "  WHERE CALENDAR_ID IN ("
			+ " SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE='W' AND START_DATE <= TO_DATE(?,'MM/dd/yyyy')"
			+ " AND START_DATE >= TO_DATE(?,'MM/dd/yyyy') - ((?-1)*7)) " + "  AND ITEM_CODE IN (%s) " + "  AND (%LOCATION_SUBQUERY%)";
	
	private static final String GET_RETAIL_PRICE = "SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, "
			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE " + " CAST_DATA(COMP_STR_NO,"
			+ Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) + " )= " + " CAST_DATA(LEVEL_ID,"
			+ Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) + ")" + " AND COMP_CHAIN_ID = ?) "
			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE " + "CAST_DATA(ZONE_NUM,"
			+ Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) + " )= " + " CAST_DATA(LEVEL_ID,"
			+ Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) + "))"
			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, " 
			+ " REG_PRICE, REG_QTY, REG_M_PRICE "
			+ " FROM SYN_RETAIL_PRICE_INFO " + "  WHERE CALENDAR_ID IN ("
			+ " SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE='W' AND START_DATE <= TO_DATE(?,'MM/dd/yyyy')"
			+ " AND START_DATE >= TO_DATE(?,'MM/dd/yyyy') - ((?-1)*7)) " + "  AND ITEM_CODE IN (%s) " + "  AND (%LOCATION_SUBQUERY%)";
	
	
	
	public HashMap<ProductKey, ItemClassificationDTO> getHighImpactHouseHoldItems(Connection conn, int productLevelId, int productId,
			int locationLevelId, int locationId, HashMap<ProductKey, ItemClassificationDTO> itemClassificationFullData) throws GeneralException {
		// Create separate hashmap
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<ProductKey, ItemClassificationDTO> maxHHImpactItemList = new HashMap<ProductKey, ItemClassificationDTO>();

		HashMap<Integer, ItemClassificationDTO> orderedHHImpactItemList = new HashMap<Integer, ItemClassificationDTO>();

		try {
			logger.debug("getHighImpactHouseHoldItems(): Start...");
			// At product major category level, location level can be storeList or Zone
			String query = GET_HIGH_HH_IMPACT_ITEMS;
			// query = query.replaceAll("%PRODUCT_LIST%" , productIdList);
			logger.debug(query);

			stmt = conn.prepareStatement(query);

			stmt.setInt(1, productId);
			stmt.setInt(2, productLevelId);
			stmt.setInt(3, locationId);
			stmt.setInt(4, locationLevelId);

			rs = stmt.executeQuery();

			while (rs.next()) {

				ItemClassificationDTO itemClassificationObj = new ItemClassificationDTO();
				if (rs.getObject("ROWNUM") != null) {
					itemClassificationObj.setRankId(rs.getInt("ROWNUM"));
				} else {
					continue;
				}
				if ((rs.getObject("PRODUCT_ID") != null) && (rs.getObject("PRODUCT_LEVEL_ID") != null)) {
					itemClassificationObj.setProductKey(new ProductKey(rs.getInt("PRODUCT_LEVEL_ID"), rs.getInt("PRODUCT_ID")));
				} else {
					continue;
				}
				if (rs.getObject("VISIT_CNT") != null) {
					itemClassificationObj.setVisitCount(rs.getInt("VISIT_CNT"));
				}
				if (rs.getObject("UNIQUE_HOUSEHOLD_CNT") != null) {
					itemClassificationObj.setUniqueHouseHoldCount(rs.getInt("UNIQUE_HOUSEHOLD_CNT"));
				}
				if (rs.getObject("ANNUAL_SPEND_DOLLAR") != null) {
					itemClassificationObj.setAnnualSpendDollar(rs.getDouble("ANNUAL_SPEND_DOLLAR"));
				}

				itemClassificationObj.setDepartmentId(productId);
				orderedHHImpactItemList.put(itemClassificationObj.getRankId(), itemClassificationObj);
				itemClassificationFullData.put(itemClassificationObj.getProductKey(), itemClassificationObj);
			}

			logger.debug("Collected all records form Item classification : records count : " + orderedHHImpactItemList.size());
			// Calculating the 30% records of total records taking a Ceiling in case total record is 1
			int maxRecordLimit = (int) (Math
					.ceil(orderedHHImpactItemList.size() * Integer.parseInt(PropertyManager.getProperty("MAX_HH_RECORDS_PERCENTAGE")) / 100));

			// If Percentage count exceeds the maximum record limit to select (i.e. 50 records)
			if (maxRecordLimit > Integer.parseInt(PropertyManager.getProperty("MAX_HH_IMPACT_RECORDS"))) {
				// updating the records as max record count limit : i.e. -50
				maxRecordLimit = Integer.parseInt(PropertyManager.getProperty("MAX_HH_IMPACT_RECORDS"));
			}

			logger.debug("TOP records count to be picked is : " + maxRecordLimit);
			// Taking the top rank records and populating the output Hashmap
			for (Integer rank : orderedHHImpactItemList.keySet()) {
				ItemClassificationDTO itemClassificationDTO = orderedHHImpactItemList.get(rank);

				maxHHImpactItemList.put(itemClassificationDTO.getProductKey(), itemClassificationDTO);
				if (--maxRecordLimit < 1) {
					break; // breaking the loop after records with required count are selected
				}
			}
		} catch (Exception e) {
			logger.error("getHighImpactHouseHoldItems(): Error while retrieving Top houseHoldImpact list " + e.getMessage());
			throw new GeneralException("Error in highImpactLirHouseHoldBased() - " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.debug("getHighImpactHouseHoldItems(): End...");
		return maxHHImpactItemList;
	}

	public HashMap<ProductKey, ItemClassificationDTO> getHHRecommendationItems(Connection conn, String weekStartDate, int productLevelId,
			String productIdList) throws GeneralException {
		// Create separate hashmap
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<ProductKey, ItemClassificationDTO> hhRecommendationItems = new HashMap<ProductKey, ItemClassificationDTO>();

		HashMap<Integer, ItemClassificationDTO> orderdedHHRecommendationItems = new HashMap<Integer, ItemClassificationDTO>();

		try {
			logger.debug("getHHRecommendationItems(): Start...");
			// At product major category level, location level can be storeList or Zone
			String query = GET_HH_REC_ITEMS;
			query = query.replaceAll("%PRODUCT_LIST%", productIdList);
			logger.debug(query);

			stmt = conn.prepareStatement(query);

			stmt.setString(1, weekStartDate);
			stmt.setInt(2, productLevelId);
			// stmt.setInt(3, productId);

			rs = stmt.executeQuery();

			while (rs.next()) {

				ItemClassificationDTO itemClassificationObj = new ItemClassificationDTO();
				if (rs.getObject("ROWNUM") != null) {
					itemClassificationObj.setRankId(rs.getInt("ROWNUM"));
				} else {
					continue;
				}
				if ((rs.getObject("PRODUCT_ID") != null) && (rs.getObject("PRODUCT_LEVEL_ID") != null)) {
					itemClassificationObj.setProductKey(new ProductKey(rs.getInt("PRODUCT_LEVEL_ID"), rs.getInt("PRODUCT_ID")));
				} else {
					continue;
				}

				itemClassificationObj.setUniqueHouseHoldCount(rs.getInt("NO_OF_HOUSEHOLD"));

				orderdedHHRecommendationItems.put(itemClassificationObj.getRankId(), itemClassificationObj);
			}

			logger.debug("Collected all records form Item classification : records count : " + orderdedHHRecommendationItems.size());
			// Calculating the 30% records of total records taking a Ceiling in case total record is 1
			int maxRecordLimit = (int) (Math
					.ceil(orderdedHHRecommendationItems.size() * Integer.parseInt(PropertyManager.getProperty("MAX_HH_RECORDS_PERCENTAGE")) / 100));

			// If Percentage count exceeds the maximum record limit to select (i.e. 50 records)
			if (maxRecordLimit > Integer.parseInt(PropertyManager.getProperty("MAX_HH_IMPACT_RECORDS"))) {
				// updating the records as max record count limit : i.e. -50
				maxRecordLimit = Integer.parseInt(PropertyManager.getProperty("MAX_HH_IMPACT_RECORDS"));
			}

			logger.debug("TOP records count to be picked is : " + maxRecordLimit);
			// Taking the top rank records and populating the output Hashmap
			for (Integer rank : orderdedHHRecommendationItems.keySet()) {
				ItemClassificationDTO itemClassificationDTO = orderdedHHRecommendationItems.get(rank);

				hhRecommendationItems.put(itemClassificationDTO.getProductKey(), itemClassificationDTO);
				if (--maxRecordLimit < 1) {
					break; // breaking the loop after records with required count are selected
				}
			}
		} catch (Exception e) {
			logger.error("getHHRecommendationItems(): Error while retrieving Top houseHoldImpact list " + e.getMessage());
			throw new GeneralException("Error in getHHRecommendationItems() - " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.debug("getHHRecommendationItems(): End...");
		return hhRecommendationItems;
	}

	public HashMap<Integer, HashMap<Integer, Integer>> getItemStoreMap(Connection conn, int productLevelId, String productIdList,
			String priceZoneStoreIds) throws GeneralException {

		// ItemDAO itemDAO = new ItemDAO();
		// HashMap<ItemCode, HashMap<StoreId, ZoneId>>
		HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap = new HashMap<Integer, HashMap<Integer, Integer>>();
		int counter = 0;
		String sql = ITEM_STORE_MAP;
		// STORES_LIST
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			sql = sql.replaceAll("%STORES_LIST%", priceZoneStoreIds);
			sql = sql.replaceAll("%DEPT_LIST%", productIdList);
			logger.debug("Item StoreMap Query is : " + sql);
			stmt = conn.prepareStatement(sql);
			stmt.setInt(++counter, productLevelId);
			stmt.setFetchSize(10000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				// HashMap<StoreId, ZoneId>

				int itemCode = rs.getInt("ITEM_CODE");

				int zoneId = -1;
				if (rs.getObject("PRICE_ZONE_ID") != null) {
					zoneId = rs.getInt("PRICE_ZONE_ID");
				} else {
					if (rs.getObject("ALT_PRICE_ZONE_ID") != null) {
						zoneId = rs.getInt("ALT_PRICE_ZONE_ID");
					}
				}
				if (zoneId != -1) { // A valid zone id has been found
					HashMap<Integer, Integer> storeZoneMap = new HashMap<Integer, Integer>();
					if (itemStoreMap.get(itemCode) != null) {
						storeZoneMap = itemStoreMap.get(itemCode);
					}
					storeZoneMap.put(rs.getInt("LEVEL_ID"), zoneId);
					// logger.debug("Item Code " + itemCode + " mapped to : ");
					// logger.debug("Store Id : " + rs.getInt("LEVEL_ID") + " and respective Zone : " + zoneId);
					itemStoreMap.put(itemCode, storeZoneMap);
				}
			}
		} catch (Exception ex) {
			throw new GeneralException("Error in getItemStoreMap() - " + ex.getMessage());
		} finally {
			if (rs != null)
				PristineDBUtil.close(rs);
			if (stmt != null)
				PristineDBUtil.close(stmt);
		}
		return itemStoreMap;
	}

	public List<PromoItemDTO> getAdDetails(Connection conn, int locationLevelId, int locationId, int productLevelId, Set<Integer> productIdSet,
			String weekStartDate, int noOfPreviousWeeks, int pageNumber, boolean excludePerishableDepartments) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		List<PromoItemDTO> promoAdDetails = new ArrayList<PromoItemDTO>();

		try {
			logger.debug("getAdDetails(): Start...");
			String query = adDetailsQuery(conn, locationLevelId, locationId, productLevelId, productIdSet, weekStartDate, noOfPreviousWeeks,
					pageNumber, excludePerishableDepartments);
			logger.debug(query);

			stmt = conn.prepareStatement(query);

			rs = stmt.executeQuery();

			while (rs.next()) {

				PromoItemDTO promoItemDTO = new PromoItemDTO();
				ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, rs.getInt("ITEM_CODE"));

				PRItemAdInfoDTO itemAdInfoDTO = new PRItemAdInfoDTO();
				itemAdInfoDTO.setWeeklyAdStartDate(rs.getString("START_DATE"));
				itemAdInfoDTO.setAdPageNo(rs.getInt("PAGE_NUMBER"));
				itemAdInfoDTO.setAdBlockNo(rs.getInt("BLOCK_NUMBER"));

				PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
				saleInfoDTO
						.setSalePrice(PRCommonUtil.getMultiplePrice(rs.getInt("SALE_QTY"), rs.getDouble("SALE_PRICE"), rs.getDouble("SALE_M_PRICE")));
				saleInfoDTO.setPromoTypeId(rs.getInt("PROMO_TYPE_ID"));

				promoItemDTO.setProductKey(productKey);
				promoItemDTO.setBrandId(rs.getInt("BRAND_ID"));
				promoItemDTO.setCategoryId(rs.getInt("CATEGORY_ID"));
				promoItemDTO.setCatName(rs.getString("CATEGORY_NAME"));
				promoItemDTO.setDeptId(rs.getInt("DEPT_ID"));
				promoItemDTO.setDeptName(rs.getString("DEPARTMENT_NAME"));
				promoItemDTO.setUpc(rs.getString("UPC"));
				promoItemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				promoItemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
				promoItemDTO.setUomID(rs.getInt("UOM_ID"));

				promoItemDTO.setRegPrice(PRCommonUtil.getMultiplePrice(rs.getInt("REG_QTY"), rs.getDouble("REG_PRICE"), rs.getDouble("REG_M_PRICE")));
				String actInd = rs.getString("ACTIVE_INDICATOR");
				if (String.valueOf(Constants.YES).equalsIgnoreCase(actInd)) {
					promoItemDTO.setActive(true);
				} else {
					promoItemDTO.setActive(false);
				}

				promoItemDTO.setAdInfo(itemAdInfoDTO);
				promoItemDTO.setSaleInfo(saleInfoDTO);

				if(promoItemDTO.isActive()) {
					promoAdDetails.add(promoItemDTO);	
				}
			}

			logger.debug("Collected all Promotion Ad details: " + promoAdDetails.size());

		} catch (Exception e) {
			logger.error("getExistingAdDetails(): Error while retrieving data " + e.getMessage());
			throw new GeneralException("Error in getExistingAdDetails() - " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.debug("getExistingAdDetails(): End...");
		return promoAdDetails;
	}

	private String adDetailsQuery(Connection conn, int locationLevelId, int locationId, int productLevelId, Set<Integer> productIdSet,
			String weekStartDate, int noOfPreviousWeeks, int pageNumber, boolean excludePerishableDeptments) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		String perishablesDeptIds = PropertyManager.getProperty("PERISHABLES_DEPARTMENT_IDS");

		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		// ProductService productService = new ProductService();
		// productService.getProductLevelRelationMap(conn, productLevelId);
		// productService.getProductLevelPointer(conn, productLevelId);

		sb.append(" SELECT * FROM ");
		sb.append(" (SELECT TO_CHAR(RC.START_DATE, 'MM/dd/yyyy') AS START_DATE, STORE_ITEM_MAP.*,  ");
		sb.append(" PRODUCT_HIERARCHY.P_ID_3 AS CATEGORY_ID, PRODUCT_HIERARCHY.P_ID_4 AS DEPT_ID, ");
		sb.append(" PR.NAME AS CATEGORY_NAME, PR1.NAME AS DEPARTMENT_NAME FROM ");
		sb.append(" (SELECT ITEMS.*, IL.UPC, BL.BRAND_ID, IL.ITEM_NAME, IL.ACTIVE_INDICATOR, IL.RET_LIR_ID, ");
		sb.append(" IL.ITEM_SIZE, IL.UOM_ID, LIG.RET_LIR_NAME,");
		sb.append(" PD.PROMO_TYPE_ID, PBI.REG_QTY,	PBI.REG_PRICE, PBI.REG_M_PRICE,	PBI.SALE_QTY, PBI.SALE_PRICE, PBI.SALE_M_PRICE ");
		sb.append(" FROM ");
		sb.append(" (SELECT CALENDAR_ID, PAGE_NUMBER, BLOCK_NUMBER, PBI.PROMO_DEFINITION_ID, PBI.ITEM_CODE FROM PM_PROMO_BUY_ITEM PBI ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PBI.ITEM_CODE ");
		sb.append(" LEFT JOIN PM_WEEKLY_AD_PROMO WAP ON WAP.PROMO_DEFINITION_ID = PBI.PROMO_DEFINITION_ID ");
		sb.append(" LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON WAB.BLOCK_ID = WAP.BLOCK_ID ");
		sb.append(" LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON WAB.PAGE_ID = WAP.PAGE_ID ");
		sb.append(" LEFT JOIN PM_WEEKLY_AD_DEFINITION WAD ON WAP.WEEKLY_AD_ID = WAD.WEEKLY_AD_ID ");
		sb.append(" WHERE WAP.PAGE_ID IN ");
		sb.append(" (SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID IN ");
		sb.append(" (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION WHERE LOCATION_LEVEL_ID = ").append(locationLevelId);
		sb.append(" AND LOCATION_ID = ").append(locationId).append(" AND CALENDAR_ID IN (");
		sb.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE ROW_TYPE='W' AND ");
		sb.append(" RC.START_DATE <= TO_DATE('").append(weekStartDate).append("','MM/DD/YYYY') ");
		sb.append(" AND RC.START_DATE >= TO_DATE('").append(weekStartDate).append("','MM/DD/YYYY') - ((").append(noOfPreviousWeeks).append("-1)*7)");
		sb.append(" )) ");
		if (pageNumber > 0) {
			sb.append(" AND PAGE_NUMBER = ").append(pageNumber);
		}
		sb.append(" )GROUP BY CALENDAR_ID, PAGE_NUMBER, BLOCK_NUMBER, PBI.PROMO_DEFINITION_ID, PBI.ITEM_CODE) ITEMS ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = ITEMS.ITEM_CODE ");
		sb.append(" LEFT JOIN BRAND_LOOKUP BL ON IL.BRAND_ID = BL.BRAND_ID ");
		sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID ");
		sb.append(" LEFT JOIN PM_PROMO_DEFINITION PD ON ITEMS.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" LEFT JOIN PM_PROMO_BUY_ITEM PBI ON ITEMS.PROMO_DEFINITION_ID = PBI.PROMO_DEFINITION_ID AND ITEMS.ITEM_CODE = PBI.ITEM_CODE ");
		sb.append(" ) STORE_ITEM_MAP ");
		sb.append(" LEFT JOIN ");
		sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, new ProductService()));
		sb.append(" LEFT JOIN PRODUCT_GROUP PR ON PR.PRODUCT_ID = PRODUCT_HIERARCHY.P_ID_3 AND PR.PRODUCT_LEVEL_ID = 4 ");
		sb.append(" LEFT JOIN PRODUCT_GROUP PR1 ON PR1.PRODUCT_ID = PRODUCT_HIERARCHY.P_ID_4 AND PR1.PRODUCT_LEVEL_ID = 5 ");
		sb.append(" LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = STORE_ITEM_MAP.CALENDAR_ID ");
		sb.append(") ");

		String departmentFilter = "";
		// TODO:: only department level is handled
		if (productLevelId == Constants.DEPARTMENTLEVELID && productIdSet.size() > 0) {
			departmentFilter = "DEPT_ID IN (" + PRCommonUtil.getCommaSeperatedStringFromIntSet(productIdSet) + ")";
		}

		if (!excludePerishableDeptments) {
			sb.append(" WHERE DEPT_ID NOT IN (").append(perishablesDeptIds).append(") ");
			sb.append(" AND ").append(departmentFilter);
		} else if (departmentFilter != "") {
			sb.append(" WHERE ").append(departmentFilter);
		}

		// for debug purpose only comment once done
		// sb.append(" AND CATEGORY_ID = 1266");

		return sb.toString();
	}

	public HashMap<Long, PromoProductGroup> getPPGGroupDetails(Connection conn, String deptIds) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		// hashmap<PPG_Group_id,List<PPGGroupDTO>>
		HashMap<Long, PromoProductGroup> ppgGroupDetailsData = new HashMap<Long, PromoProductGroup>();

		try {
			logger.debug("getPPGGroupDetails(): Start...");
			// At product major category level, location level can be storeList or Zone
			String query = PPG_GROUP_DETAILS;
			query = query.replaceAll("%DEPARTMENT_LIST%", deptIds);
			logger.debug(query);
			int totalPPGItem = 0;

			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			HashMap<ProductKey, PromoItemDTO> ppgItems = null;
			PromoProductGroup promoProductGroup = null;

			while (rs.next()) {
				PromoItemDTO promoItemDTO = new PromoItemDTO();
				ppgItems = new HashMap<ProductKey, PromoItemDTO>();

				long groupId = 0;

				if (rs.getObject("GROUP_ID") != null) {
					groupId = rs.getLong("GROUP_ID");
				} else {
					continue;
				}

				if ((rs.getObject("ITEM_ID") != null) && (rs.getObject("ITEM_LEVEL_ID") != null)) {
					promoItemDTO.setProductKey(new ProductKey(rs.getInt("ITEM_LEVEL_ID"), rs.getInt("ITEM_ID")));
				} else {
					continue;
				}

				String positive = "Y";
				if (rs.getObject("IS_LEAD_ITEM") != null) {
					promoItemDTO.setPPGLeadItem(rs.getString("IS_LEAD_ITEM").trim().equals(positive) ? true : false);
				} else {
					promoItemDTO.setPPGLeadItem(false);
				}

				// if (rs.getObject("LIG_REP_OR_ITEM_CODE") != null) {
				// promoItemDTO.setItemCode(rs.getInt("LIG_REP_OR_ITEM_CODE"));
				// }

				if (rs.getObject("LIG_OR_ITEM_NAME") != null) {
					promoItemDTO.setItemName(rs.getString("LIG_OR_ITEM_NAME"));
				}

				if (rs.getObject("BRAND_ID") != null) {
					promoItemDTO.setBrandId(rs.getInt("BRAND_ID"));
				}

				// if (rs.getObject("BRAND_NAME") != null) {
				// promoItemDTO.setBrandName(rs.getString("BRAND_NAME"));
				// }

				if (rs.getObject("CATEGORY_ID") != null) {
					promoItemDTO.setCategoryId(rs.getInt("CATEGORY_ID"));
				}

				if (rs.getObject("CATEGORY_NAME") != null) {
					promoItemDTO.setCatName(rs.getString("CATEGORY_NAME"));
				}

				if (rs.getObject("DEPARTMENT_ID") != null) {
					promoItemDTO.setDeptId(rs.getInt("DEPARTMENT_ID"));
				}

				if (rs.getObject("DEPARTMENT_NAME") != null) {
					promoItemDTO.setDeptName(rs.getString("DEPARTMENT_NAME"));
				}

				if (ppgGroupDetailsData.get(groupId) != null) {
					promoProductGroup = ppgGroupDetailsData.get(groupId);
				} else {
					promoProductGroup = new PromoProductGroup();
				}

				promoItemDTO.setPpgGroupId(groupId);
				promoProductGroup.setGroupId(groupId);

				// if lead already update, don't change it again
				if (promoItemDTO.isPPGLeadItem()) {
					promoProductGroup.setLeadItem(promoItemDTO.getProductKey());
				}

				if (promoProductGroup.getItems() != null) {
					ppgItems = promoProductGroup.getItems();
				}

				ppgItems.put(promoItemDTO.getProductKey(), promoItemDTO);
				promoProductGroup.setItems(ppgItems);
				ppgGroupDetailsData.put(groupId, promoProductGroup);

				totalPPGItem = totalPPGItem + 1;
			}

			// promoDTO.ppgGroupDetailsData.clear();
			// promoDTO.ppgGroupDetailsData.putAll(ppgGroupDetailsData);

			logger.debug("Total number of PPG groups are :  " + ppgGroupDetailsData.size());

			// logger.info("***ItemDetails: Total items in PPG's (#):" + totalPPGItem);

		} catch (Exception e) {
			logger.error("getPPGGroupDetails(): Error while retrieving PPG group details " + e.getMessage());
			throw new GeneralException("Error in getPPGGroupDetails() - " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.debug("getPPGGroupDetails(): End...");

		return ppgGroupDetailsData;
	}

	public HashMap<Integer, ArrayList<PromoAdSaleInfo>> getPastAdDetails(Connection conn, int productLevelId, String productIdList, int chainId,
			int locationLevelId, int locationId, String weekStartDate, int noOfWeekSaleData, List<Integer> stores) throws GeneralException {
		// HashMap<ItemCode, HashMap<PageNo|WeeklyAdDate, List<SalePrice>>
		HashMap<Integer, ArrayList<PromoAdSaleInfo>> saleAdDetails = new HashMap<Integer, ArrayList<PromoAdSaleInfo>>();
		int itemCode = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int counter = 0;

		try {

			String query = new String(GET_PAST_AD_DETAILS);
			String subQuery = new PricingEngineDAO().getItemsInCategoryQuery(productLevelId, productIdList);
			query = query.replaceAll("%ITEMS_IN_CATEGORY%", subQuery);

			subQuery = new PricingEngineDAO().getPromoLocationsQuery(chainId, locationLevelId, locationId, stores, false);
			query = query.replaceAll("%PROMO_LOCATIONS%", subQuery);

			stmt = conn.prepareStatement(query);
			stmt.setString(++counter, weekStartDate);
			stmt.setString(++counter, weekStartDate);
			stmt.setInt(++counter, noOfWeekSaleData);

			stmt.setString(++counter, weekStartDate);
			stmt.setString(++counter, weekStartDate);
			stmt.setInt(++counter, noOfWeekSaleData);

			logger.debug("GET_PAST_AD_DETAILS:" + query);
			logger.debug("Parameters: 1,2-" + weekStartDate + ",3-" + noOfWeekSaleData);

			stmt.setFetchSize(100000);
			rs = stmt.executeQuery();

			while (rs.next()) {

				ArrayList<PromoAdSaleInfo> saleAdDetailsOneItem = new ArrayList<PromoAdSaleInfo>();
				PromoAdSaleInfo promoAdSaleInfo = new PromoAdSaleInfo();

				itemCode = rs.getInt("ITEM_CODE");

				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(rs.getInt("SALE_QTY"), rs.getDouble("SALE_PRICE"),
						rs.getDouble("SALE_M_PRICE"));

				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(rs.getInt("REG_QTY"), rs.getDouble("REG_PRICE"), rs.getDouble("REG_M_PRICE"));

				promoAdSaleInfo.setAdPage(rs.getInt("PAGE_NUMBER"));
				promoAdSaleInfo.setAdStartDate(rs.getString("AD_DATE"));
				promoAdSaleInfo.setSalePricePoint(salePrice);
				promoAdSaleInfo.setRegPricePoint(regPrice);
				promoAdSaleInfo.setDealCost(rs.getDouble("DEAL_COST"));
				promoAdSaleInfo.setListCost(rs.getDouble("LIST_COST"));
				promoAdSaleInfo.setPromoTypeId(rs.getInt("PROMO_TYPE_ID"));

				if (saleAdDetails.get(itemCode) != null) {
					saleAdDetailsOneItem = saleAdDetails.get(itemCode);
				}
				saleAdDetailsOneItem.add(promoAdSaleInfo);

				saleAdDetails.put(itemCode, saleAdDetailsOneItem);
			}
		} catch (Exception ex) {
			logger.error("Error in getSaleAdDetails() - " + ex.toString(), ex);
			throw new GeneralException(ex.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return saleAdDetails;
	}

	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getCostHistory(Connection conn, Integer chainId, RetailCalendarDTO startWeekCalDTO,
			int noOfCostHistory, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, Set<Integer> itemCodeList,
			List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores) throws GeneralException {

		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costHistoryMap = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();

		
		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfCostHistory);

		// Get cost detail of item for a week for all locations
		// HashMap<CalendarId, HashMap<ItemCode, HashMap<RetailPriceCostKey, RetailCostDTO>>>
		HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>> costDataMap = getRetailCost(conn, noOfCostHistory, chainId,
				priceAndStrategyZoneNos, priceZoneStores, true, itemCodeList, startWeekCalDTO.getStartDate());

		logger.debug("Loading cost...");
		if (itemCodeList.size() > 0) {
			// Get cost for each week
			for (RetailCalendarDTO curCalDTO : retailCalendarList) {
				logger.debug("Running for calendar id " + curCalDTO.getCalendarId() + "(" + curCalDTO.getStartDate() + ") for items "
						+ itemCodeList.size());
				if (costDataMap.get(curCalDTO.getCalendarId()) != null) {
					// Go through each item
					for (Entry<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> itemCostData : costDataMap.get(curCalDTO.getCalendarId())
							.entrySet()) {

						int itemCode = itemCostData.getKey();

						// Go through each location's cost
						for (Entry<RetailPriceCostKey, RetailCostDTO> costData : itemCostData.getValue().entrySet()) {
							RetailCostDTO retailcostDTO = costData.getValue();
							// Keep each week cost in the output map
							HashMap<String, List<RetailCostDTO>> locationCost = new HashMap<String, List<RetailCostDTO>>();
							if (costHistoryMap.get(itemCode) != null) {
								locationCost = costHistoryMap.get(itemCode);
							}

							List<RetailCostDTO> retailCostDTOs = new ArrayList<RetailCostDTO>();
							if (locationCost.get(curCalDTO.getStartDate()) != null) {
								retailCostDTOs = locationCost.get(curCalDTO.getStartDate());
							}
							retailCostDTOs.add(retailcostDTO);
							locationCost.put(curCalDTO.getStartDate(), retailCostDTOs);

							costHistoryMap.put(itemCode, locationCost);
						}
					}
				}
			}
		}

		return costHistoryMap;
	}

	public HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>> getRetailCost(Connection conn, int noOfCostHistory,
			int chainId, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, boolean fetchStoreData, Set<Integer> itemCodeSet,
			String weekStartDate) throws GeneralException {

		HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>> costDataMap =
				new HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveRetailCost(conn, costDataMap, noOfCostHistory, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores,
						weekStartDate, values);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveRetailCost(conn, costDataMap, noOfCostHistory, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, weekStartDate,
					values);
		}

		return costDataMap;
	}

	private void retrieveRetailCost(Connection conn, HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>> costDataMap,
			int noOfWeeksCostHistory, int chainId, boolean fetchStoreData, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores,
			String weekStartDate, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		// PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		try {
			String sql = GET_RETAIL_COST;

			String pzStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(fetchStoreData, priceAndStrategyZoneNos, pzStores));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			logger.debug("GET_RETAIL_COST:" + sql);
			statement = conn.prepareStatement(sql);

			int counter = 0;
			statement.setInt(++counter, chainId);
			statement.setString(++counter, weekStartDate);
			statement.setString(++counter, weekStartDate);
			statement.setInt(++counter, noOfWeeksCostHistory);

			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 5, values);

			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_CODE");
				int levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
				int levelId = resultSet.getInt("LEVEL_ID");
				Float listCost = resultSet.getFloat("LIST_COST");
				Float listCost2 = resultSet.getFloat("LIST_COST_2");
				Float finalListCost = ((listCost2 != null && listCost2 > 0) ? listCost2 : listCost);
				RetailCostDTO retailCostDTO = new RetailCostDTO();
				retailCostDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
				retailCostDTO.setListCost(finalListCost);
				retailCostDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
				retailCostDTO.setVipCost(resultSet.getFloat("VIP_COST"));
				retailCostDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
				retailCostDTO.setDealStartDate(resultSet.getString("DEAL_START_DATE"));
				retailCostDTO.setDealEndDate(resultSet.getString("DEAL_END_DATE"));
				retailCostDTO.setLevelTypeId(levelTypeId);
				retailCostDTO.setLevelId(String.valueOf(levelId));

				RetailPriceCostKey costKey = new RetailPriceCostKey(levelTypeId, levelId);

				HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> items = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>();
				HashMap<RetailPriceCostKey, RetailCostDTO> itemLocations = new HashMap<RetailPriceCostKey, RetailCostDTO>();

				if (costDataMap.get(retailCostDTO.getCalendarId()) != null) {
					items = costDataMap.get(retailCostDTO.getCalendarId());
				}

				if (items.get(itemCode) != null) {
					itemLocations = items.get(itemCode);
				}

				itemLocations.put(costKey, retailCostDTO);
				items.put(itemCode, itemLocations);

				costDataMap.put(retailCostDTO.getCalendarId(), items);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_RETAIL_COST " + e);
			throw new GeneralException("Exception in retrieveRetailCost()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	public HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> getPriceHistory(Connection conn, Integer chainId, RetailCalendarDTO startWeekCalDTO,
			int noOfPriceHistory, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, Set<Integer> itemCodeList,
			List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores) throws GeneralException {

		HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceHistoryMap = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
		
		
		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfPriceHistory);

		// Get price detail of item for a week for all location s
		// HashMap<CalendarId, HashMap<ItemCode, HashMap<RetailPriceCostKey, RetailPriceDTO>>>
		HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap = getRetailPrice(conn, noOfPriceHistory, chainId,
				priceAndStrategyZoneNos, priceZoneStores, true, itemCodeList, startWeekCalDTO.getStartDate());

		logger.debug("Loading price...");
		if (itemCodeList.size() > 0) {
			// Get price for each week
			for (RetailCalendarDTO curCalDTO : retailCalendarList) {
				logger.debug("Running for calendar id " + curCalDTO.getCalendarId() + "(" + curCalDTO.getStartDate() + ") for items "
						+ itemCodeList.size());
				if (priceDataMap.get(curCalDTO.getCalendarId()) != null) {
					// Go through each item
					for (Entry<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> itemPriceData : priceDataMap.get(curCalDTO.getCalendarId())
							.entrySet()) {

						int itemCode = itemPriceData.getKey();

						// Go through each location's price
						for (Entry<RetailPriceCostKey, RetailPriceDTO> priceData : itemPriceData.getValue().entrySet()) {
							RetailPriceDTO retailPriceDTO = priceData.getValue();
							// Keep each week price in the output map
							HashMap<String, List<RetailPriceDTO>> locationPrice = new HashMap<String, List<RetailPriceDTO>>();
							if (priceHistoryMap.get(itemCode) != null) {
								locationPrice = priceHistoryMap.get(itemCode);
							}

							List<RetailPriceDTO> retailPriceDTOs = new ArrayList<RetailPriceDTO>();
							if (locationPrice.get(curCalDTO.getStartDate()) != null) {
								retailPriceDTOs = locationPrice.get(curCalDTO.getStartDate());
							}
							retailPriceDTOs.add(retailPriceDTO);
							locationPrice.put(curCalDTO.getStartDate(), retailPriceDTOs);

							priceHistoryMap.put(itemCode, locationPrice);
						}
					}
				}
			}
		}

		
		return priceHistoryMap;
	}
	
	
	public HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> getRetailPrice(Connection conn, int noOfPriceHistory,
			int chainId, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, boolean fetchStoreData, Set<Integer> itemCodeSet,
			String weekStartDate) throws GeneralException {

		HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap = 
				new HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveRetailPrice(conn, priceDataMap, noOfPriceHistory, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores,
						weekStartDate, values);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveRetailPrice(conn, priceDataMap, noOfPriceHistory, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, weekStartDate,
					values);
		}

		return priceDataMap;
	}
	
	private void retrieveRetailPrice(Connection conn, HashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap,
			int noOfWeeksPriceHistory, int chainId, boolean fetchStoreData, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores,
			String weekStartDate, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		// PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		try {
			String sql = GET_RETAIL_PRICE;

			String pzStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(fetchStoreData, priceAndStrategyZoneNos, pzStores));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			logger.debug("GET_RETAIL_PRICE:" + sql);
			statement = conn.prepareStatement(sql);

			int counter = 0;
			statement.setInt(++counter, chainId);
			statement.setString(++counter, weekStartDate);
			statement.setString(++counter, weekStartDate);
			statement.setInt(++counter, noOfWeeksPriceHistory);

			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 5, values);

			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_CODE");
				int levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
				int levelId = resultSet.getInt("LEVEL_ID");
				
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				retailPriceDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
				retailPriceDTO.setLevelTypeId(levelTypeId);
				retailPriceDTO.setLevelId(String.valueOf(levelId));
				
				retailPriceDTO.setRegularPrice(
						PRCommonUtil.getMultiplePrice(resultSet.getInt("REG_QTY"), resultSet.getDouble("REG_PRICE"), resultSet.getDouble("REG_M_PRICE")));

				RetailPriceCostKey priceKey = new RetailPriceCostKey(levelTypeId, levelId);

				HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> items = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>();
				HashMap<RetailPriceCostKey, RetailPriceDTO> itemLocations = new HashMap<RetailPriceCostKey, RetailPriceDTO>();

				if (priceDataMap.get(retailPriceDTO.getCalendarId()) != null) {
					items = priceDataMap.get(retailPriceDTO.getCalendarId());
				}

				if (items.get(itemCode) != null) {
					itemLocations = items.get(itemCode);
				}

				itemLocations.put(priceKey, retailPriceDTO);
				items.put(itemCode, itemLocations);

				priceDataMap.put(retailPriceDTO.getCalendarId(), items);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_RETAIL_PRICE " + e);
			throw new GeneralException("Exception in retrieveRetailPrice()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	private String getLocationSubQuery(boolean fetchStoreData, List<String> priceAndStrategyZoneNos, String priceZoneStores) {
		StringBuffer subQuery = new StringBuffer();
		String strPriceAndStrategyZones = "";
		for (String psz : priceAndStrategyZoneNos) {
			strPriceAndStrategyZones = strPriceAndStrategyZones + "," + psz;
		}
		strPriceAndStrategyZones = strPriceAndStrategyZones.substring(1);

		if (fetchStoreData) {
			subQuery.append("(LEVEL_TYPE_ID = 2 AND LEVEL_ID IN ( SELECT CAST_DATA(COMP_STR_NO,"
					+ Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) + ")" + " FROM COMPETITOR_STORE WHERE COMP_STR_ID IN ( "
					+ "" + priceZoneStores + "))) ");
			subQuery.append("OR (LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (SELECT CAST_DATA(ZONE_NUM, " + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) + 
					") FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID IN (" + strPriceAndStrategyZones + "))) ");
		} else {
			subQuery.append("(LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (" + strPriceAndStrategyZones + ")) ");
		}
		subQuery.append("OR (LEVEL_TYPE_ID = 0) ");
		return subQuery.toString();
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param candidateItemMap
	 * @throws GeneralException 
	 */
	public void savePromoRecommendationData(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap) throws GeneralException {
		
		insertPromoRecRunHeader(conn, recommendationInputDTO);
		
		insertPromoRecommendationData(conn, recommendationInputDTO, candidateItemMap);
		
	}
	
	
	
	private static final String INSERT_PROMO_REC_RUN_HEADER = "INSERT INTO OR_PROMO_REC_RUN_HEADER (RUN_ID, LOCATION_LEVEL_ID, "
			+ "	LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, START_CALENDAR_ID, END_CALENDAR_ID, CAL_TYPE, RUN_TYPE, START_RUN_TIME, END_RUN_TIME, "
			+ " PERCENT_COMPLETION, MESSAGE, RUN_STATUS, PREDICTED_BY, PREDICTED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE, SYSDATE, ?, ?, ?, ?, SYSDATE) ";
	
	
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void insertPromoRecRunHeader(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {
		PreparedStatement statement = null;
		try {

			long runId = new PromoExplainationDAO().getSequenceValue(conn, "OR_PROMO_REC_RUN_ID_SEQ");
			recommendationInputDTO.setRunId(runId);

			statement = conn.prepareStatement(INSERT_PROMO_REC_RUN_HEADER);
			int counter = 0;
			statement.setLong(++counter, recommendationInputDTO.getRunId());
			statement.setInt(++counter, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++counter, recommendationInputDTO.getLocationId());
			statement.setInt(++counter, recommendationInputDTO.getProductLevelId());
			statement.setInt(++counter, recommendationInputDTO.getProductId());
			statement.setInt(++counter, recommendationInputDTO.getStartCalendarId());
			statement.setInt(++counter, recommendationInputDTO.getEndCalendarId());
			statement.setString(++counter, Constants.CALENDAR_WEEK);
			statement.setString(++counter, String.valueOf(PRConstants.RUN_TYPE_BATCH));
			statement.setInt(++counter, 100);
			statement.setString(++counter, PRConstants.PREDICTION_STATUS_SUCCESS);
			statement.setString(++counter, PRConstants.RUN_STATUS_SUCCESS);
			statement.setString(++counter, PRConstants.BATCH_USER);
			statement.addBatch();
			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertWeeklyRunHeader() - Error while inserting INSERT_PROMO_REC_RUN_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	
	
	private static final String INSERT_PROMO_RECOMMENDATION = "INSERT INTO OR_PROMO_RECOMMENDATION (PR_PROMO_REC_ID, SCENARIO_ID, WEEK_CALENDAR_ID, "
			+ " RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, RET_LIR_ID, STRATEGY_ID, ITEM_SIZE, UOM_ID, PRICE_CHECK_LIST_ID, LIG_REP_ITEM_CODE, REG_MULTIPLE, "
			+ " REG_PRICE, ORIG_SALE_MULTIPLE, ORIG_SALE_PRICE, ORIG_PROMO_TYPE_ID, ORIG_AD_PAGE_NO, ORIG_AD_BLOCK_NO, ORIG_DISPLAY_TYPE_ID, "
			+ " ORIG_OFFER_TYPE, ORIG_OFFER_VALUE, ORIG_MIN_QTY_REQD, LIST_COST, "
			+ " DEAL_COST, REC_SALE_MULTIPLE, REC_SALE_PRICE, REC_PROMO_TYPE_ID, REC_AD_PAGE_NO, REC_AD_BLOCK_NO, REC_DISPLAY_TYPE_ID, "
			+ " REC_OFFER_TYPE, REC_OFFER_VALUE, REC_MIN_QTY_REQD, PRED_MOV_REG_PRICE, "
			+ " PRED_STATUS_REG_PRICE, PRED_SALES_REG_PRICE, PRED_MARGIN_REG_PRICE, PRED_MOV_ORIG_PROMO, PRED_STATUS_ORIG_PROMO, PRED_SALES_ORIG_PROMO, "
			+ " PRED_MARGIN_ORIG_PROMO, PRED_MOV_REC_PROMO, PRED_STATUS_REC_PROMO, PRED_SALES_REC_PROMO, PRED_MARGIN_REC_PROMO, IS_PROMO_RECOMMENDED) "
			+ " VALUES (OR_PROMO_REC_ID_SEQ.NEXTVAL, "
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?, ?, ?, ?, ?, ?)";
	
	
	private void insertPromoRecommendationData(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap) throws GeneralException {
		PreparedStatement statement = null;
		try {

			statement = conn.prepareStatement(INSERT_PROMO_RECOMMENDATION);
			int itemsInBatch = 0;
			for (Map.Entry<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weekEntry : candidateItemMap
					.entrySet()) {
				
				for(Map.Entry<ProductKey, List<PromoItemDTO>> itemMapEntry: weekEntry.getValue().entrySet()) {
					PromoItemDTO finalizedPromo = getFinalizedPromo(itemMapEntry.getValue());
					PromoItemDTO currentOrDefaultPromo = getDefaultEntry(itemMapEntry.getValue());
					if(finalizedPromo == null) {
						finalizedPromo = currentOrDefaultPromo;
					}
					int counter = 0;
					statement.setLong(++counter, finalizedPromo.getScenarioId());
					statement.setInt(++counter, weekEntry.getKey().calendarId);
					statement.setLong(++counter, recommendationInputDTO.getRunId());
					statement.setInt(++counter, itemMapEntry.getKey().getProductLevelId());
					statement.setInt(++counter, itemMapEntry.getKey().getProductId());
					statement.setInt(++counter, finalizedPromo.getRetLirId());
					statement.setLong(++counter, finalizedPromo.getStrategyDTO().getStrategyId());
					statement.setDouble(++counter, finalizedPromo.getItemSize());
					statement.setInt(++counter, finalizedPromo.getUomID());
					statement.setInt(++counter, finalizedPromo.getPriceCheckListId());
					statement.setInt(++counter, finalizedPromo.getLigRepItemCode());
					statement.setInt(++counter, finalizedPromo.getRegPrice().multiple);
					statement.setDouble(++counter, finalizedPromo.getRegPrice().price);
					statement.setInt(++counter, currentOrDefaultPromo.getSaleInfo().getSalePrice().multiple);
					statement.setDouble(++counter, currentOrDefaultPromo.getSaleInfo().getSalePrice().price);
					statement.setInt(++counter, currentOrDefaultPromo.getSaleInfo().getPromoTypeId());
					statement.setInt(++counter, currentOrDefaultPromo.getAdInfo() == null ? 0
							: currentOrDefaultPromo.getAdInfo().getAdPageNo());
					statement.setInt(++counter, currentOrDefaultPromo.getAdInfo() == null ? 0
							: currentOrDefaultPromo.getAdInfo().getAdBlockNo());
					statement.setInt(++counter, currentOrDefaultPromo.getDisplayInfo() == null ? 0
							: currentOrDefaultPromo.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId());
					if (currentOrDefaultPromo.getSaleInfo().getOfferUnitType() != null
							&& currentOrDefaultPromo.getSaleInfo().getOfferUnitType().toLowerCase().equals("none")) {
						PristineDBUtil.setString(++counter, null, statement);
					} else {
						PristineDBUtil.setString(++counter, currentOrDefaultPromo.getSaleInfo().getOfferUnitType(),
								statement);
					}
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getSaleInfo().getOfferValue(), statement);
					PristineDBUtil.setInt(++counter, currentOrDefaultPromo.getSaleInfo().getMinQtyReqd(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getListCost(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getDealCost(), statement);
					statement.setInt(++counter, finalizedPromo.getSaleInfo().getSalePrice().multiple);
					statement.setDouble(++counter, finalizedPromo.getSaleInfo().getSalePrice().price);
					statement.setInt(++counter, finalizedPromo.getSaleInfo().getPromoTypeId());
					statement.setInt(++counter, finalizedPromo.getAdInfo() == null ? 0
							: finalizedPromo.getAdInfo().getAdPageNo());
					statement.setInt(++counter, finalizedPromo.getAdInfo() == null ? 0
							: finalizedPromo.getAdInfo().getAdBlockNo());
					statement.setInt(++counter, currentOrDefaultPromo.getDisplayInfo() == null ? 0
							: finalizedPromo.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId());
					if (finalizedPromo.getSaleInfo().getOfferUnitType() != null
							&& finalizedPromo.getSaleInfo().getOfferUnitType().toLowerCase().equals("none")) {
						PristineDBUtil.setString(++counter, null, statement);
					} else {
						PristineDBUtil.setString(++counter, finalizedPromo.getSaleInfo().getOfferUnitType(),
								statement);
					}
					PristineDBUtil.setDouble(++counter, finalizedPromo.getSaleInfo().getOfferValue(), statement);
					PristineDBUtil.setInt(++counter, finalizedPromo.getSaleInfo().getMinQtyReqd(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getPredMovReg(), statement);
					PristineDBUtil.setInt(++counter, currentOrDefaultPromo.getPredStatusReg() == null ? 0
							: currentOrDefaultPromo.getPredStatusReg().getStatusCode(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getPredRevReg(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getPredMarReg(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getPredMov(), statement);
					PristineDBUtil.setInt(++counter, currentOrDefaultPromo.getPredStatus() == null ? 0
							: currentOrDefaultPromo.getPredStatus().getStatusCode(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getPredRev(), statement);
					PristineDBUtil.setDouble(++counter, currentOrDefaultPromo.getPredMar(), statement);
					PristineDBUtil.setDouble(++counter, finalizedPromo.getPredMov(), statement);
					PristineDBUtil.setInt(++counter,
							finalizedPromo.getPredStatus() == null ? 0 : finalizedPromo.getPredStatus().getStatusCode(),
							statement);
					PristineDBUtil.setDouble(++counter, finalizedPromo.getPredRev(), statement);
					PristineDBUtil.setDouble(++counter, finalizedPromo.getPredMar(), statement);
					if (currentOrDefaultPromo.getSaleInfo().getSalePrice()
							.equals(finalizedPromo.getSaleInfo().getSalePrice())) {
						statement.setInt(++counter, 0);
					} else {
						statement.setInt(++counter, 1);
					}
					
					statement.addBatch();
					itemsInBatch++;
					if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
						statement.executeBatch();
						statement.clearBatch();
						itemsInBatch = 0;
					}
				}
				
			}
			
			if(itemsInBatch > 0){
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("insertPromoRecommendationData() - Error while inserting INSERT_PROMO_RECOMMENDATION", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * 
	 * @param promoOptions
	 * @return finalized promotion
	 */
	private PromoItemDTO getFinalizedPromo(List<PromoItemDTO> promoOptions) {
		return promoOptions.stream().filter(p -> p.isFinalized()).findFirst().orElse(null);
	}
	
	private PromoItemDTO getDefaultEntry(List<PromoItemDTO> promoItemDTOs) {
		return promoItemDTOs.stream().filter(p -> p.isDefaultEntry()).findFirst().orElse(null);
	}
	
	
	
	

	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startDate
	 * @return promotions recommended
	 * @throws GeneralException 
	 */
	public HashMap<Integer, List<PRItemSaleInfoDTO>> getPromotionsFromRecommendation(Connection conn,
			int locationLevelId, int locationId, int productLevelId, int productId, String startDate)
			throws GeneralException {

		long runId = getLatestRunId(conn, locationLevelId, locationId, productLevelId, productId, startDate);

		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = getPromotionsFromRecommendation(conn, runId);

		return saleDetails;
	}
	
	
	
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startDate
	 * @return promotions recommended
	 * @throws GeneralException 
	 */
	public HashMap<Integer, List<PRItemAdInfoDTO>> getAdsFromRecommendation(Connection conn,
			int locationLevelId, int locationId, int productLevelId, int productId, String startDate)
			throws GeneralException {

		long runId = getLatestRunId(conn, locationLevelId, locationId, productLevelId, productId, startDate);

		HashMap<Integer, List<PRItemAdInfoDTO>> saleDetails = getAdsFromRecommendation(conn, runId);

		return saleDetails;
	}
	
	private static final String GET_RUN_ID = "SELECT MAX(RUN_ID) AS RUN_ID FROM OR_PROMO_REC_RUN_HEADER "
			+ " WHERE LOCATON_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
			+ " AND START_CALENDAR_ID = (SELECT CALENDAR_ID FROM %RETAIL_CALENDAR_TAB_NAME% "
			+ " WHERE START_DATE = (?, " + Constants.DB_DATE_FORMAT + ") AND ROW_TYPE='" + Constants.CALENDAR_WEEK + ")";
	
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startDate
	 * @return latest run id
	 * @throws GeneralException 
	 */
	public long getLatestRunId(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId,
			String startDate) throws GeneralException {
		long runId = -1;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String qry = PristineDBUtil.replaceCalendarName(GET_RUN_ID, "%RETAIL_CALENDAR_TAB_NAME%");
			statement = conn.prepareStatement(qry);
			int counter = 0;
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, productLevelId);
			statement.setInt(++counter, productId);
			statement.setString(++counter, startDate);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				runId = resultSet.getLong("RUN_ID");
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("Error--getting run id", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return runId;
	}
	
	
	
	private static final String GET_PROMO_RECOMMENDATION = "SELECT RC.START_DATE, RC.END_DATE, PRODUCT_ID, PRODUCT_LEVEL_ID, SCENARIO_ID, "
			+ " REC_SALE_MULTIPLE, REC_SALE_PRICE, REC_PROMO_TYPE_ID, REC_AD_PAGE_NO, REC_AD_BLOCK_NO, REC_DISPLAY_TYPE_ID, "
			+ " REC_OFFER_TYPE, REC_OFFER_VALUE, REC_MIN_QTY_REQD, PRED_MOV_REC_PROMO, PRED_STATUS_REC_PROMO, PRED_SALES_REC_PROMO, "
			+ " PRED_MARGIN_REC_PROMO FROM OR_PROMO_RECOMMENDATION OPR LEFT JOIN %RETAIL_CALENDAR_TAB_NAME% RC RC.CALENDAR_ID = OPR.WEEK_CALENDAR_ID "
			+ " OPR.WHERE RUN_ID = ? ";
	
	/**
	 * 
	 * @param conn
	 * @param runId
	 * @return recommended promotions
	 * @throws GeneralException 
	 */
	public HashMap<Integer, List<PRItemSaleInfoDTO>> getPromotionsFromRecommendation(Connection conn, long runId)
			throws GeneralException {
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String qry = PristineDBUtil.replaceCalendarName(GET_PROMO_RECOMMENDATION, "%RETAIL_CALENDAR_TAB_NAME%");
			statement = conn.prepareStatement(qry);
			statement.setLong(1, runId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int productId = resultSet.getInt("PRODUCT_ID");
				int productLevelId = resultSet.getInt("PRODUCT_LEVEL_ID");
				PRItemSaleInfoDTO prItemSaleInfoDTO = new PRItemSaleInfoDTO();
				int saleQty = resultSet.getInt("REC_SALE_MULTIPLE");
				double salePr = resultSet.getInt("REC_SALE_PRICE");
				MultiplePrice salePrice = new MultiplePrice(saleQty, salePr);
				prItemSaleInfoDTO.setSalePrice(salePrice);
				prItemSaleInfoDTO
						.setPromoTypeId(resultSet.getInt("REC_PROMO_TYPE_ID"));
				prItemSaleInfoDTO.setMinQtyReqd(resultSet.getInt("REC_MIN_QTY_REQD"));
				prItemSaleInfoDTO.setOfferUnitType(resultSet.getString("REC_OFFER_TYPE"));
				prItemSaleInfoDTO.setOfferValue(resultSet.getDouble("REC_OFFER_VALUE"));
				prItemSaleInfoDTO.setSalePredMovAtRecReg(resultSet.getDouble("PRED_MOV_REC_PROMO"));
				prItemSaleInfoDTO
						.setSalePredStatusAtCurReg(PredictionStatus.get(resultSet.getInt("PRED_STATUS_REC_PROMO")));
				prItemSaleInfoDTO.setSaleStartDate(resultSet.getString("START_DATE"));
				prItemSaleInfoDTO.setSaleEndDate(resultSet.getString("END_DATE"));
				prItemSaleInfoDTO.setSaleWeekStartDate(resultSet.getString("START_DATE"));
				List<PRItemSaleInfoDTO> saleList = new ArrayList<>();
				saleList.add(prItemSaleInfoDTO);
				saleDetails.put(productId, saleList);
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getPromotionsFromRecommendation() - Error--getting promo recommendations",
					sqlE);
		}
		return saleDetails;
	}
	
	/**
	 * 
	 * @param conn
	 * @param runId
	 * @return recommended promotions
	 * @throws GeneralException 
	 */
	public HashMap<Integer, List<PRItemAdInfoDTO>> getAdsFromRecommendation(Connection conn, long runId)
			throws GeneralException {
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String qry = PristineDBUtil.replaceCalendarName(GET_PROMO_RECOMMENDATION, "%RETAIL_CALENDAR_TAB_NAME%");
			statement = conn.prepareStatement(qry);
			statement.setLong(1, runId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int productId = resultSet.getInt("PRODUCT_ID");
				int productLevelId = resultSet.getInt("PRODUCT_LEVEL_ID");
				if (productLevelId == Constants.PRODUCT_LEVEL_ID_LIG) {
					continue;
				} else {
					PRItemAdInfoDTO prItemAdInfo = new PRItemAdInfoDTO();
					prItemAdInfo.setAdPageNo(resultSet.getInt("REC_AD_PAGE_NO"));
					prItemAdInfo.setAdBlockNo(resultSet.getInt("REC_AD_BLOCK_NO"));
					prItemAdInfo.setWeeklyAdStartDate(resultSet.getString("START_DATE"));
					List<PRItemAdInfoDTO> saleList = new ArrayList<>();
					saleList.add(prItemAdInfo);
					adDetails.put(productId, saleList);
				}
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getPromotionsFromRecommendation() - Error--getting promo recommendations",
					sqlE);
		}
		return adDetails;
	}
	
	
	private static final String GET_PROMO_CANDIDATES = "SELECT PRODUCT_LEVEL_ID, PRODUCT_ID, IS_GROUP_LEVEL_PROMO, "
			+ " ANCHOR_PRD_LEVEL_ID, ANCHOR_PRD_ID, IS_LEAD FROM OR_PROMO_ITEM_SOURCE "
			+ " WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND "
			+ " PARENT_PRODUCT_LEVEL_ID = ? AND PARENT_PRODUCT_ID = ? AND CALENDAR_ID = ? ";
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param calendarId
	 * @return items to be promoted
	 * @throws GeneralException
	 */
	public List<PromoItemDTO> getPromoCandidates(Connection conn, RecommendationInputDTO recommendationInputDTO,
			int calendarId) throws GeneralException {

		List<PromoItemDTO> candidates = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {

			statement = conn.prepareStatement(GET_PROMO_CANDIDATES);
			int counter = 0;
			statement.setInt(++counter, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++counter, recommendationInputDTO.getLocationId());
			statement.setInt(++counter, recommendationInputDTO.getProductLevelId());
			statement.setInt(++counter, recommendationInputDTO.getProductId());
			statement.setInt(++counter, calendarId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				PromoItemDTO promoItemDTO = new PromoItemDTO();
				ProductKey productKey = new ProductKey(resultSet.getInt("PRODUCT_LEVEL_ID"),
						resultSet.getInt("PRODUCT_ID"));
				promoItemDTO.setProductKey(productKey);
				if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					promoItemDTO.setRetLirId(productKey.getProductId());
				}
				
				if (resultSet.getString("IS_GROUP_LEVEL_PROMO") != null
						&& String.valueOf(Constants.YES).equals(resultSet.getString("IS_GROUP_LEVEL_PROMO"))) {
					promoItemDTO.setGroupLevelPromo(true);
					promoItemDTO.setAnchorProdLevelId(resultSet.getInt("ANCHOR_PRD_LEVEL_ID"));
					promoItemDTO.setAnchorProdId(resultSet.getInt("ANCHOR_PRD_ID"));
				}
				
				if (resultSet.getString("IS_LEAD") != null
						&& String.valueOf(Constants.YES).equals(resultSet.getString("IS_LEAD"))) {
					promoItemDTO.setLeadItem(true);
				}
				
				candidates.add(promoItemDTO);
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getPromotionsFromRecommendation() - Error--getting promo recommendations",
					sqlE);
		}
		return candidates;
	}
	
	/**
	 * 
	 * @param conn
	 * @param runId
	 * @return recommended promotions
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<PRItemSaleInfoDTO>> getPromoRecommendationForLeadZone(Connection conn, long runId)
			throws GeneralException {
		HashMap<ProductKey, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String qry = PristineDBUtil.replaceCalendarName(GET_PROMO_RECOMMENDATION, "%RETAIL_CALENDAR_TAB_NAME%");
			statement = conn.prepareStatement(qry);
			statement.setLong(1, runId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int productId = resultSet.getInt("PRODUCT_ID");
				int productLevelId = resultSet.getInt("PRODUCT_LEVEL_ID");
				ProductKey productKey = new ProductKey(productLevelId, productId);
				PRItemSaleInfoDTO prItemSaleInfoDTO = new PRItemSaleInfoDTO();
				int saleQty = resultSet.getInt("REC_SALE_MULTIPLE");
				double salePr = resultSet.getInt("REC_SALE_PRICE");
				MultiplePrice salePrice = new MultiplePrice(saleQty, salePr);
				prItemSaleInfoDTO.setSalePrice(salePrice);
				prItemSaleInfoDTO
						.setPromoTypeId(resultSet.getInt("REC_PROMO_TYPE_ID"));
				prItemSaleInfoDTO.setMinQtyReqd(resultSet.getInt("REC_MIN_QTY_REQD"));
				prItemSaleInfoDTO.setOfferUnitType(resultSet.getString("REC_OFFER_TYPE"));
				prItemSaleInfoDTO.setOfferValue(resultSet.getDouble("REC_OFFER_VALUE"));
				prItemSaleInfoDTO.setSalePredMovAtRecReg(resultSet.getDouble("PRED_MOV_REC_PROMO"));
				prItemSaleInfoDTO
						.setSalePredStatusAtCurReg(PredictionStatus.get(resultSet.getInt("PRED_STATUS_REC_PROMO")));
				prItemSaleInfoDTO.setSaleStartDate(resultSet.getString("START_DATE"));
				prItemSaleInfoDTO.setSaleEndDate(resultSet.getString("END_DATE"));
				prItemSaleInfoDTO.setSaleWeekStartDate(resultSet.getString("START_DATE"));
				List<PRItemSaleInfoDTO> saleList = new ArrayList<>();
				saleList.add(prItemSaleInfoDTO);
				saleDetails.put(productKey, saleList);
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getPromotionsFromRecommendation() - Error--getting promo recommendations",
					sqlE);
		}
		return saleDetails;
	}
	
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startDate
	 * @return promotions recommended
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<PRItemSaleInfoDTO>> getPromoRecommendationForLeadZone(Connection conn,
			int locationLevelId, int locationId, int productLevelId, int productId, String startDate)
			throws GeneralException {

		long runId = getLatestRunId(conn, locationLevelId, locationId, productLevelId, productId, startDate);

		HashMap<ProductKey, List<PRItemSaleInfoDTO>> saleDetails = getPromoRecommendationForLeadZone(conn, runId);

		return saleDetails;
	}
}
