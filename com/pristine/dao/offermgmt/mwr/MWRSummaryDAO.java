package com.pristine.dao.offermgmt.mwr;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.SecondaryZoneRecDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRSummaryDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.mwr.SummarySplitupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class MWRSummaryDAO {
	private static Logger logger = Logger.getLogger("MWRSummaryDAO");
	private static final String INSERT_PR_QUARTER_SUMMARY = "INSERT INTO PR_QUARTER_REC_SUMMARY "
			+ " (RUN_ID, UNITS_ACTUAL, REV_ACTUAL, MAR_ACTUAL, MAR_PCT_ACTUAL, INBETWEEN_WEEKS_UNITS_PRED, "
			+ " INBETWEEN_WEEKS_REV_PRED, INBETWEEN_WEEKS_MAR_PRED, INBETWEEN_WEEKS_MAR_PCT_PRED, REC_WEEKS_UNITS_PRED, "
			+ " REC_WEEKS_REV_PRED, REC_WEEKS_MAR_PRED, REC_WEEKS_MAR_PCT_PRED, UNITS_TOTAL_ILP, REV_TOTAL_ILP, MAR_TOTAL_ILP, "
			+ "	MAR_PCT_TOTAL_ILP, CURR_UNITS_ILP, CURR_REVENUE_ILP, CURR_MARGIN_ILP, CURR_MAR_PCT_ILP, "
			+ " PROMO_UNITS_ILP, PROMO_REVENUE_ILP, PROMO_MARGIN_ILP, PROMO_MAR_PCT_ILP, TOTAL_ITEMS, "
			+ " TOTAL_ITEMS_LIG, TOTAL_PROMO_ITEMS, TOTAL_PROMO_ITEMS_LIG, TOTAL_EDLP_ITEMS, "
			+ " TOTAL_EDLP_ITEMS_LIG, PRICE_INDEX, PRICE_INDEX_PROMO, PRICE_BASE_INDEX, "
			+ " UNITS_TOTAL, REV_TOTAL, MAR_TOTAL, MAR_PCT_TOTAL, "
			+ " CURR_UNITS, CURR_REVENUE, CURR_MARGIN, CURR_MAR_PCT,"
			+ " TOTAL_REC_ITEMS, TOTAL_REC_ITEMS_LIG, TOTAL_COST_CHANGES, TOTAL_COST_CHANGES_LIG, "
			+ " TOTAL_COMP_CHANGES, TOTAL_COMP_CHANGES_LIG, PRC_CHANGE_IMPACT,CURR_PRICE_INDEX, CURR_PRICE_INDEX_BASE, MARK_UP, MARK_DOWN)"
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?)";

	private static final String GET_REC_ITEMS_OF_RUNS = "SELECT PRR.RUN_ID, PRR.PR_QUARTER_REC_ID, PRR.PRODUCT_ID,  "
			+ " PRR.RET_LIR_ID, CASE WHEN PRR.PRODUCT_LEVEL_ID = " 
			+ Constants.PRODUCT_LEVEL_ID_LIG + " THEN 'Y' ELSE 'N' END AS LIR_IND,  "
			+ " PRR.REG_MULTIPLE, PRR.REG_PRICE, PRR.LIST_COST, PRR.STRATEGY_ID, PRR.PRICE_CHECK_LIST_ID, "
			+ " PRR.REC_REG_MULTIPLE, PRR.REC_REG_PRICE,  PRR.OVERRIDE_REG_PRICE, PRR.OVERRIDE_REG_MULTIPLE, IL.UPC,   "
			+ " PRR.IS_USER_OVERRIDE,PRR.COMP_STR_ID,PRS.LOCATION_LEVEL_ID AS STRATEGY_LOCATION_LEVEL_ID, "
			+ " PRS.LOCATION_ID AS STRATEGY_LOCATION_ID,  PRS.PRODUCT_LEVEL_ID AS STRATEGY_PRODUCT_LEVEL_ID, "
			+ " PRS.PRODUCT_ID AS STRATEGY_PRODUCT_ID,  PRS.APPLY_TO AS STRATEGY_APPLY_TO, "
			+ " PRS.VENDOR_ID AS STRATEGY_VENDOR_ID, PRS.STATE_ID AS STRATEGY_STATE_ID, PRR.COMP_REG_PRICE,PRR.COMP_REG_MULTIPLE,PRR.COMP_STR_ID,"
			+ " RPZ.PRICE_ZONE_ID, RPZ.ZONE_NUM, PRR.PRC_CHANGE_IMPACT, PRR.IS_SYSTEM_OVERRIDE,IL.RETAILER_ITEM_CODE,PRR.MOV_52_WEEK,PRR.WEIGHT_NEW_RETAIL,"
			+ " PRR.FUTURE_COST_EFF_DATE,PRR.FUTURE_COST,PRR.MAP_RETAIL,PRR.IS_HOLD,PRR.NEWPRICE_RECOMMENDED,PRR.IS_PENDING_RETAIL_RECOMMENDED,PRR.EXPLAIN_LOG,PRR.IS_IMPACT_INCL_IN_SMRY_CALC "
			+ " ,PRR.LIG_REP_ITEM_CODE FROM PR_QUARTER_REC_ITEM PRR LEFT JOIN ITEM_LOOKUP IL ON PRR.PRODUCT_ID = IL.ITEM_CODE  "
			+ " LEFT JOIN PR_QUARTER_REC_HEADER PRH ON PRR.RUN_ID = PRH.RUN_ID  "
			+ " LEFT JOIN PR_STRATEGY PRS ON PRR.STRATEGY_ID = PRS.STRATEGY_ID  "
			+ " LEFT JOIN RETAIL_PRICE_ZONE RPZ ON RPZ.PRICE_ZONE_ID = PRH.LOCATION_ID "
			+ " WHERE PRR.RUN_ID IN(%RUN_IDS%) AND REC_REG_PRICE IS NOT NULL AND PRR.CAL_TYPE = '" + Constants.CALENDAR_QUARTER + "'";
				
	
	private static final String UPDATE_RE_RECOMMENDATION_DETAILS = "UPDATE PR_QUARTER_REC_ITEM SET OVERRIDE_REG_MULTIPLE = ?, "
			+ " OVERRIDE_REG_PRICE = ?, OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ?, IS_SYSTEM_OVERRIDE = ?, "
			+ " OVERRIDE_PRED_UPDATE_STATUS=?, PREDICTION_STATUS = ?, "
			+ " IS_USER_OVERRIDE = ?, SYS_OVERRIDE_LOG = ? WHERE RUN_ID = ? AND ITEM_CODE = ? and LIR_IND= ? ";
	

	
	private static final String UPDATE_PR_QUARTER_SUMMARY = "UPDATE PR_QUARTER_REC_SUMMARY "
			+ " SET INBETWEEN_WEEKS_UNITS_PRED = ?, INBETWEEN_WEEKS_REV_PRED = ?, INBETWEEN_WEEKS_MAR_PRED = ?, "
			+ " INBETWEEN_WEEKS_MAR_PCT_PRED = ?, REC_WEEKS_UNITS_PRED = ?, "
			+ " REC_WEEKS_REV_PRED = ?, REC_WEEKS_MAR_PRED = ?, REC_WEEKS_MAR_PCT_PRED = ?, "
			+ " UNITS_TOTAL_ILP = ?, REV_TOTAL_ILP = ?, MAR_TOTAL_ILP = ?, "
			+ "	MAR_PCT_TOTAL_ILP = ?, "
			+ " PROMO_UNITS = ?, PROMO_REVENUE = ?, PROMO_MARGIN = ?, PROMO_MAR_PCT = ?, TOTAL_ITEMS = ?, "
			+ " TOTAL_ITEMS_LIG = ?, TOTAL_PROMO_ITEMS = ?, TOTAL_PROMO_ITEMS_LIG = ?, TOTAL_EDLP_ITEMS = ?, "
			+ " TOTAL_EDLP_ITEMS_LIG = ?, PRICE_INDEX = ?, PRICE_INDEX_PROMO = ?, PRICE_BASE_INDEX = ?, "
			+ " UNITS_TOTAL = ?, REV_TOTAL = ?, MAR_TOTAL = ?, MAR_PCT_TOTAL = ?, "
			+ " CURR_UNITS = ?, CURR_REVENUE = ?, CURR_MARGIN = ?, CURR_MAR_PCT = ?, PRC_CHANGE_IMPACT = ?, "
			+ " CURR_PRICE_INDEX_BASE = ? , MARK_UP = ? , MARK_DOWN = ?, TOTAL_REC_ITEMS = ?, TOTAL_REC_ITEMS_LIG = ?"
			+ " WHERE RUN_ID = ? ";

	
	private static final String INSERT_PR_CHILD_LOCATION_REC = "INSERT INTO PR_CHILD_LOCATION_REC "
			+ " (CHILD_REC_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, CHILD_LOCATION_LEVEL_ID, CHILD_LOCATION_ID, "
			+ " REG_PRICE, REG_MULTIPLE, REC_REG_PRICE, REC_REG_MULTIPLE, LIST_COST) "
			+ " VALUES (PR_CHILD_REC_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	
	private static final String UPDATE_PR_CHILD_LOCATION_REC = "UPDATE PR_CHILD_LOCATION_REC "
			+ " SET OVER_REG_PRICE = ?, OVER_REG_MULTIPLE = ? WHERE PRODUCT_LEVEL_ID = ? "
			+ " AND PRODUCT_ID = ? AND CHILD_LOCATION_LEVEL_ID = ? AND CHILD_LOCATION_ID = ? AND RUN_ID = ?";
	
	
	private static final String GET_PR_CHILD_LOCATION_REC = "SELECT "
			+ " CHILD_REC_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, CHILD_LOCATION_LEVEL_ID, CHILD_LOCATION_ID, "
			+ " REG_PRICE, REG_MULTIPLE, REC_REG_PRICE, REC_REG_MULTIPLE, LIST_COST, RPZ.ZONE_NUM FROM PR_CHILD_LOCATION_REC PCL "
			+ " LEFT JOIN RETAIL_PRICE_ZONE RPZ ON RPZ.PRICE_ZONE_ID = PCL.CHILD_LOCATION_ID "
			+ " WHERE RUN_ID = ?";

	 boolean populateExplainLogForUpdateRec=Boolean.parseBoolean(PropertyManager.getProperty("POPULATE_EXPLAIN_LOG_FOR_UPDATE_RECCS", "FALSE"));
	

	/**
	 * 
	 * @param conn
	 * @param mwrSummaryDTO
	 * @throws GeneralException
	 */
	public void insertMultiWeekSummary(Connection conn, MWRSummaryDTO mwrSummaryDTO) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_SUMMARY);
			int counter = 0;
			statement.setLong(++counter, mwrSummaryDTO.getRunId());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsActual());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesActual());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginActual());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctActual());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentUnitsTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentSalesTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginPctTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoUnits());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoSales());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoMargin());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoMarginPct());
			statement.setInt(++counter, mwrSummaryDTO.getTotalItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalDistinctItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalPromoItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalDistinctPromoItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalEDLPItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalDistinctEDLPItems());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndex());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndexPromo());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndexEDLPRec());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentUnitsTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentSalesTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginPctTotalClpDlp());
			statement.setInt(++counter, mwrSummaryDTO.getTotalRecommendedItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalRecommendedDistinctItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalCostChangedItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalCostChangedDistinctItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalCompChangedItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalCompChangedDistinctItems());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceChangeImpact());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentPriceIndex());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndexEDLPCurr());
			statement.setDouble(++counter, mwrSummaryDTO.getMarkUP());
			statement.setDouble(++counter, mwrSummaryDTO.getMarkDown());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertMultiWeekSummary() - Error while inserting PR_QUARTER_REC_SUMMARY", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private static final String INSERT_PR_REC_SUMMARY_SPLITUP = "INSERT INTO PR_QUARTER_REC_SUMMARY_SPLITUP "
			+ " (RUN_ID, CALENDAR_ID, UNITS_ACTUAL, REV_ACTUAL, MAR_ACTUAL, MAR_PCT_ACTUAL, UNITS_PRED, "
			+ " REV_PRED, MAR_PRED, MAR_PCT_PRED, UNITS_TOTAL_ILP, REV_TOTAL_ILP, MAR_TOTAL_ILP, MAR_PCT_TOTAL_ILP, "
			+ " CURR_UNITS_ILP, CURR_REVENUE_ILP, CURR_MARGIN_ILP, CURR_MAR_PCT_ILP, "
			+ " PROMO_UNITS_ILP, PROMO_REVENUE_ILP, PROMO_MARGIN_ILP, PROMO_MAR_PCT_ILP, CAL_TYPE, "
			+ " CURR_UNITS, CURR_REVENUE, CURR_MARGIN, CURR_MAR_PCT, "
			+ " UNITS_TOTAL, REV_TOTAL, MAR_TOTAL, MAR_PCT_TOTAL, PERIOD_CALENDAR_ID, PRC_CHANGE_IMPACT, MARK_UP, MARK_DOWN)"
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	/**
	 * 
	 * @param conn
	 * @param mwrSummaryDTO
	 * @throws GeneralException
	 */
	public void insertWeeklySummary(Connection conn, List<SummarySplitupDTO> weeklySummary) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_REC_SUMMARY_SPLITUP);

			for (SummarySplitupDTO summarySplitupDTO : weeklySummary) {
				int counter = 0;
				statement.setLong(++counter, summarySplitupDTO.getRunId());
				statement.setInt(++counter, summarySplitupDTO.getCalendarId());
				statement.setDouble(++counter, summarySplitupDTO.getUnitsActual());
				statement.setDouble(++counter, summarySplitupDTO.getSalesActual());
				statement.setDouble(++counter, summarySplitupDTO.getMarginActual());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctActual());
				statement.setDouble(++counter, summarySplitupDTO.getUnitsPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getSalesPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getUnitsTotal());
				statement.setDouble(++counter, summarySplitupDTO.getSalesTotal());
				statement.setDouble(++counter, summarySplitupDTO.getMarginTotal());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctTotal());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentUnitsTotal());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentSalesTotal());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentMarginTotal());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentMarginPctTotal());
				statement.setDouble(++counter, summarySplitupDTO.getPromoUnits());
				statement.setDouble(++counter, summarySplitupDTO.getPromoSales());
				statement.setDouble(++counter, summarySplitupDTO.getPromoMargin());
				statement.setDouble(++counter, summarySplitupDTO.getPromoMarginPct());
				statement.setString(++counter, summarySplitupDTO.getCalType());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentUnitsTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentSalesTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentMarginTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getCurrentMarginPctTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getUnitsTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getSalesTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getMarginTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctTotalClpDlp());
				statement.setInt(++counter, summarySplitupDTO.getPeriodCalendarId());
				statement.setDouble(++counter, summarySplitupDTO.getPriceChangeImpact());
				statement.setDouble(++counter,  summarySplitupDTO.getMarkUp());
				statement.setDouble(++counter, summarySplitupDTO.getMarkDown());
				statement.addBatch();
			}

			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertWeeklySummary() - Error while inserting PR_WEEK_REC_SUMMARY", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private static final String INSERT_PR_QUARTER_REC_ITEM = "INSERT INTO PR_QUARTER_REC_ITEM ("
			+ " PR_QUARTER_REC_ID, RUN_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, RET_LIR_ID, "
			+ " STRATEGY_ID, ITEM_SIZE, UOM_ID, PRICE_CHECK_LIST_ID, LIG_REP_ITEM_CODE, REG_MULTIPLE, "
			+ " REG_PRICE, SALE_MULTIPLE, SALE_PRICE, PROMO_TYPE_ID, AD_PAGE_NO, AD_BLOCK_NO, DISPLAY_TYPE_ID, "
			+ " LIST_COST, DEAL_COST, REC_REG_MULTIPLE, REC_REG_PRICE, "
			+ " EXPLAIN_LOG, COST_CHG_IND, COMP_PRICE_CHG_IND, "
			+ " COMP_STR_ID, COMP_REG_MULTIPLE, COMP_REG_PRICE, COMP_SALE_MULTIPLE, COMP_SALE_PRICE, "
			+ " COMP_PRICE_CHECK_DATE, REGULAR_UNITS, SALE_UNITS, REGULAR_REVENUE, SALE_REVENUE, "
			+ " REGULAR_MARGIN, SALE_MARGIN, CURR_UNITS, CURR_REVENUE, CURR_MARGIN, "
			+ " REC_START_CALENDAR_ID, CONFLICT, NEWPRICE_RECOMMENDED, CURR_REG_EFF_DATE, COST_EFF_DATE,"
			+ " IS_ERROR_REC, ERROR_REC_CODES, EDLP_OR_PROMO, PREDICTION_STATUS, CAL_TYPE, PERIOD_CALENDAR_ID,"
			+ " IMM_PROMO_START_DATE, IMM_PROMO_END_DATE, IS_TPR, IS_PRE_PRICED, IS_LOCK_PRICED, PREV_COST, " 
			+ " PREV_COMP_RETAIL, FUTURE_RETAIL_MULTIPLE, FUTURE_RETAIL, FUTURE_RETAIL_EFF_DATE, FUTURE_COST, "
			+ " FUTURE_COST_EFF_DATE, VENDOR_ID, "
			+ " COMP_1_STR_ID, COMP_2_STR_ID, COMP_3_STR_ID, COMP_4_STR_ID, COMP_5_STR_ID, "
			+ " COMP_1_RETAIL_MUL, COMP_1_RETAIL, COMP_2_RETAIL_MUL, COMP_2_RETAIL, COMP_3_RETAIL_MUL, "
			+ " COMP_3_RETAIL, COMP_4_RETAIL_MUL, COMP_4_RETAIL, COMP_5_RETAIL_MUL, COMP_5_RETAIL, CORE_RETAIL, CWAC_CORE_COST, "
			+ "PRC_CHANGE_IMPACT,ORIGINAL_COST,REG_EFF_DATE, MOV_52_WEEK, WEIGHT_CURR_RETAIL, WEIGHT_NEW_RETAIL, "
			+ "WEIGHT_COST, WEIGHT_COMP_1_RET, WEIGHT_COMP_2_RET, WEIGHT_COMP_3_RET, WEIGHT_COMP_4_RET, WEIGHT_COMP_5_RET, "
			+ "WEIGHT_PRIM_COMP_RET, IS_CHILD_LOC_REC, NIPO_COST, FAMILY_UNITS,STORE_COUNT, IS_FREIGHT_CHG_ADDED_TO_COST, "
			+ "MAP_RETAIL, IS_HOLD, IS_IMPACT_CALCULATED, CWAC_BASE_COST, IS_PENDING_RETAIL_RECOMMENDED, FUTURE_WEEK_PRICE, "
			+ "FUTURE_WEEK_PRICE_EFF_DATE,IS_IMPACT_INCL_IN_SMRY_CALC) "
			+ "VALUES ( PR_QUARTER_REC_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ "?, ?, ?, ?, ?, " + "	TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ "TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, ?, "
			+ "TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, ?, "
			+ "?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') , ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, "
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, "
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'),?)";

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param itemSummary
	 * @throws GeneralException
	 * @throws JsonProcessingException
	 */
	public void saveItemSummaryForQuarter(Connection conn, RecommendationInputDTO recommendationInputDTO,
			List<MWRItemDTO> itemSummary,Map<Integer, MWRItemDTO> ligMap) throws GeneralException, JsonProcessingException {

		PreparedStatement statement = null;
		ObjectMapper mapper = new ObjectMapper();
		
		/** Added for PROM-2223  ***/
		boolean calculateglobalZoneImpact= Boolean
				.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE", "FALSE"));
		/**Added configuration as this is applicable only for FF **/
		boolean markOnHoldForNewRecommendation= Boolean
				.parseBoolean(PropertyManager.getProperty("MARK_ON_HOLD_FOR_NEW_RECC", "FALSE"));
		try {
			int itemsInBatch = 0;
			statement = conn.prepareStatement(INSERT_PR_QUARTER_REC_ITEM);
			for (MWRItemDTO mwrItemDTO : itemSummary) {
				double xweekxMovement = mwrItemDTO.getxWeeksMov();
				double weightedRecPrice = 0;
				double weightedPrice = 0;
				double weightedCost = 0;
				double weightedComp1Price = 0;
				double weightedComp2Price = 0;
				double weightedComp3Price = 0;
				double weightedComp4Price = 0;
				double weightedComp5Price = 0;
				double weightedComp6Price = 0;
				
	
				/** Added for PROM-2223 by KARISHMA ***/
				// For global zone if there is a impact and new price recommended is equal to
				// curr retail
				// then mark this item as recommended as AZ wants to export the same
				//Added condition  for checking  there is no pending retail and only if its not equal to approved impact ,only in this case for global zone it should mark the row as recommended with impact 
				if (calculateglobalZoneImpact && recommendationInputDTO.isGlobalZone()) {
					if (mwrItemDTO.getPriceChangeImpact() != 0 && mwrItemDTO.getPendingRetail() == null
							&& mwrItemDTO.getPriceChangeImpact() != mwrItemDTO.getApprovedImpact()) {
						mwrItemDTO.setNewPriceRecommended(true);
					}
				}
				/** Changes End for PROM-2223 ***/

				if (mwrItemDTO.getRegPrice() != null) {
					weightedPrice = mwrItemDTO.getRegPrice() * xweekxMovement;
				}

				//Calculate the weighted reg price in case of update reccommendations
				if (mwrItemDTO.getOverrideRegPrice() != null) {
					weightedRecPrice = mwrItemDTO.getOverrideRegPrice().getUnitPrice() * xweekxMovement;
				} else if (mwrItemDTO.getRecommendedRegPrice() != null) {
					weightedRecPrice = mwrItemDTO.getRecommendedRegPrice().getUnitPrice() * xweekxMovement;
				}

				if (mwrItemDTO.getListCost() != null) {
					weightedCost = mwrItemDTO.getListCost() * xweekxMovement;
				}

				if (mwrItemDTO.getComp1Retail() != null) {
					weightedComp1Price = mwrItemDTO.getComp1Retail().getUnitPrice() * xweekxMovement;
				}
				if (mwrItemDTO.getComp2Retail() != null) {
					weightedComp2Price = mwrItemDTO.getComp2Retail().getUnitPrice() * xweekxMovement;
				}
				if (mwrItemDTO.getComp3Retail() != null) {
					weightedComp3Price = mwrItemDTO.getComp3Retail().getUnitPrice() * xweekxMovement;
				}
				if (mwrItemDTO.getComp4Retail() != null) {
					weightedComp4Price = mwrItemDTO.getComp4Retail().getUnitPrice() * xweekxMovement;
				}
				if (mwrItemDTO.getComp5Retail() != null) {
					weightedComp5Price = mwrItemDTO.getComp5Retail().getUnitPrice() * xweekxMovement;
				}
				if (mwrItemDTO.getCompRegPrice() != null) {
					weightedComp6Price = mwrItemDTO.getCompRegPrice() * xweekxMovement;
				}
				
				int colCount = 0;
				statement.setLong(++colCount, recommendationInputDTO.getRunId());
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getProductLevelId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getProductId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getRetLirId(), statement);
				statement.setLong(++colCount, mwrItemDTO.getStrategyId());
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getItemSize(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getUomId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getPriceCheckListId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getLigRepItemCode(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getRegMultiple(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRegPrice(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getSaleMultiple(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getSalePrice(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getPromoTypeId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getAdPageNo(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getAdBlockNo(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getDisplayTypeId(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getListCost(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getDealCost(), statement);
				if (mwrItemDTO.getRecommendedRegPrice() != null) {
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getRecommendedRegPrice().multiple, statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRecommendedRegPrice().price, statement);
				} else {
					PristineDBUtil.setInt(++colCount, null, statement);
					PristineDBUtil.setDouble(++colCount, null, statement);
				}
				PristineDBUtil.setString(++colCount, mapper.writeValueAsString(mwrItemDTO.getExplainLog()), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getCostChangeIndicator(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompPriceChangeIndicator(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompStrId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompRegMultiple(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCompRegPrice(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompSaleMultiple(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCompSalePrice(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getCompPriceCheckDate(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRegUnits(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getSaleUnits(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRegRevenue(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getSaleRevenue(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRegMargin(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getSaleMargin(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCurrentRegUnits(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCurrentRegRevenue(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCurrentRegMargin(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getRecWeekStartCalendarId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsConflict(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.isNewPriceRecommended() ? 1 : 0, statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getCurrPriceEffDate(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getCostEffDate(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.isRecError() ? 1 : 0, statement);
				PristineDBUtil.setString(++colCount,
						PRCommonUtil.getCommaSeperatedStringFromIntArray(mwrItemDTO.getRecErrorCodes()), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getEDLPORPROMO(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getFinalPricePredictionStatus(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getCalType(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getPeriodCalendarId(), statement);
				
				if (mwrItemDTO.getImmediatePromoInfo().getSalePrice() != null) {
					statement.setString(++colCount, mwrItemDTO.getImmediatePromoInfo().getSaleStartDate());
					statement.setString(++colCount, mwrItemDTO.getImmediatePromoInfo().getSaleEndDate());
				} else {
					statement.setString(++colCount, null);
					statement.setString(++colCount, null);
				}
				
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsTPR(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsPrePriced(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsLockPriced(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getPrevListCost(), statement);
				if(mwrItemDTO.getPrevCompPrice() != null){
					PristineDBUtil.setDouble(++colCount, PRCommonUtil.getUnitPrice(mwrItemDTO.getPrevCompPrice(), true), statement);
				}else {
					PristineDBUtil.setDouble(++colCount, null, statement);
				}
				
				if(mwrItemDTO.getFuturePrice() != null){
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getFuturePrice().multiple, statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFuturePrice().price, statement);	
				}else {
					PristineDBUtil.setInt(++colCount, null, statement);
					PristineDBUtil.setDouble(++colCount, null, statement);
				}
				PristineDBUtil.setString(++colCount, mwrItemDTO.getFuturePriceEffDate(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFutureCost(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getFutureCostEffDate(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getVendorId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp1StrId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp2StrId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp3StrId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp4StrId(), statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp5StrId(), statement);
				PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp1Retail(), statement);
				PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp1Retail(), statement);
				PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp2Retail(), statement);
				PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp2Retail(), statement);
				PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp3Retail(), statement);
				PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp3Retail(), statement);
				PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp4Retail(), statement);
				PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp4Retail(), statement);
				PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp5Retail(), statement);
				PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp5Retail(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCoreRetail(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCwacCoreCost(), statement);
				//if item has pending retail and no override then save the approved impact  which will be used for for display 
				if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getOverrideRegPrice() == null)
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getApprovedImpact(), statement);
				else
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getPriceChangeImpact(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getOriginalListCost(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getFuturePriceEffDate(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getxWeeksMov(), statement);
				PristineDBUtil.setDouble(++colCount, weightedPrice, statement);
				PristineDBUtil.setDouble(++colCount, weightedRecPrice, statement);
				PristineDBUtil.setDouble(++colCount, weightedCost, statement);
				PristineDBUtil.setDouble(++colCount, weightedComp1Price, statement);
				PristineDBUtil.setDouble(++colCount, weightedComp2Price, statement);
				PristineDBUtil.setDouble(++colCount, weightedComp3Price, statement);
				PristineDBUtil.setDouble(++colCount, weightedComp4Price, statement);
				PristineDBUtil.setDouble(++colCount, weightedComp5Price, statement);
				PristineDBUtil.setDouble(++colCount, weightedComp6Price, statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.isSecondaryZoneRecPresent() ? 1 : 0, statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getNipoBaseCost(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFamilyUnits(),statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getStoreCount(),statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsFreightChargeIncluded(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getMapRetail(), statement);
				if (markOnHoldForNewRecommendation)
					PristineDBUtil.setString(++colCount, mwrItemDTO.isNewPriceRecommended() ? "Y" : "N", statement);
				else
					PristineDBUtil.setString(++colCount, "N", statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.isIsnewImpactCalculated() ? 1 : 0, statement);
				
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCwagBaseCost(),statement);
				PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsPendingRetailRecommended(), statement);
				PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFutureWeekPrice(), statement);
				PristineDBUtil.setString(++colCount, mwrItemDTO.getFutureWeekPriceEffDate(), statement);
				
				
				if (mwrItemDTO.isLir() && ligMap != null && ligMap.containsKey(mwrItemDTO.getRetLirId())) {
					PristineDBUtil.setString(++colCount,
							String.valueOf(
									ligMap.get(mwrItemDTO.getRetLirId()).getIsImpactIncludedInSummaryCalculation()),
							statement);
				} else if (mwrItemDTO.isLir() && ligMap == null) {
					PristineDBUtil.setString(++colCount, String.valueOf("N"), statement);
				} else
					PristineDBUtil.setString(++colCount,
							String.valueOf(mwrItemDTO.getIsImpactIncludedInSummaryCalculation()), statement);

				statement.addBatch();
				itemsInBatch++;
				if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					itemsInBatch = 0;
				}
			}
			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("saveRecommendationDetails() - Error while inserting into PR_QUARTER_REC_ITEM",
					sqlE);
		}
	}

	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @return Rocommendation details for given run ids
	 * @throws GeneralException
	 */
	public HashMap<Long, List<PRItemDTO>> getRecItemsOfRunIds(Connection conn, List<Long> runIds)
			throws GeneralException {
		HashMap<Long, List<PRItemDTO>> runAndItsItem = new HashMap<Long, List<PRItemDTO>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		PRStrategyDTO strategyDTO;
		try {
			String runs = "";
			for (Long runId : runIds) {
				runs = runs + "," + runId;
			}
			runs = runs.substring(1);
			String query = new String(GET_REC_ITEMS_OF_RUNS);
			query = query.replaceAll("%RUN_IDS%", runs);
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			while (rs.next()) {

				List<PRItemDTO> itemsOfRun;
				PRItemDTO itemDTO = new PRItemDTO();

				Long runId = rs.getLong("RUN_ID");
				String lirInd = rs.getString("LIR_IND");
				if (runAndItsItem.get(runId) == null)
					itemsOfRun = new ArrayList<PRItemDTO>();
				else
					itemsOfRun = runAndItsItem.get(runId);

				itemDTO.setRunId(runId);
				itemDTO.setRecommendationId(rs.getLong("PR_QUARTER_REC_ID"));
				itemDTO.setItemCode(rs.getInt("PRODUCT_ID"));
				itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				itemDTO.setStrategyId(rs.getLong("STRATEGY_ID"));
				if (rs.getObject("LIST_COST") != null)
					itemDTO.setListCost(rs.getDouble("LIST_COST"));
				if (rs.getObject("RET_LIR_ID") != null)
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				if (rs.getObject("PRICE_CHECK_LIST_ID") != null)
					itemDTO.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
				if (rs.getObject("REC_REG_PRICE") != null)
					itemDTO.setRecommendedRegPrice(
							new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"), rs.getDouble("REC_REG_PRICE")));
				itemDTO.setRecRegPriceBeforeReRecommedation(
						new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"), rs.getDouble("REC_REG_PRICE")));
				
				if (null == rs.getObject("OVERRIDE_REG_PRICE")) {
					itemDTO.setOverriddenRegularPrice(null);
					itemDTO.setOverrideRegPrice(null);
					itemDTO.setOverrideRegMultiple(null);
				}
				else {
					MultiplePrice overriddenPrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),
							rs.getDouble("OVERRIDE_REG_PRICE"));
					itemDTO.setOverriddenRegularPrice(overriddenPrice);
					itemDTO.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));
					itemDTO.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
				}

				if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
					itemDTO.setLir(true);
				}

				itemDTO.setRegMPack(rs.getInt("REG_MULTIPLE"));
				if (itemDTO.getRegMPack() > 1)
					itemDTO.setRegMPrice(rs.getDouble("REG_PRICE"));
				else
					itemDTO.setRegPrice(rs.getDouble("REG_PRICE"));

				strategyDTO = new PRStrategyDTO();
				strategyDTO.setLocationLevelId(rs.getInt("STRATEGY_LOCATION_LEVEL_ID"));
				strategyDTO.setLocationId(rs.getInt("STRATEGY_LOCATION_ID"));
				strategyDTO.setProductLevelId(rs.getInt("STRATEGY_PRODUCT_LEVEL_ID"));
				strategyDTO.setProductId(rs.getInt("STRATEGY_PRODUCT_ID"));
				strategyDTO.setApplyTo(rs.getInt("STRATEGY_APPLY_TO"));
				strategyDTO.setVendorId(rs.getInt("STRATEGY_VENDOR_ID"));
				strategyDTO.setStateId(rs.getInt("STRATEGY_STATE_ID"));
				itemDTO.setStrategyDTO(strategyDTO);
				itemDTO.setUpc(rs.getString("UPC"));

				itemDTO.setUserOverrideFlag(rs.getInt("IS_USER_OVERRIDE"));
				itemDTO.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				itemDTO.setChildLocationId(rs.getInt("PRICE_ZONE_ID"));
				itemDTO.setPriceZoneNo(rs.getString("ZONE_NUM"));
				itemDTO.setPriceChangeImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
				itemDTO.setSystemOverrideFlag(rs.getInt("IS_SYSTEM_OVERRIDE") == 1 ? true :false);
				if(itemDTO.isSystemOverrideFlag()) {
					itemDTO.setOverriddenRegularPrice(null);

				}
				itemDTO.setSendToPrediction(true);

				if (rs.getInt("COMP_REG_PRICE") > 0 && rs.getDouble("COMP_REG_MULTIPLE") > 0) {
					LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, rs.getInt("COMP_STR_ID"));
					itemDTO.setCompStrId(locationKey);
					MultiplePrice primaryCompPrice = new MultiplePrice(rs.getInt("COMP_REG_MULTIPLE"),
							rs.getDouble("COMP_REG_PRICE"));
					itemDTO.setCompPrice(primaryCompPrice);
					itemDTO.addAllCompPrice(locationKey, primaryCompPrice);
				}
				
				itemDTO.setXweekMov(rs.getDouble("MOV_52_WEEK"));
				itemDTO.setWeightedRecRetail(rs.getDouble("WEIGHT_NEW_RETAIL"));
				
				//added on 05/07/2022 to use the 52 weeks movement for calculating impact for update reccs
				itemDTO.setxWeeksMovForTotimpact(rs.getDouble("MOV_52_WEEK"));
				
				//Added by Karishma for future cost enhancement RA
				//populate the future cost if present
				if (rs.getString("FUTURE_COST") != null) {
					itemDTO.setFutureListCost(Double.parseDouble(rs.getString("FUTURE_COST")));
					itemDTO.setFutureCostEffDate(rs.getString("FUTURE_COST_EFF_DATE"));
				}
				itemDTO.setMapRetail(rs.getDouble("MAP_RETAIL"));
				itemDTO.setActive(true);
				itemDTO.setAuthorized(true);
				
				if (rs.getString("IS_HOLD") != null && !rs.getString("IS_HOLD").isEmpty()
						&& rs.getString("IS_HOLD").equalsIgnoreCase("Y"))
					itemDTO.setOnHold(true);
				else
					itemDTO.setOnHold(false);
				
				// Changes done on 05/07/2022
				// This field is added to compare the impact that is calculated before update
				// and wil be used to compare the impact after override
				// to identify items whose oevrride is removed
				// field populated for using the approved impact for Items on pending retail in final write
				itemDTO.setApprovedImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
				
				//get the is recommended flag 
				itemDTO.setIsNewPriceRecommended(rs.getInt("NEWPRICE_RECOMMENDED"));
				itemDTO.setIsPendingRetailRecommended(rs.getInt("IS_PENDING_RETAIL_RECOMMENDED"));
				
				try {
					//Added this on 07/21/2022 to avoid circular dependency for Brand/Size Relationships
					if(populateExplainLogForUpdateRec)
					{
						Clob clob = rs.getClob("EXPLAIN_LOG");
						int clobLength = (int) clob.length();
						long i = 1;
						String stringClob = clob.getSubString(i, clobLength);
						ObjectMapper mapper = new ObjectMapper();
						PRExplainLog explainLog = mapper.readValue(stringClob, new TypeReference<PRExplainLog>() {
						});
						itemDTO.setExplainLog(explainLog);
					}
					if (rs.getString("IS_IMPACT_INCL_IN_SMRY_CALC") != null) {
						itemDTO.setIsImpactIncludedInSummaryCalculation(
								rs.getString("IS_IMPACT_INCL_IN_SMRY_CALC").charAt(0));
					}
					itemDTO.setLigRepItemCode(rs.getInt("LIG_REP_ITEM_CODE"));
					
				} catch (Exception e) {
					itemDTO.setExplainLog(null);
					logger.error("Error while reading EXPLAIN_LOG : " + e);
				}
				itemsOfRun.add(itemDTO);
				runAndItsItem.put(runId, itemsOfRun);
			}

		} catch (Exception ex) {
			throw new GeneralException(ex.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runAndItsItem;
	}
	
	
	private static final String GET_RECOMMENDATION_RUN_HEADER = "SELECT RUN_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, "
			+ " PRODUCT_ID, PRH.START_CALENDAR_ID, PRH.END_CALENDAR_ID, TO_CHAR(START_RUN_TIME, 'MM/DD/YYYY') AS START_RUN_TIME, "
			+ " TO_CHAR(RC.START_DATE, 'MM/DD/YYYY') AS START_DATE, TO_CHAR(RC.END_DATE, 'MM/DD/YYYY') AS END_DATE, "
			+ " TO_CHAR(RC1.START_DATE, 'MM/DD/YYYY') AS END_WEEK,  TO_CHAR(RC1.END_DATE, 'MM/DD/YYYY') AS Q_END_WEEK"
			+ " ,PRH.ACTUAL_START_CALENDAR_ID, PRH.ACTUAL_END_CALENDAR_ID FROM PR_QUARTER_REC_HEADER PRH"
			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PRH.START_CALENDAR_ID "
			+ " LEFT JOIN RETAIL_CALENDAR RC1 ON RC1.CALENDAR_ID = PRH.END_CALENDAR_ID "
			+ " WHERE RUN_ID = ?";
	
	
	/**
	 * 
	 * @param conn
	 * @param runId
	 * @return
	 * @throws Exception
	 */
	public RecommendationInputDTO getRecommendationRunHeader(Connection conn, long runId) throws Exception {
		RecommendationInputDTO recommendationRunHeader = new RecommendationInputDTO();
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String query = new String(GET_RECOMMENDATION_RUN_HEADER);
			stmt = conn.prepareStatement(query);
			logger.debug("getRecommendationRunHeader qry :"+ query);
			stmt.setLong(1, runId);
			rs = stmt.executeQuery();
			if (rs.next()) {
				recommendationRunHeader.setRunId(rs.getLong("RUN_ID"));
				recommendationRunHeader.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				recommendationRunHeader.setProductId(rs.getInt("PRODUCT_ID"));
				recommendationRunHeader.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				recommendationRunHeader.setLocationId(rs.getInt("LOCATION_ID"));
				recommendationRunHeader.setStartCalendarId(rs.getInt("START_CALENDAR_ID"));
				recommendationRunHeader.setEndCalendarId(rs.getInt("END_CALENDAR_ID"));
				recommendationRunHeader.setStartWeek(rs.getString("START_DATE"));
				recommendationRunHeader.setStartWeekEndDate(rs.getString("END_DATE"));
				recommendationRunHeader.setEndWeek(rs.getString("END_WEEK"));
				recommendationRunHeader.setQuarterEndDate(rs.getString("Q_END_WEEK"));
				recommendationRunHeader.setActualStartCalendarId(rs.getInt("ACTUAL_START_CALENDAR_ID"));
				recommendationRunHeader.setActualEndCalendarId(rs.getInt("ACTUAL_END_CALENDAR_ID"));
			}
		} catch (Exception ex) {
			throw new Exception();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return recommendationRunHeader;
	}

	
	public String updateReRecommendationDetails(Connection conn, List<PRItemDTO> itemListForInsert) throws GeneralException {
		Set<Integer> userOverride = new HashSet<Integer>();
		Set<Integer> sysOverride = new HashSet<Integer>();
		PreparedStatement stmt = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			stmt = conn.prepareStatement(UPDATE_RE_RECOMMENDATION_DETAILS);
			// TODO:: add override pred status
			String explainLogAsJson = "";
			for (PRItemDTO prItemDTO : itemListForInsert) {
				int counter = 0;
				if ((prItemDTO.isSystemOverrideFlag() || prItemDTO.getUserOverrideFlag() == 1) && prItemDTO.getOverrideRemoved() == 0) {
					explainLogAsJson = "";
//					int counter = 0;
					// Update System Override price only if it is different from actual rec reg price
					if (prItemDTO.isSystemOverrideFlag()) {
						if (prItemDTO.getRecRegPriceBeforeReRecommedation().price.equals(prItemDTO.getRecommendedRegPrice().price)
								&& prItemDTO.getRecRegPriceBeforeReRecommedation().multiple.equals(prItemDTO.getRecommendedRegPrice().multiple)) {
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setInt(++counter, 0);
						} else {
							sysOverride.add((Integer) (prItemDTO.getRetLirId() > 0 ? prItemDTO.getRetLirId() : prItemDTO.getItemCode()));
							if(prItemDTO.getRecommendedRegPrice() != null) {
								stmt.setInt(++counter, prItemDTO.getRecommendedRegPrice() != null ? prItemDTO.getRecommendedRegPrice().multiple : 0);
								stmt.setDouble(++counter, prItemDTO.getRecommendedRegPrice() != null ? prItemDTO.getRecommendedRegPrice().price : 0);
								stmt.setDouble(++counter, prItemDTO.getRecRetailSalesDollar());
					        	stmt.setDouble(++counter, prItemDTO.getRecRetailMarginDollar());
				        	} else {
				        		stmt.setNull(++counter, java.sql.Types.INTEGER);
				        		stmt.setNull(++counter, java.sql.Types.DOUBLE);
				        		stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
				        	}
							
							if(prItemDTO.getPredictedMovement() != null)
				        		stmt.setDouble(++counter, prItemDTO.getPredictedMovement());
				        	else
				        		stmt.setNull(++counter, java.sql.Types.DOUBLE);
							stmt.setInt(++counter, prItemDTO.isSystemOverrideFlag() ? 1 : 0);
						}
					} else {
						userOverride.add((Integer) (prItemDTO.getRetLirId() > 0 ? prItemDTO.getRetLirId() : prItemDTO.getItemCode()));
						if(prItemDTO.getRecommendedRegPrice() != null) {
							stmt.setInt(++counter, prItemDTO.getRecommendedRegPrice() != null ? prItemDTO.getRecommendedRegPrice().multiple : 0);
							stmt.setDouble(++counter, prItemDTO.getRecommendedRegPrice() != null ? prItemDTO.getRecommendedRegPrice().price : 0);
							stmt.setDouble(++counter, prItemDTO.getRecRetailSalesDollar());
				        	stmt.setDouble(++counter, prItemDTO.getRecRetailMarginDollar());
			        	} else {
			        		stmt.setNull(++counter, java.sql.Types.INTEGER);
			        		stmt.setNull(++counter, java.sql.Types.DOUBLE);
			        		stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
			        	}
						
						if(prItemDTO.getPredictedMovement() != null)
			        		stmt.setDouble(++counter, prItemDTO.getPredictedMovement());
			        	else
			        		stmt.setNull(++counter, java.sql.Types.DOUBLE);
						
						stmt.setInt(++counter, prItemDTO.isSystemOverrideFlag() ? 1 : 0);
					}
					//18th Jan 2018, no need to update
//					stmt.setInt(++counter, prItemDTO.getIsNewPriceRecommended());
					stmt.setDouble(++counter, prItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() != null
							? prItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() : Types.NULL);
					stmt.setInt(++counter, 0);
					
					if(prItemDTO.getPredictionStatus() != null)
		        		stmt.setInt(++counter, prItemDTO.getPredictionStatus());
		        	else
		        		stmt.setNull(++counter, java.sql.Types.INTEGER);
					
					stmt.setInt(++counter, prItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() != null
							? prItemDTO.getRecWeekSaleInfo().getSalePredStatusAtRecReg().getStatusCode() : 0);
					stmt.setInt(++counter, prItemDTO.getUserOverrideFlag());
					// stmt.setInt(++counter, prItemDTO.getOverrideRemoved());
					if (prItemDTO.isSystemOverrideFlag()) {
						try {
							explainLogAsJson = mapper.writeValueAsString(prItemDTO.getExplainLog());
							stmt.setString(++counter, explainLogAsJson);
						} catch (JsonProcessingException e) {
							explainLogAsJson = "";
							logger.error("Error when converting explain log to json string - " + prItemDTO.getItemCode(), e);
						}
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					
				} else {
//					int counter = 0;
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					// 18th Jan 2018, no need to update
//					stmt.setInt(++counter, prItemDTO.getIsNewPriceRecommended());
					stmt.setDouble(++counter, prItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() != null
							? prItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() : Types.NULL);
					stmt.setInt(++counter, 0);
					if(prItemDTO.getPredictionStatus() != null)
		        		stmt.setInt(++counter, prItemDTO.getPredictionStatus());
		        	else
		        		stmt.setNull(++counter, java.sql.Types.INTEGER);
					stmt.setInt(++counter, prItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() != null
							? prItemDTO.getRecWeekSaleInfo().getSalePredStatusAtRecReg().getStatusCode() : 0);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
				}
				stmt.setLong(++counter, prItemDTO.getRunId());
				stmt.setInt(++counter, prItemDTO.getItemCode());
				stmt.setString(++counter, prItemDTO.isLir() ? String.valueOf(Constants.YES):String.valueOf(Constants.NO));
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetails() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetails() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return "(# of User Override:" + userOverride.size() + " # of System Override:" + sysOverride.size() + ")";
	}

	
	
	private static final String UPDATE_PR_REC_SUMMARY_SPLITUP = "UPDATE PR_QUARTER_REC_SUMMARY_SPLITUP SET " // RUN_ID, CALENDAR_ID
			+ " UNITS_PRED = ? , REV_PRED = ?, MAR_PRED = ?, MAR_PCT_PRED = ?, UNITS_TOTAL_ILP = ?, "
			+ " REV_TOTAL_ILP = ?, MAR_TOTAL_ILP = ?, MAR_PCT_TOTAL_ILP = ?, "
			//+ " CURR_UNITS_ILP = ?, CURR_REVENUE_ILP = ?, CURR_MARGIN_ILP = ?, CURR_MAR_PCT_ILP = ?, "
			+ " PROMO_UNITS = ?, PROMO_REVENUE = ?, PROMO_MARGIN = ?, PROMO_MAR_PCT = ?, "
			// + " CURR_UNITS = ?, CURR_REVENUE = ?, CURR_MARGIN = ?, CURR_MAR_PCT = ?, "
			+ " UNITS_TOTAL = ?, REV_TOTAL = ?, MAR_TOTAL = ?, MAR_PCT_TOTAL = ?, PRC_CHANGE_IMPACT = ?,"
			+ " MARK_UP = ?, MARK_DOWN = ? WHERE RUN_ID = ? AND CALENDAR_ID = ? ";

	/**
	 * 
	 * @param conn
	 * @param mwrSummaryDTO
	 * @throws GeneralException
	 */
	public void updateSummarySplitup(Connection conn, List<SummarySplitupDTO> weeklySummary) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_REC_SUMMARY_SPLITUP);

			for (SummarySplitupDTO summarySplitupDTO : weeklySummary) {
				int counter = 0;
				statement.setDouble(++counter, summarySplitupDTO.getUnitsPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getSalesPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctPredicted());
				statement.setDouble(++counter, summarySplitupDTO.getUnitsTotal());
				statement.setDouble(++counter, summarySplitupDTO.getSalesTotal());
				statement.setDouble(++counter, summarySplitupDTO.getMarginTotal());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctTotal());
				/*
				 * statement.setDouble(++counter, summarySplitupDTO.getCurrentUnitsTotal());
				 * statement.setDouble(++counter, summarySplitupDTO.getCurrentSalesTotal());
				 * statement.setDouble(++counter, summarySplitupDTO.getCurrentMarginTotal());
				 * statement.setDouble(++counter, summarySplitupDTO.getCurrentMarginPctTotal());
				 */
				statement.setDouble(++counter, summarySplitupDTO.getPromoUnits());
				statement.setDouble(++counter, summarySplitupDTO.getPromoSales());
				statement.setDouble(++counter, summarySplitupDTO.getPromoMargin());
				statement.setDouble(++counter, summarySplitupDTO.getPromoMarginPct());
				/*
				 * statement.setDouble(++counter,
				 * summarySplitupDTO.getCurrentUnitsTotalClpDlp());
				 * statement.setDouble(++counter,
				 * summarySplitupDTO.getCurrentSalesTotalClpDlp());
				 * statement.setDouble(++counter,
				 * summarySplitupDTO.getCurrentMarginTotalClpDlp());
				 * statement.setDouble(++counter,
				 * summarySplitupDTO.getCurrentMarginPctTotalClpDlp());
				 */
				statement.setDouble(++counter, summarySplitupDTO.getUnitsTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getSalesTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getMarginTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getMarginPctTotalClpDlp());
				statement.setDouble(++counter, summarySplitupDTO.getPriceChangeImpact());
				statement.setDouble(++counter,  summarySplitupDTO.getMarkUp());
				statement.setDouble(++counter, summarySplitupDTO.getMarkDown());
				statement.setLong(++counter, summarySplitupDTO.getRunId());
				statement.setInt(++counter, summarySplitupDTO.getCalendarId());
				statement.addBatch();
			}
			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("updateSummarySplitup () - Error while Updating  PR_WEEK_REC_SUMMARY" + sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param mwrSummaryDTO
	 * @throws GeneralException
	 */
	public void updateMultiWeekSummary(Connection conn, MWRSummaryDTO mwrSummaryDTO) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_SUMMARY);
			int counter = 0;
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctInBetWeeks());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctPredicted());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginTotal());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctTotal());
			/*
			 * statement.setDouble(++counter, mwrSummaryDTO.getCurrentUnitsTotal());
			 * statement.setDouble(++counter, mwrSummaryDTO.getCurrentSalesTotal());
			 * statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginTotal());
			 * statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginPctTotal());
			 */
			statement.setDouble(++counter, mwrSummaryDTO.getPromoUnits());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoSales());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoMargin());
			statement.setDouble(++counter, mwrSummaryDTO.getPromoMarginPct());
			statement.setInt(++counter, mwrSummaryDTO.getTotalItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalDistinctItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalPromoItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalDistinctPromoItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalEDLPItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalDistinctEDLPItems());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndex());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndexPromo());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndexEDLPRec());
			statement.setDouble(++counter, mwrSummaryDTO.getUnitsTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getSalesTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getMarginPctTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentUnitsTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentSalesTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getCurrentMarginPctTotalClpDlp());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceChangeImpact());
			statement.setDouble(++counter, mwrSummaryDTO.getPriceIndexEDLPCurr());
			statement.setDouble(++counter, mwrSummaryDTO.getMarkUP());
			statement.setDouble(++counter, mwrSummaryDTO.getMarkDown());
			statement.setInt(++counter, mwrSummaryDTO.getTotalRecommendedItems());
			statement.setInt(++counter, mwrSummaryDTO.getTotalRecommendedDistinctItems());
			statement.setLong(++counter, mwrSummaryDTO.getRunId());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updateMultiWeekSummary() - Error while updating  PR_QUARTER_REC_SUMMARY", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param items
	 * @throws GeneralException
	 */
	public void insertSecondaryZoneRecs(Connection conn, RecommendationInputDTO recommendationInputDTO,
			List<MWRItemDTO> items) throws GeneralException {
		PreparedStatement statement = null;
		try {
			int itemsInBatch = 0;
			statement = conn.prepareStatement(INSERT_PR_CHILD_LOCATION_REC);
			for (MWRItemDTO mwrItemDTO : items) {
				if (mwrItemDTO.isSecondaryZoneRecPresent()) {
					logger.debug("Item with secondary zone recommendation: " + mwrItemDTO.getProductId());
					for (SecondaryZoneRecDTO secondaryZoneRecDTO : mwrItemDTO.getSecondaryZones()) {
						int colCount = 0;
						statement.setLong(++colCount, recommendationInputDTO.getRunId());
						PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getProductLevelId(), statement);
						PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getProductId(), statement);
						PristineDBUtil.setInt(++colCount, Constants.ZONE_LEVEL_ID, statement);
						PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getPriceZoneId(), statement);
						PristineDBUtil.setMultiplePrice(++colCount, secondaryZoneRecDTO.getCurrentRegPrice(), statement);
						PristineDBUtil.setMultiplePriceQty(++colCount, secondaryZoneRecDTO.getCurrentRegPrice(), statement);
						PristineDBUtil.setMultiplePrice(++colCount, secondaryZoneRecDTO.getRecommendedRegPrice(), statement);
						PristineDBUtil.setMultiplePriceQty(++colCount, secondaryZoneRecDTO.getRecommendedRegPrice(), statement);
						PristineDBUtil.setDouble(++colCount, secondaryZoneRecDTO.getListCost(), statement);
						statement.addBatch();
						itemsInBatch++;
						if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
							statement.executeBatch();
							statement.clearBatch();
							itemsInBatch = 0;
						}
					}
				}
			}
			
			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("insertSecondaryZoneRecs() - Error while inserting PR_CHILD_LOCATION_REC",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param items
	 * @throws GeneralException
	 */
	public void updateSecondaryZoneRecs(Connection conn, RecommendationInputDTO recommendationInputDTO,
			List<MWRItemDTO> items) throws GeneralException {
		PreparedStatement statement = null;
		try {
			int itemsInBatch = 0;
			statement = conn.prepareStatement(UPDATE_PR_CHILD_LOCATION_REC);
			for (MWRItemDTO mwrItemDTO : items) {
				if (mwrItemDTO.isSecondaryZoneRecPresent()) {
					for (SecondaryZoneRecDTO secondaryZoneRecDTO : mwrItemDTO.getSecondaryZones()) {
						if (secondaryZoneRecDTO.getOverrideRegPrice() != null && !secondaryZoneRecDTO
								.getOverrideRegPrice().equals(secondaryZoneRecDTO.getRecommendedRegPrice())) {
							int colCount = 0;
							PristineDBUtil.setMultiplePrice(++colCount, secondaryZoneRecDTO.getOverrideRegPrice(),
									statement);
							PristineDBUtil.setMultiplePriceQty(++colCount, secondaryZoneRecDTO.getOverrideRegPrice(),
									statement);
							PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getProductLevelId(), statement);
							PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getProductId(), statement);
							PristineDBUtil.setInt(++colCount, Constants.ZONE_LEVEL_ID, statement);
							PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getPriceZoneId(), statement);
							statement.setLong(++colCount, recommendationInputDTO.getRunId());
							statement.addBatch();
							itemsInBatch++;
							if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
								statement.executeBatch();
								statement.clearBatch();
								itemsInBatch = 0;
							}
						} else if(secondaryZoneRecDTO.getOverrideRegPrice() != null && secondaryZoneRecDTO
								.getOverrideRegPrice().equals(secondaryZoneRecDTO.getRecommendedRegPrice())) {
							int colCount = 0;
							PristineDBUtil.setDouble(++colCount, null, statement);
							PristineDBUtil.setInt(++colCount, null, statement);
							PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getProductLevelId(), statement);
							PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getProductId(), statement);
							PristineDBUtil.setInt(++colCount, Constants.ZONE_LEVEL_ID, statement);
							PristineDBUtil.setInt(++colCount, secondaryZoneRecDTO.getPriceZoneId(), statement);
							statement.setLong(++colCount, recommendationInputDTO.getRunId());
							statement.addBatch();
							itemsInBatch++;
							if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
								statement.executeBatch();
								statement.clearBatch();
								itemsInBatch = 0;
							}
						}
					}
				}
			}
			
			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("insertSecondaryZoneRecs() - Error while inserting PR_QUARTER_REC_SUMMARY",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * 
	 * @param conn
	 * @param runId
	 * @return secondary zone recs
	 * @throws Exception
	 */
	public List<SecondaryZoneRecDTO> getSecondaryZoneRecs(Connection conn, long runId)
			throws Exception {
		List<SecondaryZoneRecDTO> secondaryZoneRecs = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String query = new String(GET_PR_CHILD_LOCATION_REC);
			stmt = conn.prepareStatement(query);
			stmt.setLong(1, runId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				SecondaryZoneRecDTO secondaryZoneRecDTO = new SecondaryZoneRecDTO();
				secondaryZoneRecDTO.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				secondaryZoneRecDTO.setProductId(rs.getInt("PRODUCT_ID"));
				secondaryZoneRecDTO.setPriceZoneId(rs.getInt("CHILD_LOCATION_ID"));
				secondaryZoneRecDTO.setCurrentRegPrice(new MultiplePrice(rs.getInt("REG_MULTIPLE"), rs.getDouble("REG_PRICE")));
				secondaryZoneRecDTO.setRecommendedRegPrice(new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"), rs.getDouble("REC_REG_PRICE")));
				secondaryZoneRecDTO.setListCost(rs.getDouble("LIST_COST"));
				secondaryZoneRecDTO.setPriceZoneNo(rs.getString("ZONE_NUM"));
				secondaryZoneRecs.add(secondaryZoneRecDTO);
			}
		} catch (Exception ex) {
			throw new Exception();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return secondaryZoneRecs;
	}

	
	
}
