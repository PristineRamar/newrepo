package com.pristine.dao.offermgmt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.DashboardDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.audittool.AuditDashboardDTO;
import com.pristine.dto.offermgmt.audittool.AuditParameterDTO;
import com.pristine.dto.offermgmt.audittool.AuditParameterHeaderDTO;
import com.pristine.dto.offermgmt.audittool.AuditReportDTO;
import com.pristine.dto.offermgmt.audittool.AuditReportHeaderDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;


public class AuditEngineDAO {

	private static Logger logger = Logger.getLogger("AuditEngineDAO");
	private static final String GET_RUN_ID_FOR_PRODUCT_AND_LOCATION = "SELECT DASHBOARD_ID "
																	+ " FROM PR_QUARTER_DASHBOARD " +
																	 "WHERE LOCATION_ID = ? AND LOCATION_LEVEL_ID = ? AND " +
																	 "PRODUCT_ID = ? AND PRODUCT_LEVEL_ID = ?";
	
	
	
	private static final String INSERT_AUDIT_REPORT_HEADER = "INSERT INTO PR_AUDIT_REPORT_HEADER (AUDIT_REPORT_ID, "
															 + "AUDIT_PARAMETER_HEADER_ID, "
															 + "RUN_TYPE, "
															 + "START_RUN_TIME, "
															 + "RUN_ID, "
															 + "AUDIT_BY, "
															 + "AUDITED, AP_VER_ID) "
															 + "VALUES (?, ?, ?, SYSDATE, ?, ?, SYSDATE, ?)";
	
	private static final String GET_AUDIT_REPORT_ID = "SELECT AUDIT_REPORT_ID_SEQ.NEXTVAL AS REPORT_ID FROM DUAL";
	
	private static final String GET_AUDIT_PARAM_ID = "SELECT AUDIT_PARAMETER_HEADER_ID FROM PR_AUDIT_DASHBOARD "
													 + "WHERE LOCATION_ID = ? AND LOCATION_LEVEL_ID = ? AND "
													 + "PRODUCT_ID = ? AND PRODUCT_LEVEL_ID = ?";

	private static final String GET_DEFAULT_AUDIT_PARAM_ID = "SELECT AUDIT_PARAMETER_HEADER_ID FROM PR_AUDIT_PARAMETER_HEADER "
			 + "WHERE DEFAULT_PARAM = 'Y'";
	
	private static final String INSERT_AUDIT_DASHBOARD = "INSERT INTO PR_AUDIT_DASHBOARD (AUDIT_DASHBOARD_ID, "
														 + "LOCATION_LEVEL_ID, "
														 + "LOCATION_ID, "
														 + "PRODUCT_LEVEL_ID, "
														 + "PRODUCT_ID, "
														 + "AUDIT_PARAMETER_HEADER_ID, "
														 + "AUDIT_REPORT_ID) VALUES (AUDIT_DASHBOARD_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?) ";
	
	private static final String GET_AUDIT_DETAIL_FROM_RECOMMENDATION = "SELECT prr.run_id,il.item_code,il.RETAILER_ITEM_CODE,"
			+ "NVL(prr.OPPORTUNITY_PRICE, 0) opportunity_price,NVL(prr.OPPORTUNITY_QUANTITY, 0) opportunity_qty,"
			+ "NVL(prr.REG_PRICE, 0) curr_reg_price,NVL(prr.REG_MULTIPLE, 0) curr_reg_mul,"
			+ "prr.REC_REG_PRICE,prr.REC_REG_MULTIPLE,NVL(prr.OVERRIDE_REG_PRICE, 0) over_price,"
			+ "NVL(prr.OVERRIDE_REG_MULTIPLE, 0) over_multiple,prr.IS_MARKED_FOR_REVIEW AS marked,"
			+ "NVL(prr.LIST_COST, 0) list_cost,prr.REGULAR_UNITS,prr.CURR_UNITS,prr.ITEM_SIZE,	"
			+ " prr.COMP_1_STR_ID,prr.COMP_1_RETAIL_MUL,prr.COMP_1_RETAIL,prr.COMP_2_STR_ID,"
			+ "prr.COMP_2_RETAIL_MUL,prr.COMP_2_RETAIL,prr.COMP_3_STR_ID,prr.COMP_3_RETAIL_MUL,"
			+ "prr.COMP_3_RETAIL,prr.COMP_4_STR_ID,prr.COMP_4_RETAIL_MUL,prr.COMP_4_RETAIL,"
			+ "prr.COMP_5_STR_ID,prr.COMP_5_RETAIL_MUL,prr.COMP_5_RETAIL,"
			+ "prr.WEIGHT_CURR_RETAIL,prr.WEIGHT_NEW_RETAIL,prr.WEIGHT_COMP_1_RET,prr.WEIGHT_COMP_2_RET,prr.WEIGHT_COMP_3_RET,prr.WEIGHT_COMP_4_RET,prr.WEIGHT_COMP_5_RET, "
			+ "prr.WEIGHT_PRIM_COMP_RET,ul.name uom_name,prr.COMP_REG_MULTIPLE,prr.COMP_REG_PRICE,il.RET_LIR_ID,il.brand_id,	CASE "
			+ "WHEN prr.PRODUCT_LEVEL_ID= " + Constants.PRODUCT_LEVEL_ID_LIG + "THEN 'Y' ELSE 'N'	END AS LIG, "
			+ "prr.NEWPRICE_RECOMMENDED,prr.OVERRIDE_PRICE_PREDICTED_MOV, "
			+ "prr.PREDICTION_STATUS,prr.PRICE_CHECK_LIST_ID,pc.price_check_list_type_id, "
			+ "CASE	 WHEN prr.CONFLICT = 1 THEN (CASE WHEN LENGTH(prr.EXPLAIN_LOG) > 3999 "
			+ "THEN TO_CHAR(dbms_lob.substr(prr.EXPLAIN_LOG, 35000, 1))	"
			+ "ELSE TO_CHAR(dbms_lob.substr(prr.EXPLAIN_LOG, 8000, 1))	END ) ELSE NULL "
			+ "END AS explain_log ,prr.MOV_52_WEEK,prr.COMP_STR_ID , prr.OVERRIDE_REG_PRICE  "
			+ "FROM PR_QUARTER_REC_ITEM prr LEFT JOIN uom_lookup ul "
			+ "ON ul.id = prr.UOM_ID JOIN item_lookup il ON prr.PRODUCT_ID = il.item_code "
			+ "LEFT JOIN PRICE_CHECK_LIST pc ON pc.ID =prr.PRICE_CHECK_LIST_ID  "
			+ " WHERE prr.run_id = ? AND prr.CAL_TYPE =" + "'" + Constants.CALENDAR_QUARTER + "' AND prr.is_pending_retail_recommended=0";

	private static final String INSERT_AUIDT_REPORT = "INSERT INTO PR_AUDIT_REPORT (AUDIT_REPORT_DETAIL_ID, "
			+ " AUDIT_REPORT_ID, TOT_REVENUE, MARGIN_RATE, MARGIN_D, PRICE_INDEX, MOVEMENT_QTY, MOVEMENT_VOL, "
			+ " RETAIL_CHANGED, RETAIL_INCREASED, RETAIL_DECREASED, RETAIL_OVERRIDDEN, RETAIL_MARKED, "
			+ " RETAIL_APPROVED, LOWER_SIZE_HIGHER_PRICE, LOWER_HIGHER_PRICE_VARIATION, SIMILAR_SIZE_PRICE_VARIATION, "
			+ " RETAIL_BELOW_LIST_COST, RETAIL_BELOW_LIST_COST_NO_VIP, RETAIL_BELOW_VIP_COST, MARGIN_LT_PCT, "
			+ " MARGIN_GT_PCT, RETAIL_LT_PRIM_COMP_PCT, RETAIL_GT_PRIM_COMP_PCT, MARGIN_LT_ALL_COMP, MARGIN_GT_ALL_COMP, "
			+ " RETAIL_CHANGED_LT_PCT, RETAIL_CHANGED_GT_PCT, LIST_COST_CHANGED_GT_PCT, COMP_PRICE_PRIM_CHANGED_GT_PCT, "
			+ " RETAIL_CHANGED_NO_COST_OR_COMP, RETAIL_CHANGED_ROUNDING_VIOL, RETAIL_LIR_LINE_PRICE_VIOL, RETAIL_CHANGED_N_TIMES, "
			+ " RETAIL_NOT_CHANGED_KVI_SK, RED_RETAIL_INC_MARGIN_OPER, KVI_COMP_PRICE_OLD, KVI_WITH_COMP_PRICE_COUNT, "
			+ " KVI_WITH_COMP_PRICE_PCT, TOTAL_ITEMS, TOTAL_LIGS, PRO_MARGIN_RATE, PRO_MARGIN_D,  PRO_PRICE_INDEX, "
			+ " PRO_MOVEMENT_QTY, PRO_MOVEMENT_VOL, PRO_TOT_REVENUE, OUT_OF_NORM, OUT_OF_NORM_LIG, RETAIL_CHANGED_U,	"
			+ " RETAIL_INCREASED_U,	RETAIL_DECREASED_U,	RETAIL_OVERRIDDEN_U,	RETAIL_MARKED_U,	RETAIL_APPROVED_U,	"
			+ " LOWER_SIZE_HIGHER_PRICE_U,	LOWER_HIGH_PRICE_VARY_U,	SIMILAR_SIZE_PRICE_VARY_U,	RETAIL_BELOW_LIST_COST_U,	"
			+ " RETAIL_BELOW_LC_NO_VIP_U,	RETAIL_BELOW_VIP_COST_U,	MARGIN_LT_PCT_U,	MARGIN_GT_PCT_U,	"
			+ " RETAIL_LT_PRIM_COMP_PCT_U,	RETAIL_GT_PRIM_COMP_PCT_U,	MARGIN_LT_ALL_COMP_U,	MARGIN_GT_ALL_COMP_U,	"
			+ " RETAIL_CHANGED_LT_PCT_U,	RETAIL_CHANGED_GT_PCT_U,	LIST_COST_CHANGED_GT_PCT_U,	COMP_PRIMARY_CHANGED_GT_PCT_U,	"
			+ " RETAIL_CHG_NO_COST_OR_COMP_U,	RETAIL_CHG_ROUNDING_VIOL_U,	RETAIL_LIR_LINE_PRICE_VIOL_U,	"
			+ " RETAIL_CHANGED_N_TIMES_U,	RETAIL_NOT_CHANGED_KVI_SK_U,	RED_RETAIL_INC_MARGIN_OPER_U,	"
			+ " KVI_COMP_PRICE_OLD_U,	KVI_WITH_COMP_PRICE_COUNT_U,	KVI_WITH_COMP_PRICE_PCT_U, ZERO_CURR_RETAILS, "
			+ " ZERO_CURR_RETAILS_U, ZERO_COST_RETAILS, ZERO_COST_RETAILS_U, RETAIL_PRIM_COMP_VIOLATION, RETAIL_PRIM_COMP_VIOLATION_U,"
			+ " MARGIN_VIOLATION, MARGIN_VIOLATION_U, RETAIL_CHANGE_VIOLATION, RETAIL_CHANGE_VIOLATION_U,"
			+ " BRAND_VIOLATION, BRAND_VIOLATION_U, SIZE_VIOLATION, SIZE_VIOLATION_U, "
			+ " COMP_1_AUR, COMP_2_AUR, COMP_3_AUR, COMP_4_AUR,COMP_5_AUR,COMP_1_STR,COMP_2_STR,COMP_3_STR,COMP_4_STR,COMP_5_STR,BASE_REG_RET_AUR,BASE_REC_RET_AUR,"
			+ " UNITS_ACTUAL_ILP,REV_ACTUAL_ILP,MAR_ACTUAL_ILP,MAR_PCT_ACTUAL_ILP,CURR_UNITS_ILP,CURR_REVENUE_ILP,"
			+ " CURR_MARGIN_ILP,CURR_MAR_PCT_ILP,"
			+ " REC_RET_ABOVE_COMP1,REC_RET_BELOW_COMP1,REC_RET_EQUALS_COMP1,REC_RET_ABOVE_COMP2,REC_RET_BELOW_COMP2,REC_RET_EQUALS_COMP2,REC_RET_ABOVE_COMP3,"
			+ " REC_RET_BELOW_COMP3,REC_RET_EQUALS_COMP3,REC_RET_ABOVE_COMP4,REC_RET_BELOW_COMP4,REC_RET_EQUALS_COMP4,REC_RET_ABOVE_COMP5,REC_RET_BELOW_COMP5,"
			+ " REC_RET_EQUALS_COMP5,PRIM_COMP_STR,PRIM_COMP_AUR,REC_RET_ABOVE_PRIM_COMP,REC_RET_BELOW_PRIM_COMP,REC_RET_EQUALS_PRIM_COMP, MARK_UP, MARK_DOWN, "
			+ " BASE_VS_PRIM_COMP_AUR,BASE_VS_COMP_1_AUR,BASE_VS_COMP_2_AUR,BASE_VS_COMP_3_AUR,BASE_VS_COMP_4_AUR,BASE_VS_COMP_5_AUR, "
			+ " BASE_CUR_VS_PRIM_COMP_AUR,BASE_CUR_VS_COMP_1_AUR,BASE_CUR_VS_COMP_2_AUR,BASE_CUR_VS_COMP_3_AUR,BASE_CUR_VS_COMP_4_AUR,BASE_CUR_VS_COMP_5_AUR,"
			+ " CUR_RET_ABOVE_COMP1,CUR_RET_BELOW_COMP1,CUR_RET_EQUALS_COMP1,CUR_RET_ABOVE_COMP2,CUR_RET_BELOW_COMP2,"
			+ " CUR_RET_EQUALS_COMP2,CUR_RET_ABOVE_COMP3,CUR_RET_BELOW_COMP3,CUR_RET_EQUALS_COMP3,CUR_RET_ABOVE_COMP4,CUR_RET_BELOW_COMP4,"
			+ " CUR_RET_EQUALS_COMP4,CUR_RET_ABOVE_COMP5,CUR_RET_BELOW_COMP5,CUR_RET_EQUALS_COMP5,CUR_RET_ABOVE_PRIM_COMP,CUR_RET_BELOW_PRIM_COMP,CUR_RET_EQUALS_PRIM_COMP)"
			+ " VALUES (AUDIT_REPORT_DETAIL_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
	
	private static final String UPDATE_AUDIT_HEADER = "UPDATE PR_AUDIT_REPORT_HEADER SET MESSAGE = ?, "
													+ "PERCENT_COMPLETION = ?, "
													+ "END_RUN_TIME = SYSDATE, "
													+ "AUDITED = SYSDATE "
													+ "WHERE AUDIT_REPORT_ID = ?";
	
	private static final String GET_AUDIT_PARAMETERS = "SELECT AUDIT_PARAMETERS_ID, "
													 + "AUDIT_PARAMETER_HEADER_ID, "
													 + "AUDIT_PARAMETERS_TYPE, "
													 + "VALUE_TYPE, "
													 + "PARAMETER_VALUE, AP_VER_ID FROM PR_AUDIT_PARAMETERS WHERE AUDIT_PARAMETER_HEADER_ID = ? AND AP_VER_ID = ?";

	
	private static final String GET_AUDIT_PARAMETER_HEADER = "SELECT AUDIT_PARAMETER_HEADER_ID, AP_VER_ID FROM PR_AUDIT_PARAMETER_HEADER WHERE AUDIT_PARAMETER_HEADER_ID = ?";

	
	private static final String GET_AUDIT_DASHBOARD = "SELECT AUDIT_DASHBOARD_ID FROM PR_AUDIT_DASHBOARD "
													 + "WHERE LOCATION_ID = ? AND LOCATION_LEVEL_ID = ? AND "
													 + "PRODUCT_ID = ? AND PRODUCT_LEVEL_ID = ?";
	
	private static final String UPDATE_AUDIT_DASHBOARD = "UPDATE PR_AUDIT_DASHBOARD SET AUDIT_PARAMETER_HEADER_ID = ?, AUDIT_REPORT_ID = ? WHERE AUDIT_DASHBOARD_ID = ?";
	
	
	private static final String GET_AUDIT_PARAM_HEADER_FOR_REPORT_ID = "SELECT AUDIT_PARAMETER_HEADER_ID, AP_VER_ID FROM "
								+ " PR_AUDIT_REPORT_HEADER WHERE AUDIT_REPORT_ID = ?";
	
	private static final String GET_LOCATION_FOR_AUDIT_REPORT = "SELECT LOCATION_ID, LOCATION_LEVEL_ID, PRODUCT_ID, PRODUCT_LEVEL_ID FROM "
			+ " PR_AUDIT_DASHBOARD WHERE AUDIT_REPORT_ID = ?";
	
	
	private static final String GET_RETAIL_CHANGED_X_TIMES_IN_LAST_X_MONTHS = " SELECT ITEM_CODE, CHANGE_COUNT FROM (SELECT ITEM_CODE, "
			+ " CURR_REG_PRICE,  CURR_REG_M_PRICE, CURR_REG_QTY, PREV_REG_PRICE, PREV_REG_M_PRICE, PREV_REG_M_QTY, "
			+ " SUM(CHANGE_IND) CHANGE_COUNT FROM (SELECT ITEM_CODE, 1 AS CHANGE_IND, CURR_REG_PRICE, CURR_REG_M_PRICE, CURR_REG_QTY, "
			+ " PREV_REG_PRICE, PREV_REG_M_PRICE, PREV_REG_M_QTY, "
			+ " CURR_CAL_ID, PREV_CAL_ID FROM (SELECT RPI_1.ITEM_CODE ITEM_CODE, RPI_1.REG_PRICE CURR_REG_PRICE, "
			+ " RPI_1.REG_M_PRICE CURR_REG_M_PRICE, RPI_1.REG_QTY CURR_REG_QTY, RPI_2.REG_PRICE PREV_REG_PRICE, "
			+ " RPI_2.REG_M_PRICE PREV_REG_M_PRICE, RPI_2.REG_QTY PREV_REG_M_QTY, "
			+ " RPI_1.LEVEL_ID, RPI_1.LEVEL_TYPE_ID, RPI_2.LEVEL_ID PREV_LEVEL_ID, RPI_2.LEVEL_TYPE_ID PREV_LEVEL_TYPE_ID, "
			//+ " RPI_1.CALENDAR_ID CURR_CAL_ID, RPI_2.CALENDAR_ID PREV_CAL_ID FROM SYNONYM_RETAIL_PRICE_INFO RPI_1 LEFT JOIN "
			+ " RPI_1.CALENDAR_ID CURR_CAL_ID, RPI_2.CALENDAR_ID PREV_CAL_ID FROM SYN_RETAIL_PRICE_INFO RPI_1 LEFT JOIN "
			+ " (SELECT RC_1.CALENDAR_ID CAL_ID, RC_2.CALENDAR_ID LAST_CAL_ID FROM RETAIL_CALENDAR RC_1 JOIN RETAIL_CALENDAR RC_2 "
			+ " ON (RC_1.START_DATE - 7 = RC_2.START_DATE AND RC_1.ROW_TYPE = RC_2.ROW_TYPE) WHERE RC_1.ROW_TYPE     = 'W' "
			+ " AND RC_1.START_DATE >= TO_DATE(?, '"+ Constants.DB_DATE_FORMAT + "') AND RC_1.START_DATE <= TO_DATE(?, '" 
			+ Constants.DB_DATE_FORMAT + "') ORDER BY RC_1.START_DATE DESC) "
			//+ " WEEK_CALENDAR ON WEEK_CALENDAR.CAL_ID = RPI_1.CALENDAR_ID JOIN SYNONYM_RETAIL_PRICE_INFO RPI_2 "
			+ " WEEK_CALENDAR ON WEEK_CALENDAR.CAL_ID = RPI_1.CALENDAR_ID JOIN SYN_RETAIL_PRICE_INFO RPI_2 "
			+ " ON (RPI_1.ITEM_CODE = RPI_2.ITEM_CODE AND WEEK_CALENDAR.LAST_CAL_ID = RPI_2.CALENDAR_ID) "
			+ " WHERE RPI_1.ITEM_CODE IN (SELECT PRODUCT_ID FROM PR_QUARTER_REC_ITEM WHERE RUN_ID = ? AND  PRODUCT_ID <> RET_LIR_ID AND CAL_TYPE='Q' ) "
			+ " AND ((RPI_1.LEVEL_TYPE_ID = 1 AND RPI_1.LEVEL_ID = (SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?)) "
			+ " OR (RPI_1.LEVEL_TYPE_ID = 0)) AND ((RPI_2.LEVEL_TYPE_ID = 1 AND RPI_2.LEVEL_ID = "
			+ " (SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?)) OR (RPI_2.LEVEL_TYPE_ID = 0)))"
			+ "  WHERE (CURR_REG_PRICE <> PREV_REG_PRICE OR "
			+ " (CURR_REG_M_PRICE <> PREV_REG_M_PRICE AND CURR_REG_QTY <> CURR_REG_QTY))) "
			+ " GROUP BY ITEM_CODE, CURR_REG_PRICE, CURR_REG_M_PRICE, CURR_REG_QTY, PREV_REG_PRICE, PREV_REG_M_PRICE, PREV_REG_M_QTY) "
			+ " WHERE CHANGE_COUNT > ? ";
	
	private static final String GET_COST_CHANGED_GT_X_PCT_IN_LAST_X_MONTHS = "SELECT ITEM_CODE, CHANGE_COUNT FROM (SELECT ITEM_CODE, CURR_COST, PREV_COST, "
			+ " CHANGE_PCT, SUM(CHANGE_IND) CHANGE_COUNT FROM (SELECT ITEM_CODE, CASE WHEN PREV_COST > 0 "
			+ " THEN (CASE WHEN ROUND(ABS(((PREV_COST - CURR_COST) / PREV_COST) * 100)) > ? THEN 1 ELSE 0 END) "
			+ " ELSE 0 END CHANGE_IND, CURR_COST, PREV_COST, CURR_CAL_ID, PREV_CAL_ID, CASE WHEN PREV_COST > 0 THEN "
			+ " ROUND(ABS(((PREV_COST - CURR_COST) / PREV_COST) * 100)) ELSE 0 END CHANGE_PCT "
			+ " FROM (SELECT RCI_1.ITEM_CODE ITEM_CODE, RCI_1.LIST_COST CURR_COST, RCI_2.LIST_COST PREV_COST, RCI_1.LEVEL_ID, "
			+ " RCI_1.LEVEL_TYPE_ID, RCI_2.LEVEL_ID PREV_LEVEL_ID, RCI_2.LEVEL_TYPE_ID PREV_LEVEL_TYPE_ID, RCI_1.CALENDAR_ID CURR_CAL_ID, "
			//+ " RCI_2.CALENDAR_ID PREV_CAL_ID FROM SYNONYM_RETAIL_COST_INFO RCI_1 LEFT JOIN (SELECT RC_1.CALENDAR_ID CAL_ID, RC_2.CALENDAR_ID "
			+ " RCI_2.CALENDAR_ID PREV_CAL_ID FROM SYN_RETAIL_COST_INFO RCI_1 LEFT JOIN (SELECT RC_1.CALENDAR_ID CAL_ID, RC_2.CALENDAR_ID "
			+ " LAST_CAL_ID FROM RETAIL_CALENDAR RC_1 JOIN RETAIL_CALENDAR RC_2 ON (RC_1.START_DATE - 7 = RC_2.START_DATE AND "
			+ " RC_1.ROW_TYPE = RC_2.ROW_TYPE) WHERE RC_1.ROW_TYPE     = 'W' AND RC_1.START_DATE    >= TO_DATE(?, '"+ Constants.DB_DATE_FORMAT + "') "
			+ " AND RC_1.START_DATE    <= TO_DATE(?, '"+ Constants.DB_DATE_FORMAT + "') ORDER BY RC_1.START_DATE DESC) WEEK_CALENDAR ON WEEK_CALENDAR.CAL_ID = RCI_1.CALENDAR_ID "
			//+ " JOIN SYNONYM_RETAIL_COST_INFO RCI_2 ON (RCI_1.ITEM_CODE = RCI_2.ITEM_CODE AND WEEK_CALENDAR.LAST_CAL_ID = RCI_2.CALENDAR_ID) "
			+ " JOIN SYN_RETAIL_COST_INFO RCI_2 ON (RCI_1.ITEM_CODE = RCI_2.ITEM_CODE AND WEEK_CALENDAR.LAST_CAL_ID = RCI_2.CALENDAR_ID) "
			+ " WHERE RCI_1.ITEM_CODE IN (SELECT PRODUCT_ID FROM PR_QUARTER_REC_ITEM WHERE RUN_ID = ? AND  PRODUCT_ID <> RET_LIR_ID AND  CAL_TYPE='Q')) "
			+ " WHERE ((LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?)) "
			+ " OR (LEVEL_TYPE_ID = 0)) AND ((PREV_LEVEL_TYPE_ID = 1 AND PREV_LEVEL_ID IN (SELECT ZONE_NUM FROM "
			+ " RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ? )) OR (PREV_LEVEL_TYPE_ID = 0)) AND CURR_COST <> PREV_COST) "
			+ " GROUP BY ITEM_CODE, CURR_COST, PREV_COST, CHANGE_PCT) WHERE CHANGE_COUNT > 1" ;
	
	
	
	public static final String GET_CALENDAR_FROM_REC_RUN_HEADER = "SELECT RC.START_DATE FROM pr_quarter_rec_header PRH "
			+ " JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PRH.START_CALENDAR_ID WHERE RUN_ID = ?";
	
	
	public static final String GET_QUARTERLY_SUMMARY = "SELECT RUN_ID,UNITS_ACTUAL,REV_ACTUAL,MAR_ACTUAL,MAR_PCT_ACTUAL,INBETWEEN_WEEKS_UNITS_PRED,"
			+ "INBETWEEN_WEEKS_REV_PRED,INBETWEEN_WEEKS_MAR_PRED,INBETWEEN_WEEKS_MAR_PCT_PRED, "
			+ "REC_WEEKS_UNITS_PRED,REC_WEEKS_REV_PRED,REC_WEEKS_MAR_PRED,REC_WEEKS_MAR_PCT_PRED, "
			+ "UNITS_TOTAL,REV_TOTAL,MAR_TOTAL,MAR_PCT_TOTAL,CURR_PRICE_INDEX,PRICE_INDEX_PROMO, "
			+ "TOTAL_ITEMS,TOTAL_ITEMS_LIG,CURR_UNITS,CURR_REVENUE,CURR_MARGIN,CURR_MAR_PCT,PROMO_UNITS,"
			+ "PROMO_REVENUE,PROMO_MARGIN,PROMO_MAR_PCT,TOTAL_PROMO_ITEMS,TOTAL_PROMO_ITEMS_LIG,"
			+ "TOTAL_EDLP_ITEMS,TOTAL_EDLP_ITEMS_LIG,PRICE_BASE_INDEX,INBETWEEN_WEEKS_UNITS_PRED_ILP,"
			+ "INBETWEEN_WEEKS_REV_PRED_ILP,INBETWEEN_WEEKS_MAR_PRED_ILP,REC_WEEKS_UNITS_PRED_ILP,REC_WEEKS_REV_PRED_ILP,"
			+ "REC_WEEKS_MAR_PRED_ILP,REC_WEEKS_MAR_PCT_PRED_ILP,UNITS_TOTAL_ILP,REV_TOTAL_ILP,MAR_TOTAL_ILP,MAR_PCT_TOTAL_ILP,CURR_UNITS_ILP,"
			+ "CURR_REVENUE_ILP,CURR_MARGIN_ILP,CURR_MAR_PCT_ILP,PROMO_UNITS_ILP,PROMO_REVENUE_ILP,PROMO_MARGIN_ILP,PROMO_MAR_PCT_ILP,"
			+ "INBET_WEEKS_MAR_PCT_PRED_ILP,TOTAL_REC_ITEMS,TOTAL_REC_ITEMS_LIG,TOTAL_COST_CHANGES,TOTAL_COST_CHANGES_LIG,"
			+ "TOTAL_COMP_CHANGES,TOTAL_COMP_CHANGES_LIG,PRC_CHANGE_IMPACT,PRICE_INDEX "
			+ "FROM  PR_QUARTER_REC_SUMMARY WHERE RUN_ID=?";
	
	
	
	public DashboardDTO getPRDashboard(Connection conn, AuditDashboardDTO auditDashboardDTO){
		DashboardDTO dashboardDTO = new DashboardDTO();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		logger.info("getPRDashboard()-" + auditDashboardDTO.getLocationId() + auditDashboardDTO.getLocationLevelId()
				+ auditDashboardDTO.getProductId() + auditDashboardDTO.getProductLevelId());
		try{
			statement = conn.prepareStatement(GET_RUN_ID_FOR_PRODUCT_AND_LOCATION);
			int counter = 0;
			statement.setInt(++counter, auditDashboardDTO.getLocationId());
			statement.setInt(++counter, auditDashboardDTO.getLocationLevelId());
			statement.setInt(++counter, auditDashboardDTO.getProductId());
			statement.setInt(++counter, auditDashboardDTO.getProductLevelId());
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				
				dashboardDTO.setRecommendationRunId(resultSet.getLong("DASHBOARD_ID"));
				/*
				 * dashboardDTO.setStrategyId(resultSet.getInt("STRATEGY_ID"));
				 * dashboardDTO.setTotRevenue(resultSet.getDouble("TOT_REVENUE"));
				 */
			}
		}
		catch(SQLException sqlE){
			logger.error("getRunIdForProductAndLocation() - Error while getting run id - " + sqlE.toString()); 
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return dashboardDTO;
	}
	
	
	public long insertAuditReportHeader(Connection conn, AuditReportHeaderDTO auditReportHeaderDTO) throws GeneralException{
		long reportId = -1;
		PreparedStatement statement = null;
		try{
			reportId = getAuditReportId(conn);
		if(reportId > 0){
			logger.info("insertAuditReportHeader values"+ auditReportHeaderDTO.getRunId());
			auditReportHeaderDTO.setReportId(reportId);
			statement = conn.prepareStatement(INSERT_AUDIT_REPORT_HEADER);
			int counter = 0;
			statement.setLong(++counter, auditReportHeaderDTO.getReportId());
			statement.setLong(++counter, auditReportHeaderDTO.getParamHeaderId());
			statement.setString(++counter, String.valueOf(auditReportHeaderDTO.getRunType()));
			statement.setLong(++counter, auditReportHeaderDTO.getRunId());
			statement.setString(++counter, auditReportHeaderDTO.getAuditBy());
			statement.setLong(++counter, auditReportHeaderDTO.getApVersionId());
			statement.executeUpdate();
		}
		else{
			logger.error("Unable to insert Audit report header.");
			throw new GeneralException("Unable to insert Audit report header.");
		}
			
		}
		catch(SQLException sqlE){
			logger.error("insertAuditReportHeader() - Error while inserting audit report header - " + sqlE.toString());
		}finally{
			PristineDBUtil.close(statement);
		}
		return reportId;
	}
	
	
	private long getAuditReportId(Connection conn){
		long reportId = -1;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_AUDIT_REPORT_ID);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				reportId = resultSet.getLong("REPORT_ID");
			}
		}
		catch(SQLException sqlE){
			logger.error("getAuditReportId() - Error while getting report id - " + sqlE.toString());
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return reportId;
	}
	
	
	
	public long getAuditParamHeaderId(Connection conn, AuditDashboardDTO auditDashboardDTO){
		long auditParamHeaderId = -1;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_AUDIT_PARAM_ID);
			int counter = 0;
			statement.setInt(++counter, auditDashboardDTO.getLocationId());
			statement.setInt(++counter, auditDashboardDTO.getLocationLevelId());
			statement.setInt(++counter, auditDashboardDTO.getProductId());
			statement.setInt(++counter, auditDashboardDTO.getProductLevelId());
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				auditParamHeaderId = resultSet.getLong("AUDIT_PARAMETER_HEADER_ID");
			}
		}
		catch(SQLException sqlE){
			logger.error("getAuditParamHeaderId() - Error while audit parameter header id - " + sqlE.toString());
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return auditParamHeaderId;
	}

	public long getDefaultAuditParamHeaderId(Connection conn){
		long auditParamHeaderId = -1;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_DEFAULT_AUDIT_PARAM_ID);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				auditParamHeaderId = resultSet.getLong("AUDIT_PARAMETER_HEADER_ID");
			}
		}
		catch(SQLException sqlE){
			logger.error("getDefaultAuditParamHeaderId() - Error while audit parameter header id - " + sqlE.toString());
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return auditParamHeaderId;
	}	
	
	
	public boolean insertOrUpdateAuditDashboard(Connection conn, AuditDashboardDTO auditDashboardDTO){
		
		
		
		boolean isDashboardInserted = false;
		PreparedStatement statement = null;
		
		
		try{
			long dashboardId = checkDashboardAvailablilty(conn, auditDashboardDTO);
			if(dashboardId > 0){
				statement = conn.prepareStatement(UPDATE_AUDIT_DASHBOARD);
				int counter = 0;
				statement.setLong(++counter, auditDashboardDTO.getParamHeaderId());
				statement.setLong(++counter, auditDashboardDTO.getReportId());
				statement.setLong(++counter, dashboardId);
				int insertCount = statement.executeUpdate();
				if(insertCount > 0)
					isDashboardInserted = true;
			}
			else{
				statement = conn.prepareStatement(INSERT_AUDIT_DASHBOARD);
				int counter = 0;
				statement.setInt(++counter, auditDashboardDTO.getLocationLevelId());
				statement.setInt(++counter, auditDashboardDTO.getLocationId());
				statement.setInt(++counter, auditDashboardDTO.getProductLevelId());
				statement.setInt(++counter, auditDashboardDTO.getProductId());
				statement.setLong(++counter, auditDashboardDTO.getParamHeaderId());
				statement.setLong(++counter, auditDashboardDTO.getReportId());
				int insertCount = statement.executeUpdate();
				if(insertCount > 0)
					isDashboardInserted = true;
			}
		}
		catch(SQLException sqlE){
			logger.error("insertAuditReportHeader() - Error while inserting audit report header - " + sqlE.toString());
		}finally{
			
			PristineDBUtil.close(statement);
		}
		return isDashboardInserted;
	}
	
	
	private long checkDashboardAvailablilty(Connection conn, AuditDashboardDTO auditDashboardDTO){
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		long auditDashboardId = 0;
		try
		{
			statement = conn.prepareStatement(GET_AUDIT_DASHBOARD);
			int counter = 0;
			statement.setInt(++counter, auditDashboardDTO.getLocationId());
			statement.setInt(++counter, auditDashboardDTO.getLocationLevelId());
			statement.setInt(++counter, auditDashboardDTO.getProductId());
			statement.setInt(++counter, auditDashboardDTO.getProductLevelId());
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				auditDashboardId = resultSet.getLong("AUDIT_DASHBOARD_ID");
			}
		}
		catch(SQLException ex){
			logger.error("checkDashboardAvailablilty() - Error while getting audit dashboard - " + ex.toString()); 
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return auditDashboardId;
	}
	
	
	public List<PRItemDTO> getAuditDetailFromRecommendation(Connection conn, AuditReportHeaderDTO auditReportHeaderDTO) throws GeneralException{
		 List<PRItemDTO> prItemDTOList = new ArrayList<PRItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
	
			statement = conn.prepareStatement(GET_AUDIT_DETAIL_FROM_RECOMMENDATION);
			logger.debug("Audit query: " + GET_AUDIT_DETAIL_FROM_RECOMMENDATION + auditReportHeaderDTO.getRunId());
				logger.info(" getAuditDetailFromRecommendation()- RUN ID: "+ auditReportHeaderDTO.getRunId());
			int counter = 0;
			statement.setLong(++counter, auditReportHeaderDTO.getRunId());
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				PRItemDTO prItemDTO = new PRItemDTO();
				prItemDTO.setRunId(resultSet.getLong("RUN_ID"));
				// prItemDTO.setIsOppurtunity(resultSet.getString("IS_OPPORTUNITY"));
				prItemDTO.setOppurtunityPrice(resultSet.getDouble("OPPORTUNITY_PRICE"));
				prItemDTO.setOppurtunityQty(resultSet.getInt("OPPORTUNITY_QTY"));
				prItemDTO.setRegPrice(resultSet.getDouble("CURR_REG_PRICE"));
				prItemDTO.setRegMPack(resultSet.getInt("CURR_REG_MUL"));
//				prItemDTO.setRecommendedRegMultiple(resultSet.getInt("RECOMMENDED_REG_MULTIPLE"));
//				prItemDTO.setRecommendedRegPrice(resultSet.getDouble("RECOMMENDED_REG_PRICE"));
				prItemDTO.setRecommendedRegPrice(
						new MultiplePrice(resultSet.getInt("REC_REG_MULTIPLE"), resultSet.getDouble("REC_REG_PRICE")));
				prItemDTO.setOverrideRegPrice(resultSet.getDouble("OVER_PRICE"));
				prItemDTO.setOverrideRegMultiple(resultSet.getInt("OVER_MULTIPLE"));
				prItemDTO.setIsMarkedForReview(resultSet.getString("MARKED"));
				prItemDTO.setListCost(resultSet.getDouble("LIST_COST"));
				// prItemDTO.setVipCost(resultSet.getDouble("VIP_COST"));
				
				if (resultSet.getString("LIG").equals("Y"))
					prItemDTO.setLir(true);
				else
					prItemDTO.setLir(false);

				prItemDTO.setPredictedMovement(resultSet.getDouble("REGULAR_UNITS"));

				prItemDTO.setCurRegPricePredictedMovement(resultSet.getDouble("CURR_UNITS"));
				prItemDTO.setItemSize(resultSet.getDouble("ITEM_SIZE"));
				prItemDTO.setUOMName(resultSet.getString("UOM_NAME"));
				
				MultiplePrice compUnitPrice = new MultiplePrice(resultSet.getInt("COMP_REG_MULTIPLE"),
						resultSet.getDouble("COMP_REG_PRICE"));
				prItemDTO.setCompPrice(compUnitPrice);
				prItemDTO.setIsNewPriceRecommended(resultSet.getInt("NEWPRICE_RECOMMENDED"));

				prItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));

				prItemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				prItemDTO.setOverridePredictedMovement(resultSet.getDouble("OVERRIDE_PRICE_PREDICTED_MOV"));
				// prItemDTO.setOverridePredictionStatus(resultSet.getInt("OVERRIDE_PRICE_PRED_STATUS"));
				prItemDTO.setPredictionStatus(resultSet.getInt("PREDICTION_STATUS"));
				prItemDTO.setBrandId(resultSet.getInt("BRAND_ID"));
				prItemDTO.setPriceCheckListTypeId(resultSet.getInt("PRICE_CHECK_LIST_TYPE_ID"));
				prItemDTO.setPriceCheckListId(resultSet.getInt("PRICE_CHECK_LIST_ID"));
				/*
				 * prItemDTO.setRegPricePredReasons(resultSet.getString("REC_REG_PRED_REASONS"))
				 * ;
				 * prItemDTO.setSalePricePredReasons(resultSet.getString("REC_SALE_PRED_REASONS"
				 * ));
				 */

				// Explain log
				if (resultSet.getObject("EXPLAIN_LOG") != null) {
					ObjectMapper mapper = new ObjectMapper();
					PRExplainLog explainLogObj;
					try {
						explainLogObj = mapper.readValue(resultSet.getString("EXPLAIN_LOG"), PRExplainLog.class);
						prItemDTO.setExplainLog(explainLogObj);
					} catch (JsonParseException e) {
						logger.error("Error while parsing explain log");
					} catch (JsonMappingException e) {
						logger.error("Error while parsing explain log");
					} 
					catch (IOException e) {
						logger.error("Error while parsing explain log");
					}
				}

				prItemDTO.setComp1StrId(resultSet.getInt("COMP_1_STR_ID"));
				prItemDTO.setComp2StrId(resultSet.getInt("COMP_2_STR_ID"));
				prItemDTO.setComp3StrId(resultSet.getInt("COMP_3_STR_ID"));
				prItemDTO.setComp4StrId(resultSet.getInt("COMP_4_STR_ID"));
				prItemDTO.setComp5StrId(resultSet.getInt("COMP_5_STR_ID"));
				prItemDTO.setComp6StrId(resultSet.getInt("COMP_STR_ID"));

				MultiplePrice comp1UnitPrice = new MultiplePrice(resultSet.getInt("COMP_1_RETAIL_MUL"),
						resultSet.getDouble("COMP_1_RETAIL"));
				prItemDTO.setComp1Retail(comp1UnitPrice);
				MultiplePrice comp2UnitPrice = new MultiplePrice(resultSet.getInt("COMP_2_RETAIL_MUL"),
						resultSet.getDouble("COMP_2_RETAIL"));
				prItemDTO.setComp2Retail(comp2UnitPrice);
				MultiplePrice comp3UnitPrice = new MultiplePrice(resultSet.getInt("COMP_3_RETAIL_MUL"),
						resultSet.getDouble("COMP_3_RETAIL"));
				prItemDTO.setComp3Retail(comp3UnitPrice);
				MultiplePrice comp4UnitPrice = new MultiplePrice(resultSet.getInt("COMP_4_RETAIL_MUL"),
						resultSet.getDouble("COMP_4_RETAIL"));
				prItemDTO.setComp4Retail(comp4UnitPrice);
				MultiplePrice comp5UnitPrice = new MultiplePrice(resultSet.getInt("COMP_5_RETAIL_MUL"),
						resultSet.getDouble("COMP_5_RETAIL"));
				prItemDTO.setComp5Retail(comp5UnitPrice);

				prItemDTO.setWeightedRecRetail(resultSet.getDouble("WEIGHT_NEW_RETAIL"));
				prItemDTO.setWeightedRegretail(resultSet.getDouble("WEIGHT_CURR_RETAIL"));
				prItemDTO.setWeightedComp1retail(resultSet.getDouble("WEIGHT_COMP_1_RET"));
				prItemDTO.setWeightedComp2retail(resultSet.getDouble("WEIGHT_COMP_2_RET"));
				prItemDTO.setWeightedComp3retail(resultSet.getDouble("WEIGHT_COMP_3_RET"));
				prItemDTO.setWeightedComp4retail(resultSet.getDouble("WEIGHT_COMP_4_RET"));
				prItemDTO.setWeightedComp5retail(resultSet.getDouble("WEIGHT_COMP_5_RET"));
				prItemDTO.setWeightedComp6retail(resultSet.getDouble("WEIGHT_PRIM_COMP_RET"));
				prItemDTO.setMovementData(resultSet.getDouble("MOV_52_WEEK"));
				if (resultSet.getObject("OVERRIDE_REG_PRICE") != null)
					prItemDTO.setOverrideRegPrice(resultSet.getDouble("OVERRIDE_REG_PRICE"));
				else
					prItemDTO.setOverrideRegPrice(0.0);
				prItemDTOList.add(prItemDTO);
			}
		}
		catch(SQLException sqlE){
			logger.error("getAuditDetailFromRecommendation() - Error while getting audit detail from recommendation - " + sqlE.toString());
			throw new GeneralException("Error while getting audit detail from recommendation - " + sqlE);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return prItemDTOList;
	}
	
	
	public boolean insertAuditReport(Connection conn, AuditReportDTO auditReportDTO) throws GeneralException{
		boolean isAuditSuccess = false;
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(INSERT_AUIDT_REPORT);
			int counter = 0;
			//logger.debug("Suspectable value : " + auditReportDTO.getTotalRevenue());
			statement.setLong(++counter, auditReportDTO.getReportId());
			statement.setDouble(++counter, auditReportDTO.getTotalRevenue());
			statement.setDouble(++counter, auditReportDTO.getMarginRate());
			statement.setDouble(++counter, auditReportDTO.getMarginDollar());
			statement.setDouble(++counter, auditReportDTO.getPriceIndex());
			statement.setDouble(++counter, auditReportDTO.getMovementQty());
			statement.setDouble(++counter, auditReportDTO.getMovementVol());
			statement.setInt(++counter, auditReportDTO.getRetailChanges());
			statement.setInt(++counter, auditReportDTO.getRetailIncreased());
			statement.setInt(++counter, auditReportDTO.getRetailDecreased());
			statement.setInt(++counter, auditReportDTO.getRetailOverridden());
			statement.setInt(++counter, auditReportDTO.getRetailMarked());
			statement.setInt(++counter, auditReportDTO.getRetailApproved());
			statement.setInt(++counter, auditReportDTO.getLowerSizeHigherPrice());
			statement.setInt(++counter, auditReportDTO.getLowerHigherPriceVariation());
			statement.setInt(++counter, auditReportDTO.getSimilarSizeVariation());
			statement.setInt(++counter, auditReportDTO.getRetailBelowListCost());
			statement.setInt(++counter, auditReportDTO.getRetailBelowListCostNoVIP());
			statement.setInt(++counter, auditReportDTO.getRetailBelowVIPCost());
			statement.setInt(++counter, auditReportDTO.getMarginLTPCT());
			statement.setInt(++counter, auditReportDTO.getMarginGTPCT());
			statement.setInt(++counter, auditReportDTO.getRetailsLTPCTOfPrimaryComp());
			statement.setInt(++counter, auditReportDTO.getRetailsGTPCTOfPrimaryComp());
			statement.setInt(++counter, auditReportDTO.getMarginLTPCTAllComp());
			statement.setInt(++counter, auditReportDTO.getMarginGTPCTAllComp());
			statement.setInt(++counter, auditReportDTO.getRetailChangedLTPCT());
			statement.setInt(++counter, auditReportDTO.getRetailChangedGTPCT());
			statement.setInt(++counter, auditReportDTO.getListCostChangedGTPCT());
			statement.setInt(++counter, auditReportDTO.getCompPricePrimaryChangedGTPCT());
			statement.setInt(++counter, auditReportDTO.getRetailChangedNoCostOrNoComp());
			statement.setInt(++counter, auditReportDTO.getRetailChangedRoundingViol());
			statement.setInt(++counter, auditReportDTO.getRetailLirLinePriceViol());
			statement.setInt(++counter, auditReportDTO.getRetailChangedNtimes());
			statement.setInt(++counter, auditReportDTO.getRetailNotChangedKVIandSK());
			statement.setInt(++counter, auditReportDTO.getReducedRetailsToIncreaseMarginOpp());
			statement.setInt(++counter, auditReportDTO.getKVICompPriceOld());
			statement.setInt(++counter, auditReportDTO.getKVIWithCompPriceCount());
			statement.setDouble(++counter, auditReportDTO.getKVIWithCompPricePCT());
			statement.setInt(++counter, auditReportDTO.getTotalItems());
			statement.setInt(++counter, auditReportDTO.getTotalLig());
			statement.setDouble(++counter, auditReportDTO.getProjectedMarginRate());
			statement.setDouble(++counter, auditReportDTO.getProjectedMarginDollar());
			statement.setDouble(++counter, auditReportDTO.getProjectedPriceIndex());
			statement.setDouble(++counter, auditReportDTO.getProjectedMovementQty());
			statement.setDouble(++counter, auditReportDTO.getProjectedMovementVol());
			statement.setDouble(++counter, auditReportDTO.getProjectedTotalRevenue());
			statement.setInt(++counter, auditReportDTO.getOutOfNorm());
			statement.setInt(++counter, auditReportDTO.getOutOfNormLig());
			statement.setInt(++counter, auditReportDTO.getRetailChangesUnq());
			statement.setInt(++counter, auditReportDTO.getRetailIncreasedUnq());
			statement.setInt(++counter, auditReportDTO.getRetailDecreasedUnq());
			statement.setInt(++counter, auditReportDTO.getRetailOverriddenUnq());
			statement.setInt(++counter, auditReportDTO.getRetailMarkedUnq());
			statement.setInt(++counter, auditReportDTO.getRetailApprovedUnq());
			statement.setInt(++counter, auditReportDTO.getLowerSizeHigherPriceUnq());
			statement.setInt(++counter, auditReportDTO.getLowerHigherPriceVariationUnq());
			statement.setInt(++counter, auditReportDTO.getSimilarSizeVariationUnq());
			statement.setInt(++counter, auditReportDTO.getRetailBelowListCostUnq());
			statement.setInt(++counter, auditReportDTO.getRetailBelowListCostNoVIPUnq());
			statement.setInt(++counter, auditReportDTO.getRetailBelowVIPCostUnq());
			statement.setInt(++counter, auditReportDTO.getMarginLTPCTUnq());
			statement.setInt(++counter, auditReportDTO.getMarginGTPCTUnq());
			statement.setInt(++counter, auditReportDTO.getRetailsLTPCTOfPrimaryCompUnq());
			statement.setInt(++counter, auditReportDTO.getRetailsGTPCTOfPrimaryCompUnq());
			statement.setInt(++counter, auditReportDTO.getMarginLTPCTAllCompUnq());
			statement.setInt(++counter, auditReportDTO.getMarginGTPCTAllCompUnq());
			statement.setInt(++counter, auditReportDTO.getRetailChangedLTPCTUnq());
			statement.setInt(++counter, auditReportDTO.getRetailChangedGTPCTUnq());
			statement.setInt(++counter, auditReportDTO.getListCostChangedGTPCTUnq());
			statement.setInt(++counter, auditReportDTO.getCompPricePrimaryChangedGTPCTUnq());
			statement.setInt(++counter, auditReportDTO.getRetailChangedNoCostOrNoCompUnq());
			statement.setInt(++counter, auditReportDTO.getRetailChangedRoundingViolUnq());
			statement.setInt(++counter, auditReportDTO.getRetailLirLinePriceViolUnq());
			statement.setInt(++counter, auditReportDTO.getRetailChangedNtimesUnq());
			statement.setInt(++counter, auditReportDTO.getRetailNotChangedKVIandSKUnq());
			statement.setInt(++counter, auditReportDTO.getReducedRetailsToIncreaseMarginOppUnq());
			statement.setInt(++counter, auditReportDTO.getKVICompPriceOldUnq());
			statement.setInt(++counter, auditReportDTO.getKVIWithCompPriceCountUnq());
			statement.setDouble(++counter, auditReportDTO.getKVIWithCompPricePCTUnq());
			statement.setInt(++counter, auditReportDTO.getZeroCurrRetails());
			statement.setInt(++counter, auditReportDTO.getZeroCurrRetailsUnq());
			statement.setInt(++counter, auditReportDTO.getZeroCost());
			statement.setInt(++counter, auditReportDTO.getZeroCostUnq());
			statement.setInt(++counter, auditReportDTO.getRetailsPrimaryCompViolation());
			statement.setInt(++counter, auditReportDTO.getRetailsPrimaryCompViolationUnq());
			statement.setInt(++counter, auditReportDTO.getMarginViolation());
			statement.setInt(++counter, auditReportDTO.getMarginViolationUnq());
			statement.setInt(++counter, auditReportDTO.getRetailChangedViolation());
			statement.setInt(++counter, auditReportDTO.getRetailChangedViolationUnq());
			statement.setInt(++counter, auditReportDTO.getBrandViolation());
			statement.setInt(++counter, auditReportDTO.getBrandViolationUnq());
			statement.setInt(++counter, auditReportDTO.getSizeViolation()); 
			statement.setInt(++counter, auditReportDTO.getSizeViolationUnq());
			statement.setDouble(++counter, auditReportDTO.getCompStr1AUR());
			statement.setDouble(++counter, auditReportDTO.getCompStr2AUR());
			statement.setDouble(++counter, auditReportDTO.getCompStr3AUR());
			statement.setDouble(++counter, auditReportDTO.getCompStr4AUR());
			statement.setDouble(++counter, auditReportDTO.getCompStr5AUR());
			statement.setString(++counter, auditReportDTO.getCompStrID1());
			statement.setString(++counter, auditReportDTO.getCompStrID2());
			statement.setString(++counter, auditReportDTO.getCompStrID3());
			statement.setString(++counter, auditReportDTO.getCompStrID4());
			statement.setString(++counter, auditReportDTO.getCompStrID5());
			statement.setDouble(++counter, auditReportDTO.getBaseRetAUR());
			statement.setDouble(++counter, auditReportDTO.getBaseRecRetAUR());
			statement.setDouble(++counter, auditReportDTO.getMovementQtyILP());
			statement.setDouble(++counter, auditReportDTO.getTotalRevenueILP());
			statement.setDouble(++counter, auditReportDTO.getMarginDollarILP());
			statement.setDouble(++counter, auditReportDTO.getMarginRateILP());
			statement.setDouble(++counter, auditReportDTO.getProjectedMovQtyILP());
			statement.setDouble(++counter, auditReportDTO.getProjectedTotalRevenueILP());
			statement.setDouble(++counter, auditReportDTO.getProjectedMarginRateILP());
			statement.setDouble(++counter, auditReportDTO.getProjectedMarginDollarILP());
			statement.setInt(++counter, auditReportDTO.getRetlessComp1());
			statement.setInt(++counter, auditReportDTO.getRetgreaterComp1());
			statement.setInt(++counter, auditReportDTO.getRetequalComp1());
			statement.setInt(++counter, auditReportDTO.getRetlessComp2());
			statement.setInt(++counter, auditReportDTO.getRetgreaterComp2());
			statement.setInt(++counter, auditReportDTO.getRetequalComp2());
			statement.setInt(++counter, auditReportDTO.getRetlessComp3());
			statement.setInt(++counter, auditReportDTO.getRetgreaterComp3());
			statement.setInt(++counter, auditReportDTO.getRetequalComp3());
			statement.setInt(++counter, auditReportDTO.getRetlessComp4());
			statement.setInt(++counter, auditReportDTO.getRetgreaterComp4());
			statement.setInt(++counter, auditReportDTO.getRetequalComp4());
			statement.setInt(++counter, auditReportDTO.getRetlessComp5());
			statement.setInt(++counter, auditReportDTO.getRetgreaterComp5());
			statement.setInt(++counter, auditReportDTO.getRetequalComp5());
			statement.setString(++counter, auditReportDTO.getCompStrID6());
			statement.setDouble(++counter, auditReportDTO.getCompStr6AUR());
			statement.setInt(++counter, auditReportDTO.getRetlessComp6());
			statement.setInt(++counter, auditReportDTO.getRetgreaterComp6());
			statement.setInt(++counter, auditReportDTO.getRetequalComp6());
			statement.setInt(++counter,  auditReportDTO.getMarkUP());
			statement.setInt(++counter,  auditReportDTO.getMarkDown());
			statement.setDouble(++counter,  auditReportDTO.getBaseReccVsCompAUR6());//BASE COMPETITOR
			statement.setDouble(++counter,  auditReportDTO.getBaseReccVsCompAUR1());
			statement.setDouble(++counter,  auditReportDTO.getBaseReccVsCompAUR2());
			statement.setDouble(++counter,  auditReportDTO.getBaseReccVsCompAUR3());
			statement.setDouble(++counter,  auditReportDTO.getBaseReccVsCompAUR4());
			statement.setDouble(++counter,  auditReportDTO.getBaseReccVsCompAUR5());
			statement.setDouble(++counter,  auditReportDTO.getBaseCurrRetailVsCompAUR6());
			statement.setDouble(++counter,  auditReportDTO.getBaseCurrRetailVsCompAUR1());
			statement.setDouble(++counter,  auditReportDTO.getBaseCurrRetailVsCompAUR2());
			statement.setDouble(++counter,  auditReportDTO.getBaseCurrRetailVsCompAUR3());
			statement.setDouble(++counter,  auditReportDTO.getBaseCurrRetailVsCompAUR4());
			statement.setDouble(++counter,  auditReportDTO.getBaseCurrRetailVsCompAUR5());
			statement.setInt(++counter,  auditReportDTO.getCurRetgreaterComp1());
			statement.setInt(++counter,  auditReportDTO.getCurRetlessComp1());
			statement.setInt(++counter,  auditReportDTO.getCurRetequalComp1());
			statement.setInt(++counter,  auditReportDTO.getCurRetgreaterComp2());
			statement.setInt(++counter,  auditReportDTO.getCurRetlessComp2());
			statement.setInt(++counter,  auditReportDTO.getCurRetequalComp2());
			statement.setInt(++counter,  auditReportDTO.getCurRetgreaterComp3());
			statement.setInt(++counter,  auditReportDTO.getCurRetlessComp3());
			statement.setInt(++counter,  auditReportDTO.getCurRetequalComp3());
			statement.setInt(++counter,  auditReportDTO.getCurRetgreaterComp4());
			statement.setInt(++counter,  auditReportDTO.getCurRetlessComp4());
			statement.setInt(++counter,  auditReportDTO.getCurRetequalComp4());
			statement.setInt(++counter,  auditReportDTO.getCurRetgreaterComp5());
			statement.setInt(++counter,  auditReportDTO.getCurRetlessComp5());
			statement.setInt(++counter,  auditReportDTO.getCurRetequalComp5());
			statement.setInt(++counter,  auditReportDTO.getCurRetgreaterComp6());
			statement.setInt(++counter,  auditReportDTO.getCurRetlessComp6());
			statement.setInt(++counter,  auditReportDTO.getCurRetequalComp6());
	
			logger.debug("INSERT INTO PR_AUDIT_REPORT (AUDIT_REPORT_DETAIL_ID, AUDIT_REPORT_ID, TOT_REVENUE, MARGIN_RATE, "
					+ "MARGIN_D, PRICE_INDEX, MOVEMENT_QTY, MOVEMENT_VOL, RETAIL_CHANGED, RETAIL_INCREASED, RETAIL_DECREASED, "
					+ "RETAIL_OVERRIDDEN, RETAIL_MARKED, RETAIL_APPROVED, LOWER_SIZE_HIGHER_PRICE, LOWER_HIGHER_PRICE_VARIATION, "
					+ "SIMILAR_SIZE_PRICE_VARIATION, RETAIL_BELOW_LIST_COST, RETAIL_BELOW_LIST_COST_NO_VIP, RETAIL_BELOW_VIP_COST, "
					+ "MARGIN_LT_PCT, MARGIN_GT_PCT, RETAIL_LT_PRIM_COMP_PCT, RETAIL_GT_PRIM_COMP_PCT, MARGIN_LT_ALL_COMP, MARGIN_GT_ALL_COMP, "
					+ "RETAIL_CHANGED_LT_PCT, RETAIL_CHANGED_GT_PCT, LIST_COST_CHANGED_GT_PCT, COMP_PRICE_PRIM_CHANGED_GT_PCT, "
					+ "RETAIL_CHANGED_NO_COST_OR_COMP, RETAIL_CHANGED_ROUNDING_VIOL, RETAIL_LIR_LINE_PRICE_VIOL, RETAIL_CHANGED_N_TIMES, "
					+ "RETAIL_NOT_CHANGED_KVI_SK, RED_RETAIL_INC_MARGIN_OPER, KVI_COMP_PRICE_OLD, KVI_WITH_COMP_PRICE_COUNT, "
					+ "KVI_WITH_COMP_PRICE_PCT, TOTAL_ITEMS, TOTAL_LIGS, PRO_MARGIN_RATE, PRO_MARGIN_D,  PRO_PRICE_INDEX, PRO_MOVEMENT_QTY, "
					+ "PRO_MOVEMENT_VOL, PRO_TOT_REVENUE, OUT_OF_NORM, OUT_OF_NORM_LIG, RETAIL_CHANGED_U,	"
					+ "RETAIL_INCREASED_U,	RETAIL_DECREASED_U,	RETAIL_OVERRIDDEN_U,	RETAIL_MARKED_U,	RETAIL_APPROVED_U,	"
					+ "LOWER_SIZE_HIGHER_PRICE_U,	LOWER_HIGH_PRICE_VARY_U,	SIMILAR_SIZE_PRICE_VARY_U,	RETAIL_BELOW_LIST_COST_U,	"
					+ "RETAIL_BELOW_LC_NO_VIP_U,	RETAIL_BELOW_VIP_COST_U,	MARGIN_LT_PCT_U,	MARGIN_GT_PCT_U,	"
					+ "RETAIL_LT_PRIM_COMP_PCT_U,	RETAIL_GT_PRIM_COMP_PCT_U,	MARGIN_LT_ALL_COMP_U,	MARGIN_GT_ALL_COMP_U,	"
					+ "RETAIL_CHANGED_LT_PCT_U,	RETAIL_CHANGED_GT_PCT_U,	LIST_COST_CHANGED_GT_PCT_U,	COMP_PRIMARY_CHANGED_GT_PCT_U,	"
					+ "RETAIL_CHG_NO_COST_OR_COMP_U,	RETAIL_CHG_ROUNDING_VIOL_U,	RETAIL_LIR_LINE_PRICE_VIOL_U,	"
					+ "RETAIL_CHANGED_N_TIMES_U,	RETAIL_NOT_CHANGED_KVI_SK_U,	RED_RETAIL_INC_MARGIN_OPER_U,	"
					+ "KVI_COMP_PRICE_OLD_U,	KVI_WITH_COMP_PRICE_COUNT_U,	KVI_WITH_COMP_PRICE_PCT_U, ZERO_CURR_RETAILS, ZERO_CURR_RETAILS_U, "
					+ "ZERO_COST_RETAILS, ZERO_COST_RETAILS_U, RETAIL_PRIM_COMP_VIOLATION, RETAIL_PRIM_COMP_VIOLATION_U," 
					+ "MARGIN_VIOLATION, MARGIN_VIOLATION_U, RETAIL_CHANGE_VIOLATION, RETAIL_CHANGE_VIOLATION_U,"
					+ "COMP_1_AUR, COMP_2_AUR, COMP_3_AUR, COMP_4_AUR,COMP_5_AUR,COMP_1_STR,COMP_2_STR,COMP_3_STR,COMP_4_STR,COMP_5_STR,BASE_REG_RET_AUR,BASE_REC_RET_AUR"
					+ "MARK_UP, MARK_DOWN, BASE_VS_PRIM_COMP_AUR,BASE_VS_COMP_1_AUR,BASE_VS_COMP_2_AUR,BASE_VS_COMP_3_AUR,BASE_VS_COMP_4_AUR,BASE_VS_COMP_5_AUR"
					+"BASE_CUR_VS_PRIM_COMP_AUR,BASE_CUR_VS_COMP_1_AUR,BASE_CUR_VS_COMP_2_AUR,BASE_CUR_VS_COMP_3_AUR,BASE_CUR_VS_COMP_4_AUR,BASE_CUR_VS_COMP_5_AUR)"
					+ " VALUES (AUDIT_REPORT_DETAIL_SEQ.NEXTVAL, " 
					+ auditReportDTO.getReportId() + ", " + 	 
					 auditReportDTO.getTotalRevenue() + ", " + 	 
					 auditReportDTO.getMarginRate() + ", " + 	 auditReportDTO.getMarginDollar() + ", " + 	 
					 auditReportDTO.getPriceIndex() + ", " + 	 auditReportDTO.getMovementQty() + ", " + 	 
					 auditReportDTO.getMovementVol() + ", " + 	 auditReportDTO.getRetailChanges() + ", " + 	 
					 auditReportDTO.getRetailIncreased() + ", " + 	 auditReportDTO.getRetailDecreased() + ", " + 	 
					 auditReportDTO.getRetailOverridden() + ", " + 	 auditReportDTO.getRetailMarked() + ", " + 	 
					 auditReportDTO.getRetailApproved() + ", " + 	 auditReportDTO.getLowerSizeHigherPrice() + ", " + 	 
					 auditReportDTO.getLowerHigherPriceVariation() + ", " + 	 auditReportDTO.getSimilarSizeVariation() + ", " + 	 
					 auditReportDTO.getRetailBelowListCost() + ", " + 	 auditReportDTO.getRetailBelowListCostNoVIP() + ", " + 	 
					 auditReportDTO.getRetailBelowVIPCost() + ", " + 	 auditReportDTO.getMarginLTPCT() + ", " + 	 
					 auditReportDTO.getMarginGTPCT() + ", " + 	 auditReportDTO.getRetailsLTPCTOfPrimaryComp() + ", " + 	 
					 auditReportDTO.getRetailsGTPCTOfPrimaryComp() + ", " + 	 auditReportDTO.getMarginLTPCTAllComp() + ", " + 	 
					 auditReportDTO.getMarginGTPCTAllComp() + ", " + 	 auditReportDTO.getRetailChangedLTPCT() + ", " + 	 
					 auditReportDTO.getRetailChangedGTPCT() + ", " + 	 auditReportDTO.getListCostChangedGTPCT() + ", " + 	 
					 auditReportDTO.getCompPricePrimaryChangedGTPCT() + ", " + 	 auditReportDTO.getRetailChangedNoCostOrNoComp() + ", " + 	 
					 auditReportDTO.getRetailChangedRoundingViol() + ", " + 	 auditReportDTO.getRetailLirLinePriceViol() + ", " + 	 
					 auditReportDTO.getRetailChangedNtimes() + ", " + 	 auditReportDTO.getRetailNotChangedKVIandSK() + ", " + 	 
					 auditReportDTO.getReducedRetailsToIncreaseMarginOpp() + ", " + 	 auditReportDTO.getKVICompPriceOld() + ", " + 	 
					 auditReportDTO.getKVIWithCompPriceCount() + ", " + 	 auditReportDTO.getKVIWithCompPricePCT() + ", " + 	 
					 auditReportDTO.getTotalItems() + ", " + 	 auditReportDTO.getTotalLig() + ", " + 	 
					 auditReportDTO.getProjectedMarginRate() + ", " + 	 auditReportDTO.getProjectedMarginDollar() + ", " + 	 
					 auditReportDTO.getProjectedPriceIndex() + ", " + 	 auditReportDTO.getProjectedMovementQty() + ", " + 	 
					 auditReportDTO.getProjectedMovementVol() + ", " + 	 auditReportDTO.getProjectedTotalRevenue() + ", " + 	 
					 auditReportDTO.getOutOfNorm() + ", " + auditReportDTO.getOutOfNormLig() + ", " + 
					 auditReportDTO.getRetailChangesUnq() + ", " + 	 
					 auditReportDTO.getRetailIncreasedUnq() + ", " + 	 auditReportDTO.getRetailDecreasedUnq() + ", " + 	 
					 auditReportDTO.getRetailOverriddenUnq() + ", " + 	 auditReportDTO.getRetailMarkedUnq() + ", " + 	 
					 auditReportDTO.getRetailApprovedUnq() + ", " + 	 auditReportDTO.getLowerSizeHigherPriceUnq() + ", " + 	 
					 auditReportDTO.getLowerHigherPriceVariationUnq() + ", " + 	 auditReportDTO.getSimilarSizeVariationUnq() + ", " + 	 
					 auditReportDTO.getRetailBelowListCostUnq() + ", " + 	 auditReportDTO.getRetailBelowListCostNoVIPUnq() + ", " + 	 
					 auditReportDTO.getRetailBelowVIPCostUnq() + ", " + 	 auditReportDTO.getMarginLTPCTUnq() + ", " + 	 
					 auditReportDTO.getMarginGTPCTUnq() + ", " + 	 auditReportDTO.getRetailsLTPCTOfPrimaryCompUnq() + ", " + 	 
					 auditReportDTO.getRetailsGTPCTOfPrimaryCompUnq() + ", " + 	 auditReportDTO.getMarginLTPCTAllCompUnq() + ", " + 	 
					 auditReportDTO.getMarginGTPCTAllCompUnq() + ", " + 	 auditReportDTO.getRetailChangedLTPCTUnq() + ", " + 	 
					 auditReportDTO.getRetailChangedGTPCTUnq() + ", " + 	 auditReportDTO.getListCostChangedGTPCTUnq() + ", " + 	 
					 auditReportDTO.getCompPricePrimaryChangedGTPCTUnq() + ", " + 	 auditReportDTO.getRetailChangedNoCostOrNoCompUnq() + ", " + 	 
					 auditReportDTO.getRetailChangedRoundingViolUnq() + ", " + 	 auditReportDTO.getRetailLirLinePriceViolUnq() + ", " + 	 
					 auditReportDTO.getRetailChangedNtimesUnq() + ", " + 	 auditReportDTO.getRetailNotChangedKVIandSKUnq() + ", " + 	 
					 auditReportDTO.getReducedRetailsToIncreaseMarginOppUnq() + ", " + 	 auditReportDTO.getKVICompPriceOldUnq() + ", " + 	 
					 auditReportDTO.getKVIWithCompPriceCountUnq() + ", " + 	 auditReportDTO.getKVIWithCompPricePCTUnq() +  
							", " + auditReportDTO.getZeroCurrRetails() + ", " + auditReportDTO.getZeroCurrRetailsUnq()
							+ ", " + auditReportDTO.getZeroCost() + ", " + auditReportDTO.getZeroCostUnq() + ", "
							+ auditReportDTO.getRetailsPrimaryCompViolation() + ", "
							+ auditReportDTO.getRetailsPrimaryCompViolationUnq() + ", "
							+ auditReportDTO.getMarginViolation() + "," + auditReportDTO.getMarginViolationUnq() + ", "
							+ auditReportDTO.getRetailChangedViolation() + ", "
							+ auditReportDTO.getRetailChangedViolationUnq() + "," + auditReportDTO.getCompStr1AUR()
							+ "," + auditReportDTO.getCompStr2AUR() + "," + auditReportDTO.getCompStr3AUR() + ","
							+ auditReportDTO.getCompStr4AUR() + "," + auditReportDTO.getCompStr5AUR() + ","
							+ auditReportDTO.getCompStrID1() + "," + auditReportDTO.getCompStrID2() + ","
							+ auditReportDTO.getCompStrID3() + "," + auditReportDTO.getCompStrID4() + ","
							+ auditReportDTO.getCompStrID5() + "," + auditReportDTO.getBaseRetAUR() + ","
							+ auditReportDTO.getBaseRecRetAUR() + "," + auditReportDTO.getMarkDown() + ", "
							+ auditReportDTO.getMarkUP() + "," + auditReportDTO.getBaseReccVsCompAUR1() + ","
							+ auditReportDTO.getBaseReccVsCompAUR2() + "," + auditReportDTO.getBaseReccVsCompAUR3()
							+ "," + auditReportDTO.getBaseReccVsCompAUR4() + ","
							+ auditReportDTO.getBaseReccVsCompAUR5() + "," + auditReportDTO.getBaseReccVsCompAUR6()
							+ "," + auditReportDTO.getBaseCurrRetailVsCompAUR1() + ","
							+ auditReportDTO.getBaseCurrRetailVsCompAUR2() + ","
							+ auditReportDTO.getBaseCurrRetailVsCompAUR3() + ","
							+ auditReportDTO.getBaseCurrRetailVsCompAUR4() + ","
							+ auditReportDTO.getBaseCurrRetailVsCompAUR5() + ","
							+ auditReportDTO.getBaseCurrRetailVsCompAUR6() + "); ");

			
			int rowsAffected = statement.executeUpdate();
			if(rowsAffected > 0)
				isAuditSuccess = true;
		}
		catch(SQLException sqlE){
			logger.error("insertAuditReport() - Error while inserting audit report detail - " + sqlE.toString());
			throw new GeneralException(sqlE.toString());
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return isAuditSuccess;
		
	}
	
	public void updateAuditReportHeader(Connection conn, AuditReportHeaderDTO auditReportHeaderDTO){
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(UPDATE_AUDIT_HEADER);
			int counter = 0;
			statement.setString(++counter, auditReportHeaderDTO.getMessage());
			statement.setString(++counter, auditReportHeaderDTO.getPercentCompleted());
			statement.setLong(++counter, auditReportHeaderDTO.getReportId());
			int rowsAffected = statement.executeUpdate();
			logger.debug("updateAuditReportHeader() - rows affected - " + rowsAffected); 
		}
		catch(SQLException sqlE){
			logger.error("updateAuditReportHeader() - Error while updating audit header - " + sqlE.toString());
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	
	public List<AuditParameterDTO> getAuditParameters(Connection conn, long auditParamHeaderId, long apVerId){
		List<AuditParameterDTO> auditParameterList = new ArrayList<AuditParameterDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_AUDIT_PARAMETERS);
			int counter = 0;
			statement.setLong(++counter, auditParamHeaderId);
			statement.setLong(++counter, apVerId);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				AuditParameterDTO auditParameterDTO = new AuditParameterDTO();
				auditParameterDTO.setParametersId(resultSet.getLong("AUDIT_PARAMETERS_ID"));
				auditParameterDTO.setParameterValue(resultSet.getDouble("PARAMETER_VALUE"));
				auditParameterDTO.setParamsType(resultSet.getString("AUDIT_PARAMETERS_TYPE"));
				auditParameterDTO.setValueType(resultSet.getString("VALUE_TYPE"));
				auditParameterList.add(auditParameterDTO);
			}
		}
		catch(SQLException sqlE){
			logger.error("getAuditParameters() - Error while getting audit parameters - " + sqlE.toString());
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return auditParameterList;
	}
	
	
	public AuditParameterHeaderDTO getAuditParameterHeader(Connection conn, long auditParamHeaderId){
		AuditParameterHeaderDTO auditParameterHeaderDTO = new AuditParameterHeaderDTO();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_AUDIT_PARAMETER_HEADER);
			int counter = 0;
			statement.setLong(++counter, auditParamHeaderId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				auditParameterHeaderDTO.setApVerId(resultSet.getLong("AP_VER_ID"));
				auditParameterHeaderDTO.setParamHeaderId(resultSet.getLong("AUDIT_PARAMETER_HEADER_ID"));
			}
		}
		catch(SQLException sqlE){
			logger.error("getAuditParameters() - Error while getting audit parameters - " + sqlE.toString());
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return auditParameterHeaderDTO;
	}
	
	
	public AuditParameterHeaderDTO getAuditParamHeaderForReportId(Connection conn, long reportId) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		AuditParameterHeaderDTO auditParameterHeaderDTO = null;
		try{
			statement = conn.prepareStatement(GET_AUDIT_PARAM_HEADER_FOR_REPORT_ID);
			int colCount = 0;
			statement.setLong(++colCount, reportId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				auditParameterHeaderDTO = new AuditParameterHeaderDTO();
				auditParameterHeaderDTO.setParamHeaderId(resultSet.getLong("AUDIT_PARAMETER_HEADER_ID"));
				auditParameterHeaderDTO.setApVerId(resultSet.getLong("AP_VER_ID"));
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getAuditParamHeaderForReportId() " + sqlE.toString());
			throw new GeneralException("Error -- getAuditParamHeaderForReportId()", sqlE);
		}
		return auditParameterHeaderDTO;
	}
	
	/**
	 * 
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param locationId
	 * @param runId
	 * @return list of items for which the retail changed x times in last x months
	 * @throws GeneralException
	 */
	public Set<Integer> getRetailChangedXTimesInLastXMonths(Connection conn, String startDate, String endDate, int locationId, long runId, int retailChangeNTimes) throws GeneralException{
		// 1. start_date, 2. end_date, 3. Run id, 4.zone id, zone id, change count
		Set<Integer> itemsSet = new HashSet<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_RETAIL_CHANGED_X_TIMES_IN_LAST_X_MONTHS);
//			logger.debug("Price query -- " + GET_RETAIL_CHANGED_X_TIMES_IN_LAST_X_MONTHS);
//			logger.info("1. " + endDate + " 2. " + startDate + " 3. " + runId + " 4. " + locationId + " 5. "
//					+ locationId + "6. " + retailChangeNTimes);
			int index = 0;
			statement.setString(++index, endDate);
			statement.setString(++index, startDate);
			statement.setLong(++index, runId);
			statement.setInt(++index, locationId);
			statement.setInt(++index, locationId);
			statement.setInt(++index, retailChangeNTimes);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_CODE");
				itemsSet.add(itemCode);
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getRetailChangedXTimesInLastXMonths() - " + sqlE.toString(), sqlE);
			throw new GeneralException("Error -- getRetailChangedXTimesInLastXMonths()", sqlE);
		}
		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemsSet;
	}
	
	/**
	 * 
	 * @param conn
	 * @param startDate
	 * @param endDate
	 * @param locationId
	 * @param runId
	 * @param xPct
	 * @return list of items for which the cost change is greater than x% in last x months
	 * @throws GeneralException
	 */
	public Set<Integer> getCostChangedXPctInLastXMonths(Connection conn, String startDate, String endDate, int locationId, long runId, double xPct) throws GeneralException{
		//1. change pct, 2. start_date, 3. end date 4. run id, 5. zone id 6. zone id 
		Set<Integer> itemsSet = new HashSet<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_COST_CHANGED_GT_X_PCT_IN_LAST_X_MONTHS);
//			logger.debug("Cost query -- " + GET_COST_CHANGED_GT_X_PCT_IN_LAST_X_MONTHS);
//			logger.info("1. " + xPct + " 2. " + endDate + " 3. " + startDate + " 4. " + runId + " 5. "
//					+ locationId + "6. " + locationId);
			int index = 0;
			statement.setDouble(++index, xPct);
			statement.setString(++index, endDate);
			statement.setString(++index, startDate);
			statement.setLong(++index, runId);
			statement.setInt(++index, locationId);
			statement.setInt(++index, locationId);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				int itemCode = resultSet.getInt("ITEM_CODE");
				itemsSet.add(itemCode);
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getCostChangedXPctInLastXMonths() - " + sqlE.toString(), sqlE);
			throw new GeneralException("Error -- getCostChangedXPctInLastXMonths()", sqlE);
		}
		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemsSet;
	}
	
	
	public Date getStartDateFromRecommendation(Connection conn, long runId) throws GeneralException{
		Date startDate = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_CALENDAR_FROM_REC_RUN_HEADER);
			int index = 0;
			statement.setLong(++index, runId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				startDate = resultSet.getDate("START_DATE");
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getCalendarIdFromRecommendation() - " + sqlE.toString(), sqlE);
			throw new GeneralException("Error -- getCalendarIdFromRecommendation()", sqlE);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return startDate;
	}
	
	
	public AuditDashboardDTO getAuditDashboardData(Connection conn, long reportId) throws GeneralException{
		AuditDashboardDTO auditDashboardDTO = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_LOCATION_FOR_AUDIT_REPORT);
			int index = 0;
			statement.setLong(++index, reportId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				auditDashboardDTO = new AuditDashboardDTO();
				auditDashboardDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				auditDashboardDTO.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				auditDashboardDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				auditDashboardDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getLocationForAuditReport() - " + sqlE.toString(), sqlE);
			throw new GeneralException("Error -- getLocationForAuditReport()", sqlE);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return auditDashboardDTO;
	}
	
	
	public AuditReportDTO getQuarterlySummary(Connection conn, AuditReportDTO auditReportDTO) throws GeneralException{
	
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			logger.info("getQuarterlySummary() -Getting quarterly summary for :"+  auditReportDTO.getRunID());
			statement = conn.prepareStatement(GET_QUARTERLY_SUMMARY);
			int counter = 0;
			statement.setLong(++counter, auditReportDTO.getRunID());
		
			rs = statement.executeQuery();
			if(rs.next()){
				auditReportDTO.setTotalItems(rs.getInt("TOTAL_ITEMS"));
				auditReportDTO.setTotalLig(rs.getInt("TOTAL_ITEMS_LIG"));
				auditReportDTO.setPriceIndex(rs.getDouble("CURR_PRICE_INDEX"));
				auditReportDTO.setProjectedMovementQty(rs.getDouble("UNITS_TOTAL"));
				auditReportDTO.setProjectedTotalRevenue(rs.getDouble("REV_TOTAL"));
				auditReportDTO.setProjectedMarginDollar(rs.getDouble("MAR_TOTAL"));
				auditReportDTO.setProjectedMarginRate(rs.getDouble("MAR_PCT_TOTAL"));
				auditReportDTO.setMovementQty(rs.getDouble("CURR_UNITS"));
				auditReportDTO.setMarginDollar(rs.getDouble("CURR_MARGIN"));
				auditReportDTO.setTotalRevenue(rs.getDouble("CURR_REVENUE"));
				auditReportDTO.setMarginRate(rs.getDouble("CURR_MAR_PCT"));
				auditReportDTO.setMovementQtyILP(rs.getDouble("CURR_UNITS_ILP"));
				auditReportDTO.setTotalRevenueILP(rs.getDouble("CURR_REVENUE_ILP"));
				auditReportDTO.setMarginDollarILP(rs.getDouble("CURR_MARGIN_ILP"));
				auditReportDTO.setMarginRateILP(rs.getDouble("CURR_MAR_PCT_ILP"));
				auditReportDTO.setProjectedMovQtyILP(rs.getDouble("UNITS_TOTAL_ILP"));
				auditReportDTO.setProjectedTotalRevenueILP(rs.getDouble("REV_TOTAL_ILP"));
				auditReportDTO.setProjectedMarginRateILP(rs.getDouble("MAR_TOTAL_ILP"));
				auditReportDTO.setProjectedMarginDollarILP(rs.getDouble("MAR_PCT_TOTAL_ILP"));
				auditReportDTO.setProjectedPriceIndex(rs.getDouble("PRICE_INDEX"));
			
			}
			
		}
		catch(SQLException sqlE){
			logger.error("getQuarterlySummary() - Error while getting quarterly details - " + sqlE.toString());
			throw new GeneralException("Error -- getQuarterlySummary()", sqlE);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		
		return auditReportDTO;
	}
	
	
	
	
}
