package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.CriteriaDTO;
import com.pristine.dto.offermgmt.PRConstraintCost;
import com.pristine.dto.offermgmt.PRConstraintFreightCharge;
import com.pristine.dto.offermgmt.PRConstraintGuardRailDetail;
import com.pristine.dto.offermgmt.PRConstraintGuardrail;
import com.pristine.dto.offermgmt.PRConstraintLIG;
import com.pristine.dto.offermgmt.PRConstraintLocPrice;
import com.pristine.dto.offermgmt.PRConstraintLowerHigher;
import com.pristine.dto.offermgmt.PRConstraintMinMax;
import com.pristine.dto.offermgmt.PRConstraintPrePrice;
import com.pristine.dto.offermgmt.PRConstraintRounding;
import com.pristine.dto.offermgmt.PRConstraintThreshold;
import com.pristine.dto.offermgmt.PRConstraintsDTO;
import com.pristine.dto.offermgmt.PRGuidelineBrand;
import com.pristine.dto.offermgmt.PRGuidelineComp;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRGuidelineLeadZoneDTO;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRGuidelinesDTO;
import com.pristine.dto.offermgmt.PRObjectiveDTO;
import com.pristine.dto.offermgmt.PRRoundingTableDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.TempStrategyKey;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

@SuppressWarnings("unused")
public class StrategyDAO implements IDAO{
	private static Logger logger = Logger.getLogger("StrategyDAO");
	
	private static final String GET_ALL_ACTIVE_STRATEGIES = 
			"SELECT LOCATION_LEVEL_ID, " + 
			"   LOCATION_ID, " + 
			"   PRODUCT_LEVEL_ID, " + 
			"   PRODUCT_ID, " + 
			"   STRATEGY_ID, " + 
			"   APPLY_TO, " +
			"   VENDOR_ID, " + 
			"   STATE_ID, " + 
			"   DSD_RECOMMENDATION_FLAG, " + 
			"   START_DATE, " + 
			"   END_DATE, " + 
			"   (END_DATE - START_DATE) AS DAYS, " + 
			"   CRITERIA_ID " + 
			" FROM " + 
			"   (SELECT LOCATION_LEVEL_ID, " + 
			"     LOCATION_ID, " + 
			"     PRODUCT_LEVEL_ID, " + 
			"     PRODUCT_ID, " + 
			"     STRATEGY_ID, " + 
			"     APPLY_TO, " +
			"     VENDOR_ID, " + 
			"     STATE_ID, " + 
			"     DSD_RECOMMENDATION_FLAG, " + 
			"     START_CALENDAR_ID, " + 
			"     END_CALENDAR_ID, " +
			"     CRITERIA_ID, " + 
			"     RCS.START_DATE, " + 
			"     ( " + 
			"     CASE " + 
			"       WHEN RCE.END_DATE IS NULL " + 
			"       THEN RCE.START_DATE " + 
			"        " + 
			"       ELSE RCE.END_DATE " + 
			"     END) AS END_DATE " + 
			"   FROM PR_STRATEGY S " + 
			"   LEFT JOIN RETAIL_CALENDAR RCS " + 
			"   ON S.START_CALENDAR_ID = RCS.CALENDAR_ID " + 
			"   LEFT JOIN RETAIL_CALENDAR RCE " + 
			"   ON S.END_CALENDAR_ID                 = RCE.CALENDAR_ID " + 
					"   WHERE TEMP                          <> 'Y'  AND ACTIVE ='Y'" +
			/* Get all Strategies with Parent and child and current product and all products */
			"   AND ((PRODUCT_LEVEL_ID, PRODUCT_ID) IN ( " + 
			"     (SELECT PRODUCT_LEVEL_ID, " + 
			"       PRODUCT_ID " + 
			"     FROM " + 
			"       (SELECT PRODUCT_ID, " + 
			"         PRODUCT_LEVEL_ID " + 
			"       FROM PRODUCT_GROUP_RELATION_REC PGR " + 
			"         START WITH CHILD_PRODUCT_LEVEL_ID = ? " + 
			"       AND CHILD_PRODUCT_ID                = ? " + 
			"         CONNECT BY PRIOR PRODUCT_ID       = CHILD_PRODUCT_ID " + 
			"       AND PRIOR PRODUCT_LEVEL_ID          = CHILD_PRODUCT_LEVEL_ID " + 
			"       UNION " + 
			"       SELECT CHILD_PRODUCT_ID  AS PRODUCT_ID, " + 
			"         CHILD_PRODUCT_LEVEL_ID AS PRODUCT_LEVEL_ID " + 
			"       FROM PRODUCT_GROUP_RELATION_REC PGR " + 
			"         START WITH PRODUCT_LEVEL_ID       = ? " + 
			"       AND PRODUCT_ID                      = ? " + 
			"         CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID " + 
			"       AND PRIOR CHILD_PRODUCT_LEVEL_ID    = PRODUCT_LEVEL_ID " + 
			"       ) " + 
			"     )) " + 
			"   OR (PRODUCT_LEVEL_ID, PRODUCT_ID) IN ((?, ?)) " + 
			"   OR PRODUCT_LEVEL_ID                = 99) " + 
			/* Get all Strategies with Parent and child and current product and all products */
			/* Get all Strategies of current zone and its store, store list, zone list, division, chain*/
			  
			" AND  ((LOCATION_LEVEL_ID            = 5  " + 
			" AND LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) " + 
			" OR (LOCATION_LEVEL_ID = 7 AND LOCATION_ID IN (SELECT DISTINCT LOCATION_ID FROM " + 
			" ( SELECT LOCATION_ID, CHILD_LOCATION_ID, CHILD_LOCATION_LEVEL_ID FROM LOCATION_GROUP_RELATION PGR " + 
			" WHERE LOCATION_LEVEL_ID = 7 ) WHERE CHILD_LOCATION_LEVEL_ID = 5  " + 														
			" AND CHILD_LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?)))  " + 
			"   OR (LOCATION_LEVEL_ID            = ? " + 
			"   AND LOCATION_ID                    = ?) " + 
			"   OR (LOCATION_LEVEL_ID              = 21 " + 
			"   AND LOCATION_ID                   IN " + 
			"     (SELECT LOCATION_ID " + 
			"     FROM " + 
			"       (SELECT CHILD_LOCATION_ID, " + 
			"         CHILD_LOCATION_LEVEL_ID, " + 
			"         LOCATION_LEVEL_ID, " + 
			"         LOCATION_ID " + 
			"       FROM LOCATION_GROUP_RELATION PGR " + 
			"       WHERE LOCATION_LEVEL_ID = 21 " + 
			"       ) " + 
			"     WHERE CHILD_LOCATION_LEVEL_ID = ? " + 
			"     AND CHILD_LOCATION_ID         = ? " + 
			"     )) " + 
			"   OR (LOCATION_LEVEL_ID = 2 " + 
			"   AND LOCATION_ID       = ?) " + 
			"   OR (LOCATION_LEVEL_ID = 1 " + 
			"   AND LOCATION_ID       = " + 
			"     (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y' " + 
			"     )) )) " + 
			"   ) " + 
			" WHERE (START_DATE <= TO_DATE(?, 'MM/DD/YYYY') " + 
			" AND END_DATE      >= TO_DATE(?, 'MM/DD/YYYY')) " + 
			" OR (START_DATE    <= TO_DATE(?, 'MM/DD/YYYY') " + 
			" AND END_DATE      IS NULL) " + 
			" ORDER BY LOCATION_LEVEL_ID, " + 
			"   LOCATION_ID, " + 
			"   PRODUCT_LEVEL_ID, " + 
			"   PRODUCT_ID, " + 
			"   DAYS";
	
	private static final String GET_STRATEGY_DETAIL = "SELECT STGY.STRATEGY_ID, STGY.LOCATION_LEVEL_ID, STGY.LOCATION_ID, STGY.PRODUCT_LEVEL_ID, STGY.PRODUCT_ID, "
			+ "STGY.VENDOR_ID, STGY.STATE_ID, STGY.DSD_RECOMMENDATION_FLAG, STGY.APPLY_TO, STGY.CRITERIA_ID, "
			+ "OBJT.OBJECTIVE_TYPE_ID, OBJT.OBJECTIVE_TYPE_NAME, OBJ.MIN_OBJECTIVE_VALUE, OBJ.MAX_OBJECTIVE_VALUE, "
			+ "GUID.GUIDELINE_ID, GUID.GUIDELINE_TYPE_ID, GUID.EXEC_ORDER, GUID.EXCLUDE, "
			+ "GM.GUIDELINE_MARGIN_ID, GM.VALUE_TYPE AS MARGIN_VALUE_TYPE, GM.MIN_MARGIN_PCT, GM.MAX_MARGIN_PCT, GM.MIN_MARGIN_DOLLAR, "
			+ "GM.MAX_MARGIN_DOLLAR, GM.CURRENT_MARGIN_FLAG,GM.ITEM_FLAG, "
			+ "GM.COST_FLAG AS MARGIN_COST_FLAG, GM.IS_ITEM_LEVEL AS MAR_IS_ITEM_LEVEL, "
			+ "GPI.GUIDELINE_PI_ID, GPI.MIN_VALUE AS PI_MIN_VALUE, GPI.MAX_VALUE AS PI_MAX_VALUE, "
			+ "GPI.IS_ITEM_LEVEL AS PI_IS_ITEM_LEVEL, GPI.COMP_STR_ID AS PI_COMP_STR_ID,"
			+ "GC.GUIDELINE_COMP_ID, GCR1.OPERATOR_TEXT AS COMP_OPERATOR_TEXT, "
			+ "GC.GROUP_PRICE_TYPE, GC.LATEST_PRICE_OBSERVATION, GCD.GUIDELINE_COMP_DETAIL_ID, GCD.COMP_STR_ID,GRCD.ZONE_ID, "
			+ "GCD.VALUE_TYPE AS COMP_DETAIL_VALUE_TYPE,GCR2.OPERATOR_TEXT AS COMP_DETAIL_OPERATOR_TEXT, "
			+ "GCD.MIN_VALUE AS COMP_DETAIL_MIN_VALUE, GCD.MAX_VALUE AS COMP_DETAIL_MAX_VALUE, "
			+ "GCD.EXCLUDE AS COMP_DETAIL_EXCLUDE, GCD.GUIDELINE_TEXT, "
			+ "GB.GUIDELINE_BRAND_ID, GB.VALUE_TYPE AS BRAND_VALUE_TYPE, GB.MIN_VALUE AS BRAND_MIN_VALUE, GB.MAX_VALUE AS BRAND_MAX_VALUE, "
			+ "GB.RETAIL_TYPE AS BRAND_RETAIL_TYPE, GBR.OPERATOR_TEXT AS BRAND_OPERATOR_TEXT, GB.BRAND_TIER_ID1, "
			+ "GB.BRAND_TIER_ID2, GB.BRAND_ID1, GB.BRAND_ID2, GUID.AUC_OVERRIDE, GUID.COMP_OVERRIDE,GUID.COMP_OVERRIDE_ID, "
			+ "GS.GUIDELINE_SIZE_ID, GS.VALUE_TYPE AS SIZE_VALUE_TYPE, GS.MIN_VALUE AS SIZE_MIN_VALUE, GS.MAX_VALUE AS SIZE_MAX_VALUE, "
			+ "GSR.OPERATOR_TEXT AS SIZE_OPERATOR_TEXT, GS.HIGHER_TO_LOWER_FLAG, GS.SHELF_VALUE, "
			+ "GLZ.GUIDELINE_LEAD_ZONE_ID, GLZ.LOCATION_LEVEL_ID AS LZ_LOCATION_LEVEL_ID, GLZ.LOCATION_ID AS LZ_LOCATION_ID, "
			+ "GLZ.VALUE_TYPE AS LZ_VALUE_TYPE, GLZ.MIN_VALUE AS LZ_MIN_VALUE, GLZ.MAX_VALUE AS LZ_MAX_VALUE, "
			+ "GLZR.OPERATOR_TEXT AS LEAD_ZONE_OPERATOR_TEXT, " + "CONS.CONSTRAINT_ID, CONS.CONSTRAINT_TYPE_ID, "
			+ "CP.CONSTRAINT_PREPRICE_ID, CP.VALUE AS PRE_PRICE_VALUE, CP.QUANTITY AS PRE_PRICE_QUANTITY, "
			+ "CL.CONSTRAINT_LOCPRICE_ID, CL.VALUE AS LOC_PRICE_VALUE, CL.QUANTITY AS LOC_PRICE_QUANTITY, "
			+ "CMM.CONSTRAINT_MINMAX_ID, CMM.MIN_VALUE AS MM_MIN_VALUE, CMM.MAX_VALUE AS MM_MAX_VALUE, CMM.QUANTITY AS MM_QUANTITY, "
			+ "CR.CONSTRAINT_ROUNDING_ID, CR.ROUNDING_DIGITS, CR.DEFAULT_ROUNDING, CR.ROUNDING_TABLE_ID, "
			+ "CT.CONSTRAINT_THRESHOLD_ID, CT.VALUE_TYPE AS THRESHOLD_VALUE_TYPE, CT.MIN_VALUE AS THRESHOLD_MIN_VALUE, "
			+ "CT.MAX_VALUE AS THRESHOLD_MAX_VALUE, CT.MAX_VALUE2 AS THRESHOLD_MAX_VALUE2, "
			+ "CC.CONSTRAINT_COST_ID, CC.RECOMMEND_BELOW_COST, " + "CLIG.CONSTRAINT_LIG_ID, CLIG.LIG_FLAG, "
			+ "CLH.CONSTRAINT_LH_RETAIL_ID, CLH.RECOMMEND_RETAIL_FLAG, "
			+ "GRC.CONSTRAINT_GUARDRAIL_ID,GRC.GROUP_PRICE_TYPE AS GR_GROUP_PRICE_TYPE, "
			+ "GRC.LATEST_PRICE_OBSERVATION AS GR_PR_CHG_OBV,GRC.RELN_OPERATOR_ID AS GC_RELN_OPERATOR_ID, "
			+ "GCOT.OPERATOR_TEXT AS GC_RELN_OPERATOR_TEXT, CONS.CONSTRAINT_TEXT,  GRCD.COMP_STR_ID AS GUARDRAIL_COMP_ID, "
			+ "GRCD.VALUE_TYPE AS GURADRAIL_DETAIL_VALUE_TYPE,GR2.OPERATOR_TEXT AS GUARDRAIL_OPERATOR_TEXT, " 
			+ "GRCD.MIN_VALUE AS GUARDRAIL_MIN_VALUE, GRCD.MAX_VALUE AS GUARDRAIL_MAX_VALUE, " 
			+ "GRCD.EXCLUDE AS GUARDRAIL_EXCLUDE, GRCD.GUIDELINE_TEXT AS GUARDRAIL_GUIDELINE_TEXT,GRCD.GUARDRAIL_COMP_DETAIL_ID, "
			+ "TIER1.BRAND_TIER_NAME AS BR_TIER1, TIER2.BRAND_TIER_NAME AS BR_TIER2,GM.PRICE_INCREASE,GM.PRICE_DECREASE,CT.OVERRIDE_THRESHOLD " + "FROM PR_STRATEGY STGY "
			+ "LEFT JOIN PR_OBJECTIVE OBJ ON STGY.STRATEGY_ID = OBJ.STRATEGY_ID "
			+ "LEFT JOIN PR_OBJECTIVE_TYPE OBJT ON OBJ.OBJECTIVE_TYPE_ID = OBJT.OBJECTIVE_TYPE_ID "
			+ "LEFT JOIN PR_GUIDELINE GUID ON STGY.STRATEGY_ID = GUID.STRATEGY_ID "
			+ "LEFT JOIN PR_GUIDELINE_MARGIN GM ON GUID.GUIDELINE_ID = GM.GUIDELINE_ID "
			+ "LEFT JOIN PR_GUIDELINE_PI GPI ON GUID.GUIDELINE_ID = GPI.GUIDELINE_ID "
			+ "LEFT JOIN PR_GUIDELINE_COMP GC ON GUID.GUIDELINE_ID = GC.GUIDELINE_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GCR1 ON GC.RELN_OPERATOR_ID = GCR1.RELN_OPERATOR_ID  "
			+ "LEFT JOIN PR_GUIDELINE_COMP_DETAIL GCD ON GC.GUIDELINE_ID = GCD.GUIDELINE_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GCR2 ON  GCD.RELN_OPERATOR_ID = GCR2.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_GUIDELINE_BRAND GB ON GUID.GUIDELINE_ID = GB.GUIDELINE_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GBR ON GB.RELN_OPERATOR_ID = GBR.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_GUIDELINE_SIZE GS ON GUID.GUIDELINE_ID = GS.GUIDELINE_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GSR ON GS.RELN_OPERATOR_ID = GSR.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_GUIDELINE_LEAD_ZONE GLZ ON GUID.GUIDELINE_ID = GLZ.GUIDELINE_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GLZR ON GLZ.RELN_OPERATOR_ID = GLZR.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_CONSTRAINT CONS ON STGY.STRATEGY_ID = CONS.STRATEGY_ID "
			+ "LEFT JOIN PR_CONSTRAINT_LOCPRICE CL ON CONS.CONSTRAINT_ID = CL.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_PREPRICE CP ON CONS.CONSTRAINT_ID = CP.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_MINMAX CMM ON CONS.CONSTRAINT_ID = CMM.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_ROUNDING CR ON CONS.CONSTRAINT_ID = CR.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_THRESHOLD CT ON CONS.CONSTRAINT_ID = CT.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_LH_RETAIL CLH ON CONS.CONSTRAINT_ID = CLH.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_LIG CLIG  ON CONS.CONSTRAINT_ID = CLIG.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_COST CC ON CONS.CONSTRAINT_ID = CC.CONSTRAINT_ID "
			+ "LEFT JOIN PR_CONSTRAINT_GUARDRAIL GRC ON CONS.CONSTRAINT_ID = GRC.CONSTRAINT_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GCOT ON GRC.RELN_OPERATOR_ID = GCOT.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_GUARDRAIL_COMP_DETAIL GRCD ON GRCD.CONSTRAINT_ID  = GRC.CONSTRAINT_ID  " 
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP GR2 ON  GR2.RELN_OPERATOR_ID = GRCD.RELN_OPERATOR_ID " 
			+ "LEFT JOIN PR_BRAND_TIER_LOOKUP TIER1 ON TIER1.BRAND_TIER_ID = GB.BRAND_TIER_ID1 "
			+ "LEFT JOIN PR_BRAND_TIER_LOOKUP TIER2 ON TIER2.BRAND_TIER_ID = GB.BRAND_TIER_ID2 "
			+ "WHERE STGY.STRATEGY_ID = ?";

	private static final String GET_DEFAULT_ROUNDING_TABLE = "SELECT ROUNDING_TABLE_ID FROM PR_ROUNDING_TABLE WHERE IS_DEFAULT = 'Y'";
	
	private static final String GET_ROUNDING_TABLE_CONTENT = 
			"SELECT START_PRICE, END_PRICE, ALLOWED_END_DIGITS, ALLOWED_PRICES, " +
			"EXCLUDED_PRICES FROM PR_ROUNDING_TABLE_DETAIL WHERE ROUNDING_TABLE_ID = ?";
	
	private static final String GET_STRATEGY_ID = "SELECT DISTINCT(STRATEGY_ID) FROM PR_STRATEGY";
	
	private static final String GET_GUIDELINE_COMP = "SELECT GC.GUIDELINE_COMP_ID, GC.GUIDELINE_ID, RO1.OPERATOR_TEXT AS COMP_OPERATOR_TEXT, "
			+ "GC.GROUP_PRICE_TYPE, GC.LATEST_PRICE_OBSERVATION, CD.GUIDELINE_COMP_DETAIL_ID, CD.COMP_STR_ID, CD.VALUE_TYPE AS COMP_DETAIL_VALUE_TYPE,"
			+ "RO2.OPERATOR_TEXT AS COMP_DETAIL_OPERATOR_TEXT, CD.MIN_VALUE AS COMP_DETAIL_MIN_VALUE, CD.MAX_VALUE AS COMP_DETAIL_MAX_VALUE, "
			+ "CD.EXCLUDE AS COMP_DETAIL_EXCLUDE, CD.GUIDELINE_TEXT, GC.IGN_CMP_PRC_BLW_CST FROM PR_GUIDELINE_COMP GC "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP RO1 ON GC.RELN_OPERATOR_ID = RO1.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_GUIDELINE_COMP_DETAIL CD ON GC.GUIDELINE_ID = CD.GUIDELINE_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP RO2 ON  CD.RELN_OPERATOR_ID = RO2.RELN_OPERATOR_ID " + "WHERE GC.GUIDELINE_ID = ?"
			+ "ORDER BY CD.GUIDELINE_COMP_DETAIL_ID";
	
	
	private static final String GET_CRITERIA_LIST = "SELECT CRITERIA_ID, CRITERIA_DETAIL_ID, CRITERIA_TYPE_ID, "
			+ " RO.OPERATOR_TEXT, VALUE_TYPE, VALUE  FROM PR_CRITERIA_DETAIL CD"
			+ " LEFT JOIN PR_RELATION_OPERATOR_LOOKUP RO ON RO.RELN_OPERATOR_ID = CD.RELN_OPERATOR_ID"
			+ " WHERE CRITERIA_ID IN (%s) "; 
	
	private static final String GET_TEMP_STRATEGIES = 
			"SELECT LOCATION_LEVEL_ID, " + 
					"   LOCATION_ID, " + 
					"   PRODUCT_LEVEL_ID, " + 
					"   PRODUCT_ID, " + 
					"   STRATEGY_ID, " + 
					"   APPLY_TO, " +
					"   VENDOR_ID, " + 
					"   STATE_ID, " + 
					"   DSD_RECOMMENDATION_FLAG, " + 
					"   START_DATE, " + 
					"   END_DATE, " + 
					"   (END_DATE - START_DATE) AS DAYS, " + 
					"   CRITERIA_ID " + 
					" FROM " + 
					"   (SELECT LOCATION_LEVEL_ID, " + 
					"     LOCATION_ID, " + 
					"     PRODUCT_LEVEL_ID, " + 
					"     PRODUCT_ID, " + 
					"     STRATEGY_ID, " + 
					"     APPLY_TO, " +
					"     VENDOR_ID, " + 
					"     STATE_ID, " + 
					"     DSD_RECOMMENDATION_FLAG, " + 
					"     START_CALENDAR_ID, " + 
					"     END_CALENDAR_ID, " +
					"     CRITERIA_ID, " + 
					"     RCS.START_DATE, " + 
					"     ( " + 
					"     CASE " + 
					"       WHEN RCE.END_DATE IS NULL " + 
					"       THEN RCE.START_DATE " + 
					"        " + 
					"       ELSE RCE.END_DATE " + 
					"     END) AS END_DATE " + 
					"   FROM PR_STRATEGY S " + 
					"   LEFT JOIN RETAIL_CALENDAR RCS " + 
					"   ON S.START_CALENDAR_ID = RCS.CALENDAR_ID " + 
					"   LEFT JOIN RETAIL_CALENDAR RCE " + 
					"   ON S.END_CALENDAR_ID                 = RCE.CALENDAR_ID " + 
							"   WHERE TEMP                          = 'Y'  AND ACTIVE ='Y'" +
					/* Get all Strategies with Parent and child and current product and all products */
					"   AND ((PRODUCT_LEVEL_ID, PRODUCT_ID) IN ( " + 
					"     (SELECT PRODUCT_LEVEL_ID, " + 
					"       PRODUCT_ID " + 
					"     FROM " + 
					"       (SELECT PRODUCT_ID, " + 
					"         PRODUCT_LEVEL_ID " + 
					"       FROM PRODUCT_GROUP_RELATION_REC PGR " + 
					"         START WITH CHILD_PRODUCT_LEVEL_ID = ? " + 
					"       AND CHILD_PRODUCT_ID                = ? " + 
					"         CONNECT BY PRIOR PRODUCT_ID       = CHILD_PRODUCT_ID " + 
					"       AND PRIOR PRODUCT_LEVEL_ID          = CHILD_PRODUCT_LEVEL_ID " + 
					"       UNION " + 
					"       SELECT CHILD_PRODUCT_ID  AS PRODUCT_ID, " + 
					"         CHILD_PRODUCT_LEVEL_ID AS PRODUCT_LEVEL_ID " + 
					"       FROM PRODUCT_GROUP_RELATION_REC PGR " + 
					"         START WITH PRODUCT_LEVEL_ID       = ? " + 
					"       AND PRODUCT_ID                      = ? " + 
					"         CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID " + 
					"       AND PRIOR CHILD_PRODUCT_LEVEL_ID    = PRODUCT_LEVEL_ID " + 
					"       ) " + 
					"     )) " + 
					"   OR (PRODUCT_LEVEL_ID, PRODUCT_ID) IN ((?, ?)) " + 
					"   OR PRODUCT_LEVEL_ID                = 99) " + 
					/* Get all Strategies with Parent and child and current product and all products */
					/* Get all Strategies of current zone and its store, store list, zone list, division, chain*/
					  
					" AND  ((LOCATION_LEVEL_ID            = 5  " + 
					" AND LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) " + 
					" OR (LOCATION_LEVEL_ID = 7 AND LOCATION_ID IN (SELECT DISTINCT LOCATION_ID FROM " + 
					" ( SELECT LOCATION_ID, CHILD_LOCATION_ID, CHILD_LOCATION_LEVEL_ID FROM LOCATION_GROUP_RELATION PGR " + 
					" WHERE LOCATION_LEVEL_ID = 7 ) WHERE CHILD_LOCATION_LEVEL_ID = 5  " + 														
					" AND CHILD_LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?)))  " + 
					"   OR (LOCATION_LEVEL_ID            = ? " + 
					"   AND LOCATION_ID                    = ?) " + 
					"   OR (LOCATION_LEVEL_ID              = 21 " + 
					"   AND LOCATION_ID                   IN " + 
					"     (SELECT LOCATION_ID " + 
					"     FROM " + 
					"       (SELECT CHILD_LOCATION_ID, " + 
					"         CHILD_LOCATION_LEVEL_ID, " + 
					"         LOCATION_LEVEL_ID, " + 
					"         LOCATION_ID " + 
					"       FROM LOCATION_GROUP_RELATION PGR " + 
					"       WHERE LOCATION_LEVEL_ID = 21 " + 
					"       ) " + 
					"     WHERE CHILD_LOCATION_LEVEL_ID = ? " + 
					"     AND CHILD_LOCATION_ID         = ? " + 
					"     )) " + 
					"   OR (LOCATION_LEVEL_ID = 2 " + 
					"   AND LOCATION_ID       = ?) " + 
					"   OR (LOCATION_LEVEL_ID = 1 " + 
					"   AND LOCATION_ID       = " + 
					"     (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y' " + 
					"     )) )) " + 
					"   ) " + 
					" WHERE (START_DATE <= TO_DATE(?, 'MM/DD/YYYY') " + 
					" AND END_DATE      >= TO_DATE(?, 'MM/DD/YYYY')) " + 
					" OR (START_DATE    <= TO_DATE(?, 'MM/DD/YYYY') " + 
					" AND END_DATE      IS NULL) " + 
					" ORDER BY LOCATION_LEVEL_ID, " + 
					"   LOCATION_ID, " + 
					"   PRODUCT_LEVEL_ID, " + 
					"   PRODUCT_ID, " + 
					"   DAYS";
	
	private static final String GET_CONSTRAINT_GUARDRAIL = "SELECT GRC.CONSTRAINT_GUARDRAIL_ID, GRC.CONSTRAINT_ID, RO1.OPERATOR_TEXT AS GUARDRAIL_OPERATOR_TEXT, "
			+ "GRC.GROUP_PRICE_TYPE, GRC.LATEST_PRICE_OBSERVATION, GRD.GUARDRAIL_COMP_DETAIL_ID, GRD.COMP_STR_ID, GRD.VALUE_TYPE AS GUARDRAIL_VALUE_TYPE,"
			+ "RO2.OPERATOR_TEXT AS GUARDRAIL_DETAIL_OPERATOR_TEXT, GRD.MIN_VALUE AS GUARDRAIL_MIN_VALUE, GRD.MAX_VALUE AS GUARDRAIL_MAX_VALUE, GRD.ZONE_ID,GRD.IS_ZONE,  "
			+ "GRD.EXCLUDE AS GUARDRAIL_EXCLUDE, GRD.GUIDELINE_TEXT, GRC.IGN_CMP_PRC_BLW_CST FROM PR_CONSTRAINT_GUARDRAIL GRC "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP RO1 ON GRC.RELN_OPERATOR_ID = RO1.RELN_OPERATOR_ID "
			+ "LEFT JOIN PR_GUARDRAIL_COMP_DETAIL GRD ON GRC.CONSTRAINT_ID = GRD.CONSTRAINT_ID "
			+ "LEFT JOIN PR_RELATION_OPERATOR_LOOKUP RO2 ON  GRD.RELN_OPERATOR_ID = RO2.RELN_OPERATOR_ID " + " WHERE GRC.CONSTRAINT_ID = ?";
	
	private static final String GET_GLOBAL_STRATEGY = "SELECT STRATEGY_ID FROM PR_STRATEGY where LOCATION_ID=100 and PRODUCT_LEVEL_ID=99"
			+ " AND APPLY_TO=-1 AND ACTIVE='Y'";
	
	
	
	/*Note - As a part of improvement, the strategy detail (obj, guideline, constraints) is fetched from the 
	 * single query to avoid multiple db calls. 
	 * But the comp guideline alone is taken from a separate call as the order of stores doesn't 
	 * match as shown in the screen from that single query. So comp guideline is taken using separate query 
	 * (this can be avoided if there is order in comp table). 
	 * 
	 */
	
	public List<Long> GetAllStrategyId(Connection conn) throws GeneralException {
		List<Long> allStrategyIds = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_STRATEGY_ID);
			rs = stmt.executeQuery();
			while (rs.next()) {
				allStrategyIds.add(rs.getLong("STRATEGY_ID"));
			}
		} catch (SQLException exception) {
			logger.error("Error in GetAllStrategyId() - " + exception);
			throw new GeneralException("Error in GetAllStrategyId() - " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return allStrategyIds;
	}
	
	/**
	 * Retrieves all strategies for input location from strategy table 
	 * @param conn
	 * @return
	 */
	public HashMap<StrategyKey, List<PRStrategyDTO>> getAllActiveStrategies(Connection conn, 
			PRStrategyDTO strategyInput, int divisionIdOfZone) throws OfferManagementException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<StrategyKey, List<PRStrategyDTO>> locationProductStrategyMap = 
				new HashMap<StrategyKey, List<PRStrategyDTO>>();
		List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
		String sql = GET_ALL_ACTIVE_STRATEGIES;
		
		try{
			stmt = conn.prepareStatement(sql);
			int counter = 0;
			stmt.setInt(++counter, strategyInput.getProductLevelId());
			stmt.setInt(++counter, strategyInput.getProductId());
			stmt.setInt(++counter, strategyInput.getProductLevelId());
			stmt.setInt(++counter, strategyInput.getProductId());
			stmt.setInt(++counter, strategyInput.getProductLevelId());
			stmt.setInt(++counter, strategyInput.getProductId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, strategyInput.getLocationLevelId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, strategyInput.getLocationLevelId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, divisionIdOfZone);
			stmt.setString(++counter, strategyInput.getStartDate());
			stmt.setString(++counter, strategyInput.getEndDate());
			stmt.setString(++counter, strategyInput.getStartDate());
			
//			logger.debug("Strategy Fetching Query:" + sql);
//			logger.debug("Location Level Id: " + strategyInput.getLocationLevelId() + ", Location Id: " + strategyInput.getLocationId() + 
//					", Product Level Id: " + strategyInput.getProductLevelId() + ",Product Id: " + strategyInput.getProductId() + 
//					", division Id:" + divisionIdOfZone + ",Start Date: " + strategyInput.getStartDate() + ",End Date: " + strategyInput.getEndDate());
	
			rs = stmt.executeQuery();
			
			while(rs.next()){
				PRStrategyDTO strategyDTO = new PRStrategyDTO();
				strategyDTO.setStrategyId(rs.getLong("STRATEGY_ID"));
				strategyDTO.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				strategyDTO.setLocationId(rs.getInt("LOCATION_ID"));
				strategyDTO.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				strategyDTO.setProductId(rs.getInt("PRODUCT_ID"));

				strategyDTO.setPriceCheckListId(rs.getInt("APPLY_TO"));
				strategyDTO.setVendorId(rs.getInt("VENDOR_ID"));
				strategyDTO.setStateId(rs.getInt("STATE_ID"));
				
				strategyDTO.setDsdRecommendationFlag(rs.getString("DSD_RECOMMENDATION_FLAG").charAt(0));
				strategyDTO.setCriteriaId(rs.getInt("CRITERIA_ID"));
				
				StrategyKey strategyKey = new StrategyKey(strategyDTO.getLocationLevelId(), strategyDTO.getLocationId(), 
						strategyDTO.getProductLevelId(), strategyDTO.getProductId());
				
//				logger.debug("Strategy key: " + strategyKey.toString());
				
				//If any strategy available for the strategy key
				if(locationProductStrategyMap.get(strategyKey) == null){				
					strategyList = new ArrayList<PRStrategyDTO>();
				}else{
					strategyList = locationProductStrategyMap.get(strategyKey);
				}
				
				PRStrategyDTO strategyFullDetails = getStrategyDefinition(conn, strategyDTO.getStrategyId());
				//strategyFullDetails.copy(strategyDTO);
				
				strategyList.add(strategyFullDetails);			
				locationProductStrategyMap.put(strategyKey, strategyList);
			}
		}catch(Exception exception){
			throw new OfferManagementException("Error in getAllActiveStrategies() - " + exception, 
					RecommendationErrorCode.DB_GET_ALL_STRATEGIES);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return locationProductStrategyMap;
	}
	
	public PRStrategyDTO getStrategyDefinition(Connection conn, long strategyId) throws OfferManagementException{
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		List<StrategyDetail> strategyDetails;
		try {
			strategyDetails = getStrategyDetail(conn, strategyId);
			strategyDTO = getStrategy(strategyDetails, strategyId);
			if(strategyDTO == null) {
				throw new OfferManagementException("Error -- Recommendation is not updated. Strategy is deleted!",
						RecommendationErrorCode.STRATEGY_DELETED);
			}
			strategyDTO.setObjective(getObjective(strategyDetails, strategyId));
			strategyDTO.setGuidelines(getGuidelines(conn, strategyDetails, strategyId));
			strategyDTO.setConstriants(getConstraints(conn, strategyDetails, strategyId));
		} catch (GeneralException e) {
			throw new OfferManagementException("Error in getStrategyDefinition() - " + e, 
					RecommendationErrorCode.DB_GET_STRATEGY_DEFINITION);
		}
		return strategyDTO;
	}
	
	/**
	 * Get Full Detail of a Strategy (Objective, Guideline, Constraints) in a single query	
	 * This function is written to avoid many db calls to get the strategy details
	 * @param conn
	 * @param strategyId
	 * @return
	 * @throws GeneralException
	 */
	private List<StrategyDetail> getStrategyDetail(Connection conn, long strategyId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<StrategyDetail> strategyDetails = new ArrayList<StrategyDetail>();

		try {
			stmt = conn.prepareStatement(GET_STRATEGY_DETAIL);
			stmt.setLong(1, strategyId);

//			logger.debug("Strategy Detail Qry:" + GET_STRATEGY_DETAIL);
//			logger.debug("Strategy Id:" + strategyId);
			
			rs = stmt.executeQuery();
			while (rs.next()) {
				StrategyDetail strategyDetail = new StrategyDetail();
				strategyDetail.strategyId = rs.getLong("STRATEGY_ID");
				strategyDetail.locationLevelId = rs.getInt("LOCATION_LEVEL_ID");
				strategyDetail.locationId = rs.getInt("LOCATION_ID");
				strategyDetail.productLevelId = rs.getInt("PRODUCT_LEVEL_ID");
				strategyDetail.productId = rs.getInt("PRODUCT_ID");
				strategyDetail.applyTo = rs.getInt("APPLY_TO");
				strategyDetail.vendorId = rs.getInt("VENDOR_ID");
				strategyDetail.stateId = rs.getInt("STATE_ID");
				strategyDetail.criteriaId = rs.getInt("CRITERIA_ID");
				strategyDetail.dsdRecommendationFlag = rs.getString("DSD_RECOMMENDATION_FLAG").charAt(0);

				strategyDetail.objectiveTypeId = rs.getInt("OBJECTIVE_TYPE_ID");
				strategyDetail.objectiveTypeName = rs.getString("OBJECTIVE_TYPE_NAME");
				strategyDetail.minObjVal = rs.getDouble("MIN_OBJECTIVE_VALUE");
				strategyDetail.maxObjVal = rs.getDouble("MAX_OBJECTIVE_VALUE");

				strategyDetail.guidelineId = rs.getInt("GUIDELINE_ID");
				strategyDetail.guidelineTypeId = rs.getInt("GUIDELINE_TYPE_ID");
				strategyDetail.guidelineExecOrder = rs.getInt("EXEC_ORDER");
				if (rs.getObject("EXCLUDE") != null)
					strategyDetail.isExcludeGuideline = ((rs.getString("EXCLUDE").charAt(0) == Constants.YES) ? true : false);

				strategyDetail.guidelineMarginId = rs.getLong("GUIDELINE_MARGIN_ID");
				if (rs.getObject("MARGIN_VALUE_TYPE") != null)
					strategyDetail.marginValueType = rs.getString("MARGIN_VALUE_TYPE").charAt(0);
				if (rs.getObject("CURRENT_MARGIN_FLAG") != null)
					strategyDetail.currentMargin = rs.getString("CURRENT_MARGIN_FLAG").charAt(0);
				if (rs.getObject("MIN_MARGIN_PCT") != null)
					strategyDetail.minMarginPct = rs.getFloat("MIN_MARGIN_PCT");
				else
					strategyDetail.minMarginPct = Constants.DEFAULT_NA;
				if (rs.getObject("MAX_MARGIN_PCT") != null)
					strategyDetail.maxMarginPct = rs.getFloat("MAX_MARGIN_PCT");
				else
					strategyDetail.maxMarginPct = Constants.DEFAULT_NA;
				if (rs.getObject("MIN_MARGIN_DOLLAR") != null)
					strategyDetail.minMargin$ = rs.getDouble("MIN_MARGIN_DOLLAR");
				else
					strategyDetail.minMargin$ = Constants.DEFAULT_NA;
				if (rs.getObject("MAX_MARGIN_DOLLAR") != null)
					strategyDetail.maxMargin$ = rs.getDouble("MAX_MARGIN_DOLLAR");
				else
					strategyDetail.maxMargin$ = Constants.DEFAULT_NA;
				
				if(rs.getString("MARGIN_COST_FLAG") != null)
					strategyDetail.marginCostFlag =  rs.getString("MARGIN_COST_FLAG").charAt(0);
				
				if(rs.getString("MAR_IS_ITEM_LEVEL") != null)
					strategyDetail.marginIsItemLevel = rs.getString("MAR_IS_ITEM_LEVEL").charAt(0);
				
				//added for FF to store the item Flag
				if(rs.getString("ITEM_FLAG") != null)
				strategyDetail.itemFlag=rs.getString("ITEM_FLAG").charAt(0);

				strategyDetail.guidelineCompId = rs.getLong("GUIDELINE_COMP_ID");
				strategyDetail.compOperatorText = rs.getString("COMP_OPERATOR_TEXT");
				if (rs.getObject("GROUP_PRICE_TYPE") != null)
					strategyDetail.groupPriceType = rs.getString("GROUP_PRICE_TYPE").charAt(0);
				strategyDetail.latestPriceObservation = rs.getInt("LATEST_PRICE_OBSERVATION");

				strategyDetail.guidelineCompDetailId = rs.getInt("GUIDELINE_COMP_DETAIL_ID");
				if (rs.getObject("COMP_DETAIL_EXCLUDE") != null)
					strategyDetail.compDetailExclude = rs.getString("COMP_DETAIL_EXCLUDE").charAt(0);
				strategyDetail.compStrId = rs.getInt("COMP_STR_ID");
				if (rs.getObject("COMP_DETAIL_VALUE_TYPE") != null)
					strategyDetail.compDetailValueType = rs.getString("COMP_DETAIL_VALUE_TYPE").charAt(0);
				else
					strategyDetail.compDetailValueType = 'O';
				if (rs.getObject("COMP_DETAIL_MIN_VALUE") != null)
					strategyDetail.compDetailMinValue = rs.getFloat("COMP_DETAIL_MIN_VALUE");
				else
					strategyDetail.compDetailMinValue = Constants.DEFAULT_NA;
				if (rs.getObject("COMP_DETAIL_MAX_VALUE") != null)
					strategyDetail.compDetailMaxValue = rs.getFloat("COMP_DETAIL_MAX_VALUE");
				else
					strategyDetail.compDetailMaxValue = Constants.DEFAULT_NA;

				if (rs.getObject("COMP_DETAIL_OPERATOR_TEXT") != null)
					strategyDetail.compDetailOperatorText = rs.getString("COMP_DETAIL_OPERATOR_TEXT");
				else
					strategyDetail.compDetailOperatorText = "";
				if (rs.getObject("GUIDELINE_TEXT") != null)
					strategyDetail.guidelineText = rs.getString("GUIDELINE_TEXT");
				else
					strategyDetail.guidelineText = "";

				strategyDetail.guidelineBrandId = rs.getInt("GUIDELINE_BRAND_ID");

				if (rs.getObject("BRAND_VALUE_TYPE") != null)
					strategyDetail.brandValueType = rs.getString("BRAND_VALUE_TYPE").charAt(0);
				else
					strategyDetail.brandValueType = 'O';
				if (rs.getObject("BRAND_MIN_VALUE") != null)
					strategyDetail.brandMinValue = rs.getFloat("BRAND_MIN_VALUE");
				else
					strategyDetail.brandMinValue = Constants.DEFAULT_NA;
				if (rs.getObject("BRAND_MAX_VALUE") != null)
					strategyDetail.brandMaxValue = rs.getFloat("BRAND_MAX_VALUE");
				else
					strategyDetail.brandMaxValue = Constants.DEFAULT_NA;
				if (rs.getObject("BRAND_RETAIL_TYPE") != null)
					strategyDetail.brandRetailType = rs.getString("BRAND_RETAIL_TYPE").charAt(0);
				strategyDetail.brandOperatorText = rs.getString("BRAND_OPERATOR_TEXT");
				strategyDetail.brandTierId1 = rs.getInt("BRAND_TIER_ID1");
				strategyDetail.brandTierId2 = rs.getInt("BRAND_TIER_ID2");
				strategyDetail.brandId1 = rs.getInt("BRAND_ID1");
				strategyDetail.brandId2 = rs.getInt("BRAND_ID2");

				strategyDetail.guidelineSizeId = rs.getInt("GUIDELINE_SIZE_ID");
				if (rs.getObject("SIZE_VALUE_TYPE") != null)
					strategyDetail.sizeValueType = rs.getString("SIZE_VALUE_TYPE").charAt(0);
				else
					strategyDetail.sizeValueType = 'O';
				if (rs.getObject("SIZE_MIN_VALUE") != null)
					strategyDetail.sizeMinValue = rs.getFloat("SIZE_MIN_VALUE");
				else
					strategyDetail.sizeMinValue = Constants.DEFAULT_NA;
				if (rs.getObject("SIZE_MAX_VALUE") != null)
					strategyDetail.sizeMaxValue = rs.getFloat("SIZE_MAX_VALUE");
				else
					strategyDetail.sizeMaxValue = Constants.DEFAULT_NA;
				strategyDetail.sizeOperatorText = rs.getString("SIZE_OPERATOR_TEXT");
				if (rs.getObject("HIGHER_TO_LOWER_FLAG") != null)
					strategyDetail.higherToLowerFlag = rs.getString("HIGHER_TO_LOWER_FLAG").charAt(0);

				if (rs.getObject("SHELF_VALUE") != null)
					strategyDetail.shelfValue = rs.getFloat("SHELF_VALUE");
				else
					strategyDetail.shelfValue = Constants.DEFAULT_NA;
				
				strategyDetail.guidelinePIId = rs.getLong("GUIDELINE_PI_ID");
				if (rs.getObject("PI_MIN_VALUE") != null)
					strategyDetail.piMinValue = rs.getDouble("PI_MIN_VALUE");
				else
					strategyDetail.piMinValue = Constants.DEFAULT_NA;
				if (rs.getObject("PI_MAX_VALUE") != null)
					strategyDetail.piMaxValue = rs.getDouble("PI_MAX_VALUE");
				else
					strategyDetail.piMaxValue = Constants.DEFAULT_NA;

				if(rs.getString("PI_IS_ITEM_LEVEL") != null)
					strategyDetail.piIsItemLevel = rs.getString("PI_IS_ITEM_LEVEL").charAt(0);
				
				strategyDetail.piCompStrId = rs.getInt("PI_COMP_STR_ID");
				
				strategyDetail.guidelineLZId = rs.getLong("GUIDELINE_LEAD_ZONE_ID");
				strategyDetail.lzLocationLevelId = rs.getInt("LZ_LOCATION_LEVEL_ID");
				strategyDetail.lzLocationId = rs.getInt("LZ_LOCATION_ID");
				if (rs.getObject("LZ_VALUE_TYPE") != null)
					strategyDetail.lzValueType = rs.getString("LZ_VALUE_TYPE").charAt(0);
				else
					strategyDetail.lzValueType = 'O';
				
				if (rs.getObject("LZ_MIN_VALUE") != null)
					strategyDetail.lzMinValue = rs.getDouble("LZ_MIN_VALUE");
				else
					strategyDetail.lzMinValue = Constants.DEFAULT_NA;
				
				if (rs.getObject("LZ_MAX_VALUE") != null)
					strategyDetail.lzMaxValue = rs.getDouble("LZ_MAX_VALUE");
				else
					strategyDetail.lzMaxValue = Constants.DEFAULT_NA;

				if (rs.getObject("LEAD_ZONE_OPERATOR_TEXT") != null)
					strategyDetail.lzOperatorText = rs.getString("LEAD_ZONE_OPERATOR_TEXT");
				else
					strategyDetail.lzOperatorText = "";
				
				strategyDetail.constraintTypeId = rs.getInt("CONSTRAINT_TYPE_ID");
				strategyDetail.constraintId = rs.getInt("CONSTRAINT_ID");

				strategyDetail.constraintPrePriceId = rs.getInt("CONSTRAINT_PREPRICE_ID");
				strategyDetail.prePriceValue = rs.getDouble("PRE_PRICE_VALUE");
				strategyDetail.prePriceQuantity = rs.getInt("PRE_PRICE_QUANTITY");

				strategyDetail.constraintLocPriceId = rs.getInt("CONSTRAINT_LOCPRICE_ID");
				strategyDetail.locPriceValue = rs.getDouble("LOC_PRICE_VALUE");
				strategyDetail.locPriceQuantity = rs.getInt("LOC_PRICE_QUANTITY");

				strategyDetail.constraintMinMaxId = rs.getInt("CONSTRAINT_MINMAX_ID");
				strategyDetail.mmMinValue = rs.getDouble("MM_MIN_VALUE");
				strategyDetail.mmMaxValue = rs.getDouble("MM_MAX_VALUE");
				strategyDetail.mmQuantity = rs.getInt("MM_QUANTITY");

				strategyDetail.constraintThresholdId = rs.getInt("CONSTRAINT_THRESHOLD_ID");
				if (rs.getObject("THRESHOLD_VALUE_TYPE") != null)
					strategyDetail.thresholdValueType = rs.getString("THRESHOLD_VALUE_TYPE").charAt(0);
				if (rs.getObject("THRESHOLD_MIN_VALUE") != null)
					strategyDetail.thresholdMinValue = rs.getDouble("THRESHOLD_MIN_VALUE");
				else
					strategyDetail.thresholdMinValue = Constants.DEFAULT_NA;
				if (rs.getObject("THRESHOLD_MAX_VALUE") != null)
					strategyDetail.thresholdMaxValue = rs.getDouble("THRESHOLD_MAX_VALUE");
				else
					strategyDetail.thresholdMaxValue = Constants.DEFAULT_NA;
				if (rs.getObject("THRESHOLD_MAX_VALUE2") != null)
					strategyDetail.thresholdMaxValue2 = rs.getDouble("THRESHOLD_MAX_VALUE2");
				else
					strategyDetail.thresholdMaxValue2 = Constants.DEFAULT_NA;

				strategyDetail.constraintLigId = rs.getInt("CONSTRAINT_LIG_ID");
				if (rs.getObject("LIG_FLAG") != null) 
					strategyDetail.ligFlag = rs.getString("LIG_FLAG").charAt(0);

				strategyDetail.constraintCostId = rs.getInt("CONSTRAINT_COST_ID");
				if (rs.getObject("RECOMMEND_BELOW_COST") != null) {
					if (rs.getString("RECOMMEND_BELOW_COST").charAt(0) == 'Y') {
						strategyDetail.recommendBelowCost = true;
					}
				}

				strategyDetail.constraintLHRetailId = rs.getInt("CONSTRAINT_LH_RETAIL_ID");
				// Process only first occurrence
				if (rs.getObject("RECOMMEND_RETAIL_FLAG") != null) {
					strategyDetail.recommendRetailFlag = rs.getString("RECOMMEND_RETAIL_FLAG").charAt(0);
				}

				strategyDetail.constraintRoundingId = rs.getInt("CONSTRAINT_ROUNDING_ID");
				strategyDetail.defaultRounding = rs.getString("DEFAULT_ROUNDING");
				strategyDetail.roundingTableId = rs.getInt("ROUNDING_TABLE_ID");
				if (rs.getObject("ROUNDING_DIGITS") != null) {
					strategyDetail.roundingDigitsString = rs.getString("ROUNDING_DIGITS");					
				}
				// Guardrail constraint
				strategyDetail.guardrailCompStrId = rs.getInt("GUARDRAIL_COMP_ID");
				
				if (rs.getObject("GR_GROUP_PRICE_TYPE") != null)
					strategyDetail.groupPriceTypeGR = rs.getString("GR_GROUP_PRICE_TYPE").charAt(0);
				
				strategyDetail.latestPriceObservation = rs.getInt("GR_PR_CHG_OBV");
		
				strategyDetail.guardrailRelationalOperatorId = rs.getInt("GC_RELN_OPERATOR_ID");
				strategyDetail.guardrailRelationalOperatorText = rs.getString("GC_RELN_OPERATOR_TEXT");
				
				if (rs.getObject("GURADRAIL_DETAIL_VALUE_TYPE") != null)
					strategyDetail.guardrailValueType = rs.getString("GURADRAIL_DETAIL_VALUE_TYPE").charAt(0);
				else
					strategyDetail.guardrailValueType = 'O';
				
				if (rs.getObject("GUARDRAIL_MIN_VALUE") != null) 
					strategyDetail.guardrailMinValue = rs.getDouble("GUARDRAIL_MIN_VALUE");
				
				if (rs.getObject("GUARDRAIL_MAX_VALUE") != null) 
					strategyDetail.guardrailMaxValue = rs.getDouble("GUARDRAIL_MAX_VALUE");
		
				if (rs.getObject("GUARDRAIL_EXCLUDE") != null)
					strategyDetail.guardRailDetailExclude = rs.getString("GUARDRAIL_EXCLUDE").charAt(0);
				
				if (rs.getObject("GUARDRAIL_OPERATOR_TEXT") != null)
					strategyDetail.guardRailDetailsOperatorText = rs.getString("GUARDRAIL_OPERATOR_TEXT");
				else
					strategyDetail.guardRailDetailsOperatorText = "";
				
				if (rs.getObject("GUARDRAIL_GUIDELINE_TEXT") != null)
					strategyDetail.guardrailGuidelineText = rs.getString("GUARDRAIL_GUIDELINE_TEXT");
				else
					strategyDetail.guardrailGuidelineText = "";
				
				strategyDetail.guardRailDetailID=rs.getLong("GUARDRAIL_COMP_DETAIL_ID");
				
				strategyDetail.constraintText = rs.getString("CONSTRAINT_TEXT");
				if(String.valueOf(Constants.YES).equals(rs.getString("AUC_OVERRIDE"))) {
					strategyDetail.aucOverrideEnabled = true;
				}else {
					strategyDetail.aucOverrideEnabled = false;
				}
				
				strategyDetail.brandTier1 = rs.getString("BR_TIER1");
				strategyDetail.brandTier2 = rs.getString("BR_TIER2");
				if(String.valueOf(Constants.YES).equals(rs.getString("COMP_OVERRIDE"))) {
					strategyDetail.compOverrideEnabled= true;
				}else {
					strategyDetail.compOverrideEnabled = false;
				}
				strategyDetail.compOverrideId= rs.getInt("COMP_OVERRIDE_ID");
				strategyDetail.priceZoneID = rs.getInt("ZONE_ID");
				
				strategyDetail.priceIncrease = rs.getInt("PRICE_INCREASE");
				strategyDetail.priceDecrease = rs.getInt("PRICE_DECREASE");
				
				if (rs.getString("OVERRIDE_THRESHOLD") != null
						&& rs.getString("OVERRIDE_THRESHOLD") != Constants.EMPTY) {
					if (rs.getString("OVERRIDE_THRESHOLD").equalsIgnoreCase("Y")) {
						strategyDetail.overrideThreshold = 'Y';
					} else
						strategyDetail.overrideThreshold = 'N';
				} else
					strategyDetail.overrideThreshold = 'Y';
				
				strategyDetails.add(strategyDetail);
			}

		} catch (Exception exception) {
			logger.error("Error when retrieving strategy detail - " + exception);
			throw new GeneralException("Error when retrieving strategy detail - " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return strategyDetails;
	}
	
	private PRStrategyDTO getStrategy(List<StrategyDetail> strategyDetails, long strategyId) throws GeneralException{
		PRStrategyDTO strategyDTO = null;
		try{
			for(StrategyDetail strategyDetail : strategyDetails){
				strategyDTO = new PRStrategyDTO();
				strategyDTO.setStrategyId(strategyDetail.strategyId);
				strategyDTO.setLocationLevelId(strategyDetail.locationLevelId);
				strategyDTO.setLocationId(strategyDetail.locationId);
				strategyDTO.setProductLevelId(strategyDetail.productLevelId);
				strategyDTO.setProductId(strategyDetail.productId);
				strategyDTO.setPriceCheckListId(strategyDetail.applyTo);
				strategyDTO.setVendorId(strategyDetail.vendorId);
				strategyDTO.setStateId(strategyDetail.stateId);
				strategyDTO.setCriteriaId(strategyDetail.criteriaId);
				strategyDTO.setDsdRecommendationFlag(strategyDetail.dsdRecommendationFlag);
				//First row itself will have the required information
				break;
			}
		}catch(Exception ex) {
			logger.error("Error in getStrategy() - " + ex);
			throw new GeneralException("Error in getStrategy() - " + ex);
		}finally{
			 
		}
		return strategyDTO;
	}
	
	private PRObjectiveDTO getObjective(List<StrategyDetail> strategyDetails, long strategyId) throws GeneralException {
		PRObjectiveDTO objectiveDTO = null;
		try {
			for(StrategyDetail strategyDetail : strategyDetails){
				// First occurrence of objective information
				if (strategyDetail.objectiveTypeId > 0) {
					objectiveDTO = new PRObjectiveDTO();
					objectiveDTO.setObjectiveTypeId(strategyDetail.objectiveTypeId);
					objectiveDTO.setObjectiveTypeName(strategyDetail.objectiveTypeName);
					objectiveDTO.setMinObjVal(strategyDetail.minObjVal);
					objectiveDTO.setMaxObjVal(strategyDetail.maxObjVal);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error when retrieving objective for strategy - " + exception);
			throw new GeneralException("Error when retrieving objective for strategy - " + exception);
		} finally {
		}
		return objectiveDTO;
	}
	
	private PRGuidelinesDTO getGuidelines(Connection conn, List<StrategyDetail> strategyDetails, 
			long strategyId) throws GeneralException {
		PRGuidelinesDTO guidelinesDTO = new PRGuidelinesDTO();
		HashMap<Integer, Integer> processedGuidelineId = new HashMap<Integer, Integer>();

		try {
			boolean isBrandOrSizeGuidelinePresent = false;
			for (StrategyDetail strategyDetail : strategyDetails) {
				// Ignore if already processed
				if (strategyDetail.guidelineId > 0 && processedGuidelineId.get(strategyDetail.guidelineId) == null) {
					int execOrder = strategyDetail.guidelineExecOrder;
					boolean isExclude = strategyDetail.isExcludeGuideline;

					if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.MARGIN.getGuidelineTypeId()) {
						if (!isExclude) {
							guidelinesDTO.setMarginGuideline(getGuidelineMargin(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));
						}
					} else if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.PRICE_INDEX.getGuidelineTypeId()) {
						if (!isExclude) {
							guidelinesDTO.setPiGuideline(getGuidelinePI(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));
						}
					} else if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.COMPETITION.getGuidelineTypeId()) {
						if (!isExclude) {
							/*guidelinesDTO.setCompGuideline(getGuidelineComp(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));*/
							guidelinesDTO.setCompGuideline(getGuidelineComp(conn, strategyDetail.guidelineId));
						}
					} else if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.BRAND.getGuidelineTypeId()) {
						if (!isExclude) {
							guidelinesDTO.setBrandGuideline(getGuidelineBrand(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));
							isBrandOrSizeGuidelinePresent = true;
						}
					} else if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.SIZE.getGuidelineTypeId()) {
						if (!isExclude) {
							guidelinesDTO.setSizeGuideline(getGuidelineSize(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));
							isBrandOrSizeGuidelinePresent = true;
						}
					} else if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.LEAD_ZONE.getGuidelineTypeId()) {
						if (!isExclude) {
							guidelinesDTO.setLeadZoneGuideline(getGuidelineLeadZone(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));
						}
					}
					if (!isExclude) {
						guidelinesDTO.addExecOrderMap(execOrder, strategyDetail.guidelineId);
						guidelinesDTO.addGuidelineIdMap(strategyDetail.guidelineId, strategyDetail.guidelineTypeId);
					}

					processedGuidelineId.put(strategyDetail.guidelineId, strategyDetail.guidelineId);
				}
			}
			// If both brand and size is excluded don't apply relation from
			// price group
			// if(!isBrandOrSizeGuidelinePresent){
			// guidelinesDTO.addExecOrderMap(0, 0);
			// guidelinesDTO.addGuidelineIdMap(0, 7);
			// }
		} catch (Exception exception) {
			logger.error("Error in getGuidelines() - " + exception);
			throw new GeneralException("Error in getGuidelines() - " + exception);
		} finally {
		}
		return guidelinesDTO;
	}
	
	private List<PRGuidelineMargin> getGuidelineMargin(List<StrategyDetail> strategyDetails, int inpGuidelineId, int inpGuidelineTypeId)
			throws GeneralException {
		PRGuidelineMargin guidelineDTO = null;
		List<PRGuidelineMargin> marginGuidelines = new ArrayList<PRGuidelineMargin>();
		HashMap<Long, Long> processedGuidelineMarginId = new HashMap<Long, Long>();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				int guidelineId = strategyDetail.guidelineId;
				int guidelineTypeId = strategyDetail.guidelineTypeId;
				long guidelineMarginId = strategyDetail.guidelineMarginId;

				if (guidelineId == inpGuidelineId && guidelineTypeId == inpGuidelineTypeId && guidelineMarginId > 0
						&& processedGuidelineMarginId.get(guidelineMarginId) == null) {
					guidelineDTO = new PRGuidelineMargin();
					guidelineDTO.setValueType(strategyDetail.marginValueType);
					guidelineDTO.setCurrentMargin(strategyDetail.currentMargin);
					guidelineDTO.setMinMarginPct(strategyDetail.minMarginPct);
					guidelineDTO.setMaxMarginPct(strategyDetail.maxMarginPct);
					guidelineDTO.setMinMargin$(strategyDetail.minMargin$);
					guidelineDTO.setMaxMargin$(strategyDetail.maxMargin$);
					guidelineDTO.setCostFlag(strategyDetail.marginCostFlag);
					guidelineDTO.setItemLevelFlag(strategyDetail.marginIsItemLevel);
					guidelineDTO.setPriceIncrease(strategyDetail.priceIncrease);
					guidelineDTO.setPriceDecrease(strategyDetail.priceDecrease);
					guidelineDTO.setItemFlag(strategyDetail.itemFlag);
					processedGuidelineMarginId.put(guidelineMarginId, guidelineMarginId);
					marginGuidelines.add(guidelineDTO);
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getGuidelineMargin() - " + exception);
			throw new GeneralException("Error in getGuidelineMargin() - " + exception);
		} finally {
		}
		return marginGuidelines;
	}
	
	/**
	 * Returns Guideline Comp for the input guideline Id
	 * @param conn
	 * @param guidelineId
	 * @return
	 * @throws GeneralException
	 */
	private PRGuidelineComp getGuidelineComp(Connection conn, int guidelineId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		PRGuidelineComp guidelineDTO = null;
		List<PRGuidelineCompDetail> guidelineCompDetail = new ArrayList<PRGuidelineCompDetail>();
		PRGuidelineCompDetail prGuidelineCompDetail;
		try{
			stmt = conn.prepareStatement(GET_GUIDELINE_COMP);
			stmt.setInt(1, guidelineId);
			rs = stmt.executeQuery();
			
			while(rs.next()){
				guidelineDTO = new PRGuidelineComp();
				guidelineDTO.setGuidelineCompId(rs.getLong("GUIDELINE_COMP_ID"));
				guidelineDTO.setGuidelineId(rs.getLong("GUIDELINE_ID"));				
				guidelineDTO.setRelationalOperatorText(rs.getString("COMP_OPERATOR_TEXT"));				
				guidelineDTO.setGroupPriceType(rs.getString("GROUP_PRICE_TYPE").charAt(0));
				guidelineDTO.setLatestPriceObservationDays(rs.getInt("LATEST_PRICE_OBSERVATION"));
				if(String.valueOf(Constants.YES).equals(rs.getString("IGN_CMP_PRC_BLW_CST"))) {
					guidelineDTO.setIgnoreCompBelowCost(true);
				} else {
					guidelineDTO.setIgnoreCompBelowCost(false);
				}
				
				if(rs.getObject("GUIDELINE_COMP_DETAIL_ID") != null){
					if(rs.getString("COMP_DETAIL_EXCLUDE").charAt(0) == Constants.NO){
						prGuidelineCompDetail = new PRGuidelineCompDetail();
						prGuidelineCompDetail.setCompStrId(rs.getInt("COMP_STR_ID"));
						prGuidelineCompDetail.setGuidelineCompDetailId(rs.getInt("GUIDELINE_COMP_DETAIL_ID"));
						if(rs.getObject("COMP_DETAIL_VALUE_TYPE") != null)
							prGuidelineCompDetail.setValueType(rs.getString("COMP_DETAIL_VALUE_TYPE").charAt(0));
						else
							prGuidelineCompDetail.setValueType('O');
						if(rs.getObject("COMP_DETAIL_MIN_VALUE") != null)
							prGuidelineCompDetail.setMinValue(rs.getFloat("COMP_DETAIL_MIN_VALUE"));
						else
							prGuidelineCompDetail.setMinValue(Constants.DEFAULT_NA);
						if(rs.getObject("COMP_DETAIL_MAX_VALUE") != null)
							prGuidelineCompDetail.setMaxValue(rs.getFloat("COMP_DETAIL_MAX_VALUE"));
						else
							prGuidelineCompDetail.setMaxValue(Constants.DEFAULT_NA);
						
						if(rs.getObject("COMP_DETAIL_OPERATOR_TEXT") != null)
							prGuidelineCompDetail.setRelationalOperatorText(rs.getString("COMP_DETAIL_OPERATOR_TEXT"));
						else
							prGuidelineCompDetail.setRelationalOperatorText("");						
						
						if(rs.getObject("GUIDELINE_TEXT") != null)
							prGuidelineCompDetail.setGuidelineText(rs.getString("GUIDELINE_TEXT"));
						else
							prGuidelineCompDetail.setGuidelineText("");		
						
						guidelineCompDetail.add(prGuidelineCompDetail);
					}
				}
			}
			if(guidelineDTO != null)
				guidelineDTO.setCompetitorDetails(guidelineCompDetail);
			
		}catch(Exception exception){
			logger.error("Error when retrieving guideline comp - " + exception);
			throw new GeneralException("Error when retrieving guideline comp - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return guidelineDTO;
	}
	
	@SuppressWarnings("unused")
	private PRGuidelineComp getGuidelineComp(List<StrategyDetail> strategyDetails, int inpGuidelineId, int inpGuidelineTypeId)
			throws GeneralException {
		PRGuidelineComp guidelineDTO = null;
		List<PRGuidelineCompDetail> guidelineCompDetail = new ArrayList<PRGuidelineCompDetail>();
		PRGuidelineCompDetail prGuidelineCompDetail;
		boolean firstOccurrence = true;
		HashMap<Long, Long> processedGuidelineCompDetailId = new HashMap<Long, Long>();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				int guidelineId = strategyDetail.guidelineId;
				int guidelineTypeId = strategyDetail.guidelineTypeId;
				long guidelineCompId = strategyDetail.guidelineCompId;

				if (guidelineId == inpGuidelineId && guidelineTypeId == inpGuidelineTypeId && guidelineCompId > 0) {

					if (firstOccurrence) {
						guidelineDTO = new PRGuidelineComp();
						guidelineDTO.setGuidelineCompId(strategyDetail.guidelineCompId);
						guidelineDTO.setGuidelineId(strategyDetail.guidelineId);
						guidelineDTO.setRelationalOperatorText(strategyDetail.compOperatorText);
						guidelineDTO.setGroupPriceType(strategyDetail.groupPriceType);
						guidelineDTO.setLatestPriceObservationDays(strategyDetail.latestPriceObservation);
						firstOccurrence = false;
					}
					long guidelineCompDetailId = strategyDetail.guidelineCompDetailId;
					if (guidelineCompDetailId > 0 && processedGuidelineCompDetailId.get(guidelineCompDetailId) == null) {
						if (strategyDetail.compDetailExclude == Constants.NO) {
							prGuidelineCompDetail = new PRGuidelineCompDetail();
							prGuidelineCompDetail.setGuidelineCompDetailId(guidelineCompDetailId);
							prGuidelineCompDetail.setCompStrId(strategyDetail.compStrId);
							prGuidelineCompDetail.setValueType(strategyDetail.compDetailValueType);
							prGuidelineCompDetail.setMinValue(strategyDetail.compDetailMinValue);
							prGuidelineCompDetail.setMaxValue(strategyDetail.compDetailMaxValue);
							prGuidelineCompDetail.setRelationalOperatorText(strategyDetail.compDetailOperatorText);
							prGuidelineCompDetail.setGuidelineText(strategyDetail.guidelineText);
							guidelineCompDetail.add(prGuidelineCompDetail);
						}
						processedGuidelineCompDetailId.put(guidelineCompDetailId, guidelineCompDetailId);
					}
				}
			}
			if (guidelineDTO != null) {
				guidelineDTO.setCompetitorDetails(guidelineCompDetail);
				
				//Sort by comp detail id, to maintain the order as given by the user
				guidelineDTO.sortByCompDetailId(guidelineCompDetail);
			}
		} catch (Exception exception) {
			logger.error("Error in getGuidelineComp() - " + exception);
			throw new GeneralException("Error in getGuidelineComp() - " + exception);
		} finally {
		}
		return guidelineDTO;
	}
	 
	private ArrayList<PRGuidelineBrand> getGuidelineBrand(List<StrategyDetail> strategyDetails, int inpGuidelineId, int inpGuidelineTypeId)
			throws GeneralException {
		PRGuidelineBrand guidelineDTO = null;
		ArrayList<PRGuidelineBrand> brandGuidelines = new ArrayList<PRGuidelineBrand>();
		HashMap<Long, Long> processedGuidelineBrandId = new HashMap<Long, Long>();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				int guidelineId = strategyDetail.guidelineId;
				int guidelineTypeId = strategyDetail.guidelineTypeId;
				long guidelineBrandId = strategyDetail.guidelineBrandId;
				if (guidelineId == inpGuidelineId && guidelineTypeId == inpGuidelineTypeId && guidelineBrandId > 0
						&& processedGuidelineBrandId.get(guidelineBrandId) == null) {
					guidelineDTO = new PRGuidelineBrand();
					guidelineDTO.setgId(strategyDetail.guidelineId);
					guidelineDTO.setValueType(strategyDetail.brandValueType);
					guidelineDTO.setMinValue(strategyDetail.brandMinValue);
					guidelineDTO.setMaxValue(strategyDetail.brandMaxValue);
					guidelineDTO.setRetailType(strategyDetail.brandRetailType);
					guidelineDTO.setOperatorText(strategyDetail.brandOperatorText);
					guidelineDTO.setBrandTierId1(strategyDetail.brandTierId1);
					guidelineDTO.setBrandTierId2(strategyDetail.brandTierId2);
					guidelineDTO.setBrandId1(strategyDetail.brandId1);
					guidelineDTO.setBrandId2(strategyDetail.brandId2);
					guidelineDTO.setAUCOverrideEnabled(strategyDetail.aucOverrideEnabled);
					guidelineDTO.setBrandTier1(strategyDetail.brandTier1);
					guidelineDTO.setBrandTier2(strategyDetail.brandTier2);
					guidelineDTO.setCompOverrideEnabled(strategyDetail.compOverrideEnabled);
					guidelineDTO.setCompOverrideId(strategyDetail.compOverrideId);
					processedGuidelineBrandId.put(guidelineBrandId, guidelineBrandId);
					brandGuidelines.add(guidelineDTO);
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getGuidelineBrand() - " + exception);
			throw new GeneralException("Error in getGuidelineBrand()- " + exception);
		} finally {
		}
		return brandGuidelines;
	}
	
	private ArrayList<PRGuidelineSize> getGuidelineSize(List<StrategyDetail> strategyDetails, int inpGuidelineId, int inpGuidelineTypeId)
			throws GeneralException {
		ArrayList<PRGuidelineSize> sizeGuidelines = new ArrayList<PRGuidelineSize>();
		HashMap<Long, Long> processedGuidelineSizeId = new HashMap<Long, Long>();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				int guidelineId = strategyDetail.guidelineId;
				int guidelineTypeId = strategyDetail.guidelineTypeId;
				long guidelineSizeId = strategyDetail.guidelineSizeId;
				if (guidelineId == inpGuidelineId && guidelineTypeId == inpGuidelineTypeId && guidelineSizeId > 0
						&& processedGuidelineSizeId.get(guidelineSizeId) == null) {
					PRGuidelineSize guidelineDTO = new PRGuidelineSize();
					guidelineDTO.setValueType(strategyDetail.sizeValueType);
					guidelineDTO.setMinValue(strategyDetail.sizeMinValue);
					guidelineDTO.setMaxValue(strategyDetail.sizeMaxValue);
					guidelineDTO.setOperatorText(strategyDetail.sizeOperatorText);
					guidelineDTO.setHtol(strategyDetail.higherToLowerFlag);
					guidelineDTO.setShelfValue(strategyDetail.shelfValue);
					processedGuidelineSizeId.put(guidelineSizeId, guidelineSizeId);
					sizeGuidelines.add(guidelineDTO);
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getGuidelineSize() - " + exception);
			throw new GeneralException("Error in getGuidelineSize() - " + exception);
		} finally {
		}
		return sizeGuidelines;
	}
	
	private List<PRGuidelinePI> getGuidelinePI(List<StrategyDetail> strategyDetails, int inpGuidelineId, int inpGuidelineTypeId)
			throws GeneralException {
		PRGuidelinePI guidelineDTO = null;
		List<PRGuidelinePI> piGuidelines = new ArrayList<PRGuidelinePI>();
		HashMap<Long, Long> processedGuidelinePIId = new HashMap<Long, Long>();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				int guidelineId = strategyDetail.guidelineId;
				int guidelineTypeId = strategyDetail.guidelineTypeId;
				long guidelinePIId = strategyDetail.guidelinePIId;

				// Process only first occurrence
				if (guidelineId == inpGuidelineId && guidelineTypeId == inpGuidelineTypeId && guidelinePIId > 0
						&& processedGuidelinePIId.get(guidelinePIId) == null) {
					guidelineDTO = new PRGuidelinePI();
					guidelineDTO.setgId(strategyDetail.guidelineId);
					guidelineDTO.setMinValue(strategyDetail.piMinValue);
					guidelineDTO.setMaxValue(strategyDetail.piMaxValue);
					guidelineDTO.setItemLevelFlag(strategyDetail.piIsItemLevel);
					guidelineDTO.setCompStrId(strategyDetail.piCompStrId);
					processedGuidelinePIId.put(guidelinePIId, guidelinePIId);
					piGuidelines.add(guidelineDTO);
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getGuidelinePI() - " + exception);
			throw new GeneralException("Error in getGuidelinePI() - " + exception);
		} finally {
		}
		return piGuidelines;
	}
	
	
	private PRGuidelineLeadZoneDTO getGuidelineLeadZone(List<StrategyDetail> strategyDetails, int inpGuidelineId, int inpGuidelineTypeId)
			throws GeneralException {
		PRGuidelineLeadZoneDTO guidelineLeadZoneDTO = null;
		HashMap<Long, Long> processedGuidelineLZId = new HashMap<Long, Long>();
		try {

			for (StrategyDetail strategyDetail : strategyDetails) {
				int guidelineId = strategyDetail.guidelineId;
				int guidelineTypeId = strategyDetail.guidelineTypeId;
				long guidelineLZId = strategyDetail.guidelineLZId;

				// Process only first occurrence
				if (guidelineId == inpGuidelineId && guidelineTypeId == inpGuidelineTypeId && guidelineLZId > 0
						&& processedGuidelineLZId.get(guidelineLZId) == null) {
					guidelineLeadZoneDTO = new PRGuidelineLeadZoneDTO();
					guidelineLeadZoneDTO.setGuidelineId(strategyDetail.guidelineId);
					guidelineLeadZoneDTO.setLocationLevelId(strategyDetail.lzLocationLevelId);
					guidelineLeadZoneDTO.setLocationId(strategyDetail.lzLocationId);
					guidelineLeadZoneDTO.setValueType(strategyDetail.lzValueType);
					guidelineLeadZoneDTO.setMinValue(strategyDetail.lzMinValue);
					guidelineLeadZoneDTO.setMaxValue(strategyDetail.lzMaxValue);
					guidelineLeadZoneDTO.setOperatorText(strategyDetail.lzOperatorText);
					processedGuidelineLZId.put(guidelineLZId, guidelineLZId);
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getGuidelineLeadZone() - " + exception);
			throw new GeneralException("Error in getGuidelineLeadZone() - " + exception);
		} finally {
		}
		return guidelineLeadZoneDTO;
	}
	
	
	private PRConstraintsDTO getConstraints(Connection conn, List<StrategyDetail> strategyDetails, long strategyId) throws GeneralException {
		PRConstraintsDTO constraintsDTO = new PRConstraintsDTO();
		HashMap<Long, Long> processedConstraintId = new HashMap<Long, Long>();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintId = strategyDetail.constraintId;
				// Ignore if already processed
				if (constraintId != 0 && processedConstraintId.get(constraintId) == null) {
					/*if (constraintTypeId == ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId()) {
						constraintsDTO.setPrePriceConstraint(getConstraintPreprice(strategyDetails, constraintId, constraintTypeId));
					} else*/ if (constraintTypeId == ConstraintTypeLookup.LOCKED_PRICE.getConstraintTypeId()) {
						constraintsDTO.setLocPriceConstraint(getConstraintLocprice(strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.MIN_MAX.getConstraintTypeId()) {
						constraintsDTO.setMinMaxConstraint(getConstraintMinMax(strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.ROUNDING.getConstraintTypeId()) {
						constraintsDTO.setRoundingConstraint(getConstraintRounding(conn, strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.THRESHOLD.getConstraintTypeId()) {
						constraintsDTO.setThresholdConstraint(getConstraintThreshold(strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.LIG.getConstraintTypeId()) {
						constraintsDTO.setLigConstraint(getConstraintLIG(strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.COST.getConstraintTypeId()) {
						constraintsDTO.setCostConstraint(getConstraintCost(strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.LOWER_HIGHER.getConstraintTypeId()) {
						constraintsDTO.setLowerHigherConstraint(getConstraintLowerHigher(strategyDetails, constraintId, constraintTypeId));
					} else if (constraintTypeId == ConstraintTypeLookup.GUARD_RAIL.getConstraintTypeId()) {
						constraintsDTO.setGuardrailConstraint(getGuardrailConstraint(conn, constraintId));
					} else if (constraintTypeId == ConstraintTypeLookup.FREIGHT.getConstraintTypeId()) {
						constraintsDTO.setFreightChargeConstraint(getFreightChargeConstraint(strategyDetails, constraintId, constraintTypeId));
					}
					processedConstraintId.put(constraintId, constraintId);
				}
			}
			
			// If Rounding Constraint is Null, then set rounding
			// constraint to the default rounding table
			if (constraintsDTO.getRoundingConstraint() == null) {
				// String useDefaultRoundingTable =
				// PropertyManager.getProperty("PR_DEFAULT_ROUNDING_TABLE");
				String useDefaultRoundingTable = "YES";
				if (useDefaultRoundingTable.equalsIgnoreCase("YES")) {
					PRConstraintRounding roundingConstraint = new PRConstraintRounding();
					int defaultTableId = getDefaultRoundingTable(conn);
					roundingConstraint.setRoundingTableId(defaultTableId);
					roundingConstraint.setRoundingTableContent(getRoundingTableDetail(conn, defaultTableId));
					constraintsDTO.setRoundingConstraint(roundingConstraint);
				} else {
					PRConstraintRounding roundingConstraint = new PRConstraintRounding();
					roundingConstraint.setNoRounding('Y');
					constraintsDTO.setRoundingConstraint(roundingConstraint);
				}
			}

			// If Threshold Constraint is Null, set new threshold
			// constraint object
			if (constraintsDTO.getThresholdConstraint() == null) {
				PRConstraintThreshold thresholdConstraint = new PRConstraintThreshold();
				constraintsDTO.setThresholdConstraint(thresholdConstraint);
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraints() - " + exception);
			throw new GeneralException("Error in getConstraints() - " + exception);
		} finally {
		}
		return constraintsDTO;
	}
	
	private PRConstraintPrePrice getConstraintPreprice(List<StrategyDetail> strategyDetails, long inpConstraintId, int inpConstraintTypeId)
			throws GeneralException {
		PRConstraintPrePrice constraintDTO = null;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintPrePriceId = strategyDetail.constraintPrePriceId;
				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintPrePriceId > 0) {
					constraintDTO = new PRConstraintPrePrice();
					constraintDTO.setValue(strategyDetail.prePriceValue);
					constraintDTO.setQuantity(strategyDetail.prePriceQuantity);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintPreprice() - " + exception);
			throw new GeneralException("Error in getConstraintPreprice() - " + exception);
		} finally {
		}
		return constraintDTO;
	}
	
	private PRConstraintLocPrice getConstraintLocprice(List<StrategyDetail> strategyDetails, long inpConstraintId, int inpConstraintTypeId)
			throws GeneralException {
		PRConstraintLocPrice constraintDTO = null;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintLocPriceId = strategyDetail.constraintLocPriceId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintLocPriceId > 0) {
					constraintDTO = new PRConstraintLocPrice();
					constraintDTO.setValue(strategyDetail.locPriceValue);
					constraintDTO.setQuantity(strategyDetail.locPriceQuantity);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintLocprice() - " + exception);
			throw new GeneralException("Error in getConstraintLocprice() - " + exception);
		} finally {
		}
		return constraintDTO;
	}
	
	private PRConstraintMinMax getConstraintMinMax(List<StrategyDetail> strategyDetails, long inpConstraintId, int inpConstraintTypeId) 
			throws GeneralException {
		PRConstraintMinMax constraintDTO = null;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintMinMaxId = strategyDetail.constraintMinMaxId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintMinMaxId > 0) {
					constraintDTO = new PRConstraintMinMax();
					constraintDTO.setMinValue(strategyDetail.mmMinValue);
					constraintDTO.setMaxValue(strategyDetail.mmMaxValue);
					constraintDTO.setQuantity(strategyDetail.mmQuantity);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintMinMax() - " + exception);
			throw new GeneralException("Error in getConstraintMinMax()  - " + exception);
		} finally {
		}
		return constraintDTO;
	}

	private PRConstraintThreshold getConstraintThreshold(List<StrategyDetail> strategyDetails, long inpConstraintId, int inpConstraintTypeId)
			throws GeneralException {
		PRConstraintThreshold constraintDTO = null;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintThresholdId = strategyDetail.constraintThresholdId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintThresholdId > 0) {
					constraintDTO = new PRConstraintThreshold();
					constraintDTO.setValueType(strategyDetail.thresholdValueType);
					constraintDTO.setMinValue(strategyDetail.thresholdMinValue);
					constraintDTO.setMaxValue(strategyDetail.thresholdMaxValue);
					constraintDTO.setMaxValue2(strategyDetail.thresholdMaxValue2);
					constraintDTO.setOverrideThreshold(strategyDetail.overrideThreshold);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintThreshold() - " + exception);
			throw new GeneralException("Error in getConstraintThreshold() - " + exception);
		} finally {
		}
		return constraintDTO;
	}

	private PRConstraintLIG getConstraintLIG(List<StrategyDetail> strategyDetails, long inpConstraintId, int inpConstraintTypeId)
			throws GeneralException {
		PRConstraintLIG constraintDTO = null;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintLigId = strategyDetail.constraintLigId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintLigId > 0) {
					constraintDTO = new PRConstraintLIG();
					constraintDTO.setValue(strategyDetail.ligFlag);
					constraintDTO.setLigConstraintText(strategyDetail.constraintText);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintLIG() - " + exception);
			throw new GeneralException("Error in getConstraintLIG() - " + exception);
		} finally {
		}
		return constraintDTO;
	}
	
	private PRConstraintCost getConstraintCost(List<StrategyDetail> strategyDetails, long inpConstraintId, int inpConstraintTypeId)
			throws GeneralException {
		PRConstraintCost constraintDTO = new PRConstraintCost();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintCostId = strategyDetail.constraintCostId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintCostId > 0) {
					constraintDTO.setIsRecBelowCost(strategyDetail.recommendBelowCost);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintCost() - " + exception);
			throw new GeneralException("Error in getConstraintCost() - " + exception);
		} finally {
		}
		return constraintDTO;
	}
	
	private PRConstraintLowerHigher getConstraintLowerHigher(List<StrategyDetail> strategyDetails, long inpConstraintId,
			int inpConstraintTypeId) throws GeneralException {
		PRConstraintLowerHigher constraintDTO = new PRConstraintLowerHigher();
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintLowerHigherId = strategyDetail.constraintLHRetailId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintLowerHigherId > 0) {
					constraintDTO.setLowerHigherRetailFlag(strategyDetail.recommendRetailFlag);
					break;
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getConstraintLowerHigher() - " + exception);
			throw new GeneralException("Error in getConstraintLowerHigher() - " + exception);
		} finally {
		}
		return constraintDTO;
	}
	
	private int getDefaultRoundingTable(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int roundingTableId = 0;
		try{
			stmt = conn.prepareStatement(GET_DEFAULT_ROUNDING_TABLE);
			rs = stmt.executeQuery();
			if(rs.next()){
				roundingTableId = rs.getInt("ROUNDING_TABLE_ID");
			}
		}catch(Exception exception){
			logger.error("Error when retrieving default rounding table - " + exception.toString());
			throw new GeneralException("Error when retrieving default rounding table - " + exception.toString());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return roundingTableId;
	}
	
	private PRConstraintRounding getConstraintRounding(Connection conn, List<StrategyDetail> strategyDetails, long inpConstraintId,
			int inpConstraintTypeId) throws GeneralException {
		PRConstraintRounding constraintDTO = null;
		boolean isRoundingDigitsPresent = false;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {
				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;
				long constraintRoundingId = strategyDetail.constraintRoundingId;

				// Process only first occurrence
				if (constraintId == inpConstraintId && constraintTypeId == inpConstraintTypeId && constraintRoundingId > 0) {
					constraintDTO = new PRConstraintRounding();
					if (strategyDetail.roundingDigitsString != null) {
						String[] roundingDigits = strategyDetail.roundingDigitsString.split(",");
						int[] roundingDigitsInt = new int[roundingDigits.length];
						int counter = 0;
						for (String digit : roundingDigits) {
							roundingDigitsInt[counter] = Integer.parseInt(digit);
							counter++;
						}
						constraintDTO.setRoundingDigits(roundingDigitsInt);
						isRoundingDigitsPresent = true;
					}
					/*
					 * if(rs.getObject("NEXT_ROUNDING") != null)
					 * constraintDTO.setNext
					 * (rs.getString("NEXT_ROUNDING").charAt(0));
					 * if(rs.getObject("NEAREST_ROUNDING") != null)
					 * constraintDTO.setNearest
					 * (rs.getString("NEAREST_ROUNDING").charAt(0));
					 */
					constraintDTO.setRoundingTableId(strategyDetail.roundingTableId);

					if (constraintDTO.getRoundingTableId() > 0) {
						constraintDTO.setRoundingTableContent(getRoundingTableDetail(conn, constraintDTO.getRoundingTableId()));
					} else {
						if (!isRoundingDigitsPresent || "Y".equalsIgnoreCase(strategyDetail.defaultRounding)) {
							int defaultTableId = getDefaultRoundingTable(conn);
							constraintDTO.setRoundingTableContent(getRoundingTableDetail(conn, defaultTableId));
						}
					}
					break;
				}

			}
		} catch (Exception exception) {
			logger.error("Error when retrieving constraint rounding - " + exception);
			throw new GeneralException("Error when retrieving constraint rounding - " + exception);
		} finally {
		}
		return constraintDTO;
	}
	
	private TreeMap<String, PRRoundingTableDTO> getRoundingTableDetail(Connection conn, int roundingTableId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();
		try {
			stmt = conn.prepareStatement(GET_ROUNDING_TABLE_CONTENT);
			stmt.setInt(1, roundingTableId);
			rs = stmt.executeQuery();

			while (rs.next()) {
				PRRoundingTableDTO rtDTO = new PRRoundingTableDTO();
				rtDTO.setStartPrice(rs.getDouble("START_PRICE"));
				if (rs.getObject("END_PRICE") != null)
					rtDTO.setEndPrice(rs.getDouble("END_PRICE"));
				else
					rtDTO.setEndPrice(PRConstants.DEFAULT_MAX_PRICE);
				rtDTO.setAllowedEndDigits(rs.getString("ALLOWED_END_DIGITS"));
				rtDTO.setAllowedPrices(rs.getString("ALLOWED_PRICES"));
				rtDTO.setExcludedPrices(rs.getString("EXCLUDED_PRICES"));
				String key = rs.getString("START_PRICE") + "-"
						+ (rs.getString("END_PRICE") == null ? PRConstants.DEFAULT_MAX_PRICE_STR : rs.getString("END_PRICE"));
				roundingTableContent.put(key, rtDTO);
			}
		} catch (Exception exception) {
			logger.error("Error when retrieving default rounding table detail - " + exception.toString());
			throw new GeneralException("Error when retrieving default rounding table detail - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return roundingTableContent;
	}
	
	/**
	 * 
	 * @param strategyDetails
	 * @param inpConstraintId
	 * @param inpConstraintTypeId
	 * @return guardrail constraint
	 * @throws GeneralException
	 */
	private PRConstraintGuardrail getGuardrailConstraint(Connection conn, long inpConstraintId)
			throws GeneralException {
		PRConstraintGuardrail guardrail = null;
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		PRConstraintGuardrail guardRailConstraint = null;
		List<PRConstraintGuardRailDetail> guardRailDetail = new ArrayList<PRConstraintGuardRailDetail>();

		PRConstraintGuardRailDetail prConstraintGuardRail;
		
		try{
			stmt = conn.prepareStatement(GET_CONSTRAINT_GUARDRAIL);
			stmt.setLong(1, inpConstraintId);
			rs = stmt.executeQuery();
			
//			logger.debug("GET_CONSTRAINT_GUARDRAIL query "+ GET_CONSTRAINT_GUARDRAIL);
			
//			logger.info("inpConstraintId :"+ inpConstraintId);
			while(rs.next()){
				
				if(rs.getString("GROUP_PRICE_TYPE") !=null)
				{
					guardRailConstraint = new PRConstraintGuardrail();
					guardRailConstraint.setGuardRailCompId(rs.getLong("CONSTRAINT_GUARDRAIL_ID"));
					guardRailConstraint.setGuidelineId(rs.getLong("CONSTRAINT_ID"));				
					guardRailConstraint.setRelationalOperatorText(rs.getString("GUARDRAIL_OPERATOR_TEXT"));	
				guardRailConstraint.setGroupPriceType(rs.getString("GROUP_PRICE_TYPE").charAt(0));
				guardRailConstraint.setLatestPriceObservationDays(rs.getInt("LATEST_PRICE_OBSERVATION"));
				if(String.valueOf(Constants.YES).equals(rs.getString("IGN_CMP_PRC_BLW_CST"))) {
					guardRailConstraint.setIgnoreCompBelowCost(true);
				} else {
					guardRailConstraint.setIgnoreCompBelowCost(false);
				}
					if (rs.getString("IS_ZONE") != null) {
						if (String.valueOf(Constants.YES).equals(rs.getString("IS_ZONE"))) {
							guardRailConstraint.setZonePresent(true);
						} else {
							guardRailConstraint.setZonePresent(false);
						}

					} else
						guardRailConstraint.setZonePresent(false);
				
				if(rs.getObject("GUARDRAIL_COMP_DETAIL_ID") != null){
					if(rs.getString("GUARDRAIL_EXCLUDE").charAt(0) == Constants.NO){
						prConstraintGuardRail = new PRConstraintGuardRailDetail();
						prConstraintGuardRail.setCompStrId(rs.getInt("COMP_STR_ID"));
						prConstraintGuardRail.setGrConstraintDetailId(rs.getInt("GUARDRAIL_COMP_DETAIL_ID"));
						if(rs.getObject("GUARDRAIL_VALUE_TYPE") != null)
							prConstraintGuardRail.setValueType(rs.getString("GUARDRAIL_VALUE_TYPE").charAt(0));
						else
							prConstraintGuardRail.setValueType('O');
						if(rs.getObject("GUARDRAIL_MIN_VALUE") != null)
							prConstraintGuardRail.setMinValue(rs.getFloat("GUARDRAIL_MIN_VALUE"));
						else
							prConstraintGuardRail.setMinValue(Constants.DEFAULT_NA);
						if(rs.getObject("GUARDRAIL_MAX_VALUE") != null)
							prConstraintGuardRail.setMaxValue(rs.getFloat("GUARDRAIL_MAX_VALUE"));
						else
							prConstraintGuardRail.setMaxValue(Constants.DEFAULT_NA);
						
						if(rs.getObject("GUARDRAIL_DETAIL_OPERATOR_TEXT") != null)
							prConstraintGuardRail.setRelationalOperatorText(rs.getString("GUARDRAIL_DETAIL_OPERATOR_TEXT"));
						else
							prConstraintGuardRail.setRelationalOperatorText("");						
						
						if(rs.getObject("GUIDELINE_TEXT") != null)
							prConstraintGuardRail.setGuidelineText(rs.getString("GUIDELINE_TEXT"));
						else
							prConstraintGuardRail.setGuidelineText("");		
						
						prConstraintGuardRail.setPriceZoneID(rs.getInt("ZONE_ID"));
						
						guardRailDetail.add(prConstraintGuardRail);
					}
				}
				}
			}
			if(guardRailConstraint != null)
				guardRailConstraint.setCompetitorDetails(guardRailDetail);
			
		}catch(Exception exception){
			logger.error("Error when retrieving guardrail constraint - " + exception);
			throw new GeneralException("Error when retrieving guardrail constraint - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return guardRailConstraint;
	}
	
	/**
	 * 
	 * @param conn
	 * @param criteriaId
	 * @return criteria specfified in strategies
	 * @throws GeneralException
	 */
	public List<CriteriaDTO> getStrategyCriteria(Connection conn, Set<Integer> criteriaId) throws GeneralException {
		List<CriteriaDTO> criteriaList = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String sql = GET_CRITERIA_LIST.replaceAll("%s", PRCommonUtil.getCommaSeperatedStringFromIntSet(criteriaId));
//			logger.debug("getStrategyCriteria() - criteria query: " + sql);
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();

			while (rs.next()) {
				CriteriaDTO criteriaDTO = new CriteriaDTO();
				criteriaDTO.setCriteriaId(rs.getInt("CRITERIA_ID"));
				criteriaDTO.setCriteriaDetailId(rs.getInt("CRITERIA_DETAIL_ID"));
				criteriaDTO.setCriteriaTypeId(rs.getInt("CRITERIA_TYPE_ID"));
				criteriaDTO.setOprertorText(rs.getString("OPERATOR_TEXT"));
				criteriaDTO.setValueType(rs.getString("VALUE_TYPE"));
				criteriaDTO.setValue(rs.getString("VALUE"));
				criteriaList.add(criteriaDTO);
			}
		} catch (Exception exception) {
			logger.error("Error when retrieving default rounding table detail - " + exception.toString());
			throw new GeneralException("Error when retrieving default rounding table detail - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return criteriaList;
	}
	
	private PRConstraintFreightCharge getFreightChargeConstraint(List<StrategyDetail> strategyDetails,
			long constraintId1, int constraintTypeId1) throws GeneralException {

		PRConstraintFreightCharge freight = null;
		try {
			for (StrategyDetail strategyDetail : strategyDetails) {

				long constraintId = strategyDetail.constraintId;
				int constraintTypeId = strategyDetail.constraintTypeId;

				if (constraintId == constraintId1 && constraintTypeId == constraintTypeId1) {
					freight = new PRConstraintFreightCharge();
					freight.setConstraintID(strategyDetail.constraintId);
					freight.setConstraintTypeID(strategyDetail.constraintTypeId);
					freight.setStrategyID(strategyDetail.strategyId);
					freight.setIsfreightcharge(true);
					break;

				}
			}
		} catch (Exception exception) {
			logger.error("Error in getFreightChargeConstraint() - " + exception);
			throw new GeneralException("Error in getFreightChargeConstraint() - " + exception);
		} finally {
		}
		return freight;
	}
	
	/**
	 * Retrieves all strategies for input location from strategy table 
	 * @param conn
	 * @param strategyId 
	 * @return
	 */
	public List<Long> getAllActiveTempStrategies(Connection conn, 
			PRStrategyDTO strategyInput, int divisionIdOfZone) throws OfferManagementException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<TempStrategyKey, Long> locationProductStrategyMap = 
				new HashMap<TempStrategyKey, Long>();
		
		List<Long>whatIfStrategyIDs=new ArrayList<Long>();
		
		String sql = GET_TEMP_STRATEGIES;
		
		try{
			stmt = conn.prepareStatement(sql);
			int counter = 0;
			stmt.setInt(++counter, strategyInput.getProductLevelId());
			stmt.setInt(++counter, strategyInput.getProductId());
			stmt.setInt(++counter, strategyInput.getProductLevelId());
			stmt.setInt(++counter, strategyInput.getProductId());
			stmt.setInt(++counter, strategyInput.getProductLevelId());
			stmt.setInt(++counter, strategyInput.getProductId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, strategyInput.getLocationLevelId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, strategyInput.getLocationLevelId());
			stmt.setInt(++counter, strategyInput.getLocationId());
			stmt.setInt(++counter, divisionIdOfZone);
			stmt.setString(++counter, strategyInput.getStartDate());
			stmt.setString(++counter, strategyInput.getEndDate());
			stmt.setString(++counter, strategyInput.getStartDate());
			
//			logger.info("Temp Strategy Fetching Query:" + sql);
//			logger.info("Location Level Id: " + strategyInput.getLocationLevelId() + ", Location Id: " + strategyInput.getLocationId() + 
//					", Product Level Id: " + strategyInput.getProductLevelId() + ",Product Id: " + strategyInput.getProductId() + 
//					", division Id:" + divisionIdOfZone + ",Start Date: " + strategyInput.getStartDate() + ",End Date: " + strategyInput.getEndDate());
			
			rs = stmt.executeQuery();
			
			while (rs.next()) {
				PRStrategyDTO strategyDTO = new PRStrategyDTO();
//				logger.info("Stategy Id Fetched : "+ rs.getLong("STRATEGY_ID"));
				strategyDTO.setStrategyId(rs.getLong("STRATEGY_ID"));
				strategyDTO.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				strategyDTO.setLocationId(rs.getInt("LOCATION_ID"));
				strategyDTO.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				strategyDTO.setProductId(rs.getInt("PRODUCT_ID"));

				strategyDTO.setPriceCheckListId(rs.getInt("APPLY_TO"));
				strategyDTO.setVendorId(rs.getInt("VENDOR_ID"));
				strategyDTO.setStateId(rs.getInt("STATE_ID"));

				strategyDTO.setDsdRecommendationFlag(rs.getString("DSD_RECOMMENDATION_FLAG").charAt(0));
				strategyDTO.setCriteriaId(rs.getInt("CRITERIA_ID"));

				TempStrategyKey strategyKey = new TempStrategyKey(strategyDTO.getLocationLevelId(),
						strategyDTO.getLocationId(), strategyDTO.getProductLevelId(), strategyDTO.getProductId(),
						strategyDTO.getPriceCheckListId(), strategyDTO.getVendorId(), strategyDTO.getStateId(),
						strategyDTO.getDsdRecommendationFlag(), strategyDTO.getCriteriaId());

				//logger.info("Strategy key: " + strategyKey.toString());

				// If any strategy available for the strategy key
				if (locationProductStrategyMap.get(strategyKey) == null) {
					
					locationProductStrategyMap.put(strategyKey, strategyDTO.getStrategyId());
				} else {
					Long strategyID = locationProductStrategyMap.get(strategyKey);
					Long maxStratID = Math.max(strategyID, strategyDTO.getStrategyId());

					locationProductStrategyMap.put(strategyKey, strategyDTO.getStrategyId());

					locationProductStrategyMap.put(strategyKey, maxStratID);
				}
			}
			
			
			locationProductStrategyMap.forEach((strategyKey, StrategyID) -> {
				whatIfStrategyIDs.add(StrategyID);
			});
			
		}catch(Exception exception){
			throw new OfferManagementException("Error in getAllActiveTempStrategies() - " + exception, 
					RecommendationErrorCode.DB_GET_ALL_STRATEGIES);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
	
		return whatIfStrategyIDs;
	}

	public Long getGlobalStrategy(Connection conn) throws OfferManagementException {
		Long strategyId = null;

		PreparedStatement stmt = null;
		ResultSet rs = null;

		String sql = GET_GLOBAL_STRATEGY;

		try {
			stmt = conn.prepareStatement(sql);

			rs = stmt.executeQuery();

			while (rs.next()) {

				strategyId = rs.getLong("STRATEGY_ID");
			}
		} catch (Exception exception) {
			throw new OfferManagementException("Error in getGlobalStrategy() - " + exception,
					RecommendationErrorCode.DB_GET_ALL_STRATEGIES);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return strategyId;
	}
	
	
}


class StrategyDetail {
	public long strategyId;
	public int locationLevelId;
	public int locationId;
	public int productLevelId;
	public int productId;
	public int applyTo;
	public int vendorId;
	public char dsdRecommendationFlag = ' ';
	public int stateId;
	public int objectiveTypeId;
	public String objectiveTypeName;
	public double minObjVal;
	public double maxObjVal;
	public int guidelineId;
	public int guidelineTypeId;
	public int guidelineExecOrder;
	public boolean isExcludeGuideline;
	public long guidelineMarginId;
	public char marginValueType;
	public double minMarginPct;
	public double maxMarginPct;
	public double minMargin$;
	public double maxMargin$;
	public char currentMargin;
	public char marginCostFlag;
	public char marginIsItemLevel;
	public long guidelinePIId;
	public double piMinValue;
	public double piMaxValue;
	public char piIsItemLevel;
	public int piCompStrId;
	public long guidelineCompId;
	public String compOperatorText;
	public char groupPriceType;
	public int latestPriceObservation;
	public long guidelineCompDetailId;
	public int compStrId;
	public char compDetailValueType;
	public String compDetailOperatorText;
	public double compDetailMinValue;
	public double compDetailMaxValue;
	public char compDetailExclude;
	public String guidelineText;
	public long guidelineBrandId;
	public char brandValueType;
	public float brandMinValue;
	public float brandMaxValue;
	public char brandRetailType;
	public String brandOperatorText;
	public int brandTierId1;
	public int brandTierId2;
	public int brandId1;
	public int brandId2;
	public long guidelineSizeId;
	public char sizeValueType;
	public float sizeMinValue;
	public float sizeMaxValue;
	public String sizeOperatorText;
	public char higherToLowerFlag;
	public float shelfValue;
	
	public long guidelineLZId;
	public int lzLocationLevelId;
	public int lzLocationId;
	public char lzValueType;
	public double lzMinValue;
	public double lzMaxValue;
	public String lzOperatorText;
	
	public long constraintId;
	public int constraintTypeId;
	public long constraintPrePriceId;
	public double prePriceValue;
	public int prePriceQuantity;
	public long constraintLocPriceId;
	public double locPriceValue;
	public int locPriceQuantity;
	public long constraintMinMaxId;
	public double mmMinValue;
	public double mmMaxValue;
	public int mmQuantity;
	public long constraintRoundingId;
	public int[] roundingDigits;
	public String roundingDigitsString = null;
	public String defaultRounding;
	public int roundingTableId;
	public long constraintThresholdId;
	public char thresholdValueType;
	public double thresholdMinValue = Constants.DEFAULT_NA;
	public double thresholdMaxValue = Constants.DEFAULT_NA;
	public double thresholdMaxValue2 = Constants.DEFAULT_NA;
	public long constraintCostId;
	public boolean recommendBelowCost = false;
	public long constraintLigId;
	public char ligFlag;
	public long constraintLHRetailId;
	public char recommendRetailFlag;
	public int guardrailRelationalOperatorId;
	public int guardrailCompStrId; 
	public char guardrailValueType;
	public double guardrailMinValue = Constants.DEFAULT_NA;
	public double guardrailMaxValue = Constants.DEFAULT_NA;
	public String guardrailRelationalOperatorText;
	public String constraintText;
	public int criteriaId;
	public boolean aucOverrideEnabled;
	public String brandTier1;
	public String brandTier2;
	public boolean compOverrideEnabled;
	public int compOverrideId;
	public char groupPriceTypeGR;
	public int latestPriceObservationGR;
	public String  guardRailDetailsOperatorText;
	public char guardRailDetailExclude;
	public String guardrailGuidelineText;
	public long guardRailDetailID;
	//Added for GE
	public int priceZoneID;
	//Added for RA
	public int priceIncrease;
	public int  priceDecrease;
	public char overrideThreshold;
	
	//Added for FF to store the itemFlag(applied to moving or non moving)
	public char itemFlag='A';
	
}