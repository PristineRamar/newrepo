package com.pristine.dao.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRGuidelinesDTO;
import com.pristine.dto.offermgmt.PRObjectiveDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PromoStrategyDAO implements IDAO{
	private static Logger logger = Logger.getLogger("StrategyDAO");
	
	private static final String GET_ALL_ACTIVE_STRATEGIES = 
			"SELECT LOCATION_LEVEL_ID, " + 
			"   LOCATION_ID, " + 
			"   PRODUCT_LEVEL_ID, " + 
			"   PRODUCT_ID, " + 
			"   STRATEGY_ID, " + 
			"   APPLY_TO, " +
			"   START_DATE, " + 
			"   END_DATE, " + 
			"   (END_DATE - START_DATE) AS DAYS " + 
			" FROM " + 
			"   (SELECT LOCATION_LEVEL_ID, " + 
			"     LOCATION_ID, " + 
			"     PRODUCT_LEVEL_ID, " + 
			"     PRODUCT_ID, " + 
			"     STRATEGY_ID, " + 
			"     APPLY_TO, " +
			"     START_CALENDAR_ID, " + 
			"     END_CALENDAR_ID, " + 
			"     RCS.START_DATE, " + 
			"     ( " + 
			"     CASE " + 
			"       WHEN RCE.END_DATE IS NULL " + 
			"       THEN RCE.START_DATE " + 
			"        " + 
			"       ELSE RCE.END_DATE " + 
			"     END) AS END_DATE " + 
			"   FROM OR_STRATEGY S " + 
			"   LEFT JOIN %RETAIL_CALENDAR_TAB_NAME% RCS " + 
			"   ON S.START_CALENDAR_ID = RCS.CALENDAR_ID " + 
			"   LEFT JOIN %RETAIL_CALENDAR_TAB_NAME% RCE " + 
			"   ON S.END_CALENDAR_ID                 = RCE.CALENDAR_ID " + 
			"   WHERE TEMP                          <> 'Y' " +
			/* Get all Strategies with Parent and child and current product and all products */
			"   AND ((PRODUCT_LEVEL_ID, PRODUCT_ID) IN ( " + 
			"     (SELECT PRODUCT_LEVEL_ID, " + 
			"       PRODUCT_ID " + 
			"     FROM " + 
			"       (SELECT PRODUCT_ID, " + 
			"         PRODUCT_LEVEL_ID " + 
			"       FROM PRODUCT_GROUP_RELATION PGR " + 
			"         START WITH CHILD_PRODUCT_LEVEL_ID = ? " + 
			"       AND CHILD_PRODUCT_ID                = ? " + 
			"         CONNECT BY PRIOR PRODUCT_ID       = CHILD_PRODUCT_ID " + 
			"       AND PRIOR PRODUCT_LEVEL_ID          = CHILD_PRODUCT_LEVEL_ID " + 
			"       UNION " + 
			"       SELECT CHILD_PRODUCT_ID  AS PRODUCT_ID, " + 
			"         CHILD_PRODUCT_LEVEL_ID AS PRODUCT_LEVEL_ID " + 
			"       FROM PRODUCT_GROUP_RELATION PGR " + 
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
	
	private static final String GET_STRATEGY_DETAIL =
			"SELECT STGY.STRATEGY_ID, STGY.LOCATION_LEVEL_ID, STGY.LOCATION_ID, STGY.PRODUCT_LEVEL_ID, STGY.PRODUCT_ID, " +
					"STGY.APPLY_TO, " +
					"OBJT.OBJECTIVE_TYPE_ID, OBJT.OBJECTIVE_TYPE_NAME, " +
					"GUID.GUIDELINE_ID, GUID.GUIDELINE_TYPE_ID, GUID.EXEC_ORDER, GUID.EXCLUDE, " +
					"GM.GUIDELINE_MARGIN_ID, GM.VALUE_TYPE AS MARGIN_VALUE_TYPE, GM.MIN_MARGIN_PCT, GM.MAX_MARGIN_PCT, GM.MIN_MARGIN_DOLLAR, " +
					"GM.MAX_MARGIN_DOLLAR, GM.CURRENT_MARGIN_FLAG, " +
					"GM.COST_FLAG AS MARGIN_COST_FLAG, GM.IS_ITEM_LEVEL AS MAR_IS_ITEM_LEVEL " +
					"FROM OR_STRATEGY STGY " +
					"LEFT JOIN OR_OBJECTIVE OBJ ON STGY.STRATEGY_ID = OBJ.STRATEGY_ID " +
					"LEFT JOIN OR_OBJECTIVE_TYPE OBJT ON OBJ.OBJECTIVE_TYPE_ID = OBJT.OBJECTIVE_TYPE_ID " +
					"LEFT JOIN OR_GUIDELINE GUID ON STGY.STRATEGY_ID = GUID.STRATEGY_ID " +
					"LEFT JOIN OR_GUIDELINE_MARGIN GM ON GUID.GUIDELINE_ID = GM.GUIDELINE_ID " +
					"WHERE STGY.STRATEGY_ID = ?";

	/**
	 * Retrieves all strategies for input location from strategy table 
	 * @param conn
	 * @return
	 */
	public HashMap<StrategyKey, List<PRStrategyDTO>> getAllActivePromoStrategies(Connection conn, 
			PRStrategyDTO strategyInput, int divisionIdOfZone) throws OfferManagementException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<StrategyKey, List<PRStrategyDTO>> locationProductStrategyMap = 
				new HashMap<StrategyKey, List<PRStrategyDTO>>();
		List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
		String sql = GET_ALL_ACTIVE_STRATEGIES;
		
		try{
			
			String calType = PropertyManager.getProperty("RETAIL_CALENDAR_TYPE", Constants.RETAIL_CALENDAR_BUSINESS);
			if (calType.equals(Constants.RETAIL_CALENDAR_PROMO)) {
				sql = sql.replaceAll("%RETAIL_CALENDAR_TAB_NAME%", "RETAIL_CALENDAR_PROMO");
			} else {
				sql = sql.replaceAll("%RETAIL_CALENDAR_TAB_NAME%", "RETAIL_CALENDAR");
			}
			
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
			
			logger.debug("Strategy Fetching Query:" + sql);
			logger.debug("Location Level Id: " + strategyInput.getLocationLevelId() + ", Location Id: " + strategyInput.getLocationId() + 
					", Product Level Id: " + strategyInput.getProductLevelId() + ",Product Id: " + strategyInput.getProductId() + 
					", division Id:" + divisionIdOfZone + ",Start Date: " + strategyInput.getStartDate() + ",End Date: " + strategyInput.getEndDate());
			
			rs = stmt.executeQuery();
			
			while(rs.next()){
				logger.info("Strategy found");
				PRStrategyDTO strategyDTO = new PRStrategyDTO();
				strategyDTO.setStrategyId(rs.getLong("STRATEGY_ID"));
				strategyDTO.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				strategyDTO.setLocationId(rs.getInt("LOCATION_ID"));
				strategyDTO.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				strategyDTO.setProductId(rs.getInt("PRODUCT_ID"));
				strategyDTO.setPriceCheckListId(rs.getInt("APPLY_TO"));
				
				StrategyKey strategyKey = new StrategyKey(strategyDTO.getLocationLevelId(), strategyDTO.getLocationId(), 
						strategyDTO.getProductLevelId(), strategyDTO.getProductId());
				
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
			strategyDTO.setObjective(getObjective(strategyDetails, strategyId));
			strategyDTO.setGuidelines(getGuidelines(conn, strategyDetails, strategyId));
			
			// Temp code
			if(strategyDTO.getGuidelines().getMarginGuideline().size() > 0) {
				PRGuidelineMargin prGuidelineMargin = strategyDTO.getGuidelines().getMarginGuideline().get(0);
				strategyDTO.getObjective().setTargetObjectiveValueType(PRConstants.VALUE_TYPE_PCT);
				strategyDTO.getObjective().setTargetObjectiveValue(prGuidelineMargin.getMinMarginPct());	
			}
			//strategyDTO.setConstriants(getConstraints(conn, strategyDetails, strategyId));
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

			rs = stmt.executeQuery();
			while (rs.next()) {
				StrategyDetail strategyDetail = new StrategyDetail();
				strategyDetail.strategyId = rs.getLong("STRATEGY_ID");
				strategyDetail.locationLevelId = rs.getInt("LOCATION_LEVEL_ID");
				strategyDetail.locationId = rs.getInt("LOCATION_ID");
				strategyDetail.productLevelId = rs.getInt("PRODUCT_LEVEL_ID");
				strategyDetail.productId = rs.getInt("PRODUCT_ID");
				strategyDetail.applyTo = rs.getInt("APPLY_TO");
				strategyDetail.objectiveTypeId = rs.getInt("OBJECTIVE_TYPE_ID");
				strategyDetail.objectiveTypeName = rs.getString("OBJECTIVE_TYPE_NAME");
				
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
			for (StrategyDetail strategyDetail : strategyDetails) {
				// Ignore if already processed
				if (strategyDetail.guidelineId > 0 && processedGuidelineId.get(strategyDetail.guidelineId) == null) {
					boolean isExclude = strategyDetail.isExcludeGuideline;

					if (strategyDetail.guidelineTypeId == GuidelineTypeLookup.MARGIN.getGuidelineTypeId()) {
						if (!isExclude) {
							guidelinesDTO.setMarginGuideline(getGuidelineMargin(strategyDetails, strategyDetail.guidelineId,
									strategyDetail.guidelineTypeId));
						}
					}
					processedGuidelineId.put(strategyDetail.guidelineId, strategyDetail.guidelineId);
				}
			}
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
}