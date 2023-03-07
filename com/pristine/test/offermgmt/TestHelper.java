package com.pristine.test.offermgmt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRCompDetailLog;
import com.pristine.dto.offermgmt.PRConstraintCost;
import com.pristine.dto.offermgmt.PRConstraintLIG;
import com.pristine.dto.offermgmt.PRConstraintLocPrice;
import com.pristine.dto.offermgmt.PRConstraintLowerHigher;
import com.pristine.dto.offermgmt.PRConstraintMinMax;
//import com.pristine.dto.offermgmt.PRConstraintPrePrice;
import com.pristine.dto.offermgmt.PRConstraintRounding;
import com.pristine.dto.offermgmt.PRConstraintThreshold;
import com.pristine.dto.offermgmt.PRConstraintsDTO;
import com.pristine.dto.offermgmt.PRExplainAdditionalDetail;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineBrand;
import com.pristine.dto.offermgmt.PRGuidelineComp;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRGuidelinesDTO;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRObjectiveDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRRoundingTableDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ObjectiveTypeLookup;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
//import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * @author anand
 * 
 */
public class TestHelper {
	public static PRItemDTO getTestItem(Integer itemCode, int curRegMultiple, Double curRegPrice, Double curRegMPrice, Double curListCost,
			Double preListCost, Integer costChgIndicator, Integer compStrId, Double compPrice, PRStrategyDTO strategyDTO, long lastXWeeksMov) {

		return getTestItemInternal(itemCode, curRegMultiple, curRegPrice, curRegMPrice, curListCost, preListCost, costChgIndicator, compStrId,
				compPrice, strategyDTO, lastXWeeksMov, 0, null, null, null, 0, false, 0, 0, 0, 0, 0, null, null, null, 0, "");

	}

	public static PRItemDTO getTestItem(Integer itemCode, int curRegMultiple, Double curRegPrice, Double curRegMPrice, Integer costChgIndicator,
			String priceRange) {

		return getTestItemInternal(itemCode, curRegMultiple, curRegPrice, curRegMPrice, 0d, 0d, costChgIndicator, 0, 0d, null, 0, 0, null, null, null,
				0, false, 0, 0, 0, 0, 0, 0d, 0d, priceRange, 0, "");

	}

	public static PRItemDTO getTestItem1(Integer itemCode, int curRegMultiple, Double curRegPrice, Double curRegMPrice, Double curListCost,
			Double preListCost, Integer costChgIndicator, Integer compStrId, Double compPrice, PRStrategyDTO strategyDTO, long lastXWeeksMov,
			int curSaleMultipe, Double curSalePrice, Double curSaleMPrice, String promoWeekStartDate, int retLirId, boolean isLir,
			int predictionStatus, double predictedMov, int curPredictionStatus, double curPredictedMov, int recRegMultiple, Double recRegPrice,
			Double recRegMPrice) {

		return getTestItemInternal(itemCode, curRegMultiple, curRegPrice, curRegMPrice, curListCost, preListCost, costChgIndicator, compStrId,
				compPrice, strategyDTO, lastXWeeksMov, curSaleMultipe, curSalePrice, curSaleMPrice, promoWeekStartDate, retLirId, isLir,
				predictionStatus, predictedMov, curPredictionStatus, curPredictedMov, recRegMultiple, recRegPrice, recRegMPrice, null, 0, "");

	}

	public static PRItemDTO getTestItem2(Integer itemCode, int curRegMultiple, Double curRegPrice, Double curRegMPrice, Double curListCost,
			Double preListCost, Integer costChgIndicator, Integer compStrId, Double compPrice, PRStrategyDTO strategyDTO, long lastXWeeksMov,
			int curSaleMultipe, Double curSalePrice, Double curSaleMPrice, String promoWeekStartDate, int retLirId, boolean isLir,
			int predictionStatus, double predictedMov, int curPredictionStatus, double curPredictedMov, int recRegMultiple, Double recRegPrice,
			Double recRegMPrice, int locationId, String upc) {

		return getTestItemInternal(itemCode, curRegMultiple, curRegPrice, curRegMPrice, curListCost, preListCost, costChgIndicator, compStrId,
				compPrice, strategyDTO, lastXWeeksMov, curSaleMultipe, curSalePrice, curSaleMPrice, promoWeekStartDate, retLirId, isLir,
				predictionStatus, predictedMov, curPredictionStatus, curPredictedMov, recRegMultiple, recRegPrice, recRegMPrice, null, locationId,
				upc);

	}

	public static PRItemSaleInfoDTO getItemSaleInfoDTO(int quantity, Double price, int promoTypeId, String saleStartDate, String saleEndDate,
			String saleWeekStartDate) {
		PRItemSaleInfoDTO itemSaleInfoDTO = new PRItemSaleInfoDTO();
		itemSaleInfoDTO.setSalePrice(new MultiplePrice(quantity, price));
		itemSaleInfoDTO.setPromoTypeId(promoTypeId);
		itemSaleInfoDTO.setSaleStartDate(saleStartDate);
		itemSaleInfoDTO.setSaleEndDate(saleEndDate);
		itemSaleInfoDTO.setSaleWeekStartDate(saleWeekStartDate);
		return itemSaleInfoDTO;
	}

	private static PRItemDTO getTestItemInternal(Integer itemCode, int curRegMultiple, Double curRegPrice, Double curRegMPrice, Double curListCost,
			Double preListCost, Integer costChgIndicator, Integer compStrId, Double compPrice, PRStrategyDTO strategyDTO, long lastXWeeksMov,
			int curSaleMultipe, Double curSalePrice, Double curSaleMPrice, String promoWeekStartDate, int retLirId, boolean isLir,
			int predictionStatus, double predictedMov, int curPredictionStatus, double curPredictedMov, int recRegMultiple, Double recRegPrice,
			Double recRegMPrice, String priceRange, int locationId, String upc) {
		PRItemDTO item = new PRItemDTO();
		HashMap<LocationKey, MultiplePrice> compPriceMap = new HashMap<LocationKey, MultiplePrice>();
		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, compStrId);
		MultiplePrice compMulPrice = new MultiplePrice(1, compPrice);
		item.setRegMPack(curRegMultiple);

		if (curRegMultiple > 1) {
			item.setRegPrice(curRegMPrice / curRegMultiple);
		} else {
			if (curRegPrice != null && curRegPrice > 0) {
				item.setRegPrice(curRegPrice);
			}
		}

		/*
		 * if (curRegPrice != null && curRegPrice > 0) { item.setRegPrice(curRegPrice); }
		 */
		if (curRegMPrice != null && curRegMPrice > 0) {
			item.setRegMPrice(curRegMPrice);
		}
		if (compStrId > 0)
			compPriceMap.put(locationKey, compMulPrice);
		item.setItemCode(itemCode);

		if (curListCost != null && curListCost > 0)
			item.setListCost(curListCost);
		if (preListCost != null && preListCost > 0)
			item.setPreListCost(preListCost);
		item.setAllCompPrice(compPriceMap);
		item.setCostChgIndicator(costChgIndicator);
		item.setStrategyDTO(strategyDTO);
		item.setLastXWeeksMov(lastXWeeksMov);

		MultiplePrice curSaleMultiplePrice = PRCommonUtil.getMultiplePrice(curSaleMultipe, curSalePrice, curSaleMPrice);
		item.getCurSaleInfo().setSalePrice(curSaleMultiplePrice);
		item.getCurSaleInfo().setSaleWeekStartDate(promoWeekStartDate);
		item.setRetLirId(retLirId);
		item.setLir(isLir);
		item.setPredictionStatus(predictionStatus);
		item.setPredictedMovement(predictedMov);
		item.setCurRegPricePredictedMovement(curPredictedMov);
		// MultiplePrice RecMultiplePrice = PRCommonUtil.getMultiplePrice(recRegMultiple, recRegPrice, recRegMPrice);
		// item.setRecommendedRegMultiple(recRegMultiple);
		// item.setRecommendedRegPrice(recRegPrice);
		if (recRegPrice != null) {
			item.setRecommendedRegPrice(new MultiplePrice(recRegMultiple, recRegPrice));
		} else {
			item.setRecommendedRegPrice(null);
		}
		if (priceRange != null || priceRange == "") {
			String[] prices = priceRange.split(",");
			Double[] pricePoints = new Double[prices.length];
			for (int i = 0; i < prices.length; i++) {
				pricePoints[i] = Double.valueOf(prices[i]);
			}
			item.setPriceRange(pricePoints);
		}

		item.setChildLocationId(locationId);
		item.setUpc(upc);
		if (strategyDTO != null && strategyDTO.getObjective() != null) {
			item.setObjectiveTypeId(strategyDTO.getObjective().getObjectiveTypeId());
		}
		
		item.setActive(true);
		item.setAuthorized(true);
		
		return item;
	}

	/**
	 * 
	 * @param strategyId
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startDate
	 * @param endDate
	 * @param isRecommendAtStoreLevel
	 * @return
	 */
	public static PRStrategyDTO getStrategy(long strategyId, int locationLevelId, int locationId, int productLevelId, int productId, String startDate,
			String endDate, boolean isRecommendAtStoreLevel, int priceCheckListId, int vendorId, int stateId) {
		PRStrategyDTO strategyDTO = new PRStrategyDTO();
		PRConstraintCost constraintCost = new PRConstraintCost();
		PRConstraintLowerHigher constraintLowerHigher = new PRConstraintLowerHigher();

		strategyDTO.setStrategyId(strategyId);
		strategyDTO.setLocationLevelId(locationLevelId);
		strategyDTO.setLocationId(locationId);
		strategyDTO.setProductLevelId(productLevelId);
		strategyDTO.setProductId(productId);
		strategyDTO.setStartDate(startDate);
		strategyDTO.setEndDate(endDate);
		if (isRecommendAtStoreLevel)
			strategyDTO.setDsdRecommendationFlag('S');
		else
			strategyDTO.setDsdRecommendationFlag('Z');
		strategyDTO.setPriceCheckListId(priceCheckListId);
		strategyDTO.setVendorId(vendorId);
		strategyDTO.setStateId(stateId);

		strategyDTO.setObjective(new PRObjectiveDTO());

		PRGuidelinesDTO guidelinesDTO = new PRGuidelinesDTO();
		guidelinesDTO.setGuidelineIdMap(new HashMap<Integer, Integer>());

		TreeMap<Integer, ArrayList<Integer>> execOrderMap = new TreeMap<Integer, ArrayList<Integer>>();
		// execOrderMap.put(1, new ArrayList<Integer>());
		guidelinesDTO.setExecOrderMap(execOrderMap);
		strategyDTO.setGuidelines(guidelinesDTO);

		strategyDTO.setConstriants(new PRConstraintsDTO());
		strategyDTO.getConstriants().setCostConstraint(constraintCost);
		strategyDTO.getConstriants().setLowerHigherConstraint(constraintLowerHigher);

		return strategyDTO;
	}

	public static void setObjectiveFollowGuidelinesAndConstraints(PRStrategyDTO strategy) {
		strategy.getObjective().setObjectiveTypeId(13);
	}

	public static void setObjectiveFollowHighestMarginDollar(PRStrategyDTO strategy) {
		strategy.getObjective().setObjectiveTypeId(ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR.getObjectiveTypeId());
	}

	public static void setObjectiveFollowHighestRevenueDollar(PRStrategyDTO strategy) {
		strategy.getObjective().setObjectiveTypeId(4);
	}

	public static void setObjectiveFollowLowestPriceToCustomer(PRStrategyDTO strategy) {
		strategy.getObjective().setObjectiveTypeId(1);
	}

	public static void setObjectiveMaximizeMar$ByMaintaningSale$(PRStrategyDTO strategy) {
		strategy.getObjective().setObjectiveTypeId(ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_MAINTAIN_CUR_SALE.getObjectiveTypeId());
	}

	public static void setObjectiveMaximizeMar$ByMaintaningUnits(PRStrategyDTO strategy) {
		strategy.getObjective().setObjectiveTypeId(ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_MAINTAIN_CUR_MOV.getObjectiveTypeId());
	}

	/*** Return Guidelines ***/

	/**
	 * @param valueType
	 * @param minValue
	 * @param maxValue
	 * @return
	 */
	public static void setMarginGuideline(PRStrategyDTO strategy, char valueType, double minValue, double maxValue) {
		List<PRGuidelineMargin> marginGuidelines = new ArrayList<PRGuidelineMargin>();
		PRGuidelineMargin prGuidelineMargin = new PRGuidelineMargin();
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();

		if (valueType == PRConstants.VALUE_TYPE_PCT) {
			prGuidelineMargin.setValueType(PRConstants.VALUE_TYPE_PCT);
			prGuidelineMargin.setMinMarginPct(minValue);
			prGuidelineMargin.setMaxMarginPct(maxValue);
			
		} else if (valueType == PRConstants.VALUE_TYPE_$) {
			prGuidelineMargin.setValueType(PRConstants.VALUE_TYPE_$);
			prGuidelineMargin.setMinMargin$(minValue);
			prGuidelineMargin.setMaxMargin$(maxValue);
		}
		prGuidelineMargin.setCostFlag(PRConstants.MAR_COST_ALL_FLAG);
		prGuidelineMargin.setItemLevelFlag(Constants.YES);
		marginGuidelines.add(prGuidelineMargin);
		strategy.getGuidelines().setMarginGuideline(marginGuidelines);
		strategy.getGuidelines().getGuidelineIdMap().put(61, 1);
		// strategy.getGuidelines().getExecOrderMap().get(1).add(61);

		execOrderList.add(61);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	public static void setMarginGuidelineMaintainCurrentMargin(PRStrategyDTO strategy) {
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();
		List<PRGuidelineMargin> marginGuidelines = new ArrayList<PRGuidelineMargin>();
		PRGuidelineMargin prGuidelineMargin = new PRGuidelineMargin();
		prGuidelineMargin.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelineMargin.setCurrentMargin(PRConstants.VALUE_TYPE_I);
		prGuidelineMargin.setCostFlag(PRConstants.MAR_COST_CHANGED_FLAG);
		prGuidelineMargin.setItemLevelFlag(Constants.YES);
		marginGuidelines.add(prGuidelineMargin);
		strategy.getGuidelines().setMarginGuideline(marginGuidelines);
		strategy.getGuidelines().getGuidelineIdMap().put(61, 1);
		// strategy.getGuidelines().getExecOrderMap().get(1).add(61);

		execOrderList.add(61);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	public static void setMGItemMaintainPennyProfitOnCostChange(PRStrategyDTO strategy) {
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();
		List<PRGuidelineMargin> marginGuidelines = new ArrayList<PRGuidelineMargin>();
		PRGuidelineMargin prGuidelineMargin = new PRGuidelineMargin();
		prGuidelineMargin.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelineMargin.setCurrentMargin(PRConstants.VALUE_TYPE_$);
		prGuidelineMargin.setCostFlag(PRConstants.MAR_COST_INCREASED_FLAG);
		prGuidelineMargin.setItemLevelFlag(Constants.YES);
		prGuidelineMargin.setMinMargin$(Constants.DEFAULT_NA);
		prGuidelineMargin.setMaxMargin$(Constants.DEFAULT_NA);
		prGuidelineMargin.setMinMarginPct(Constants.DEFAULT_NA);
		prGuidelineMargin.setMaxMarginPct(Constants.DEFAULT_NA);
		marginGuidelines.add(prGuidelineMargin);

		prGuidelineMargin = new PRGuidelineMargin();
		prGuidelineMargin.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelineMargin.setCurrentMargin(PRConstants.VALUE_TYPE_$);
		prGuidelineMargin.setCostFlag(PRConstants.MAR_COST_DECREASED_FLAG);
		prGuidelineMargin.setItemLevelFlag(Constants.YES);
		prGuidelineMargin.setMinMargin$(Constants.DEFAULT_NA);
		prGuidelineMargin.setMaxMargin$(Constants.DEFAULT_NA);
		prGuidelineMargin.setMinMarginPct(Constants.DEFAULT_NA);
		prGuidelineMargin.setMaxMarginPct(Constants.DEFAULT_NA);
		marginGuidelines.add(prGuidelineMargin);

		strategy.getGuidelines().setMarginGuideline(marginGuidelines);
		strategy.getGuidelines().getGuidelineIdMap().put(61, 1);

		execOrderList.add(61);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	public static void setPIGuideline(PRStrategyDTO strategy, double minValue, double maxValue) {
		PRGuidelinePI prGuidelinePI = new PRGuidelinePI();
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();
		List<PRGuidelinePI> piGuidelines = new ArrayList<PRGuidelinePI>();
		prGuidelinePI.setMaxValue(maxValue);
		prGuidelinePI.setMinValue(minValue);
		piGuidelines.add(prGuidelinePI);
		strategy.getGuidelines().setPiGuideline(piGuidelines);
		strategy.getGuidelines().getGuidelineIdMap().put(62, 2);
		execOrderList.add(62);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	/**
	 * 
	 * @param strategy
	 * @param groupPriceType
	 *            PRConstants.GROUP_PRICE_TYPE_AVG / PRConstants.GROUP_PRICE_TYPE_MIN
	 * @param relationalOperator
	 *            PRICE_GROUP_EXPR_LESSER_SYM / PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM
	 * @param latestPriceObs
	 *            days(e.g. 30/60/90)
	 */
	public static void setMultiCompGuideline(PRStrategyDTO strategy, char groupPriceType, String relationalOperator, int latestPriceObs) {
		PRGuidelineComp guidelineComp = new PRGuidelineComp();
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();

		guidelineComp.setGroupPriceType(groupPriceType);
		guidelineComp.setLatestPriceObservationDays(latestPriceObs);
		guidelineComp.setRelationalOperatorText(relationalOperator);

		strategy.getGuidelines().setCompGuideline(guidelineComp);
		strategy.getGuidelines().getGuidelineIdMap().put(65, 3);
		// strategy.getGuidelines().getExecOrderMap().get(1).add(65);
		execOrderList.add(65);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	/***
	 * 
	 * @param strategy
	 * @param valueType
	 *            O/PRConstants.VALUE_TYPE_$/PRConstants.VALUE_TYPE_PCT
	 * @param minValue
	 *            Constants.DEFAULT_NA / Actual Value
	 * @param maxValue
	 *            Constants.DEFAULT_NA / Actual Value
	 * @param relationalOperator
	 *            PRICE_GROUP_EXPR_ABOVE / PRICE_GROUP_EXPR_BELOW / PRICE_GROUP_EXPR_EQUAL_SYM / PRICE_GROUP_EXPR_GREATER_SYM /
	 *            PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM / PRICE_GROUP_EXPR_LESSER_SYM / PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM
	 * @param isExclude
	 *            true/false
	 */
	public static void setMultiCompDetailGuideline(PRStrategyDTO strategy, int compStrId, char valueType, double minValue, double maxValue,
			String relationalOperator, boolean isExclude) {
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();

		guidelineCompDetail.setCompStrId(compStrId);
		guidelineCompDetail.setValueType(valueType);
		guidelineCompDetail.setMinValue(minValue);
		guidelineCompDetail.setMaxValue(maxValue);
		guidelineCompDetail.setRelationalOperatorText(relationalOperator);
		guidelineCompDetail.setIsExclude(isExclude);
		guidelineCompDetail.setGuidelineText(relationalOperator);

		strategy.getGuidelines().getCompGuideline().getCompetitorDetails().add(guidelineCompDetail);

	}

	public static void addMutiCompData(HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData, int compStrId, int itemCode, float regPrice,
			int regMPack, float regMPrice, String checkDate) {
		if (multiCompData == null)
			multiCompData = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();

		MultiCompetitorKey multiCompetitorKey;
		CompetitiveDataDTO compDataDTO = new CompetitiveDataDTO();

		multiCompetitorKey = new MultiCompetitorKey(compStrId, itemCode);

		compDataDTO.compStrId = compStrId;
		compDataDTO.itemcode = itemCode;
		compDataDTO.regPrice = regPrice;
		compDataDTO.regMPack = regMPack;
		compDataDTO.regMPrice = regMPrice;
		compDataDTO.checkDate = checkDate;

		multiCompData.put(multiCompetitorKey, compDataDTO);
	}

	public static void setBrandGuideline(PRStrategyDTO strategy) {
		PRGuidelineBrand prGuidelineBrand;
		ArrayList<PRGuidelineBrand> prGuidelinesBrand = new ArrayList<PRGuidelineBrand>();
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();
		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(2);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(15);
		prGuidelineBrand.setMinValue(10);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(9);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(55);
		prGuidelineBrand.setMinValue(45);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(8);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(45);
		prGuidelineBrand.setMinValue(40);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(7);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(40);
		prGuidelineBrand.setMinValue(35);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(6);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(35);
		prGuidelineBrand.setMinValue(30);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(5);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(30);
		prGuidelineBrand.setMinValue(25);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(4);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(25);
		prGuidelineBrand.setMinValue(20);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		prGuidelineBrand = new PRGuidelineBrand();
		prGuidelineBrand.setBrandTierId1(3);
		prGuidelineBrand.setBrandTierId2(10);
		prGuidelineBrand.setgId(63);
		prGuidelineBrand.setMaxValue(20);
		prGuidelineBrand.setMinValue(15);
		prGuidelineBrand.setOperatorText(PRConstants.PRICE_GROUP_EXPR_BELOW);
		prGuidelineBrand.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
		prGuidelineBrand.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelinesBrand.add(prGuidelineBrand);

		strategy.getGuidelines().setBrandGuideline(prGuidelinesBrand);
		strategy.getGuidelines().getGuidelineIdMap().put(63, 4);
		// strategy.getGuidelines().getExecOrderMap().get(1).add(63);
		execOrderList.add(63);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	public static void setSizeGuideline(PRStrategyDTO strategy, float shelfPct) {
		PRGuidelineSize prGuidelineSize = new PRGuidelineSize();
		ArrayList<PRGuidelineSize> prGuidelinesSize = new ArrayList<PRGuidelineSize>();
		ArrayList<Integer> execOrderList = new ArrayList<Integer>();
		prGuidelineSize.setHtol('Y');
		prGuidelineSize.setOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		prGuidelineSize.setValueType(PRConstants.VALUE_TYPE_PCT);
		prGuidelineSize.setShelfValue(shelfPct);
		prGuidelinesSize.add(prGuidelineSize);

		strategy.getGuidelines().setSizeGuideline(prGuidelinesSize);
		strategy.getGuidelines().getGuidelineIdMap().put(64, 5);
		// strategy.getGuidelines().getExecOrderMap().get(1).add(64);
		execOrderList.add(64);
		strategy.getGuidelines().getExecOrderMap().put(strategy.getGuidelines().getExecOrderMap().size() + 1, execOrderList);
	}

	public static void setPriceGroupLig(PRItemDTO item, double itemSize, int brandClassId, char brandSizePrecedence, char isPriceGroupLead,
			char isSizeFamilyLead) {
		PRPriceGroupDTO prPriceGroupDTO = setPriceGroup(item, itemSize, brandClassId, brandSizePrecedence, isPriceGroupLead, isSizeFamilyLead, true);
		item.setPgData(prPriceGroupDTO);
	}
	
	public static void setPriceGroup(PRItemDTO item, double itemSize, int brandClassId, char brandSizePrecedence, char isPriceGroupLead,
			char isSizeFamilyLead) {
		PRPriceGroupDTO prPriceGroupDTO = setPriceGroup(item, itemSize, brandClassId, brandSizePrecedence, isPriceGroupLead, isSizeFamilyLead, false);
		item.setPgData(prPriceGroupDTO);
	}
	/**
	 * 
	 * @param item
	 * @param itemSize
	 * @param brandClassId
	 *            STORE OR NATIONAL from BrandClassLookup
	 * @param brandSizePrecedence
	 *            PRConstants.BRAND_RELATION/PRConstants.SIZE_RELATION/X
	 * @param isPriceGroupLead
	 *            Y/''
	 * @param isSizeFamilyLead
	 *            Y/''
	 */
	private static PRPriceGroupDTO setPriceGroup(PRItemDTO item, double itemSize, int brandClassId, char brandSizePrecedence, char isPriceGroupLead,
			char isSizeFamilyLead, boolean isLig) {
		PRPriceGroupDTO prPriceGroupDTO = new PRPriceGroupDTO();
		prPriceGroupDTO.setItemSize(itemSize);
		prPriceGroupDTO.setBrandClassId(brandClassId);
		prPriceGroupDTO.setBrandSizePrecedence(brandSizePrecedence);
		if ((isPriceGroupLead == 'Y'))
			prPriceGroupDTO.setIsPriceGroupLead(isPriceGroupLead);

		if ((isSizeFamilyLead == 'Y'))
			prPriceGroupDTO.setIsSizeFamilyLead(isSizeFamilyLead);

		prPriceGroupDTO.setSizeRelationText("unit retail of higher size < unit retail of lower size");

		prPriceGroupDTO.setIsLig(isLig);
		return prPriceGroupDTO;
	}

	public static void setBrandRelationLig(PRItemDTO item, int relatedItemCode, int brandPrecedence, char higherToLower, double relatedItemSize,
			char relationType, double minValue, double maxValue, String operatorText, char retailType, char valueType) {
		setBrandRelationInternal(item, relatedItemCode, brandPrecedence, higherToLower, relatedItemSize, relationType, minValue, maxValue, operatorText,
				retailType, valueType, true);
	}
	
	public static void setBrandRelation(PRItemDTO item, int relatedItemCode, int brandPrecedence, char higherToLower, double relatedItemSize,
			char relationType, double minValue, double maxValue, String operatorText, char retailType, char valueType) {
		setBrandRelationInternal(item, relatedItemCode, brandPrecedence, higherToLower, relatedItemSize, relationType, minValue, maxValue, operatorText,
				retailType, valueType, false);
	}
	/**
	 * 
	 * @param item
	 * @param brandPrecedence
	 *            0/1
	 * @param higherToLower
	 *            'Y'/'N'/'X'
	 * @param relatedItemSize
	 * @param relationType
	 *            PRConstants.BRAND_RELATION/PRConstants.SIZE_RELATION
	 * @param minValue
	 * @param maxValue
	 * @param operatorText
	 *            PRICE_GROUP_EXPR_ABOVE / PRICE_GROUP_EXPR_BELOW / PRICE_GROUP_EXPR_EQUAL_SYM / PRICE_GROUP_EXPR_GREATER_SYM /
	 *            PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM / PRICE_GROUP_EXPR_LESSER_SYM / PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM
	 * @param retailType
	 *            PRConstants.RETAIL_TYPE_SHELF / PRConstants.RETAIL_TYPE_UNIT
	 * @param valueType
	 *            PRConstants.VALUE_TYPE_$/PRConstants.VALUE_TYPE_PCT
	 */
	private static void setBrandRelationInternal(PRItemDTO item, int relatedItemCode, int brandPrecedence, char higherToLower, double relatedItemSize,
			char relationType, double minValue, double maxValue, String operatorText, char retailType, char valueType, boolean isLig) {
		PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO = new PRPriceGroupRelatedItemDTO();
		PRPriceGroupDTO prPriceGroupDTO = item.getPgData();
		TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList;
		relationList = prPriceGroupDTO.getRelationList();
		ArrayList<PRPriceGroupRelatedItemDTO> prRelatedItems;

		if (relationList.get(PRConstants.BRAND_RELATION) == null) {
			prRelatedItems = new ArrayList<PRPriceGroupRelatedItemDTO>();
			relationList.put(PRConstants.BRAND_RELATION, prRelatedItems);
		} else {
			prRelatedItems = relationList.get(PRConstants.BRAND_RELATION);
		}

		prPriceGroupRelatedItemDTO.setRelatedItemCode(relatedItemCode);
		 
		prPriceGroupRelatedItemDTO.setBrandPrecedence(brandPrecedence);
		prPriceGroupRelatedItemDTO.setIsLig(isLig);
		prPriceGroupRelatedItemDTO.setHtol(higherToLower);
		prPriceGroupRelatedItemDTO.setRelatedItemSize(relatedItemSize);
		prPriceGroupRelatedItemDTO.setRelationType(relationType);

		PRPriceGroupRelnDTO priceGroupRelnDTO = new PRPriceGroupRelnDTO();
		priceGroupRelnDTO.setMaxValue(maxValue);
		priceGroupRelnDTO.setMinValue(minValue);
		priceGroupRelnDTO.setOperatorText(operatorText);
		priceGroupRelnDTO.setRetailType(retailType);
		priceGroupRelnDTO.setValueType(valueType);
		prPriceGroupRelatedItemDTO.setPriceRelation(priceGroupRelnDTO);

		prRelatedItems.add(prPriceGroupRelatedItemDTO);
	}

	/**
	 * 
	 * @param item
	 * @param brandPrecedence
	 *            0/1
	 * @param higherToLower
	 *            'Y'/'N'/'X'
	 * @param relatedItemSize
	 * @param relationType
	 *            PRConstants.BRAND_RELATION/PRConstants.SIZE_RELATION
	 * @param minValue
	 * @param maxValue
	 * @param operatorText
	 *            PRICE_GROUP_EXPR_ABOVE / PRICE_GROUP_EXPR_BELOW / PRICE_GROUP_EXPR_EQUAL_SYM / PRICE_GROUP_EXPR_GREATER_SYM /
	 *            PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM / PRICE_GROUP_EXPR_LESSER_SYM / PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM
	 * @param retailType
	 *            PRConstants.RETAIL_TYPE_SHELF / PRConstants.RETAIL_TYPE_UNIT
	 * @param valueType
	 *            PRConstants.VALUE_TYPE_$/PRConstants.VALUE_TYPE_PCT
	 */
	public static void setSizeRelation(PRItemDTO item, int relatedItemCode, int brandPrecedence, char higherToLower, double relatedItemSize,
			char relationType, double minValue, double maxValue, String operatorText, char retailType, char valueType) {
		PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO = new PRPriceGroupRelatedItemDTO();
		PRPriceGroupDTO prPriceGroupDTO = item.getPgData();
		TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList;
		relationList = prPriceGroupDTO.getRelationList();
		ArrayList<PRPriceGroupRelatedItemDTO> prRelatedItems;

		if (relationList.get(PRConstants.SIZE_RELATION) == null) {
			prRelatedItems = new ArrayList<PRPriceGroupRelatedItemDTO>();
			relationList.put(PRConstants.SIZE_RELATION, prRelatedItems);
		} else {
			prRelatedItems = relationList.get(PRConstants.SIZE_RELATION);
		}

		prPriceGroupRelatedItemDTO.setRelatedItemCode(relatedItemCode);
		prPriceGroupRelatedItemDTO.setBrandPrecedence(brandPrecedence);
		prPriceGroupRelatedItemDTO.setHtol(higherToLower);
		prPriceGroupRelatedItemDTO.setRelatedItemSize(relatedItemSize);
		prPriceGroupRelatedItemDTO.setRelationType(relationType);

		PRPriceGroupRelnDTO priceGroupRelnDTO = new PRPriceGroupRelnDTO();
		priceGroupRelnDTO.setMaxValue(maxValue);
		priceGroupRelnDTO.setMinValue(minValue);
		priceGroupRelnDTO.setOperatorText(operatorText);
		priceGroupRelnDTO.setRetailType(retailType);
		priceGroupRelnDTO.setValueType(valueType);
		prPriceGroupRelatedItemDTO.setPriceRelation(priceGroupRelnDTO);

		prRelatedItems.add(prPriceGroupRelatedItemDTO);
	}

	public static PRItemDTO getRelatedItem(int itemCode, double recommendedPrice) {
		PRItemDTO item = new PRItemDTO();
		item.setItemCode(itemCode);
		item.setProcessed(true);
		item.setRecommendedRegPrice(new MultiplePrice(1, recommendedPrice));
		return item;
	}
	
	public static PRItemDTO getRelatedItemLig(int itemCode, double recommendedPrice) {
		PRItemDTO item = new PRItemDTO();
		item.setItemCode(itemCode);
		item.setLir(true);
		item.setProcessed(true);
		item.setRecommendedRegPrice(new MultiplePrice(1, recommendedPrice));
		return item;
	}

	/*** Return Guidelines ***/

	public static void setThreasholdConstraint(PRStrategyDTO strategy, char valueType, double maxValue, double maxValue2) {
		PRConstraintThreshold prConstraintThreshold = new PRConstraintThreshold();
		prConstraintThreshold.setValueType(valueType);
		prConstraintThreshold.setMaxValue(maxValue);
		prConstraintThreshold.setMaxValue2(maxValue2);
		strategy.getConstriants().setThresholdConstraint(prConstraintThreshold);
	}

	public static void setLigConstraint(PRStrategyDTO strategy, char ligConstraintType) {
		PRConstraintLIG prConstraintLIG = new PRConstraintLIG();
		prConstraintLIG.setValue(ligConstraintType);
		strategy.getConstriants().setLigConstraint(prConstraintLIG);
	}

	public static void setCostConstraint(PRStrategyDTO strategy, boolean isRecommendBelowCost) {
		PRConstraintCost constraintCost = new PRConstraintCost();
		constraintCost.setIsRecBelowCost(isRecommendBelowCost);
		strategy.getConstriants().setCostConstraint(constraintCost);
	}

	/**
	 * 
	 * @param strategy
	 * @param lowerHigherFlag
	 *            //PRConstants.CONSTRAINT_HIGHER / PRConstants.CONSTRAINT_LOWER / PRConstants.CONSTRAINT_NO_LOWER_HIGHER
	 */
	public static void setLowerHigherConstraint(PRStrategyDTO strategy, char lowerHigherFlag) {
		PRConstraintLowerHigher constraintLowerHigher = new PRConstraintLowerHigher();
		constraintLowerHigher.setLowerHigherRetailFlag(lowerHigherFlag);
		strategy.getConstriants().setLowerHigherConstraint(constraintLowerHigher);
	}

	public static void setRoundingConstraint(PRStrategyDTO strategy, TreeMap<String, PRRoundingTableDTO> roundingTable) {
		PRConstraintRounding rConstraintDTO = new PRConstraintRounding();
		rConstraintDTO.setRoundingTableContent(roundingTable);
		strategy.getConstriants().setRoundingConstraint(rConstraintDTO);
	}

	public static void setMinMaxConstraint(PRStrategyDTO strategy, double minValue, double maxValue, int quantity) {
		PRConstraintMinMax prConstraintMinMax = new PRConstraintMinMax();

		prConstraintMinMax.setMinValue(minValue);
		prConstraintMinMax.setMaxValue(maxValue);
		prConstraintMinMax.setQuantity(quantity);
		strategy.getConstriants().setMinMaxConstraint(prConstraintMinMax);
	}

	// public static void setPrePriceConstraint(PRStrategyDTO strategy, int
	// quantity, double value){
	// PRConstraintPrePrice prConstraintPrePrice = new PRConstraintPrePrice();
	//
	// prConstraintPrePrice.setQuantity(quantity);
	// prConstraintPrePrice.setValue(value);
	// strategy.getConstriants().setPrePriceConstraint(prConstraintPrePrice);
	// }

	public static void setLocPriceConstraint(PRStrategyDTO strategy, int quantity, double value) {
		PRConstraintLocPrice connstraintLocPrice = new PRConstraintLocPrice();

		connstraintLocPrice.setQuantity(quantity);
		connstraintLocPrice.setValue(value);
		strategy.getConstriants().setLocPriceConstraint(connstraintLocPrice);
	}

	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTable1() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits(
				"0.03,0.05,0.07,0.09,0.13,0.15,0.17,0.19,0.23,0.25,0.27,0.29,0.35,0.39,0.45,0.49,0.55,0.59,0.65,0.69,0.75,0.79,0.85,0.89,0.95,0.99");
		roundingTableContent.put("0-0.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.09,0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("1-1.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("2-2.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("3-9.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29,0.49,0.69,0.79,0.99");
		roundingTableContent.put("10-9999", roundingTableDTO);

		return roundingTableContent;
	}

	/*** Set Guidelines Log ***/
	public static void setPrePriceLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setLockedPriceLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.LOCKED_PRICE.getConstraintTypeId());
		/*
		 * guidelineAndConstraintLog.setRecommendedRegMultiple( recommendedRegMultiple);
		 * guidelineAndConstraintLog.setRecommendedRegPrice (recommendedRegPrice);
		 */

		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	/**
	 * 
	 * @param explainLog
	 * @param guidelineAndConstraintLog
	 * @param isGuidelineOrConstraintApplied
	 * @param isConflict
	 * @param guidelineStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param guidelineEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param message
	 */
	public static void setMarginLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setCostNotAvailableLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	/**
	 * 
	 * @param explainLog
	 * @param guidelineAndConstraintLog
	 * @param isGuidelineOrConstraintApplied
	 * @param isConflict
	 * @param guidelineStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param guidelineEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param message
	 */
	public static void setPriceIndexLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message, MultiplePrice compPrice) {
		PRRange prRange;
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.PRICE_INDEX.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setIndexCompPrice(compPrice);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	/**
	 * 
	 * @param explainLog
	 * @param guidelineAndConstraintLog
	 * @param isGuidelineOrConstraintApplied
	 * @param isConflict
	 * @param guidelineStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param guidelineEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param relationItemCode
	 * @param isLirInd
	 */
	public static void setBrandLog(PRItemDTO itemInfo, PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, int relationItemCode, boolean isLirInd) {
		PRRange prRange;
		String relationText = "";

		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = itemInfo.getPgData().getRelationList();

		for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
			if (entry.getKey() == PRConstants.BRAND_RELATION) {
				for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
					if (relatedItem.getRelatedItemCode() == relationItemCode) {
						relationText = relatedItem.getPriceRelation().formRelationText(entry.getKey());
						guidelineAndConstraintLog.setOperatorText(relatedItem.getPriceRelation().getOperatorText());
					}
				}
			}
		}

		guidelineAndConstraintLog.setRelationText(relationText);

		guidelineAndConstraintLog.setRelationItemCode(relationItemCode);
		guidelineAndConstraintLog.setIsLirInd(isLirInd);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	/**
	 * 
	 * @param explainLog
	 * @param guidelineAndConstraintLog
	 * @param isGuidelineOrConstraintApplied
	 * @param isConflict
	 * @param guidelineStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param guidelineEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param relationItemCode
	 * @param isLirInd
	 */
	public static void setSizeLog(PRItemDTO itemInfo, PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, int relationItemCode, boolean isLirInd, double relatedItemSize) {
		PRRange prRange;
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.SIZE.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		if (itemInfo.getPgData().getSizeRelationText() != null)
			guidelineAndConstraintLog.setRelationText(itemInfo.getPgData().getSizeRelationText());
		else
			guidelineAndConstraintLog.setRelationText("");

		guidelineAndConstraintLog.setRelationItemCode(relationItemCode);
		guidelineAndConstraintLog.setRelatedItemSize(String.valueOf(relatedItemSize));
		guidelineAndConstraintLog.setIsLirInd(isLirInd);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setThresholdLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double threshold1StartVal, double threshold1EndVal, double threshold2StartVal,
			double threshold2EndVal, double outputStartVal, double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(threshold1StartVal);
		prRange.setEndVal(threshold1EndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(threshold2StartVal);
		prRange.setEndVal(threshold2EndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange2(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setRoundingLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog, boolean isConflict,
			List<Double> roundingDigits) {
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.ROUNDING.getConstraintTypeId());
		guidelineAndConstraintLog.setRoundingDigits(roundingDigits);
		guidelineAndConstraintLog.setIsConflict(isConflict);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setMinMaxLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.MIN_MAX.getConstraintTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setCostConstraintLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.COST.getConstraintTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	public static void setLowerHigherConstraintLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.LOWER_HIGHER.getConstraintTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}

	/**
	 * 
	 * @param explainLog
	 * @param guidelineAndConstraintLog
	 * @param isGuidelineOrConstraintApplied
	 * @param isConflict
	 * @param guidelineStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param guidelineEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputStartVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param outputEndVal
	 *            Set Constants.DEFAULT_NA if not applicable
	 * @param message
	 * @throws Exception
	 */
	public static void setMultiCompLog(PRItemDTO itemInfo, PRStrategyDTO strategy, HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData,
			PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog, boolean isGuidelineOrConstraintApplied,
			boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal, double outputEndVal, String message)
			throws Exception {
		PRRange prRange;
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.COMPETITION.getGuidelineTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);

		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		// setCompDetailLog(itemInfo, strategy, multiCompData,
		// guidelineAndConstraintLog);
	}

	public static void setCompDetailLog(PRItemDTO itemInfo, PRStrategyDTO strategy, HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData,
			PRGuidelineAndConstraintLog guidelineAndConstraintLog, PRGuidelineCompDetail guidelineCompDetail, String guidelineText,
			double guidelineStartVal, double guidelineEndVal, double outputStartVal, double outputEndVal) throws Exception {
		MultiCompetitorKey multiCompetitorKey;
		PRGuidelineComp compGuideline = strategy.getGuidelines().getCompGuideline();
		// List<PRGuidelineCompDetail> compDetails =
		// compGuideline.getCompetitorDetails();

		// for (PRGuidelineCompDetail guidelineCompDetail : compDetails) {
		multiCompetitorKey = new MultiCompetitorKey(guidelineCompDetail.getCompStrId(), itemInfo.getItemCode());
		// Get item price from map
		if (multiCompData.get(multiCompetitorKey) != null) {
			CompetitiveDataDTO competitiveDataDTO = multiCompData.get(multiCompetitorKey);
			double tempPrice = getPriceInLastObsDay(strategy, competitiveDataDTO, compGuideline.getLatestPriceObservationDays());
			writeCompDetailLog(guidelineAndConstraintLog, competitiveDataDTO.compStrId, competitiveDataDTO.checkDate, competitiveDataDTO.regMPack,
					tempPrice, competitiveDataDTO.regMPrice, guidelineText, guidelineStartVal, guidelineEndVal, outputStartVal, outputEndVal);
		} else {
			writeCompDetailLog(guidelineAndConstraintLog, guidelineCompDetail.getCompStrId(), "", 0, 0, 0, guidelineText, guidelineStartVal,
					guidelineEndVal, outputStartVal, outputEndVal);
		}
		// }
	}

	// public static void setPrePriceLog(PRExplainLog explainLog,
	// PRGuidelineAndConstraintLog guidelineAndConstraintLog,
	// int multiple, Double curRegPrice){
	// guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId());
	// guidelineAndConstraintLog.setRecommendedRegMultiple(multiple);
	// guidelineAndConstraintLog.setRecommendedRegPrice(curRegPrice);
	// explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	// }

	private static void writeCompDetailLog(PRGuidelineAndConstraintLog guidelineAndConstraintLog, int compStrId, String checkDate, int regMPack,
			double regUnitPrice, double regMPrice, String guidelineText, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal) {
		// Set logs
		
		PRCompDetailLog prCompDetailLog = new PRCompDetailLog();
		PRRange prRange;
		prCompDetailLog.setCompStrId(compStrId);
		prCompDetailLog.setIsPricePresent(false);
		if (regUnitPrice > 0) {
			prCompDetailLog.setCheckDate(checkDate);
			// prCompDetailLog.setRegPrice(format.format(regUnitPrice));
			MultiplePrice multiplePrice = PRCommonUtil.getMultiplePrice(regMPack, regUnitPrice, regMPrice);
			prCompDetailLog.setMultiple(String.valueOf(multiplePrice.multiple));
			prCompDetailLog.setRegPrice(String.valueOf(multiplePrice.price));
			prCompDetailLog.setIsPricePresent(true);
		}
		prCompDetailLog.setMaxRetailText(guidelineText);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		prCompDetailLog.setGuidelineRange(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		prCompDetailLog.setFinalRange(prRange);

		guidelineAndConstraintLog.getCompDetails().add(prCompDetailLog);
	}

	private static double getPriceInLastObsDay(PRStrategyDTO strategyDTO, CompetitiveDataDTO competitiveDataDTO, int obsDay) throws Exception {
		Date currentWeekStartDate, itemCheckDate;
		double regUnitPrice = 0d;
		String curWeekStartDate = DateUtil.getWeekStartDate(0);
		try {
			if (curWeekStartDate != null && competitiveDataDTO.checkDate != null) {

				currentWeekStartDate = DateUtil.toDate(curWeekStartDate);
				itemCheckDate = DateUtil.toDate(competitiveDataDTO.checkDate);
				Calendar cal = Calendar.getInstance();
				cal.setTime(currentWeekStartDate);
				cal.add(Calendar.DATE, -(obsDay));
				Date lastObsDay = cal.getTime();

				// If check date is before last observation day or it is equal
				// (include both days, strategy first day and last obs day)
				if (itemCheckDate.after(lastObsDay) || itemCheckDate.equals(lastObsDay)) {
					regUnitPrice = (double) (competitiveDataDTO.regMPack > 1 ? (competitiveDataDTO.regMPrice / competitiveDataDTO.regMPack)
							: competitiveDataDTO.regPrice);
				}

			} else {
				// logger.debug("Start Date or Check Date is null : Strategy Id: "
				// + strategyDTO.getStrategyId() + "Item Code: " +
				// this.inputItem.getItemCode());
			}

		} catch (GeneralException e) {
			// logger.error("Exception in getPriceInLastObsDay() -- " +
			// e.toString(), e);
			throw new Exception();
		}
		return regUnitPrice;
	}

	/*** Set Guidelines Log ***/

	public static PRItemDTO setItemDTO(int itemCode, int segmentId, int subCategoryId, int categoryId, int portfolioId, int departmentId,
			int retLirId, int retLirItemCode, double itemSize, String upc, boolean lirInd, int vendorId, int stateId) {
		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(itemCode);
		prItemDTO.setSegmentProductId(segmentId);
		prItemDTO.setSubCatProductId(subCategoryId);
		prItemDTO.setCategoryProductId(categoryId);
		prItemDTO.setPortfolioProductId(portfolioId);
		prItemDTO.setDeptProductId(departmentId);
		prItemDTO.setRetLirId(retLirId);
		// prItemDTO.setRetLirItemCode(retLirItemCode);
		prItemDTO.setItemSize(itemSize);
		prItemDTO.setUpc(upc);
		prItemDTO.setLir(lirInd);
		prItemDTO.setVendorId(vendorId);
		prItemDTO.setStateId(stateId);
		return prItemDTO;
	}

	public static PRItemDTO setLIGItemDTO(int retLirId, long lastXWeeksMov, int locationId, PRStrategyDTO strategyDTO) {
		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setRetLirId(retLirId);
		prItemDTO.setLir(true);
		prItemDTO.setItemCode(retLirId);
		prItemDTO.setLastXWeeksMov(lastXWeeksMov);
		prItemDTO.setChildLocationId(locationId);
		prItemDTO.setStrategyDTO(strategyDTO);
		return prItemDTO;
	}

	public static PRItemDTO setItemDTO(int locationId, int itemCode, int retLirId, double size, String uomName, String regPriceEffDate,
			Double curRegPrice, Double preRegPrice, Double curListCost, Double preListCost, Double curVipCost, Double preVipCost, int compStrId,
			String compPriceCheckDate, Double curCompReg, Double preCompReg, Integer recRegMultiple, Double recRegPrice, double avgMovement,
			double avgRevenue) {

		HashMap<LocationKey, String> compPriceCheckDateMap = new HashMap<LocationKey, String>();
		HashMap<LocationKey, MultiplePrice> compPriceMap = new HashMap<LocationKey, MultiplePrice>();
		HashMap<LocationKey, MultiplePrice> preCompRegPriceMap = new HashMap<LocationKey, MultiplePrice>();
		MultiplePrice compMulPrice = new MultiplePrice(1, curCompReg);
		MultiplePrice preCompMulPrice = new MultiplePrice(1, preCompReg);

		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setChildLocationId(locationId);
		prItemDTO.setItemCode(itemCode);
		prItemDTO.setRetLirId(retLirId);
		prItemDTO.setItemSize(size);
		prItemDTO.setUOMName(uomName);
		prItemDTO.setCurRegPriceEffDate(regPriceEffDate);
		prItemDTO.setRegPrice(curRegPrice);
		prItemDTO.setPreRegPrice(preRegPrice);
		prItemDTO.setListCost(curListCost);
		prItemDTO.setPreviousCost(preListCost);
		prItemDTO.setVipCost(curVipCost);
		prItemDTO.setPreVipCost(preVipCost);

		LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, compStrId);
		compPriceCheckDateMap.put(locationKey, compPriceCheckDate);
		prItemDTO.setAllCompPriceCheckDate(compPriceCheckDateMap);
		compPriceMap.put(locationKey, compMulPrice);
		prItemDTO.setAllCompPrice(compPriceMap);
		preCompRegPriceMap.put(locationKey, preCompMulPrice);
		prItemDTO.setAllCompPreviousPrice(preCompRegPriceMap);

		// prItemDTO.setRecommendedRegMultiple(recRegMultiple);
		// prItemDTO.setRecommendedRegPrice(recRegPrice);
		prItemDTO.setRecommendedRegPrice(new MultiplePrice(recRegMultiple, recRegPrice));
		prItemDTO.setAvgMovement(avgMovement);
		prItemDTO.setAvgRevenue(avgRevenue);

		return prItemDTO;
	}

	public static HashMap<ItemKey, PRItemDTO> setItemDataMap(HashMap<ItemKey, PRItemDTO> itemDataMap, PRItemDTO prItemDTO) {
		if (itemDataMap == null)
			itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		ItemKey itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		itemDataMap.put(itemKey, prItemDTO);

		return itemDataMap;
	}

	public static HashMap<ItemKey, PRItemDTO> setLigItemDataMap(HashMap<ItemKey, PRItemDTO> itemDataMap, PRItemDTO prItemDTO) {
		if (itemDataMap == null)
			itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		ItemKey itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.LIG_ITEM_INDICATOR);
		itemDataMap.put(itemKey, prItemDTO);

		return itemDataMap;
	}

	public static PRItemDTO updateItemDTO(PRItemDTO prItemDTO, MultiplePrice curRegPrice) {
		prItemDTO.setRegMPack(curRegPrice.multiple);
		if (curRegPrice.multiple > 1) {
			prItemDTO.setRegMPrice(curRegPrice.price);
		} else {
			prItemDTO.setRegPrice(curRegPrice.price);
		}

		return prItemDTO;
	}

	// @param retLirMap Map containing RET_LIR_ID as key and map containing
	// RET_LIR_ITEM_CODE as key and items under that group as its value

	public static HashMap<Integer, List<PRItemDTO>> setRetLirMap(HashMap<Integer, List<PRItemDTO>> retLirMap, int retLirId, int retLirItemCode,
			int itemCode) {
		List<PRItemDTO> ligMembers;
		PRItemDTO ligMember = new PRItemDTO();
		ligMember.setItemCode(itemCode);
		ligMember.setLir(false);
		ligMember.setRetLirId(retLirId);

		if (retLirMap == null)
			retLirMap = new HashMap<Integer, List<PRItemDTO>>();

		// //Ret Lir Id
		// if(retLirMap.get(retLirId) == null){
		// retLirItemCodeMap = new HashMap<Integer, ArrayList<Integer>>();
		// }else{
		// retLirItemCodeMap = retLirMap.get(retLirId);
		// }
		//
		// //Ret Lir Item Code
		// if(retLirItemCodeMap.get(retLirItemCode) == null){
		// ligMembers = new ArrayList<Integer>();
		// }else{
		// ligMembers = retLirItemCodeMap.get(retLirItemCode);
		// }

		// Ret Lir Item Code
		if (retLirMap.get(retLirId) == null) {
			ligMembers = new ArrayList<PRItemDTO>();
		} else {
			ligMembers = retLirMap.get(retLirId);
		}

		// Lig members
		ligMembers.add(ligMember);
		// retLirItemCodeMap.put(retLirItemCode, ligMembers);
		retLirMap.put(retLirId, ligMembers);

		return retLirMap;
	}

	// * @param retLirConstraintMap Map containing Child Location Id as key and
	// map containing RET_LIR_ID and S-Same/D-Different LIG constraint as its
	// value
	public static HashMap<Integer, HashMap<Integer, Character>> setRetLirConstraintMap(
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap, int locationId, int retLirId, char ligConstraint) {
		HashMap<Integer, Character> retLirIdMap;

		if (retLirConstraintMap == null)
			retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();

		// Location Id
		if (retLirConstraintMap.get(locationId) == null) {
			retLirIdMap = new HashMap<Integer, Character>();
		} else {
			retLirIdMap = retLirConstraintMap.get(locationId);
		}

		retLirIdMap.put(retLirId, ligConstraint);
		retLirConstraintMap.put(locationId, retLirIdMap);

		return retLirConstraintMap;
	}

	// * @param itemPriceRangeMap Map containing Child Location Id as key and
	// list of items for that location as value
	public static HashMap<Integer, List<PRItemDTO>> setItemPriceRangeMap(HashMap<Integer, List<PRItemDTO>> itemPriceRangeMap, int locationId,
			PRItemDTO prItemDTO) {
		List<PRItemDTO> itemList;

		if (itemPriceRangeMap == null)
			itemPriceRangeMap = new HashMap<Integer, List<PRItemDTO>>();

		if (itemPriceRangeMap.get(locationId) == null) {
			itemList = new ArrayList<PRItemDTO>();
		} else {
			itemList = itemPriceRangeMap.get(locationId);
		}

		itemList.add(prItemDTO);
		itemPriceRangeMap.put(locationId, itemList);

		return itemPriceRangeMap;
	}

	public static HashMap<StrategyKey, List<PRStrategyDTO>> setStrategyMap(HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap,
			PRStrategyDTO inputStrategy) {
		List<PRStrategyDTO> strategies;

		if (inpStrategyMap == null) {
			inpStrategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		}
		StrategyKey strategyKey = new StrategyKey(inputStrategy.getLocationLevelId(), inputStrategy.getLocationId(),
				inputStrategy.getProductLevelId(), inputStrategy.getProductId());

		if (inpStrategyMap.get(strategyKey) != null) {
			strategies = inpStrategyMap.get(strategyKey);
		} else {
			strategies = new ArrayList<PRStrategyDTO>();
		}
		strategies.add(inputStrategy);
		inpStrategyMap.put(strategyKey, strategies);
		return inpStrategyMap;
	}

	public static void setPrePriceStatus(PRItemDTO itemDTO, int prePriceStatus) {
		itemDTO.setIsPrePriced(prePriceStatus);
	}

	public static void setCompId(HashMap<Integer, LocationKey> compIdMap, int compType, int locationLevelId, int locationId) {
		LocationKey locationKey = new LocationKey(locationLevelId, locationId);
		compIdMap.put(PRConstants.COMP_TYPE_1, locationKey);
	}

	public static RetailCalendarDTO getCalendarDetails(String startDate, String endDate) {
		RetailCalendarDTO retailCalendarDTO = new RetailCalendarDTO();
		retailCalendarDTO.setStartDate(startDate);
		return retailCalendarDTO;
	}

	public static PRRecommendationRunHeader getRecommendationRunHeader(String startDate) {
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();

		recommendationRunHeader.setStartDate(startDate);

		return recommendationRunHeader;
	}

	public static PRSubstituteItem getSubstituteItem(int baseProductLevelId, int baseProductId, int subsProductLevelId, int subsProductId,
			double impactFactor) {
		PRSubstituteItem susbstituteItem = new PRSubstituteItem();
		ItemKey baseProductKey = PRCommonUtil.getItemKey(baseProductId, (baseProductLevelId == Constants.PRODUCT_LEVEL_ID_LIG ? true : false));
		ItemKey subsProductKey = PRCommonUtil.getItemKey(subsProductId, (subsProductLevelId == Constants.PRODUCT_LEVEL_ID_LIG ? true : false));

		susbstituteItem.setBaseProductKey(baseProductKey);
		susbstituteItem.setSubsProductKey(subsProductKey);
		susbstituteItem.setImpactFactor(impactFactor);

		return susbstituteItem;
	}

	public static void setItemZonePrice(HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory, int itemCode, String weekStartDate,
			int regMPack, double regPrice, double regMPrice, String regEffectiveDate, int saleMPack, double salePrice, double saleMPrice,
			String saleEffectiveDate) {
		HashMap<String, RetailPriceDTO> priceHistory = new HashMap<String, RetailPriceDTO>();
		if (itemZonePriceHistory.get(itemCode) != null) {
			priceHistory = itemZonePriceHistory.get(itemCode);
		}

		RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
		retailPriceDTO.setRegMPack(regMPack);
		retailPriceDTO.setRegPrice((float) (regPrice));
		retailPriceDTO.setRegMPrice((float) (regMPrice));
		retailPriceDTO.setRegEffectiveDate(regEffectiveDate);

		retailPriceDTO.setSaleMPack(saleMPack);
		retailPriceDTO.setSalePrice((float) (salePrice));
		retailPriceDTO.setSaleMPrice((float) (saleMPrice));

		priceHistory.put(weekStartDate, retailPriceDTO);
		itemZonePriceHistory.put(itemCode, priceHistory);
	}

	public static PredictionDetailKey getPredictionDetailKey(int locationLevelId, int locationId, long lirOrItemCode, int regQuantity,
			Double regPrice, int saleQuantity, Double salePrice, int adPageNo, int blockNo, int promoTypeId, int displayTypeId) {
		PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, lirOrItemCode, regQuantity, regPrice,
				saleQuantity, salePrice, adPageNo, blockNo, promoTypeId, displayTypeId);
		return predictionDetailKey;
	}

	public static PredictionDetailDTO getPredictionDetailDTO(int startCalendarId, int endCalendarId, int locationLevelId, int locationId,
			long lirOrItemCode, int regularQuantity, Double regularPrice, int saleQuantity, Double salePrice, int adPageNo, int blockNo,
			int displayTypeId, int promoTypeId, double predictedMovement, int predictionStatus, double predictedMovementWithoutSubsEffect) {
		PredictionDetailDTO predictionDetailDTO = new PredictionDetailDTO();

		predictionDetailDTO.setStartCalendarId(startCalendarId);
		predictionDetailDTO.setEndCalendarId(endCalendarId);
		predictionDetailDTO.setLocationLevelId(locationLevelId);
		predictionDetailDTO.setLocationId(locationId);
		predictionDetailDTO.setLirOrItemCode(lirOrItemCode);
		predictionDetailDTO.setRegularQuantity(regularQuantity);
		predictionDetailDTO.setRegularPrice(regularPrice);
		predictionDetailDTO.setSaleQuantity(saleQuantity);
		predictionDetailDTO.setSalePrice(salePrice);
		predictionDetailDTO.setAdPageNo(adPageNo);
		predictionDetailDTO.setBlockNo(blockNo);
		predictionDetailDTO.setDisplayTypeId(displayTypeId);
		predictionDetailDTO.setPromoTypeId(promoTypeId);
		predictionDetailDTO.setPredictedMovement(predictedMovement);
		predictionDetailDTO.setPredictionStatus(predictionStatus);
		predictionDetailDTO.setPredictedMovementWithoutSubsEffect(predictedMovementWithoutSubsEffect);

		return predictionDetailDTO;
	}

	public static void setRegPricePredictionMap(HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap, int regMPack, Double regPrice,
			Double predictedMov, PredictionStatus predictionStatus) {
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(regMPack);
		pricePointDTO.setRegPrice(regPrice);
		pricePointDTO.setPredictedMovement(predictedMov);
		pricePointDTO.setPredictionStatus(predictionStatus);

		regPricePredictionMap.put(new MultiplePrice(regMPack, regPrice), pricePointDTO);
	}

	public static void setPricePoints(PRItemDTO item, String pricePoints) {
		if (pricePoints != null || pricePoints == "") {
			String[] prices = pricePoints.split(",");
			Double[] pp = new Double[prices.length];
			for (int i = 0; i < prices.length; i++) {
				pp[i] = Double.valueOf(prices[i]);
			}
			item.setPriceRange(pp);
		}
	}

	public static void setSaleDetails(HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, int itemCode, String saleWeekStartDate,
			String actualSaleEndDate) {

		List<PRItemSaleInfoDTO> saleInfoList = new ArrayList<PRItemSaleInfoDTO>();
		if (saleDetails.get(itemCode) != null) {
			saleInfoList = saleDetails.get(itemCode);
		}

		PRItemSaleInfoDTO itemSaleInfoDTO = new PRItemSaleInfoDTO();

		itemSaleInfoDTO.setSaleWeekStartDate(saleWeekStartDate);
		itemSaleInfoDTO.setSaleEndDate(actualSaleEndDate);
		saleInfoList.add(itemSaleInfoDTO);
		saleDetails.put(itemCode, saleInfoList);
	}

	public static RetailCostDTO setRetailCostDTO(int calendarId, int levelTypeId, int levelId, Float listCost, Float listCost2,
			String effListCostDate, Float vipCost, Float dealCost, String dealStratDate, String dealEndDate) {
		RetailCostDTO retailCostDTO = new RetailCostDTO();
		Float finalListCost = ((listCost2 != null && listCost2 > 0) ? listCost2 : listCost);

		retailCostDTO.setCalendarId(calendarId);
		retailCostDTO.setListCost(finalListCost);
		retailCostDTO.setEffListCostDate(effListCostDate);
		retailCostDTO.setVipCost((vipCost != null ? vipCost : 0));
		retailCostDTO.setDealCost((dealCost != null ? dealCost : 0));
		retailCostDTO.setDealStartDate(dealStratDate);
		retailCostDTO.setDealEndDate(dealEndDate);
		retailCostDTO.setLevelTypeId(levelTypeId);
		retailCostDTO.setLevelId(String.valueOf(levelId));
		return retailCostDTO;
	}

	public static HashMap<String, List<RecommendationRuleMapDTO>> setRecommendationRuleMap(String ruleCode, int objectiveTypeId, boolean enabled) {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();

		RecommendationRuleMapDTO recommendationRuleMapDTO = new RecommendationRuleMapDTO();
		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();

		recommendationRuleMapDTO.setRuleCode(ruleCode);
		recommendationRuleMapDTO.setObjectiveTypeId(objectiveTypeId);
		recommendationRuleMapDTO.setEnabled(enabled);

		recommendationRules.add(recommendationRuleMapDTO);
		recommendationRuleMap.put(ruleCode, recommendationRules);

		return recommendationRuleMap;
	}

	public static HashMap<String, List<RecommendationRuleMapDTO>> setMultipleRecommendationRuleMap(
			List<RecommendationRuleMapDTO> recommendationRules) {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();

		List<RecommendationRuleMapDTO> recommendationRulesTemp = new ArrayList<RecommendationRuleMapDTO>();

		for (RecommendationRuleMapDTO recommendationRuleMapDTO : recommendationRules) {

			recommendationRulesTemp = new ArrayList<RecommendationRuleMapDTO>();
			if (recommendationRuleMap.containsKey(recommendationRuleMapDTO.getRuleCode())) {
				recommendationRulesTemp = recommendationRuleMap.get(recommendationRuleMapDTO.getRuleCode());
			}
			recommendationRulesTemp.add(recommendationRuleMapDTO);
			recommendationRuleMap.put(recommendationRuleMapDTO.getRuleCode(), recommendationRulesTemp);
		}

		return recommendationRuleMap;
	}

	/**
	 * @return
	 */
	public static HashMap<String, List<RecommendationRuleMapDTO>> getMultipleRecommendationRuleMapFull() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();

		List<String> codeList = Arrays.asList("R1", "R2", "R3", "R4", "R4A", "R4B", "R4C", "R4D-1", "R5", "R6", "R7", "R8", "R9", "R10", "R11", "R12",
				"R13","R16");
		for (String filterCode : codeList) {
			recommendationRuleMap.put(filterCode, Arrays.asList(new RecommendationRuleMapDTO(filterCode, 0, true)));
		}
		return recommendationRuleMap;
	}

	public static HashMap<ItemKey, List<PriceCheckListDTO>> getCheckList(HashMap<ItemKey, List<PriceCheckListDTO>> allCheckList, int checkListId,
			int checkListTypeId, int locationLevelId, int locationId, int itemCode, int lirOrNonLig) {
		ItemKey itemkey = new ItemKey(itemCode, lirOrNonLig);
		List<PriceCheckListDTO> pDto = new ArrayList<PriceCheckListDTO>();
		if (allCheckList.containsKey(itemkey)) {
			pDto = allCheckList.get(itemkey);
		}
		PriceCheckListDTO priceCheckListDTO = new PriceCheckListDTO();
		priceCheckListDTO.setLocationId(locationId);
		priceCheckListDTO.setLocationLevelId(locationLevelId);
		priceCheckListDTO.setPriceCheckListId(checkListId);
		priceCheckListDTO.setPriceCheckListTypeId(checkListTypeId);
		pDto.add(priceCheckListDTO);
		allCheckList.put(itemkey, pDto);
		return allCheckList;

	}

	public static void getRecItemDetailsBasedOnRunId(HashMap<ItemKey, PRItemDTO> itemDataMap, Long runId, Long recommendationId, String isSubstitute,
			int itemCode, Long strategyId, int costChangeInd, double curListCost, double curVIPCost, int retLirId, String lirInd, int priceCheckList,
			int recommendedRegMultiple, double recommendedRegPrice, double overrideRegPrice, int isRecPriceAdjusted, double recPriceBeforeAdjust,
			int curCompRegMultiple, double curCompRegPrice, int isConflict, String log, int predictionStatus, double predictedMovement,
			double opportunityPrice, int opportunityQty, String isOppurtunity, double avgMovement, int isPrePriced, int isLockedPrice,
			int curRegMultiple, double curRegPrice, String recRegPredReasons, String recSalePredReasons, int strategyLocationLevelId,
			int strategyLocationId, int strategyProdLevelId, int strategyProdId, int strategyApplyTo, int StrategyVendorId, int strategyStateId,
			int overrideRegMultiple, String UPC, int recWeekSaleMultiple, double recWeeksalePrice, String recWeekSaleStartDate,
			String recWeekSaleEndDate, int recWeekPromoTypeId, int isAd, int recWeekAdPageNo, int recWeekAdBlockNo, int isUserOverride,
			PRStrategyDTO strategyDTO) throws JsonParseException, JsonMappingException, IOException {
	
		PRItemDTO itemDTO = new PRItemDTO();

		itemDTO.setRunId(runId);
		itemDTO.setRecommendationId(recommendationId);
		itemDTO.setItemCode(itemCode);
		itemDTO.setStrategyId(strategyId);
		itemDTO.setCostChgIndicator(costChangeInd);
		if (curListCost > 0)
			itemDTO.setListCost(curListCost);
		if (curVIPCost > 0)
			itemDTO.setVipCost(curVIPCost);
		if (retLirId > 0)
			itemDTO.setRetLirId(retLirId);
		if (priceCheckList > 0)
			itemDTO.setPriceCheckListId(priceCheckList);
		if (recommendedRegPrice > 0)
			itemDTO.setRecommendedRegPrice(new MultiplePrice(recommendedRegMultiple, recommendedRegPrice));
		if (overrideRegPrice > 0)
			itemDTO.setOverrideRegPrice(overrideRegPrice);
		int priceAdjustedFlag = isRecPriceAdjusted;
		if (priceAdjustedFlag == 1)
			itemDTO.setIsPriceAdjusted(true);
		else
			itemDTO.setIsPriceAdjusted(false);
		// if (rs.getObject("REC_PRICE_BEFORE_ADJUST") != null)
		// itemDTO.setRecPriceBeforeAdjustment(rs.getDouble("REC_PRICE_BEFORE_ADJUST"));
		if (recPriceBeforeAdjust > 0)
			itemDTO.setRecPriceBeforeAdjustment(new MultiplePrice(PRConstants.DEFAULT_REG_MULTIPLE, recPriceBeforeAdjust));
		if (curCompRegPrice > 0) {
			MultiplePrice multiplePrice = new MultiplePrice(curCompRegMultiple, curCompRegPrice);
			itemDTO.setCompPrice(multiplePrice);
		} else {
			itemDTO.setCompPrice(null);
		}
		if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
			itemDTO.setLir(true);
		}
		itemDTO.setIsConflict(isConflict);
		// itemDTO.setExplainLog(mapper.readValue(log, PRExplainLog.class));
		// itemDTO.setChildLocationLevelId(rs.getInt("CHILD_LOCATION_LEVEL_ID"));
		// itemDTO.setChildLocationId(rs.getInt("CHILD_LOCATION_ID"));
		// itemDTO.setStartCalendarId(rs.getInt("CALENDAR_ID"));
		// itemDTO.setEndCalendarId(rs.getInt("CALENDAR_ID"));
		// itemDTO.setPredictionStatus(rs.getInt("PREDICTION_STATUS"));
		if (predictionStatus > 0)
			itemDTO.setPredictionStatus(predictionStatus);
		else
			itemDTO.setPredictionStatus(null);

		if (predictedMovement > 0)
			itemDTO.setPredictedMovement(predictedMovement);

		if (opportunityPrice > 0)
			itemDTO.setOppurtunityPrice(opportunityPrice);

		if (opportunityQty > 0)
			itemDTO.setOppurtunityQty(opportunityQty);

		if (isOppurtunity != null)
			itemDTO.setIsOppurtunity(isOppurtunity);

		itemDTO.setAvgMovement(avgMovement);

		itemDTO.setIsPrePriced(isPrePriced);

		if (String.valueOf(Constants.YES).equalsIgnoreCase(isSubstitute)) {
			itemDTO.setIsPartOfSubstituteGroup(true);
		}
		itemDTO.setIsLocPriced(isLockedPrice);

		itemDTO.setRegMPack(curRegMultiple);
		if (itemDTO.getRegMPack() > 1)
			itemDTO.setRegMPrice(curRegPrice);
		else
			itemDTO.setRegPrice(curRegPrice);

		itemDTO.setRegPricePredReasons(recRegPredReasons);
		itemDTO.setSalePricePredReasons(recSalePredReasons);

		// strategyDTO = new PRStrategyDTO();
		// strategyDTO.setLocationLevelId(strategyLocationLevelId);
		// strategyDTO.setLocationId(strategyLocationId);
		// strategyDTO.setProductLevelId(strategyProdLevelId);
		// strategyDTO.setProductId(strategyProdId);
		// strategyDTO.setApplyTo(strategyApplyTo);
		// strategyDTO.setVendorId(StrategyVendorId);
		// strategyDTO.setStateId(strategyStateId);

		// Added overridden Reg price and qty info
		itemDTO.setStrategyDTO(strategyDTO);
		if (overrideRegMultiple > 0) {
			MultiplePrice overriddenPrice = new MultiplePrice(overrideRegMultiple, overrideRegPrice);
			itemDTO.setOverriddenRegularPrice(overriddenPrice);
		} else {
			itemDTO.setOverriddenRegularPrice(null);
		}
		// Added UPC info
		itemDTO.setUpc(UPC);
		// Sale Price info
		PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
		if (recWeekSaleMultiple > 0 && recWeeksalePrice > 0) {
			saleInfoDTO.setSalePrice(PRCommonUtil.getMultiplePrice(recWeekSaleMultiple, recWeekSaleMultiple > 1 ? 0 : recWeeksalePrice,
					recWeekSaleMultiple > 1 ? recWeeksalePrice : 0));
			saleInfoDTO.setSaleStartDate(recWeekSaleStartDate);
			saleInfoDTO.setSaleEndDate(recWeekSaleEndDate);
			saleInfoDTO.setPromoTypeId(recWeekPromoTypeId);
			itemDTO.setRecWeekSaleInfo(saleInfoDTO);
		}
		// Ad info
		itemDTO.setIsOnAd(isAd);
		PRItemAdInfoDTO adInfoDTO = new PRItemAdInfoDTO();
		adInfoDTO.setAdPageNo(recWeekAdPageNo);
		adInfoDTO.setAdBlockNo(recWeekAdBlockNo);
		itemDTO.setRecWeekAdInfo(adInfoDTO);
		itemDTO.setUserOverrideFlag(isUserOverride);
		itemDataMap.put(PRCommonUtil.getItemKey(itemDTO), itemDTO);
	}

	public static PRRecommendationRunHeader getRecommendationRunHeader(long runId, int locationLevelId, int locationId, int productId,
			int productLevelId, int calendarId, String startRunTime, String startDate) {
		PRRecommendationRunHeader recRunHeader = new PRRecommendationRunHeader();
		recRunHeader.setRunId(runId);
		recRunHeader.setLocationId(locationId);
		recRunHeader.setLocationLevelId(locationLevelId);
		recRunHeader.setProductId(productId);
		recRunHeader.setProductLevelId(productLevelId);
		recRunHeader.setCalendarId(calendarId);
		recRunHeader.setStartRunTime(startRunTime);
		recRunHeader.setStartDate(startDate);
		return recRunHeader;

	}

	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRule() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		applyRecommendationRule(recommendationRuleMap, "R1", false);
		applyRecommendationRule(recommendationRuleMap, "R2", false);
		applyRecommendationRule(recommendationRuleMap, "R3", false);
		applyRecommendationRule(recommendationRuleMap, "R4", true);
		applyRecommendationRule(recommendationRuleMap, "R4A", false);
		applyRecommendationRule(recommendationRuleMap, "R4B", false);
		applyRecommendationRule(recommendationRuleMap, "R4C", false);
		applyRecommendationRule(recommendationRuleMap, "R4D-1", true);
		applyRecommendationRule(recommendationRuleMap, "R5", true);
		applyRecommendationRule(recommendationRuleMap, "R6", true);
		applyRecommendationRule(recommendationRuleMap, "R7", true);
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R10", true);
		applyRecommendationRule(recommendationRuleMap, "R11", true);
		applyRecommendationRule(recommendationRuleMap, "R12", true);

		return recommendationRuleMap;
	}

	public static void applyRecommendationRule(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, String ruleCode,
			boolean isEnabled) {
		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		RecommendationRuleMapDTO recommendationRuleMapDTO = new RecommendationRuleMapDTO();
		recommendationRuleMapDTO.setRuleCode(ruleCode);
		recommendationRuleMapDTO.setEnabled(isEnabled);
		recommendationRuleMapDTO.setObjectiveTypeId(0);
		recommendationRules.add(recommendationRuleMapDTO);
		recommendationRuleMap.put(ruleCode, recommendationRules);

	}

	public static List<PRProductGroupProperty> addProductGroupProperty(List<PRProductGroupProperty> productProperties, int productLevelId,
			int productId, boolean usePrediction) {
		if (productProperties == null) {
			productProperties = new ArrayList<PRProductGroupProperty>();
		}

		PRProductGroupProperty productGroupProperty = new PRProductGroupProperty();
		productGroupProperty.setProductLevelId(productLevelId);
		productGroupProperty.setProductId(productId);
		productGroupProperty.setIsUsePrediction(usePrediction);
		productProperties.add(productGroupProperty);
		return productProperties;
	}

	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleAllEnabled() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		applyRecommendationRule(recommendationRuleMap, "R1", true);
		applyRecommendationRule(recommendationRuleMap, "R2", true);
		applyRecommendationRule(recommendationRuleMap, "R3", true);
		applyRecommendationRule(recommendationRuleMap, "R4", true);
		applyRecommendationRule(recommendationRuleMap, "R4A", true);
		applyRecommendationRule(recommendationRuleMap, "R4B", true);
		applyRecommendationRule(recommendationRuleMap, "R4C", true);
		applyRecommendationRule(recommendationRuleMap, "R4D-1", true);
		applyRecommendationRule(recommendationRuleMap, "R5", true);
		applyRecommendationRule(recommendationRuleMap, "R6", true);
		applyRecommendationRule(recommendationRuleMap, "R7", true);
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R10", true);
		applyRecommendationRule(recommendationRuleMap, "R11", true);
		applyRecommendationRule(recommendationRuleMap, "R12", true);
		applyRecommendationRule(recommendationRuleMap, "R15", true);
		applyRecommendationRule(recommendationRuleMap, "R16", true);
		return recommendationRuleMap;
	}
	
	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRule2() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		applyRecommendationRule(recommendationRuleMap, "R1", true);
		applyRecommendationRule(recommendationRuleMap, "R2", true);
		applyRecommendationRule(recommendationRuleMap, "R3", true);
		applyRecommendationRule(recommendationRuleMap, "R4", true);
		applyRecommendationRule(recommendationRuleMap, "R4A", true);
		applyRecommendationRule(recommendationRuleMap, "R4B", true);
		applyRecommendationRule(recommendationRuleMap, "R4C", true);
		applyRecommendationRule(recommendationRuleMap, "R4D-1", true);
		applyRecommendationRule(recommendationRuleMap, "R5", true);
		applyRecommendationRule(recommendationRuleMap, "R6", true);
		applyRecommendationRule(recommendationRuleMap, "R7", true);
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R10", false);
		applyRecommendationRule(recommendationRuleMap, "R11", true);
		applyRecommendationRule(recommendationRuleMap, "R12", true);
		applyRecommendationRule(recommendationRuleMap, "R15", true);
		applyRecommendationRule(recommendationRuleMap, "R16", true);
		return recommendationRuleMap;
	}
	
	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleAllDisabled() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		return recommendationRuleMap;
	}
	

	public static void setPredictionOutputDTO(PredictionOutputDTO predictionOutputDTO, int locationLevelId, int locationId, int startCalendarId,
			int endCalendarId, List<PredictionItemDTO> predictionItems) {

		predictionOutputDTO.locationLevelId = locationLevelId;
		predictionOutputDTO.locationId = locationId;
		predictionOutputDTO.startCalendarId = startCalendarId;
		predictionOutputDTO.endCalendarId = endCalendarId;
		predictionOutputDTO.predictionItems = predictionItems;
	}

	public static PredictionItemDTO setPredictionItemDTO(int itemCode, String upc, MultiplePrice price1, PredictionStatus price1PredStatus,
			Double price1PredMov, MultiplePrice price2, PredictionStatus price2PredStatus, Double price2PredMov, MultiplePrice price3,
			PredictionStatus price3PredStatus, Double price3PredMov, MultiplePrice price4, PredictionStatus price4PredStatus, Double price4PredMov) {

		PredictionItemDTO predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.itemCodeOrLirId = itemCode;
		predictionItemDTO.upc = upc;
		if (predictionItemDTO.pricePoints == null) {
			predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		}
		PricePointDTO pricePointDTO = new PricePointDTO();
		if (price1 != null && price1PredStatus != null) {
			pricePointDTO.setRegQuantity(price1.multiple);
			pricePointDTO.setRegPrice(price1.price);
			pricePointDTO.setPredictionStatus(price1PredStatus);
			pricePointDTO.setPredictedMovement(price1PredMov);
			predictionItemDTO.pricePoints.add(pricePointDTO);
		}

		pricePointDTO = new PricePointDTO();
		if (price2 != null && price2PredStatus != null) {
			pricePointDTO.setRegQuantity(price2.multiple);
			pricePointDTO.setRegPrice(price2.price);
			pricePointDTO.setPredictionStatus(price2PredStatus);
			pricePointDTO.setPredictedMovement(price2PredMov);
			predictionItemDTO.pricePoints.add(pricePointDTO);
		}

		pricePointDTO = new PricePointDTO();
		if (price3 != null && price3PredStatus != null) {
			pricePointDTO.setRegQuantity(price3.multiple);
			pricePointDTO.setRegPrice(price3.price);
			pricePointDTO.setPredictionStatus(price3PredStatus);
			pricePointDTO.setPredictedMovement(price3PredMov);
			predictionItemDTO.pricePoints.add(pricePointDTO);
		}

		pricePointDTO = new PricePointDTO();
		if (price4 != null && price4PredStatus != null) {
			pricePointDTO.setRegQuantity(price4.multiple);
			pricePointDTO.setRegPrice(price4.price);
			pricePointDTO.setPredictionStatus(price4PredStatus);
			pricePointDTO.setPredictedMovement(price4PredMov);
			predictionItemDTO.pricePoints.add(pricePointDTO);
		}

		return predictionItemDTO;
	}

	public static HashMap<String, ArrayList<Integer>> setProductListMap(HashMap<String, ArrayList<Integer>> productListMap, int childProductLevelId,
			int childProductId, int parentProductLevelId) {
		ArrayList<Integer> parentIds = new ArrayList<Integer>();
		String key = childProductLevelId + "-" + childProductId;
		if (productListMap.get(key) != null) {
			parentIds = productListMap.get(key);
		}
		parentIds.add(parentProductLevelId);

		productListMap.put(key, parentIds);
		return productListMap;
	}

	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTableGE() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.05,0.09,0.15,0.19,0.25,0.29,0.35,0.39,0.45,0.49,0.55,0.59,0.65,0.69,0.75,0.79,0.85,0.89,0.95,0.99");
		roundingTableContent.put("0-0.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.00");
		roundingTableContent.put("1-1.09", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19");
		roundingTableContent.put("1-1.24", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29");
		roundingTableContent.put("1.25-1.35", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.39");
		roundingTableContent.put("1.36-1.44", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49");
		roundingTableContent.put("1.45-1.54", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.59");
		roundingTableContent.put("1.55-1.64", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.69");
		roundingTableContent.put("1.65-1.74", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.79");
		roundingTableContent.put("1.75-1.84", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.89");
		roundingTableContent.put("1.85-1.94", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("1.95-2.04", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.09");
		roundingTableContent.put("2.05-2.14", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19");
		roundingTableContent.put("2.15-2.24", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29");
		roundingTableContent.put("2.25-2.34", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.39");
		roundingTableContent.put("2.35-2.44", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49");
		roundingTableContent.put("2.45-2.54", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.59");
		roundingTableContent.put("2.55-2.64", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.69");
		roundingTableContent.put("2.65-2.74", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.79");
		roundingTableContent.put("2.75-2.84", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.89");
		roundingTableContent.put("2.85-2.94", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("2.95-3.14", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29");
		roundingTableContent.put("3.15-3.39", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49");
		roundingTableContent.put("3.4-3.65", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.79");
		roundingTableContent.put("3.66-3.89", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("3.9-4.14", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29");
		roundingTableContent.put("4.15-4.39", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49");
		roundingTableContent.put("4.4-4.65", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.79");
		roundingTableContent.put("4.66-4.89", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("4.9-5.24", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49,0.99");
		roundingTableContent.put("5.25-20.49", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("20.5-150", roundingTableDTO);

		return roundingTableContent;
	}
	
	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTableTops() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits(
				"0.03,0.05,0.07,0.09,0.13,0.15,0.17,0.19,0.23,0.25,0.27,0.29,0.35,0.39,0.45,0.49,0.55,0.59,0.65,0.69,0.75,0.79,0.85,0.89,0.95,0.99");
		roundingTableContent.put("0-0.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.09,0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("1-1.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("2-2.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("3-9.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29,0.49,0.69,0.79,0.99");
		roundingTableContent.put("10-9999", roundingTableDTO);

		return roundingTableContent;
	}
	
	/**
	 * Taken from production as on 30th Jan 2018
	 * @return
	 */
	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTableTops1() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits(
				"0.09,0.19,0.25,0.29,0.35,0.39,0.45,0.49,0.55,0.59,0.65,0.69,0.75,0.79,0.85,0.89,0.95,0.99");
		roundingTableContent.put("0-0.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.09,0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.89,0.99");
		roundingTableContent.put("1-1.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("2-2.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("3-3.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("4-4.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("5-6.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("7-7.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("8-19.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49,0.99");
		roundingTableContent.put("20-49.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("50-9999", roundingTableDTO);

		return roundingTableContent;
	}
	
	/**
	 * Rules as on 30th Jan 2018
	 * @return
	 */
	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleTops1() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		applyRecommendationRule(recommendationRuleMap, "R1", false);
		applyRecommendationRule(recommendationRuleMap, "R2", false);
		applyRecommendationRule(recommendationRuleMap, "R3", false);
		applyRecommendationRule(recommendationRuleMap, "R4", true);
		applyRecommendationRule(recommendationRuleMap, "R4A", false);
		applyRecommendationRule(recommendationRuleMap, "R4B", false);
		applyRecommendationRule(recommendationRuleMap, "R4C", false);
		applyRecommendationRule(recommendationRuleMap, "R4D-1", true);
		applyRecommendationRule(recommendationRuleMap, "R5", true);
		applyRecommendationRule(recommendationRuleMap, "R6", true);
		applyRecommendationRule(recommendationRuleMap, "R7", true);
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R10", true);
		applyRecommendationRule(recommendationRuleMap, "R11", true);
		applyRecommendationRule(recommendationRuleMap, "R12", true);
		applyRecommendationRule(recommendationRuleMap, "R13", true);
		applyRecommendationRule(recommendationRuleMap, "R15", true);
		applyRecommendationRule(recommendationRuleMap, "R16", true);
		return recommendationRuleMap;
	}
	
	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleGE1() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		applyRecommendationRule(recommendationRuleMap, "R1", false);
		applyRecommendationRule(recommendationRuleMap, "R2", false);
		applyRecommendationRule(recommendationRuleMap, "R3", false);
		applyRecommendationRule(recommendationRuleMap, "R4", true);
		applyRecommendationRule(recommendationRuleMap, "R4A", false);
		applyRecommendationRule(recommendationRuleMap, "R4B", false);
		applyRecommendationRule(recommendationRuleMap, "R4C", false);
		applyRecommendationRule(recommendationRuleMap, "R4D-1", true);
		applyRecommendationRule(recommendationRuleMap, "R5", true);
		applyRecommendationRule(recommendationRuleMap, "R6", true);
		applyRecommendationRule(recommendationRuleMap, "R7", true);
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R10", true);
		applyRecommendationRule(recommendationRuleMap, "R11", true);
		applyRecommendationRule(recommendationRuleMap, "R12", true);
		applyRecommendationRule(recommendationRuleMap, "R13", true);
		applyRecommendationRule(recommendationRuleMap, "R15", true);
		return recommendationRuleMap;
	}
	
	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleGE2() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
		applyRecommendationRule(recommendationRuleMap, "R1", false);
		applyRecommendationRule(recommendationRuleMap, "R2", false);
		applyRecommendationRule(recommendationRuleMap, "R3", true);
		applyRecommendationRule(recommendationRuleMap, "R4", true);
		applyRecommendationRule(recommendationRuleMap, "R4A", false);
		applyRecommendationRule(recommendationRuleMap, "R4B", false);
		applyRecommendationRule(recommendationRuleMap, "R4C", false);
		applyRecommendationRule(recommendationRuleMap, "R4D-1", true);
		applyRecommendationRule(recommendationRuleMap, "R5", true);
		applyRecommendationRule(recommendationRuleMap, "R6", true);
		applyRecommendationRule(recommendationRuleMap, "R7", true);
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R10", false);
		applyRecommendationRule(recommendationRuleMap, "R11", true);
		applyRecommendationRule(recommendationRuleMap, "R12", true);
		applyRecommendationRule(recommendationRuleMap, "R13", true);
		applyRecommendationRule(recommendationRuleMap, "R15", true);
		return recommendationRuleMap;
	}
	
	public static void setAdditionalLog(PRExplainLog explainLog, ExplainRetailNoteTypeLookup explainRetailNoteTypeLookup, String noteValues) {
		
		if(explainLog.getExplainAdditionalDetail() == null) { 
			explainLog.setExplainAdditionalDetail(new ArrayList<PRExplainAdditionalDetail>());
		}
		
		explainLog.getExplainAdditionalDetail().add(setAdditionalLog(explainRetailNoteTypeLookup, noteValues));
	}
	
	public static void setAdditionalLog(PRExplainLog explainLog, ExplainRetailNoteTypeLookup explainRetailNoteTypeLookup, List<String> additionalInfo) {
		
		if(explainLog.getExplainAdditionalDetail() == null) { 
			explainLog.setExplainAdditionalDetail(new ArrayList<PRExplainAdditionalDetail>());
		}
		PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
		expDetail.setExplainRetailNoteTypeLookupId(explainRetailNoteTypeLookup.getNoteTypeLookupId());
		expDetail.setNoteValues(additionalInfo);
		List<PRExplainAdditionalDetail> temp = new ArrayList<>();
		temp = explainLog.getExplainAdditionalDetail();
		temp.add(expDetail);
		explainLog.setExplainAdditionalDetail(temp);
	}	
	public static PRExplainAdditionalDetail setAdditionalLog(ExplainRetailNoteTypeLookup explainRetailNoteTypeLookup, String noteValues) {
		PRExplainAdditionalDetail explainAdditionalDetail = new PRExplainAdditionalDetail();
		
		explainAdditionalDetail.setExplainRetailNoteTypeLookupId(explainRetailNoteTypeLookup.getNoteTypeLookupId());
		explainAdditionalDetail.setNoteValues((noteValues != "" ? Arrays.asList(noteValues) : new ArrayList<String>()));
		
		return explainAdditionalDetail;
	}
	
	public static void setSaleDetails(HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, int itemCode, double salePrice, 
			int SaleQty, String actualSaleStartDate, String actualSaleEndDate, String saleWeekStartDate){
		
		List<PRItemSaleInfoDTO> saleInfoList = new ArrayList<>();
		if(saleDetails.get(itemCode)!= null){
			saleInfoList = saleDetails.get(itemCode);
		}
		PRItemSaleInfoDTO prItemSaleInfoDTO = new PRItemSaleInfoDTO();
		prItemSaleInfoDTO.setSalePrice(new MultiplePrice(SaleQty, salePrice));
		prItemSaleInfoDTO.setSaleWeekStartDate(saleWeekStartDate);
		prItemSaleInfoDTO.setSaleStartDate(actualSaleStartDate);
		prItemSaleInfoDTO.setSaleEndDate(actualSaleEndDate);
		
		saleInfoList.add(prItemSaleInfoDTO);
		saleDetails.put(itemCode, saleInfoList);
	}
	
	public static void setAdDetails(HashMap<Integer, List<PRItemAdInfoDTO>> adDetails, int itemCode, String startDate, int adPageNo, int adBlockNo) {
		
		List<PRItemAdInfoDTO> adDetailList = new ArrayList<>();
		if(adDetails.get(itemCode)!=null){
			adDetailList = adDetails.get(itemCode);
		}
		
		PRItemAdInfoDTO prItemAdInfoDTO = new PRItemAdInfoDTO();
		prItemAdInfoDTO.setAdBlockNo(adBlockNo);
		prItemAdInfoDTO.setAdPageNo(adPageNo);
		prItemAdInfoDTO.setWeeklyAdStartDate(startDate);
		
		adDetailList.add(prItemAdInfoDTO);
		adDetails.put(itemCode, adDetailList);
	}
	
	public static void setLigConstraintLog(PRExplainLog explainLog, PRGuidelineAndConstraintLog guidelineAndConstraintLog,
			boolean isGuidelineOrConstraintApplied, boolean isConflict, double guidelineStartVal, double guidelineEndVal, double outputStartVal,
			double outputEndVal, String message, double priceRange) {
		PRRange prRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.LIG.getConstraintTypeId());
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(isGuidelineOrConstraintApplied);
		guidelineAndConstraintLog.setIsConflict(isConflict);

		prRange = new PRRange();
		prRange.setStartVal(guidelineStartVal);
		prRange.setEndVal(guidelineEndVal);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(prRange);

		prRange = new PRRange();
		prRange.setStartVal(outputStartVal);
		prRange.setEndVal(outputEndVal);
		guidelineAndConstraintLog.setOutputPriceRange(prRange);
		guidelineAndConstraintLog.setRecommendedRegPrice(priceRange>0? priceRange:null);
		guidelineAndConstraintLog.setMessage(message);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
	}
	
	/**
	 * Taken from production as on 30th Jan 2018
	 * @return
	 */
	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTableGE1() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits(
				"0.09,0.19,0.25,0.29,0.35,0.39,0.45,0.49,0.55,0.59,0.65,0.69,0.75,0.79,0.85,0.89,0.95,0.99");
		roundingTableContent.put("0-0.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.09,0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.89,0.99");
		roundingTableContent.put("1-1.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("2-2.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("3-3.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("4-4.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("5-6.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49,0.79,0.99");
		roundingTableContent.put("7-7.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
//		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableDTO.setAllowedEndDigits("0.29,0.49,0.79,0.99");
		roundingTableContent.put("8-19.99", roundingTableDTO);
		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49,0.99");
		roundingTableContent.put("20-49.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("50-9999", roundingTableDTO);

		return roundingTableContent;
	}
	
	public static void getAuthItems(List<PRItemDTO> itemData, Integer itemCode, int zoneId, String zoneNum, boolean isDsdItem, int storeId){
		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(itemCode);
		prItemDTO.setPriceZoneId(zoneId);
		prItemDTO.setPriceZoneNo(zoneNum);
		prItemDTO.setDSDItem(isDsdItem);
		prItemDTO.setChildLocationId(storeId);
		itemData.add(prItemDTO);
	}
	
	public static RetailPriceDTO setRetailPriceDTO(int calendarId, int levelTypeId, int levelId, Float regPrice,
			Float regMPrice, int regQty, String effDate) {
		RetailPriceDTO retailPriceDTO = new RetailPriceDTO();

		retailPriceDTO.setCalendarId(calendarId);
		retailPriceDTO.setLevelTypeId(levelTypeId);
		retailPriceDTO.setLevelId(String.valueOf(levelId));
		retailPriceDTO.setRegEffectiveDate(effDate);
		retailPriceDTO.setRegPrice(regPrice);
		retailPriceDTO.setRegMPrice(regMPrice);
		retailPriceDTO.setRegQty(regQty);

		return retailPriceDTO;
	}
	
	public static HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> getPriceMap(
			HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap, int levelTypeId, int levelId,
			int itemCode, RetailPriceDTO retailPriceDTO) {
		
		RetailPriceCostKey retailPriceCostKey = new RetailPriceCostKey(levelTypeId, levelId);
		
		HashMap<RetailPriceCostKey, RetailPriceDTO> prMap = new HashMap<>();
		if(priceMap.containsKey(itemCode)){
			prMap = priceMap.get(itemCode);
		}
		prMap.put(retailPriceCostKey, retailPriceDTO);
		
		priceMap.put(itemCode, prMap);
		
		return priceMap;
	}
	
	
	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTableAZ() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.89,0.99");
		roundingTableDTO.setExcludedPrices("0.09,1.09,2.09,3.09,4.09,5.09,6.09,7.09,8.09,9.09,10.09");
		roundingTableContent.put("0-11", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableContent.put("111.01-100", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("110.99,120.99,130.99,140.99,150.99,160.99,170.99,180.99,190.99,200.99,210.99,220.99,230.99,240.99,250.99,260.99,270.99,280.99,290.99,300.99,310.99,320.99,330.99,340.99");
		roundingTableContent.put("100.01-350", roundingTableDTO);

		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("350.99,360.99,370.99,380.99,390.99,400.99,410.99,420.99,430.99,440.99,450.99,460.99,470.99,480.99,490.99,500.99,510.99,520.99,530.99,540.99,550.99,560.99,570.99,580.99");
		roundingTableContent.put("350.01-581", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("590.99,600.99,610.99,620.99,630.99,640.99,650.99,660.99,670.99,680.99,690.99,700.99,710.99,720.99,730.99,740.99,750.99,760.99,770.99,780.99,790.99,800.99,810.99,820.99");
		roundingTableContent.put("581.01-821", roundingTableDTO);

		
		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("830.99,840.99,850.99,860.99,870.99,880.99,890.99,900.99,910.99,920.99,930.99,940.99,950.99,960.99,970.99,980.99,990.99,1000.99,1010.99,1020.99,1030.99,1040.99,1050.99");
		roundingTableContent.put("821.01-1051", roundingTableDTO);

		return roundingTableContent;
	}
	
	
	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTableALL_99_BATT() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("20.99,30.99,40.99,50.99,60.99,70.99,80.99,90.99,100.99,110.99,120.99,130.99,140.99,150.99,160.99,170.99,180.99,190.99,200.99,210.99,220.99,230.99,240.99,250.99,260.99,270.99,280.99,290.99");
		roundingTableContent.put("0-299.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("300.99,310.99,320.99,330.99,340.99,350.99,360.99,370.99,380.99,390.99,400.99,410.99,420.99,430.99,440.99,450.99,460.99,470.99,480.99,490.99");
		roundingTableContent.put("300-499.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("500.99,510.99,520.99,530.99,540.99,550.99,560.99,570.99,580.99,590.00,600.99,610.99,620.99,630.99,640.99,650.99,660.99,670.99,680.99,690.99,700.99,710.99,720.99,730.99,740.99,750.99,760.99,770.99,780.99,790.99,800.99,810.99,820.99,830.99,840.99,850.99,860.99,870.99,880.99,890.99");
		roundingTableContent.put("500-899.99", roundingTableDTO);

		return roundingTableContent;
	}

	static PRItemDTO setTestItem(Integer itemCode, int curRegMultiple, Double curRegPrice, Double curRegMPrice, Double curListCost,
			Double preListCost, Integer costChgIndicator, HashMap<Integer,Double> compData, PRStrategyDTO strategyDTO, long lastXWeeksMov,
			int curSaleMultipe, Double curSalePrice, Double curSaleMPrice, String promoWeekStartDate, int retLirId, boolean isLir,
			int predictionStatus, double predictedMov, int curPredictionStatus, double curPredictedMov, int recRegMultiple, Double recRegPrice,
			Double recRegMPrice, String priceRange, int locationId, String upc,HashMap<Integer,String> compCheckDates) {
		PRItemDTO item = new PRItemDTO();

		item.setRegMPack(curRegMultiple);

		if (curRegMultiple > 1) {
			item.setRegPrice(curRegMPrice / curRegMultiple);
		} else {
			if (curRegPrice != null && curRegPrice > 0) {
				item.setRegPrice(curRegPrice);
			}
		}

		/*
		 * if (curRegPrice != null && curRegPrice > 0) { item.setRegPrice(curRegPrice); }
		 */
		if (curRegMPrice != null && curRegMPrice > 0) {
			item.setRegMPrice(curRegMPrice);
		}
		
		item.setItemCode(itemCode);

		if (curListCost != null && curListCost > 0)
			item.setListCost(curListCost);
		if (preListCost != null && preListCost > 0)
			item.setPreListCost(preListCost);
		
		if(compData.size()>0)
		{
		item.setAllCompPrice(setMultiCompPrice(compData));
		item.setAllCompPriceCheckDate(setCompCheckDates(compCheckDates));
		}
		
		item.setCostChgIndicator(costChgIndicator);
		item.setStrategyDTO(strategyDTO);
		item.setLastXWeeksMov(lastXWeeksMov);

		MultiplePrice curSaleMultiplePrice = PRCommonUtil.getMultiplePrice(curSaleMultipe, curSalePrice, curSaleMPrice);
		item.getCurSaleInfo().setSalePrice(curSaleMultiplePrice);
		item.getCurSaleInfo().setSaleWeekStartDate(promoWeekStartDate);
		item.setRetLirId(retLirId);
		item.setLir(isLir);
		item.setPredictionStatus(predictionStatus);
		item.setPredictedMovement(predictedMov);
		item.setCurRegPricePredictedMovement(curPredictedMov);
		// MultiplePrice RecMultiplePrice = PRCommonUtil.getMultiplePrice(recRegMultiple, recRegPrice, recRegMPrice);
		// item.setRecommendedRegMultiple(recRegMultiple);
		// item.setRecommendedRegPrice(recRegPrice);
		if (recRegPrice != null) {
			item.setRecommendedRegPrice(new MultiplePrice(recRegMultiple, recRegPrice));
		} else {
			item.setRecommendedRegPrice(null);
		}
		if (priceRange != null || priceRange == "") {
			String[] prices = priceRange.split(",");
			Double[] pricePoints = new Double[prices.length];
			for (int i = 0; i < prices.length; i++) {
				pricePoints[i] = Double.valueOf(prices[i]);
			}
			item.setPriceRange(pricePoints);
		}

		item.setChildLocationId(locationId);
		item.setUpc(upc);
		if (strategyDTO != null && strategyDTO.getObjective() != null) {
			item.setObjectiveTypeId(strategyDTO.getObjective().getObjectiveTypeId());
		}
		
		item.setActive(true);
		item.setAuthorized(true);
		
		return item;
	}
	
	
	private static HashMap<LocationKey, String> setCompCheckDates(HashMap<Integer, String> compCheckDates) {

		HashMap<LocationKey, String> compCheckDatesMap = new HashMap<>();
		for (Map.Entry<Integer, String> comp : compCheckDates.entrySet()) {
			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, comp.getKey());
			compCheckDatesMap.put(locationKey, comp.getValue());
		}

		return compCheckDatesMap;
	}

	private static HashMap<LocationKey, MultiplePrice> setMultiCompPrice(HashMap<Integer, Double> compData) {

		HashMap<LocationKey, MultiplePrice> compPriceMap = new HashMap<LocationKey, MultiplePrice>();
		for (Map.Entry<Integer, Double> comp : compData.entrySet()) {
			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, comp.getKey());
			MultiplePrice compMulPrice = new MultiplePrice(1, comp.getValue());
			compPriceMap.put(locationKey, compMulPrice);
		}

		return compPriceMap;
	}
	
	public static HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleAZ() {
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<String, List<RecommendationRuleMapDTO>>();
	
		applyRecommendationRule(recommendationRuleMap, "R8", true);
		applyRecommendationRule(recommendationRuleMap, "R9", true);
		applyRecommendationRule(recommendationRuleMap, "R16", true);
		return recommendationRuleMap;
	}
	
	public static TreeMap<String, PRRoundingTableDTO> getRoundingTableTablePROD_49_99_ROUNDING_DEF() {
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.89,0.99");
		roundingTableDTO.setExcludedPrices("0.09,1.09,2.09,3.09,4.09,5.09,6.09,7.09,8.09,9.09,10.09");
		roundingTableContent.put("0-11", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.49,0.99");
		roundingTableDTO.setExcludedPrices("20.49,30.49,40.49,50.49,60.49,70.49,80.49,90.49,20.99,30.99,40.99,50.99,60.99,70.99,80.99,90.99");
		roundingTableContent.put("11.01-100", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.99");
		roundingTableDTO.setExcludedPrices("110.99,120.99,130.99,140.99,150.99,160.99,170.99,180.99,190.99,200.99,210.99,220.99,230.99,240.99,250.99,260.99,270.99,280.99,290.99,300.99,310.99,320.99,330.99,340.99");
		roundingTableContent.put("100.01-350", roundingTableDTO);

		return roundingTableContent;
	}

	 
}
