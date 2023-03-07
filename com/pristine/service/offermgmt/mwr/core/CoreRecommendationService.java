package com.pristine.service.offermgmt.mwr.core;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.PRExplainAdditionalDetail;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.service.offermgmt.PriceRollbackService;
import com.pristine.service.offermgmt.PricingEngineHelper;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.service.offermgmt.mwr.core.finalizeprice.MultiWeekPriceFinalizer;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * Responsible for recommending prices for X weeks/Quarter
 * 
 * @author Pradeepkumar
 *
 */
public class CoreRecommendationService {

	private static Logger logger = Logger.getLogger("CoreRecommendationService");
		
	/**
	 * 
	 * @param recommendationInputDTO
	 * @return recommended item list
	 * @throws OfferManagementException 
	 * @throws GeneralException 
	 * @throws Exception 
	 */
	public void recommendPrice(Connection conn, BaseData baseData, CommonDataHelper commonDataHelper, 
			RecommendationInputDTO recommendationInputDTO)
			throws Exception, GeneralException, OfferManagementException {
		String recMode = MultiWeekRecConfigSettings.getMwrRecommendationMode();
		if (recMode.equals(PRConstants.RECOMMEND_BY_MULTI_WEEK_PRED)) {
			recommendPriceByMultiWeekPrediction(conn, baseData, commonDataHelper, recommendationInputDTO);
		} else if (recMode.equals(PRConstants.RECOMMEND_BY_SINGLE_WEEK_PRED)) {
			recommendPriceBySingleWeekPrediction(conn, baseData, commonDataHelper, recommendationInputDTO);
		}
	}
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @throws Exception
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	public void recommendPriceByMultiWeekPrediction(Connection conn, BaseData baseData, 
			CommonDataHelper commonDataHelper, RecommendationInputDTO recommendationInputDTO)
			throws Exception, GeneralException, OfferManagementException {
		// 1. Initialize core classes required
		PricePointFinder pricePointFinder = new PricePointFinder();
		MultiWeekConverter multiWeekConverter = new MultiWeekConverter();
		MutltiWeekPredInputBuilder mutltiWeekPredInputBuilder = new MutltiWeekPredInputBuilder();
		MutliWeekPredictionComponent mutliWeekPredictionComponent = new MutliWeekPredictionComponent(conn);
		PRRecommendationRunHeader prRecommendationRunHeader = new PRRecommendationRunHeader();
		prRecommendationRunHeader.setStartDate(recommendationInputDTO.getStartWeek());
		
		// 2. Initialize collections required
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<>();
		HashMap<Integer, List<PRItemDTO>> finalLigMap = new HashMap<>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = new ItemAttributeService()
				.getRetLirIdMap(baseData.getItemDataMap());
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<>();
		RetailCalendarDTO retailCalendarDTO = commonDataHelper.getAllWeekCalendarDetails()
				.get(recommendationInputDTO.getBaseWeek());

		// 1. Apply strategy
		List<PRItemDTO> itemsWithLIG = pricePointFinder.findPricePoints(recommendationInputDTO,
				baseData.getItemDataMap(), retLirMap, retLirConstraintMap, finalLigMap, baseData.getCompetitorMap(),
				baseData.getMultiCompLatestPriceMap());

		
		// 2. Convert current data to multiple weeks data
		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap = multiWeekConverter
				.convertToMultiWeekItemDataMap(recommendationInputDTO, baseData.getItemDataMap(),
						commonDataHelper.getAllWeekCalendarDetails(), baseData.getPriceHistory(),
						baseData.getRecommendationRuleMap(), baseData.getMovementDataMap(), baseData.getParentRecInfo(),
						baseData.getSaleDetails(), baseData.getAdDetails(), baseData.getDisplayDetails(), retLirMap,
						commonDataHelper.getFullCalendar(), true,baseData.getFutureCost(), baseData.getFuturePrice());

		// 3. Get inputs for prediction
		List<MultiWeekPredEngineItemDTO> multiWeekPredInput = mutltiWeekPredInputBuilder
				.buildInputForMultiWeekPrediction(weeklyItemDataMap, recommendationInputDTO);

		logger.info("recommendPrice() - Calling multi week prediction...");
		
		logger.info("recommendPrice() - # of price points to be predicted: " + multiWeekPredInput.size());
		
		// 4. Call predicition
		List<MultiWeekPredEngineItemDTO> multiWeekPredOuput = mutliWeekPredictionComponent.callMultiWeekPrediction(
				recommendationInputDTO, multiWeekPredInput, commonDataHelper.getAllWeekCalendarDetails(),
				baseData.getPredictionCache());

		logger.info("recommendPrice() - # of price points predicted: " + multiWeekPredOuput.size());
		
		// 5. Update prediction to weekly map
		mutliWeekPredictionComponent.updateWeeklyPredictedMovement(weeklyItemDataMap, multiWeekPredOuput,
				commonDataHelper.getAllWeekCalendarDetails());

		// 5. Update prediction for LIG
		mutliWeekPredictionComponent.updateLigLevelMovement(weeklyItemDataMap, retLirMap);
		
		// 7. Update mutliweek/Quarter level predicted movement in item datamap
		mutliWeekPredictionComponent.updateMultiWeekOrQuarterLevelPredMov(baseData.getItemDataMap(), weeklyItemDataMap,
				recommendationInputDTO);

		// 8. Finalize/Recommend price
		new ObjectiveService().applyObjectiveAndSetRecPrice(itemsWithLIG, baseData.getPriceHistory(),
				retailCalendarDTO, baseData.getRecommendationRuleMap());

		// 9. Apply lig constraint
		HashMap<Integer, List<PRItemDTO>> ligMap = new PricingEngineService().formLigMap(baseData.getItemDataMap());
		
		new LIGConstraint().applyLIGConstraint(ligMap, baseData.getItemDataMap(), retLirConstraintMap);

		
		logger.info("recommendPrice() - Recommending prices...");
		
		// 10. Recommended again all the related items
		new PriceGroupService().recommendPriceGroupRelatedItems(prRecommendationRunHeader, itemsWithLIG,
				baseData.getItemDataMap(), baseData.getCompetitorMap(), retLirConstraintMap,
				baseData.getMultiCompLatestPriceMap(), recommendationInputDTO.getBaseWeek(), leadZoneDetails, false,
				baseData.getPriceHistory(), retailCalendarDTO, MultiWeekRecConfigSettings.getMwrMaxUnitPriceDiff(),
				baseData.getRecommendationRuleMap());

		
		logger.info("recommendPrice() - Recommending prices completed.");
		
		// 11. Update item level attributes like current and recommended price prediction
		new PricingEngineService().updateItemAttributes(itemsWithLIG, prRecommendationRunHeader);

		// 12. Apply lig constraint
		new LIGConstraint().applyLIGConstraint(ligMap, baseData.getItemDataMap(), retLirConstraintMap);

		
		logger.info("recommendPrice() - Applying rules to retain current retail...");
		// 12. Retain current retail based on rules
		new PriceRollbackService().retainCurrentRetail(itemsWithLIG, prRecommendationRunHeader,
				baseData.getSaleDetails(), baseData.getAdDetails(), baseData.getStoreList().size(),
				baseData.getItemDataMap(), baseData.getCompetitorMap(), retLirConstraintMap,
				baseData.getMultiCompLatestPriceMap(), recommendationInputDTO.getBaseWeek(), leadZoneDetails, false,
				baseData.getPriceHistory(), retailCalendarDTO, MultiWeekRecConfigSettings.getMwrMaxUnitPriceDiff(),
				baseData.getRecommendationRuleMap());
		
		logger.info("recommendPrice() - Rules applied.");
		
		// 14. Set multi week item map to base data for saving data to DB
		baseData.setWeeklyItemDataMap(weeklyItemDataMap);
		
	}
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @return items with recommendation
	 * @throws Exception
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	public void recommendPriceBySingleWeekPrediction(Connection conn, BaseData baseData, 
			CommonDataHelper commonDataHelper, RecommendationInputDTO recommendationInputDTO)
			throws Exception, GeneralException, OfferManagementException {
		// 1. Initialize core classes required
		boolean isOnline = false;
		if (recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE) {
			isOnline = true;
		}
		MultiWeekConverter multiWeekConverter = new MultiWeekConverter();
		MutltiWeekPredInputBuilder mutltiWeekPredInputBuilder = new MutltiWeekPredInputBuilder();
		MutliWeekPredictionComponent mutliWeekPredictionComponent = new MutliWeekPredictionComponent(conn);
		PredictionComponent predictionComponent = new PredictionComponent();
		PRRecommendationRunHeader prRecommendationRunHeader = new PRRecommendationRunHeader();
		prRecommendationRunHeader.setStartDate(recommendationInputDTO.getStartWeek());
		PricingEngineWS pricingEngineWS = new PricingEngineWS();
		// 2. Initialize collections required
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<>();
		HashMap<Integer, List<PRItemDTO>> finalLigMap = new HashMap<>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = new ItemAttributeService()
				.getRetLirIdMap(baseData.getItemDataMap());
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<>();
		RetailCalendarDTO retailCalendarDTO = commonDataHelper.getAllWeekCalendarDetails()
				.get(recommendationInputDTO.getBaseWeek());
		
		RetailCalendarDTO recWeekCalendarDTO = commonDataHelper.getAllWeekCalendarDetails()
				.get(recommendationInputDTO.getStartWeek());
		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		inputDTO.setStartCalendarId(recWeekCalendarDTO.getCalendarId());
		inputDTO.setEndCalendarId(recWeekCalendarDTO.getCalendarId());
		
		
		PredictionService predictionService = new PredictionService(baseData.getMovementDataMap(),
				baseData.getPriceHistoryAll(), retLirMap);

		HashMap<String, HashMap<ItemKey, PRItemDTO>> itemsClassified = new PricePointFinder()
				.getItemsClassified(baseData.getItemDataMap(), retLirMap);

		HashMap<ItemKey, PRItemDTO> independentItemDataMap = itemsClassified.get(PRConstants.INDEPENDENT_ITEMS);

		logger.info("# of independent items: " + independentItemDataMap.size());

		// All functionalities which decides the price of an item is put here
		pricingEngineWS.recommendPrice(conn, independentItemDataMap, inputDTO,
				prRecommendationRunHeader, retLirMap, retLirConstraintMap, baseData.getCompetitorMap(),
				baseData.getMultiCompLatestPriceMap(), finalLigMap, baseData.getProductGroupProperties(),
				predictionComponent, leadZoneDetails, false, baseData.getPriceHistory(), retailCalendarDTO, isOnline,
				baseData.getPriceHistoryAll(), baseData.getMovementDataMap(), baseData.getRecommendationRuleMap(),
				baseData.getSaleDetails(), baseData.getAdDetails(), baseData.getStoreList(),
				recommendationInputDTO.getBaseWeek(), predictionService,recommendationInputDTO.isGlobalZone());

		// All functionalities which decides the price of an item is put here
		pricingEngineWS.recommendPrice(conn, baseData.getItemDataMap(), inputDTO,
				prRecommendationRunHeader, retLirMap, retLirConstraintMap, baseData.getCompetitorMap(),
				baseData.getMultiCompLatestPriceMap(), finalLigMap, baseData.getProductGroupProperties(),
				predictionComponent, leadZoneDetails, false, baseData.getPriceHistory(), retailCalendarDTO, isOnline,
				baseData.getPriceHistoryAll(), baseData.getMovementDataMap(), baseData.getRecommendationRuleMap(),
				baseData.getSaleDetails(), baseData.getAdDetails(), baseData.getStoreList(),
				recommendationInputDTO.getBaseWeek(), predictionService,recommendationInputDTO.isGlobalZone());

		independentItemDataMap.clear();
		
		

		boolean usePrediction = PricingEngineHelper.isUsePrediction(baseData.getProductGroupProperties(),
				inputDTO.getProductLevelId(), inputDTO.getProductId(), inputDTO.getLocationLevelId(),
				inputDTO.getLocationId());
		
		recommendationInputDTO.setUsePrediction(usePrediction);
		
		
		boolean checkItemType = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_TYPE_FOR_POSTDATING", "FALSE"));
		//This check is added for AZ for post dating the reccs based on type of item
		if (checkItemType) {
			PostDateRecByitemType(baseData.getItemDataMap());
		}
		
		// Clear the curent retail retained log for lig member if the recc retail is not
		// same as curr retail after Lig same rule is applied
		//added on 09/06/22
		ClearLogs(baseData.getItemDataMap());

		// 2. Convert current data to multiple weeks data
		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap = multiWeekConverter
				.convertToMultiWeekItemDataMap(recommendationInputDTO, baseData.getItemDataMap(),
						commonDataHelper.getAllWeekCalendarDetails(), baseData.getPriceHistory(),
						baseData.getRecommendationRuleMap(), baseData.getMovementDataMap(), baseData.getParentRecInfo(),
						baseData.getSaleDetails(), baseData.getAdDetails(), baseData.getDisplayDetails(), retLirMap,
						commonDataHelper.getFullCalendar(), usePrediction,baseData.getFutureCost(), baseData.getFuturePrice());
		
		if (usePrediction && !recommendationInputDTO.getRecType().equals(PRConstants.MW_WEEK_RECOMMENDATION)) {
			// 3. Get inputs for prediction
			List<MultiWeekPredEngineItemDTO> multiWeekPredInput = mutltiWeekPredInputBuilder
					.buildInputForPrediction(weeklyItemDataMap, recommendationInputDTO);

			logger.info("recommendPrice() - Calling multi week prediction...");

			logger.info("recommendPrice() - # of price points to be predicted: " + multiWeekPredInput.size());

			// 4. Call predicition
			List<MultiWeekPredEngineItemDTO> multiWeekPredOuput = mutliWeekPredictionComponent.callMultiWeekPrediction(
					recommendationInputDTO, multiWeekPredInput, commonDataHelper.getAllWeekCalendarDetails(),
					baseData.getPredictionCache());

			logger.info("recommendPrice() - # of price points predicted: " + multiWeekPredOuput.size());

			// 5. Update prediction to weekly map
			mutliWeekPredictionComponent.updateWeeklyPredictedMovement(weeklyItemDataMap, multiWeekPredOuput,
					commonDataHelper.getAllWeekCalendarDetails());
	
		}
		
		// 5. Update prediction for LIG
		mutliWeekPredictionComponent.updateLigLevelMovement(weeklyItemDataMap, retLirMap);
		// 7. Update mutliweek/Quarter level predicted movement in item datamap
		mutliWeekPredictionComponent.updateMultiWeekOrQuarterLevelPredMov(baseData.getItemDataMap(), weeklyItemDataMap,
				recommendationInputDTO);
		// 14. Set multi week item map to base data for saving data to DB
		baseData.setWeeklyItemDataMap(weeklyItemDataMap);

		new MultiWeekPriceFinalizer().applyRecommendationToAllWeeks(recommendationInputDTO, baseData.getItemDataMap(),
				weeklyItemDataMap, baseData.getPriceHistory(), baseData.getRecommendationRuleMap());
		
		//clear the explain logs if current retail is not retained/map is recommended as its belwo current retail for LIG memebers
		weeklyItemDataMap.forEach((recWeekKey, weeklyMap) -> {
			List<MWRItemDTO>itemListWithLIG = new ArrayList<>();
			//condition corrected to check the logs for non lig memebrs as well
			weeklyMap.forEach((itemKey, mwrItemDTO) -> {
				if (!mwrItemDTO.isLir()) //&& mwrItemDTO.getRetLirId() > 0)
					itemListWithLIG.add(mwrItemDTO);
			});
			new PricingEngineService().explainLogClearForLIGMembers(itemListWithLIG);
		});
		
		
		// Aggregate LIG level metrics
		aggregateUnitsSalesAndMarginAtLIGLevel(weeklyItemDataMap, retLirMap);
		
		//Added for AZ
				//Set flag as Yfor items whose  impact is included  in summary
				//items which do not have pending retail recommended and impact calculated will be marked as Y
				//LIG row's flag will be marked  based on the members.If any1 memenr has Flag as Y then LIg row will be marked as Y
				setImpctInclFlag(weeklyItemDataMap);
	}

	/**
	 * 
	 * @param itemDataMap
	 * @throws GeneralException
	 */
	private void PostDateRecByitemType(HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException {

		for (Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()) {

			PRItemDTO item = entry.getValue();
			if (item.getRecPriceEffectiveDate() != null
					&& !entry.getValue().getRecPriceEffectiveDate().equals(Constants.EMPTY)) {

				if (item.getUserAttr6() != null) {
					// logger.info("PostDateRecByitemType()-" +item.getItemCode() + " date:" +
					// item.getRecPriceEffectiveDate() );
//					logger.info(item.getItemCode() + "item type:" + item.getUserAttr6() + "priceeffectivedate:" + item.getRecPriceEffectiveDate() );
					item.setRecPriceEffectiveDate(
							setDateOfItemByType(item.getRecPriceEffectiveDate(), item.getUserAttr6()));

				}

			}

		}
	}

	/**
	 * @param futurePriceEffDate
	 * @param itemType
	 * @return
	 * @throws GeneralException
	 */
	private String setDateOfItemByType(String futurePriceEffDate, String itemType) throws GeneralException {

		int weekday = DateUtil.getdayofWeek(futurePriceEffDate, Constants.APP_DATE_FORMAT);
		if (itemType != null) {
			if (itemType.equalsIgnoreCase(Constants.SALE_FLOOR_ITEMS)) {
				if (weekday < 5) {
					String weekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(futurePriceEffDate), 0);

					futurePriceEffDate = DateUtil.dateToString(
							DateUtil.incrementDate(DateUtil.toDate(weekStartDate), 4), Constants.APP_DATE_FORMAT);

				} else if (weekday > 5) {
					String weekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(futurePriceEffDate), -1);

					futurePriceEffDate = DateUtil.dateToString(
							DateUtil.incrementDate(DateUtil.toDate(weekStartDate), 4), Constants.APP_DATE_FORMAT);
					logger.debug("futurePriceEffDate2:"+ futurePriceEffDate);
				}
			} else if (itemType.equalsIgnoreCase(Constants.HARD_PART_ITEMS)) {

				
				/*
				 * if (weekday <= 5) { futurePriceEffDate = DateUtil.dateToString(
				 * DateUtil.incrementDate(DateUtil.toDate(futurePriceEffDate), 1),
				 * Constants.APP_DATE_FORMAT); logger.info("futurePriceEffDate3:" +
				 * futurePriceEffDate); } else
				 */if (weekday == 7) {

					futurePriceEffDate = DateUtil.dateToString(
							DateUtil.incrementDate(DateUtil.toDate(futurePriceEffDate), 2), Constants.APP_DATE_FORMAT);
					logger.debug("futurePriceEffDate4:" + futurePriceEffDate);
				}
			} else if (weekday == 1) {
				logger.debug("futurePriceEffDate5:" + futurePriceEffDate);
				futurePriceEffDate = DateUtil.dateToString(
						DateUtil.incrementDate(DateUtil.toDate(futurePriceEffDate), 1), Constants.APP_DATE_FORMAT);
			}
		}

		return futurePriceEffDate;
	}

	
	private void aggregateUnitsSalesAndMarginAtLIGLevel(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		for(Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weekEntry: weeklyItemDataMap.entrySet()) {
			HashMap<ItemKey, MWRItemDTO> itemMapWeekly = weekEntry.getValue();
			for(Map.Entry<ItemKey, MWRItemDTO> itemEntry: itemMapWeekly.entrySet()) {
				MWRItemDTO mwrItemDTO = itemEntry.getValue();
				if(mwrItemDTO.isLir()) {
					mwrItemDTO.setFinalPricePredictedMovement(0D);
					mwrItemDTO.setFinalPriceMargin(0D);
					mwrItemDTO.setFinalPriceRevenue(0D);
					mwrItemDTO.setCurrentRegUnits(0D);
					mwrItemDTO.setCurrentRegMargin(0D);
					mwrItemDTO.setCurrentRegRevenue(0D);
					if(retLirMap.containsKey(mwrItemDTO.getRetLirId())) {
						List<PRItemDTO> ligMembers = retLirMap.get(mwrItemDTO.getRetLirId());
						for(PRItemDTO prItemDTO: ligMembers) {
							ItemKey itemKey = PRCommonUtil.getItemKey(prItemDTO);
							if(itemMapWeekly.containsKey(itemKey)) {
								MWRItemDTO ligMember = itemMapWeekly.get(itemKey);
								mwrItemDTO.setFinalPricePredictedMovement(mwrItemDTO.getFinalPricePredictedMovement()
										+ checkNullAndReturnZero(ligMember.getFinalPricePredictedMovement()));
								mwrItemDTO.setFinalPriceRevenue(mwrItemDTO.getFinalPriceRevenue()
										+ checkNullAndReturnZero(ligMember.getFinalPriceRevenue()));
								mwrItemDTO.setFinalPriceMargin(mwrItemDTO.getFinalPriceMargin()
										+ checkNullAndReturnZero(ligMember.getFinalPriceMargin()));
								mwrItemDTO.setCurrentRegUnits(mwrItemDTO.getCurrentRegUnits()
										+ checkNullAndReturnZero(ligMember.getCurrentRegUnits()));
								mwrItemDTO.setCurrentRegMargin(mwrItemDTO.getCurrentRegMargin()
										+ checkNullAndReturnZero(ligMember.getCurrentRegMargin()));
								mwrItemDTO.setCurrentRegRevenue(mwrItemDTO.getCurrentRegRevenue()
										+ checkNullAndReturnZero(ligMember.getCurrentRegRevenue()));
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param value
	 * @return 0 or value
	 */
	private double checkNullAndReturnZero(Double value){
		if(value == null){
			return 0;
		}else{
			return value;
		}
	}
	
	private void setImpctInclFlag(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weekEntry : weeklyItemDataMap.entrySet()) {
			HashMap<ItemKey, MWRItemDTO> itemMapWeekly = weekEntry.getValue();
			Map<Integer, MWRItemDTO> ligMap = new HashMap<>();
			Map<Integer, List<MWRItemDTO>> ligMembersMap = new HashMap<>();
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : itemMapWeekly.entrySet()) {

				MWRItemDTO mwrItemDTO = itemEntry.getValue();
				if (mwrItemDTO.isLir()) {
					ligMap.put(mwrItemDTO.getRetLirId(), mwrItemDTO);
				} else if (mwrItemDTO.getRetLirId() > 0) {
					setImpactField(mwrItemDTO);
					List<MWRItemDTO> tem = new ArrayList<>();
					if (ligMembersMap.containsKey(mwrItemDTO.getRetLirId())) {
						tem = ligMembersMap.get(mwrItemDTO.getRetLirId());
					}
					tem.add(mwrItemDTO);
					ligMembersMap.put(mwrItemDTO.getRetLirId(), tem);
				} else {
					setImpactField(mwrItemDTO);
				}
			}

			setImpctInclFlagForLIG(ligMembersMap, ligMap);
		}
	}

	
	public static void setImpctInclFlagForLIG(Map<Integer, List<MWRItemDTO>> ligMembersMap,
			Map<Integer, MWRItemDTO> ligMap) {
		for (Map.Entry<Integer, MWRItemDTO> lig : ligMap.entrySet()) {
			if (ligMembersMap.containsKey(lig.getKey())) {
				List<MWRItemDTO> ligMembers = ligMembersMap.get(lig.getKey());
				int flag = 0;
				for (MWRItemDTO item : ligMembers) {
					if (item.getIsImpactIncludedInSummaryCalculation() == 'Y') {
						flag = 1;
						break;
					}
				}
				if (flag == 1)

					lig.getValue().setIsImpactIncludedInSummaryCalculation('Y');
				else

					lig.getValue().setIsImpactIncludedInSummaryCalculation('N');
			}
		}
	}

	public static void setImpactField(MWRItemDTO mwrItemDTO) {
		if (mwrItemDTO.getIsPendingRetailRecommended() == 0 && mwrItemDTO.getPriceChangeImpact() != 0)
			mwrItemDTO.setIsImpactIncludedInSummaryCalculation('Y');
		else
			mwrItemDTO.setIsImpactIncludedInSummaryCalculation('N');
	}
	
	/**
	 * 
	 * @param itemDataMap
	 */
	private void ClearLogs(HashMap<ItemKey, PRItemDTO> itemDataMap) {

		itemDataMap.forEach((itemKey, tDTO) -> {

			if (tDTO.getRetLirId() > 0 && tDTO.isCurPriceRetained()) {

				if (tDTO.getRecommendedRegPrice() != null && tDTO.getRegPrice() != null && tDTO.getRegPrice() > 0
						&& tDTO.getRecommendedRegPrice().getUnitPrice() != tDTO.getRegPrice()) {
					tDTO.setCurrentPriceRetained(false);
					List<PRExplainAdditionalDetail> explainAdditionalDetail = (tDTO.getExplainLog()
							.getExplainAdditionalDetail() != null ? tDTO.getExplainLog().getExplainAdditionalDetail()
									: new ArrayList<PRExplainAdditionalDetail>());

					Map<Integer, List<PRExplainAdditionalDetail>> explainLogMap = null;
					if (explainAdditionalDetail != null) {
						explainLogMap = explainAdditionalDetail.stream().collect(
								Collectors.groupingBy(PRExplainAdditionalDetail::getExplainRetailNoteTypeLookupId));
					}

					if (explainLogMap != null) {
						for (Map.Entry<Integer, List<PRExplainAdditionalDetail>> explainLogEntry : explainLogMap
								.entrySet()) {
							if (explainLogEntry
									.getKey() == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_ITEM_HAS_FUTURE_PRICE
											.getNoteTypeLookupId()
									|| explainLogEntry
											.getKey() == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_ITEM_IS_ON_CLEARNCE
													.getNoteTypeLookupId() || explainLogEntry
											.getKey() == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_LIG_HAS_FUTURE_PRICE
													.getNoteTypeLookupId()) {
								tDTO.getExplainLog()
										.setExplainAdditionalDetail(new ArrayList<PRExplainAdditionalDetail>());
							}

						}
					}

				}

			}
		});

	}
}



