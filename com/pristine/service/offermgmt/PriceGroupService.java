package com.pristine.service.offermgmt;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineBrand;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.recommendationrule.CheckFuturePromoEndsInXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckOnGoingPromoEndsInXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckRegRetailChangedInLastXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckRetailCurMultiplePriceRule;
import com.pristine.service.offermgmt.recommendationrule.FutureRetailChangeRule;
import com.pristine.service.offermgmt.recommendationrule.PastOverridenRetailRule;
import com.pristine.service.offermgmt.recommendationrule.RetainCurrentRetailRule;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PriceGroupService {
	private static Logger logger = Logger.getLogger("PriceGroupService");

	public void recommendPriceGroupRelatedItems(PRRecommendationRunHeader recommendationRunHeader, List<PRItemDTO> itemListWithRecPrice,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, String curWeekStartDate,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails, boolean isRecommendAtStoreLevel,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory, RetailCalendarDTO curCalDTO, double maxUnitPriceDiff, 
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws Exception, GeneralException, OfferManagementException {

		PricingEngineService pricingEngineService = new PricingEngineService();
		ApplyStrategy applyStrategy = new ApplyStrategy();
		
		List<PRItemDTO> emptyList = new ArrayList<PRItemDTO>();

		logger.debug("Recommending price group related items again started...");
		
		logger.info("Setting processed status...");
		// set all related item as un-processed
		Set<Integer> ligWithRelation = new HashSet<Integer>();
		for (PRItemDTO itemDTO : itemListWithRecPrice) {
			// Items dependent on other items (i.e. size or brand relation applied)
			if ((!itemDTO.isLir() && itemDTO.getRetLirId() > 0) || (!itemDTO.isLir() && itemDTO.getRetLirId() <= 0)) {
				boolean isBrandGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.BRAND.getGuidelineTypeId());
				boolean isSizeGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.SIZE.getGuidelineTypeId());
				if ((isBrandGuidelineApplied || isSizeGuidelineApplied)) {
					itemDTO.setProcessed(false);
					if (itemDTO.getRetLirId() > 0) {
						ligWithRelation.add(itemDTO.getRetLirId());
					}
				}
			}
		}
		
		// set lig which has at least one dependent item as non-processed
		for (Integer retLirId : ligWithRelation) {
			for (PRItemDTO ligItem : itemListWithRecPrice) {
				if (ligItem.isLir() && ligItem.getItemCode() == retLirId) {
					ligItem.setProcessed(false);
					break;
				}
			}
		}
		
		logger.info("Setting processed status is completed");

		int loopThresholdCnt = 0;
		int loopThreshold = Integer.parseInt(PropertyManager.getProperty("PR_WHILE_LOOP_THRESHOLD"));
		// loop till all the related items are processed
		while (true) {
			loopThresholdCnt++;
			// Is still any item pending
			if (noOfItemYetToProcess(itemListWithRecPrice) == 0) {
				break;
			}

			for (PRItemDTO itemDTO : itemListWithRecPrice) {
				// Items dependent on other items (i.e. size or brand relation applied)
				if ((!itemDTO.isLir() && itemDTO.getRetLirId() > 0) || (!itemDTO.isLir() && itemDTO.getRetLirId() <= 0)) {
					boolean isBrandGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO,
							GuidelineTypeLookup.BRAND.getGuidelineTypeId());
					boolean isSizeGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.SIZE.getGuidelineTypeId());
					if ((isBrandGuidelineApplied || isSizeGuidelineApplied)) {

						// try to change price, only if all its related items are processed
						if (!itemDTO.isProcessed() && pricingEngineService.isAllParentItemProcessed(itemDTO, itemDataMap)) {
							// If the adjusted price is not in conflict, then don't change it
							// e.g. price of a dependent would have been rollback as there is no sufficient margin
							// and it will be changed again here that may not have sufficient margin

							// Dinesh::04-JAN-18, Apply Strategy functions calling restructured to apply for current processing
							// item,
							// following to that updating conflicts and other process to continue. This changes done since the
							// related item
							// price got changed while recommending related item, but GuideLineAndConstraints were not updated for
							// processing
							// after new recommended price of related item which caused an issue.

							// Apply strategy
							applyStrategy.applyStrategies(itemDTO, emptyList, itemDataMap, compIdMap, recommendationRunHeader.getRunId(),
									retLirConstraintMap, multiCompLatestPriceMap, curWeekStartDate, leadZoneDetails, isRecommendAtStoreLevel,
									recommendationRunHeader);

							pricingEngineService.updateConflicts(itemDTO);
							if (itemDTO.isPromoEndsWithinXWeeks() && itemDTO.isCurPriceRetained()
									&& itemDTO.getIsConflict() == 0) {
								
								updateExplainLogsByRules(recommendationRunHeader, itemDTO, pricingEngineService,
										recommendationRuleMap, itemZonePriceHistory, itemListWithRecPrice,
										maxUnitPriceDiff, curCalDTO, false);
								
								itemDTO.setProcessed(true);
							} else {

								updateExplainLogsByRules(recommendationRunHeader, itemDTO, pricingEngineService,
										recommendationRuleMap, itemZonePriceHistory, itemListWithRecPrice,
										maxUnitPriceDiff, curCalDTO, true);
							}
						} else {
							//logger.debug("Related item of " + itemDTO.getItemCode() + " is not yet processed");
						}
					}
				}
			}

			// If all the members in a lig is processed, then process lig and set the flag as processed
			for (Integer retLirId : ligWithRelation) {
				for (PRItemDTO ligItem : itemListWithRecPrice) {
					if (ligItem.isLir() && ligItem.getItemCode() == retLirId) {
						List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(ligItem, itemListWithRecPrice);
						// lig is not yet processed, but all lig members processed
						if (!ligItem.isProcessed() && noOfItemYetToProcess(ligMembersOrNonLig) == 0) {
							HashMap<Integer, List<PRItemDTO>> ligMap = new HashMap<Integer, List<PRItemDTO>>();
							ligMap.put(ligItem.getChildLocationId(), ligMembersOrNonLig);
							new LIGConstraint().applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);
							ligItem.setProcessed(true);
						}
					}
				}

			}

			// This is added to avoid the while loop keep running
			if (loopThresholdCnt > loopThreshold) {
				throw new OfferManagementException("Infinite While Loop, Program Terminated ", RecommendationErrorCode.INFINITE_LOOP);
			}
		}
		logger.debug("Recommending price group related items again completed...");
	}

	/**
	 * 
	 * @param recommendationRunHeader
	 * @param itemDTO
	 * @param pricingEngineService
	 * @param recommendationRuleMap
	 * @param itemZonePriceHistory
	 * @param itemListWithRecPrice
	 * @param maxUnitPriceDiff
	 * @param curCalDTO
	 * @param applyObjective
	 * @throws Exception
	 * @throws GeneralException
	 */
	private void updateExplainLogsByRules(PRRecommendationRunHeader recommendationRunHeader, PRItemDTO itemDTO,
			PricingEngineService pricingEngineService,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			List<PRItemDTO> itemListWithRecPrice, double maxUnitPriceDiff, RetailCalendarDTO curCalDTO,
			boolean applyObjective) throws Exception, GeneralException {
		ObjectiveService objectiveService = new ObjectiveService();

		int longTermPromoWeeks = Integer.parseInt(PropertyManager.getProperty("PROMO_X_WEEKS_FROM_REC_WEEK"));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

		List<PRItemDTO> tempList = new ArrayList<PRItemDTO>();
		tempList.add(itemDTO);

		// clear addition detail log (cur price retained), as it may not valid, because price
		// is following brand and size guideline
		pricingEngineService.clearAdditionalExplainLog(itemDTO);

		// If Item is on TPR and Recommended Price is not violated Size and Brand relation then don't
		// recommend these items
		// Changed By Dinesh(10/10/2017)
		// Dinesh:: 03-JAN-18, code changes to handle TPR items if the Related item is recommended with
		// new price
		// Find the Price range this item based on related items and check Size/Brand relation is within
		// limit
		RetainCurrentRetailRule checkRetailCurMultiplePriceRule = new CheckRetailCurMultiplePriceRule(itemDTO,
				recommendationRuleMap);
		RetainCurrentRetailRule checkFuturePromoEndsInXWeeksRule = new CheckFuturePromoEndsInXWeeksRule(itemDTO,
				recommendationRuleMap);
		RetainCurrentRetailRule checkOnGoingPromoEndsInXWeeksRule = new CheckOnGoingPromoEndsInXWeeksRule(itemDTO,
				recommendationRuleMap);
		RetainCurrentRetailRule futureRetailChangeRule = new FutureRetailChangeRule(itemDTO, recommendationRuleMap);
		int processingItemCode = itemDTO.isLir() ? itemDTO.getLigRepItemCode() : itemDTO.getItemCode();
		RetainCurrentRetailRule checkRegRetailChangedInLastXWeeksRule = new CheckRegRetailChangedInLastXWeeksRule(
				itemDTO, recommendationRuleMap, itemZonePriceHistory.get(processingItemCode), curCalDTO.getStartDate());
		PastOverridenRetailRule pastOverrideRule = new PastOverridenRetailRule(itemDTO, recommendationRuleMap);
		// If a regular retail was changed during the last 13 weeks, then the retail will not be changed
		// for 13 weeks
		// unless the current retail is going to break the brand, size or the cost constraint.
		if (checkRegRetailChangedInLastXWeeksRule.isCurrentRetailRetained()) {
			logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
					+ itemDTO.getItemCode() + " as regular retail was changed during within last 13 weeks ***");
			int regRetailChangeInLastXWeeks = Integer
					.parseInt(PropertyManager.getProperty("REG_RETAIL_CHANGE_IN_LAST_X_WEEKS"));
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(regRetailChangeInLastXWeeks));
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS, additionalDetails);
		} else if (itemDTO.isCurPriceRetained() && !itemDTO.isPromoEndsWithinXWeeks()
				&& pricingEngineService.checkPriceRangeUsingRelatedItem(itemDTO)) {
			itemDTO.setProcessed(true);
			MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
					itemDTO.getRegMPrice());
			// Explain log for on-going promotion
			if ((itemDTO.isOnGoingPromotion() && !itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
					&& checkOnGoingPromoEndsInXWeeksRule.isCurrentRetailRetained())
					|| (itemDTO.isOnGoingPromotion() && itemDTO.isFuturePromotion()
							&& !itemDTO.isPromoEndsWithinXWeeks() && !checkFuturePromoEndsInXWeeksRule.isRuleEnabled()
							&& checkOnGoingPromoEndsInXWeeksRule.isCurrentRetailRetained())) {
				if (!itemDTO.getRecommendedRegPrice().equals(curRegPrice)) {
					List<String> addtLogDetails = new ArrayList<String>();
					addtLogDetails
							.add(formatter.format(LocalDate.parse(recommendationRunHeader.getStartDate(), formatter)
									.plus(longTermPromoWeeks, ChronoUnit.WEEKS).plus(6, ChronoUnit.DAYS)));

					pricingEngineService.writeAddtExpLogForTPRRelLigItems(itemDTO, itemListWithRecPrice,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, addtLogDetails);
				}
			}

			// Explain log for Future promotions
			else if (itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
					&& checkFuturePromoEndsInXWeeksRule.isCurrentRetailRetained()) {
				if (!itemDTO.getRecommendedRegPrice().equals(curRegPrice)) {
					List<String> addtLogDetails = new ArrayList<String>();
					addtLogDetails
							.add(formatter.format(LocalDate.parse(recommendationRunHeader.getStartDate(), formatter)
									.plus(longTermPromoWeeks, ChronoUnit.WEEKS).plus(6, ChronoUnit.DAYS)));

					pricingEngineService.writeAddtExpLogForTPRRelLigItems(itemDTO, itemListWithRecPrice,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, addtLogDetails);
				}
			}
		} else if (futureRetailChangeRule.isCurrentRetailRetained()) {

			logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
					+ itemDTO.getItemCode() + " as there is a future retail " + itemDTO.getFutureRecRetail().toString()
					+ " in " + itemDTO.getRecPriceEffectiveDate() + " ***");

			List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(itemDTO,
					itemListWithRecPrice);

			for (PRItemDTO itemOrLig : ligMembersOrNonLig) {
				itemOrLig.setFutureRetailPresent(true);
				itemOrLig.setRecPriceEffectiveDate(itemDTO.getRecPriceEffectiveDate());
			}

			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(itemDTO.getFutureRecRetail().multiple > 1 ? itemDTO.getFutureRecRetail().toString()
					: PRFormatHelper.roundToTwoDecimalDigit(itemDTO.getFutureRecRetail().price));
			additionalDetails.add(itemDTO.getRecPriceEffectiveDate());

			new PricingEngineService().writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_FUTURE_RETAIL_CHANGE_IN_X_WEEKS, additionalDetails);
		} else if (pastOverrideRule.isOverriddeRetailRetained()) {
			
			new OverrideService().retainOverrides(itemDTO, itemListWithRecPrice);
		
		} else if (applyObjective) {
			objectiveService.applyObjectiveAndSetRecPrice(tempList, itemZonePriceHistory, curCalDTO,
					recommendationRuleMap);

			// 10th Mar 2017, Retain current retail when the recommended unit price is within x cents of
			// current unit price
			// (when current unit price is in multiples). It is called again here to correct the dependent
			// item also
			// To handle relationship like A depends on B, C depends on A
			if (checkRetailCurMultiplePriceRule.isCurrentRetailRetained() && pricingEngineService
					.retainCurrentMultipleRetail(itemDTO, itemListWithRecPrice, maxUnitPriceDiff)) {

				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
						itemDTO.getRegMPrice());
				itemDTO.setRecommendedRegPrice(curRegPrice);
			}
		}
		
		
		if (itemDTO.isAucOverride()) {
			List<String> additionalDetails = new ArrayList<String>();
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.AUC_OVERRIDE_PRICE, additionalDetails);
		}
		if (itemDTO.isCompOverCost())
		{
			List<String> additionalDetails = new ArrayList<String>();
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.COMP_OVERRIDE_COST, additionalDetails);
		}
		if (itemDTO.isCompOverride() && !itemDTO.isCompOverCost()) {
			List<String> additionalDetails = new ArrayList<String>();
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.COMP_OVERRIDE_PRICE, additionalDetails);
		}
		if (itemDTO.isBrandGuidelineApplied()) {
			List<String> additionalDetails = new ArrayList<String>();
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.BRAND_GUIDELINE, additionalDetails);
		}

		if (itemDTO.getMissingTierInfo().size() > 0) {
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.MISSING_TIER, itemDTO.getMissingTierInfo());
		}

		/*if (itemDTO.getPricePointsFiltered() > 0) {
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(itemDTO.getPricePointsFiltered()));
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.PRICE_POINTS_FILTERED, additionalDetails);
		}*/
	}
	
	private int noOfItemYetToProcess(List<PRItemDTO> itemListWithRecPrice) {
		int noOfItemYetToProcess = 0;
		for (PRItemDTO itemDTO : itemListWithRecPrice) {
			if (!itemDTO.isProcessed()) {
				noOfItemYetToProcess++;
			}
		}
		return noOfItemYetToProcess;
	}

	public void updatePriceRelationFromStrategy(HashMap<ItemKey, PRItemDTO> itemDataMap) {

		for (PRItemDTO item : itemDataMap.values()) {
			PRStrategyDTO strategyDTO = item.getStrategyDTO();
			List<PRGuidelineBrand> brandGuidelines = null;
			List<PRGuidelineSize> sizeGuidelines = null;

			if (!item.isLir() && strategyDTO != null) {
				if (strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getBrandGuideline() != null) {
					brandGuidelines = strategyDTO.getGuidelines().getBrandGuideline();
				}

				if (strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getSizeGuideline() != null) {
					sizeGuidelines = strategyDTO.getGuidelines().getSizeGuideline();
				}

				updatePriceRelationFromStrategy(item, brandGuidelines, sizeGuidelines);
			}
		}
	}

	// Brand guideline has to be taken from strategy. e.g. For store brand relation where just tier is specified
	private void updatePriceRelationFromStrategy(PRItemDTO itemInfo, List<PRGuidelineBrand> brandGuidelines, List<PRGuidelineSize> sizeGuidelines) {

		if (itemInfo.getPgData() != null) {
			if (itemInfo.getPgData().getRelationList() != null) {
				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = null;

				navigableMap = itemInfo.getPgData().getRelationList();

				/*if(itemInfo.getItemCode() == 295235) {
					logger.debug("Lead items size: " + navigableMap.size());
				}*/
				//logger.debug("Processing item: " + itemInfo.getItemCode());
				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
					for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
						// Set strategy level price relations if price relations
						// are not mentioned in price group
						// Added operator text condition to support price group
						// relations with only retail_type and no actual
						// relation.
						// (When only retail type is specified in price group)
						/*if(itemInfo.getItemCode() == 295235) {
							logger.debug("relatedItem: " + relatedItem.toString());
						}*/
						if (relatedItem.getPriceRelation() == null || relatedItem.getPriceRelation().getOperatorText() == null) {
							// logger.debug("Copying from strategy for " +
							// entry.getKey());
							/*if(itemInfo.getItemCode() == 295235) {
								logger.debug("No price relation in price group: " + relatedItem.toString());
							}*/
							//logger.debug("No price relation in price group: " + relatedItem.toString());
							if (entry.getKey() == PRConstants.BRAND_RELATION) {
								// logger.debug("Input Brand Tier " +
								// itemInfo.getPgData().getBrandTierId() + "\t"
								// +
								// relatedItem.getRelatedItemBrandTierId());
								// logger.debug("Input Brand Id " +
								// itemInfo.getPgData().getBrandId() + "\t" +
								// relatedItem.getRelatedItemBrandId());
								/*if(itemInfo.getItemCode() == 295235) {
									logger.debug("Brand relation: " + relatedItem.toString());
								}*/
								boolean isStrategyRelationSet = false;
								if (brandGuidelines != null) {
									// Iterate through list of brand guidelines
									// in strategy
									/*if(itemInfo.getItemCode() == 295235) {
										logger.debug("Guidelines found: " + brandGuidelines.size());
									}*/
									//logger.debug("Guidelines found: " + brandGuidelines.size());
									for (PRGuidelineBrand brandGuideline : brandGuidelines) {
										if (!isStrategyRelationSet) {
											// Check if the brand tier id of
											// item and related item matches
											// with what is defined in strategy
											/*
											 * logger.debug("Guideline - Tier1: " + brandGuideline.getBrandTierId1() +
											 * ", Tier2: " + brandGuideline.getBrandTierId2());
											 */
											if (brandGuideline.getBrandTierId1() > 0 && brandGuideline.getBrandTierId2() > 0) {
												//logger.debug("Guidelines found: " + brandGuidelines.size());
												if (itemInfo.getPgData().getBrandTierId() == brandGuideline.getBrandTierId1()
														&& relatedItem.getRelatedItemBrandTierId() == brandGuideline.getBrandTierId2()) {
													/*
													 * logger.debug("Strategy Brand Tier " +
													 * brandGuideline.getBrandTierId1() + "\t" +
													 * brandGuideline.getBrandTierId2());
													 */
													PRPriceGroupRelnDTO strategyRelation = new PRPriceGroupRelnDTO();
													strategyRelation.copy(brandGuideline);
													// Overriding retail_type
													// from Price
													// Group Relation
													if (relatedItem.getPriceRelation() != null) {
														char priceRelationRetailType = relatedItem.getPriceRelation().getRetailType();
														strategyRelation.setRetailType(priceRelationRetailType);
													}
													relatedItem.setPriceRelation(strategyRelation);
													isStrategyRelationSet = true;
													break;
												} else {
													// if(itemInfo.getItemCode() == 295235) {
													/*
													 * logger.debug("Not matching: Tier id dep:" +
													 * itemInfo.getPgData().getBrandTierId() + ", Tier id lead: " +
													 * relatedItem.getRelatedItemBrandTierId() + ", guideline tier 1: "
													 * + brandGuideline.getBrandTierId1() + ", guideline tier 2: " +
													 * brandGuideline.getBrandTierId2()); // }
													 */												}
											}
										}

										if (!isStrategyRelationSet) {
											// Check if the brand of item and
											// related item matches with what is
											// defined in strategy
											if (brandGuideline.getBrandId1() > 0 && brandGuideline.getBrandId2() > 0) {
												if (itemInfo.getPgData().getBrandId() == brandGuideline.getBrandId1()
														&& relatedItem.getRelatedItemBrandId() == brandGuideline.getBrandId2()) {
													/*
													 * logger.debug("Strategy Brand Id " + brandGuideline.getBrandId1()
													 * + "\t" + brandGuideline.getBrandId2());
													 */
													PRPriceGroupRelnDTO strategyRelation = new PRPriceGroupRelnDTO();
													strategyRelation.copy(brandGuideline);
													// Overriding retail_type
													// from Price
													// Group Relation
													if (relatedItem.getPriceRelation() != null) {
														char priceRelationRetailType = relatedItem.getPriceRelation().getRetailType();
														strategyRelation.setRetailType(priceRelationRetailType);
													}
													relatedItem.setPriceRelation(strategyRelation);
													isStrategyRelationSet = true;
													break;
												}
											}
										}
										
										if(!isStrategyRelationSet) {
											adjustBrandGuidelineByMissingTiers(itemInfo, brandGuidelines, relatedItem);
										}
									}
								}
							}

							if (entry.getKey() == PRConstants.SIZE_RELATION) {
								boolean isStrategyRelationSet = false;
								if (sizeGuidelines != null) {
									char htol = 'Y';
									if (itemInfo.getPgData().getItemSize() < relatedItem.getRelatedItemSize())
										htol = 'N';
									// Iterate through size relations defined in
									// the strategy
									for (PRGuidelineSize sizeGuideline : sizeGuidelines) {
										if (sizeGuideline.getHtol() == htol) {
											PRPriceGroupRelnDTO strategyRelation = new PRPriceGroupRelnDTO();
											strategyRelation.copy(sizeGuideline);
											relatedItem.setPriceRelation(strategyRelation);
											relatedItem.setHtol(sizeGuideline.getHtol());
											isStrategyRelationSet = true;
											break;
										}
									}

									// Copy Size guideline
									if (!isStrategyRelationSet) {
										PRPriceGroupRelnDTO strategyRelation = new PRPriceGroupRelnDTO();
										strategyRelation.copy(sizeGuidelines.get(0));
										relatedItem.setPriceRelation(strategyRelation);
										relatedItem.setHtol(sizeGuidelines.get(0).getHtol());
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param itemInfo
	 * @param brandGuidelines
	 * @param relatedItem
	 * @return
	 */
	private void adjustBrandGuidelineByMissingTiers(PRItemDTO itemInfo,
			List<PRGuidelineBrand> brandGuidelines, PRPriceGroupRelatedItemDTO relatedItem) {
		try {
			for (PRGuidelineBrand brandGuideline : brandGuidelines) {
				if (brandGuideline.getBrandTierId1() > 0 && brandGuideline.getBrandTierId2() > 0) {
					if (itemInfo.getPgData().getBrandTierId() == brandGuideline.getBrandTierId1()
							&& relatedItem.getRelatedItemBrandTierId() != brandGuideline.getBrandTierId2()) {
						PRGuidelineBrand nextGuideline = null;
						for (PRGuidelineBrand brandGuideline2 : brandGuidelines) {
							if (brandGuideline.getBrandTierId2() == brandGuideline2.getBrandTierId1()) {
								nextGuideline = brandGuideline2;
								break;
							}
						}
						if (nextGuideline != null) {
							PRPriceGroupRelnDTO strategyRelation = new PRPriceGroupRelnDTO();
							PRGuidelineBrand brandGuidelineAdjusted = (PRGuidelineBrand) brandGuideline.clone();
							
							
							if (brandGuidelineAdjusted.getMinValue() != Constants.DEFAULT_NA
									&& nextGuideline.getMinValue() != Constants.DEFAULT_NA) {
								brandGuidelineAdjusted.setMinValue(
										brandGuidelineAdjusted.getMinValue() + nextGuideline.getMinValue());
							}
							
							
							if (brandGuidelineAdjusted.getMaxValue() != Constants.DEFAULT_NA
									&& nextGuideline.getMaxValue() != Constants.DEFAULT_NA) {
								brandGuidelineAdjusted
										.setMaxValue(brandGuidelineAdjusted.getMaxValue() + nextGuideline.getMaxValue());
							}
							
							strategyRelation.copy(brandGuidelineAdjusted);
							
							if (relatedItem.getPriceRelation() != null) {
								char priceRelationRetailType = relatedItem.getPriceRelation().getRetailType();
								strategyRelation.setRetailType(priceRelationRetailType);
							}
							
							
							logger.debug("Origianl min: " + brandGuideline.getMinValue() + ", Adjusted max: "
									+ brandGuideline.getMaxValue());
							
							logger.debug("Origianl min: " + nextGuideline.getMinValue() + ", Adjusted max: "
									+ nextGuideline.getMaxValue());
							
							logger.debug("Adjusted min: " + brandGuidelineAdjusted.getMinValue() + ", Adjusted max: "
									+ brandGuidelineAdjusted.getMaxValue());
							
							logger.debug("Missing tier: " + brandGuideline.getBrandTier2() + ", Next Tier: "
									+ nextGuideline.getBrandTier2());
							
							itemInfo.getMissingTierInfo().add(brandGuideline.getBrandTier2());
							itemInfo.getMissingTierInfo().add(nextGuideline.getBrandTier2());
							
							relatedItem.setPriceRelation(strategyRelation);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("adjustBrandGuidelineByMissingTiers() - Error while adjusting missing tiers", e);
		}
	}
	
	
	public void setDefaultBrandPrecedence(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap) {
		// There was an item with 3 brand relation and all 3 items has relationship as 10 - 20% below,
		// but at the end, the recommended price matched 2 of the national brand. This is because, they
		// haven't assigned any brand precedence. Also they wanted it to be lower than the lowest price of all three.
		// so it is decided to change the precedence based on the relation operator. If below/above is present
		// then item with lowest/highest price is given higher precedence, so that it recommends below/above the
		// lowest/highest
		// look for jira issue :: PROM-1287

	//	PricingEngineService pricingEngineService = new PricingEngineService();
	//	List<PRItemDTO> allItems = new ArrayList<PRItemDTO>();
		// Loop each item 
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			PRPriceGroupDTO priceGroupDTO = itemDTO.getPgData();

			if (!itemDTO.isLir() && priceGroupDTO != null) {
				//commented by karishma on 06/29/2022 to solve the memory issue
				//This loop is getting populated again anad again which is not required
				/*
				 * for(PRItemDTO tempItem: itemDataMap.values()) { allItems.add(tempItem); }
				 */
				// Loop inside each relationship
				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationMap : priceGroupDTO.getRelationList().entrySet()) {
					ArrayList<PRPriceGroupRelatedItemDTO> relatedItemList = relationMap.getValue();
					boolean isBrandPrecedenceDefined = false;
					boolean isAboveRelationPresent = false;
					boolean isBelowRelationPresent = false;

					for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
						// If at least one precedence defined, then don't change anything
						if (priceGroupRelatedItemDTO.getBrandPrecedence() > 0) {
							isBrandPrecedenceDefined = true;
							break;
						}
					}

					// Check if the item has more than one brand relation and brand precedence is not defined
					if (relationMap.getKey() == PRConstants.BRAND_RELATION && relatedItemList.size() > 1 && !isBrandPrecedenceDefined) {
						// Check if below or above relation used
						for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
							if (priceGroupRelatedItemDTO.getPriceRelation() != null
									&& priceGroupRelatedItemDTO.getPriceRelation().getOperatorText() != null) {
								String operatorText = priceGroupRelatedItemDTO.getPriceRelation().getOperatorText();

								if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)
										|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
										|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
									isAboveRelationPresent = true;
									break;
								} else {
									isBelowRelationPresent = true;
									break;
								}
							}
						}

						// Keep item detail in a temp list with related item unite price
						List<PRItemDTO> tempRelatedItemList = new ArrayList<PRItemDTO>();
						// find related item cur reg price for lig / non-lig
						// If the related item is not authorized or there is no cur price, still add in the list
						// with 0 as cur price
						for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
							PRItemDTO tempItemDTO = new PRItemDTO();
							if (priceGroupRelatedItemDTO.getIsLig()) {
								
								 List<PRItemDTO> ligMembers =null;
								/*
								 * List<PRItemDTO> ligMembers =
								 * pricingEngineService.getLigMembers(priceGroupRelatedItemDTO.
								 * getRelatedItemCode(), allItems);
								 */
								
								if (retLirMap.containsKey(priceGroupRelatedItemDTO.getRelatedItemCode())) {
									ligMembers = retLirMap.get(priceGroupRelatedItemDTO.getRelatedItemCode());
								}

								tempItemDTO.setItemCode(priceGroupRelatedItemDTO.getRelatedItemCode());
								tempItemDTO.setLir(true);

								if (ligMembers!=null && ligMembers.size() > 0) {
									MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
									Object object = mostOccurrenceData.getMaxOccurance(ligMembers, "CurRegPriceInMultiples");
									if (object != null) {
										MultiplePrice curRegPrice = (MultiplePrice) object;
										tempItemDTO.setRegMPack(curRegPrice.multiple);
										if (curRegPrice.multiple > 1) {
											tempItemDTO.setRegMPrice(curRegPrice.price);
										} else {
											tempItemDTO.setRegPrice(curRegPrice.price);
										}
									}
								}
							} else {
								tempItemDTO.setItemCode(priceGroupRelatedItemDTO.getRelatedItemCode());
								tempItemDTO.setLir(false);
								ItemKey itemKey = PRCommonUtil.getItemKey(tempItemDTO);
								if (itemDataMap.get(itemKey) != null) {
									tempItemDTO.setRegMPack(itemDataMap.get(itemKey).getRegMPack());
									tempItemDTO.setRegPrice(itemDataMap.get(itemKey).getRegPrice());
									tempItemDTO.setRegMPrice(itemDataMap.get(itemKey).getRegMPrice());
								}
							}

							tempRelatedItemList.add(tempItemDTO);
						}

						// Sort based on operator text
						if (isAboveRelationPresent) {
							// Set the precedence sorted by current unit price in descending order
							Collections.sort(tempRelatedItemList, new Comparator<PRItemDTO>() {
								public int compare(PRItemDTO a, PRItemDTO b) {
									Double unitPrice1 = PRCommonUtil.getUnitPrice(a.getRegMPack(), a.getRegPrice(), a.getRegMPrice(), true);
									Double unitPrice2 = PRCommonUtil.getUnitPrice(b.getRegMPack(), b.getRegPrice(), b.getRegMPrice(), true);
									return unitPrice2.compareTo(unitPrice1);
								}
							});
						} else if (isBelowRelationPresent) {
							// Set the precedence sorted by current unit price in ascending order
							Collections.sort(tempRelatedItemList, new Comparator<PRItemDTO>() {
								public int compare(PRItemDTO a, PRItemDTO b) {
									Double unitPrice1 = PRCommonUtil.getUnitPrice(a.getRegMPack(), a.getRegPrice(), a.getRegMPrice(), true);
									Double unitPrice2 = PRCommonUtil.getUnitPrice(b.getRegMPack(), b.getRegPrice(), b.getRegMPrice(), true);
									return unitPrice1.compareTo(unitPrice2);
								}
							});
						}

						int precendence = 1;
						// Assign precedence
						
						for (PRItemDTO tempItemDTO : tempRelatedItemList) {
							for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
								ItemKey relatedItemKey = new ItemKey(priceGroupRelatedItemDTO.getRelatedItemCode(),
										(priceGroupRelatedItemDTO.getIsLig() ? PRConstants.LIG_ITEM_INDICATOR : PRConstants.NON_LIG_ITEM_INDICATOR));

								ItemKey sortedItemKey = PRCommonUtil.getItemKey(tempItemDTO);

								// look for the item and assign precedence
								if (sortedItemKey.equals(relatedItemKey)) {
									priceGroupRelatedItemDTO.setBrandPrecedence(precendence);
									break;
								}
							}
							precendence = precendence + 1;
						}
					}
				}
			}
		}
	}
	
}
