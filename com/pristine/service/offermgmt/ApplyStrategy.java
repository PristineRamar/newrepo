package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.PRConstraintLIG;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineBrand;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRGuidelinesDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.constraint.CostConstraint;
import com.pristine.service.offermgmt.constraint.FreightChargeConstraint;
import com.pristine.service.offermgmt.constraint.GuardrailConstraint;
import com.pristine.service.offermgmt.constraint.LockedPriceConstraint;
import com.pristine.service.offermgmt.constraint.LowerHigherConstraint;
import com.pristine.service.offermgmt.constraint.MapConstraint;
import com.pristine.service.offermgmt.constraint.MinMaxConstraint;
import com.pristine.service.offermgmt.constraint.PrePriceConstraint;
import com.pristine.service.offermgmt.guideline.LeadZoneGuideline;
import com.pristine.service.offermgmt.guideline.MarginGuideline;
import com.pristine.service.offermgmt.guideline.MultiCompGuideline;
import com.pristine.service.offermgmt.guideline.PriceIndexGuideline;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

@SuppressWarnings("unused")
public class ApplyStrategy {
	private static Logger logger = Logger.getLogger("ApplyStrategy");

	private String costChangeLogic = PropertyManager.getProperty("PR_COST_CHANGE_BEHAVIOR", "");

	private String applyMapConstraint = PropertyManager.getProperty("APPLY_MAP_CONSTRAINT", "FALSE");

	public void applyStrategies(PRItemDTO item, List<PRItemDTO> prItemList, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, LocationKey> compIdMap, long runId,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, String curWeekStartDate,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails, boolean isRecommendAtStoreLevel,
			PRRecommendationRunHeader recommendationRunHeader) throws Exception {
		PRItemDTO itemInfo = item;
		PRStrategyDTO strategyDTO = itemInfo.getStrategyDTO();
		ItemKey itemKey = PRCommonUtil.getItemKey(itemInfo);
		PricingEngineService pricingEngineService = new PricingEngineService();
		// logger.debug("applyStrategies(), ItemKey:" + itemKey.toString());
		// If an item has strategy assigned to it, then apply the strategy. If
		// not add the item to final list and mark it as processed.
		if (strategyDTO != null) {

			int itemCode = itemInfo.getItemCode();
			//logger.info("Item Code - " + itemCode + "Strategy Id - " + strategyDTO.getStrategyId());
			// If an item is LIR (LIR_IND - Y) in Item_Lookup, do not proceed
			if (itemInfo.isLir()) {
				return;
			}

			// Set run id
			itemInfo.setRunId(runId);

			// Set objective type
			itemInfo.setObjectiveTypeId(strategyDTO.getObjective().getObjectiveTypeId());

			// Set strategy id
			itemInfo.setStrategyId(strategyDTO.getStrategyId());

			//logger.debug("Item Code - " + itemCode + "Strategy Id - " + itemInfo.getStrategyId());
			/*
			 * Copy Primary competitor details to item If the competitor map has a primary
			 * competitor, populate comp_str_id, competition price, comp price change
			 * indicator, comp price check date, previous competition price in item object
			 */
			pricingEngineService.setCompPriceOfItem(itemInfo, compIdMap);

			/*
			 * Keep Brand and size Guideline in a separate variable. Used when size/brand
			 * relation is take from strategy instead of price group Retrieve all brand and
			 * size guidelines in a strategy. (Used when copying relations from strategy
			 * when only the related item but not the actual relation is defined in price
			 * groups.
			 */
			//List<PRGuidelineBrand> brandGuidelines = null;
			List<PRGuidelineSize> sizeGuidelines = null;
			double sizeShelfPCT = Constants.DEFAULT_NA;
			/*if (strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getBrandGuideline() != null) {
				brandGuidelines = strategyDTO.getGuidelines().getBrandGuideline();
			}*/
			if (strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getSizeGuideline() != null) {
				sizeGuidelines = strategyDTO.getGuidelines().getSizeGuideline();
				if (sizeGuidelines.size() > 0)
					sizeShelfPCT = sizeGuidelines.get(0).getShelfValue();
			}

			/*
			 * Set LIG Constraint(Same/Diff) for each LIG Representing Item If the strategy
			 * has LIG constraint defined, populate the constraint value. If the strategy
			 * does not have LIG constraint defined, default it to Different.
			 */

			PRConstraintLIG ligConstraint = strategyDTO.getConstriants().getLigConstraint();
			if (ligConstraint != null) {
				if (retLirConstraintMap.get(itemInfo.getChildLocationId()) != null) {

					HashMap<Integer, Character> tMap = retLirConstraintMap.get(itemInfo.getChildLocationId());
					if (tMap.containsKey(itemInfo.getRetLirId())) {
						if (!tMap.get(itemInfo.getRetLirId()).equals(PRConstants.LIG_GUIDELINE_SAME)) {
							tMap.put(itemInfo.getRetLirId(), ligConstraint.getValue());
						}
					} else {
						tMap.put(itemInfo.getRetLirId(), ligConstraint.getValue());
					}

					//logger.debug("Lir id - " + itemInfo.getRetLirId() + ", Constraint - " + ligConstraint.getValue());
					retLirConstraintMap.put(itemInfo.getChildLocationId(), tMap);
				} else {
					HashMap<Integer, Character> tMap = new HashMap<Integer, Character>();
					tMap.put(itemInfo.getRetLirId(), ligConstraint.getValue());
					//logger.debug("Lir id - " + itemInfo.getRetLirId() + ", Constraint - " + ligConstraint.getValue());
					retLirConstraintMap.put(itemInfo.getChildLocationId(), tMap);
				}
			} else {
				if (retLirConstraintMap.get(itemInfo.getChildLocationId()) != null) {
					HashMap<Integer, Character> tMap = retLirConstraintMap.get(itemInfo.getChildLocationId());
					if (tMap.containsKey(itemInfo.getRetLirId())) {
						if (!tMap.get(itemInfo.getRetLirId()).equals(PRConstants.LIG_GUIDELINE_SAME)) {
							tMap.put(itemInfo.getRetLirId(), PRConstants.LIG_GUIDELINE_DIFF);
						}
					} else {
						tMap.put(itemInfo.getRetLirId(), PRConstants.LIG_GUIDELINE_DIFF);
					}
					retLirConstraintMap.put(itemInfo.getChildLocationId(), tMap);
				} else {
					HashMap<Integer, Character> tMap = new HashMap<Integer, Character>();
					tMap.put(itemInfo.getRetLirId(), PRConstants.LIG_GUIDELINE_DIFF);
					//logger.debug("Lir id - " + itemInfo.getRetLirId() + ", Constraint - " + PRConstants.LIG_GUIDELINE_DIFF);
					retLirConstraintMap.put(itemInfo.getChildLocationId(), tMap);
				}
			}
			// Explain Log
			PRExplainLog explainLog = new PRExplainLog();
			PRRange priceRange = new PRRange();

			// Apply guidelines
			PRGuidelinesDTO guidelinesDTO = strategyDTO.getGuidelines();
			TreeMap<Integer, ArrayList<Integer>> execOrderMap = guidelinesDTO.getExecOrderMap();
			HashMap<Integer, Integer> guidelineIdMap = guidelinesDTO.getGuidelineIdMap();

			if (itemInfo != null) {

				// Flag that says if further guidelines/constraints needs to be executed.
				// Mostly used when an item has a brand/size related item and related item is not processed.
				boolean isIgnoreFurtherProcessing = false;
				// Flag that says no guideline was applied. Recommended price is
				// defaulted to current price in such scenario.
				boolean noGuidelineApplied = true;

				/*
				 * Flags that say if brand/size guidelines are applied on a item. Used when
				 * applying relations based on precedence in price groups.
				 */
				boolean isBrandGuidelineApplied = false;
				boolean isSizeGuidelineApplied = false;
				FilterPriceRange filterPriceRange = new FilterPriceRange();

				//updatePriceRelationFromStrategy(itemInfo, brandGuidelines, sizeGuidelines);

				// Find the Cost Change Behavior logic
				findCostChangeBehaviorLogic(itemInfo);

				//logger.debug("Cost Change Indicator: " + itemInfo.getCostChgIndicator());
				//logger.debug("Cost Change Behavior Logic: " + itemInfo.getCostChangeBehavior());

				// Don't try to find price again, as zone level price would have been already found
				// with common store level, if this flag is set
				if (itemInfo.getIsMostCommonStorePriceRecAsZonePrice()) {
					itemDataMap.get(itemKey).setProcessed(true);
					prItemList.add(itemInfo);
					return;
				}

				// If item is marked as error and it's not considered for recommendation
				// mark item as processed and don't recommend
				if (itemInfo.getIsRecError() && !itemInfo.getErrorButRecommend()) {
					itemDataMap.get(itemKey).setProcessed(true);
					prItemList.add(itemInfo);
					return;
				}

				/** PROM-2274 changes start **/
				// Added on 17/11/2021 by Karishma for AZ
				// Check if Freight Charge is selected then add the Freight Charge to listCost
				/** Apply Freight Charge Constraint **/
				if (strategyDTO.getConstriants().getFreightChargeConstraint() != null && !itemInfo.isFreightCostSet()) {
					FreightChargeConstraint freightchargeConstraint = new FreightChargeConstraint(itemInfo, priceRange,
							explainLog);
					freightchargeConstraint.applyFreightChargeToCost();

				}
				/** PROM-2274 changes end **/

				//logger.debug("Pre-Price Status of Item: " + itemInfo.getItemCode() + "--" + itemInfo.getIsPrePriced());
				// Check if item is marked as pre-priced item
				if (itemInfo.getIsPrePriced() == 1) {
					// Apply Current Price
					PrePriceConstraint prePriceConstraint = new PrePriceConstraint(itemInfo, explainLog);
					prePriceConstraint.applyPrePriceConstraint();
					itemDataMap.get(itemKey).setProcessed(true);
					pricingEngineService.updateConflicts(itemInfo);
					prItemList.add(itemInfo);
					return;
				}

				// Apply locprice constraint
				if (strategyDTO.getConstriants().getLocPriceConstraint() != null) {

					LockedPriceConstraint locPriceConstraint = new LockedPriceConstraint(itemInfo, explainLog, costChangeLogic);
					locPriceConstraint.applyLockedPriceConstraint(applyMapConstraint);
					itemDataMap.get(itemKey).setProcessed(true);
					itemInfo.setIsLocPriced(1);
					pricingEngineService.updateConflicts(itemInfo);
					prItemList.add(itemInfo);

					return;
				}

				// If the objective is Highest Margin $ using Current Retail,
				// then recommend current retail as recommended retail
				if (itemInfo.getObjectiveTypeId() == ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL
						.getObjectiveTypeId()) {
					if (itemInfo.getRegPrice() != null) {
						itemInfo.setPriceRange(new Double[] { itemInfo.getRegPrice() });
					}
					itemDataMap.get(itemKey).setProcessed(true);
					prItemList.add(itemInfo);
					return;
				}

				ShipperItemService shipperItemService = new ShipperItemService(itemInfo, explainLog);
				// 15th July 2016, if an non-lig item or all item in a lig is
				// shipper item, then retain current price
				if (shipperItemService.isShipperItemHandled(prItemList, itemDataMap)) {
					return;
				}

				// Don't do this step if there is store recommendation, as store level movement is not taken
				if (!isRecommendAtStoreLevel) {
					// 16th Jul2 2106, if an non-lig item or all item in a lig
					// is didn't move in last x months, then retain current price
					// We will not apply guidelines and constraints to non moving items if flag is > 0
					if (MultiWeekRecConfigSettings.getHistoryWeeksForMovingItems() > 0) {
						NoMovementItemService noMovementItemService = new NoMovementItemService(itemInfo, explainLog);
						if (noMovementItemService.isNoMovementItemHandled(prItemList, itemDataMap)) {
							return;
						}
					}
				}

				// If Cost is not available pass input range as output range
				if ((itemInfo.getListCost() == null || itemInfo.getListCost() <= 0)
						&& (itemInfo.getRegPrice() != null && itemInfo.getRegPrice() > 0)) {
					PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					priceRange.setStartVal(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(itemInfo.getRegPrice())));
					priceRange.setEndVal(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(itemInfo.getRegPrice())));
					guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
					guidelineAndConstraintLog.setMessage("Cost not available");
					guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange);
					guidelineAndConstraintLog.setOutputPriceRange(priceRange);
					explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
				}

				// Give priority to Brand/Size Precedence defined in price
				// group. e.g. If S(ize) is given as precedence in price group,
				// but in strategy if Brand is given precedence, the change the
				// order (execute size first and then brand) and vice versa
				TreeMap<Integer, ArrayList<Integer>> reOrderedExecOrderMap = reOrderSizeAndBrand(execOrderMap, itemInfo,
						guidelineIdMap);

				// Iterate and apply guidelines based on execution order
				// for(ArrayList<Integer> guidelineList :
				// execOrderMap.values()){

				PRRange compRange = new PRRange();
				for (ArrayList<Integer> guidelineList : reOrderedExecOrderMap.values()) {
					for (Integer guidelineId : guidelineList) {
						// Related item is not processed
						if (isIgnoreFurtherProcessing) {
							continue;
						}

						int guidelineTypeId = guidelineIdMap.get(guidelineId);

						if (guidelineTypeId == GuidelineTypeLookup.LEAD_ZONE.getGuidelineTypeId()) {
							PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
							// Apply lead zone
							LeadZoneGuideline leadZoneGuideline = new LeadZoneGuideline(itemInfo, priceRange,
									explainLog, leadZoneDetails);
							guidelineAndConstraintOutputLocal = leadZoneGuideline.applyLeadZoneGuideline();

							priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
							if (guidelineAndConstraintOutputLocal.isGuidelineApplied) {
								noGuidelineApplied = false;
							}
						} else if (guidelineTypeId == 1) {
							PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
							// Apply margin guideline.
							MarginGuideline marginGuideline = new MarginGuideline(itemInfo, priceRange, explainLog,
									costChangeLogic);
							guidelineAndConstraintOutputLocal = marginGuideline.applyMarginGuideline();

							if (guidelineAndConstraintOutputLocal.isGuidelineApplied) {
								noGuidelineApplied = false;
							}

							priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
						} else if (guidelineTypeId == 2) {
							boolean ignorePI = false;
							// Ignore Price Index if Multi Competitor is Present
							if (itemInfo.getStrategyDTO().getGuidelines().getCompGuideline() != null
									&& itemInfo.getStrategyDTO().getGuidelines().getCompGuideline()
									.getCompetitorDetails().size() > 0) {
								ignorePI = true;
							}
							if (!ignorePI) {
								// Apply PI guideline.
								PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
								PriceIndexGuideline priceIndexGuideline = new PriceIndexGuideline(itemInfo, priceRange,	compIdMap, explainLog);
								guidelineAndConstraintOutputLocal = priceIndexGuideline.applyPriceIndexGuideline();

								priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;

								if (guidelineAndConstraintOutputLocal.isGuidelineApplied)
									compRange = guidelineAndConstraintOutputLocal.outputPriceRange;
								else
									compRange = null;

								if (guidelineAndConstraintOutputLocal.isGuidelineApplied) {
									noGuidelineApplied = false;
								}
							}
						} else if (guidelineTypeId == 3) {
							// Multi Competitor
							PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
							MultiCompGuideline multiCompGuideline = new MultiCompGuideline(itemInfo, priceRange,
									explainLog, curWeekStartDate);
							guidelineAndConstraintOutputLocal = multiCompGuideline.applyMultipleCompGuideline();

							priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;

							if (guidelineAndConstraintOutputLocal.isGuidelineApplied)
								compRange = guidelineAndConstraintOutputLocal.outputPriceRange;
							else
								compRange = null;

							if (guidelineAndConstraintOutputLocal.isGuidelineApplied) {
								noGuidelineApplied = false;
							}

						} 
						else if (guidelineTypeId == 4 || guidelineTypeId == 5) {
							// int brandRelationAppliedCount = 0;
							int totalBrandRelation = 0;
							itemInfo.setBrandRelationAppliedCount(0);
							/*
							 * Guideline Type Id - 4 (Brand guideline is present in strategy) Guideline Type
							 * Id - 5 (Size guideline is present in strategy) Guideline Type Id - 7
							 * (Brand/Size guidelines are not present in strategy. This guideline type id is
							 * manually added to the strategy when there is no brand/size guideline in the
							 * strategy. This is added to apply relations defined in price group by default
							 * (precedence - high) even when there is no brand/size guideline in strategy)
							 * -- This is not applicable now not work now
							 */
							// If an item has price group related data to it, apply price group relations
							if (itemInfo.getPgData() != null) {

								//	 logger.debug("Brand-itemcode :" + itemInfo.getItemCode() + "tier:"+ itemInfo.getBrandTierId());
								if (itemInfo.getPgData().getRelationList() != null) {

									// Get Size precedence from Price group
									char brandSizePrecedence = itemInfo.getPgData().getBrandSizePrecedence();
									if (guidelineTypeId == 7) {
										if (brandSizePrecedence == PRConstants.BRAND_SIZE_PRECECENDENCE_NA) {
											// Use default precedence from property file when brand size
											// precedence is not defined in price group
											brandSizePrecedence = PropertyManager
													.getProperty("DEFAULT_BRAND_SIZE_PRECEDENCE", "S").charAt(0);
										}
									}
									// TreeMap<B-Brand/S-Size, List of Related Item Info>
									NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = null;
									if (brandSizePrecedence == PRConstants.SIZE_RELATION)
										// If brand size precedence is S, sort relations is descending order, 
										// which will bring Size relation to top
										navigableMap = itemInfo.getPgData().getRelationList().descendingMap();
									else
										// If brand size precedence is B, do not
										// sort since the relations already
										// sorted in ascending
										navigableMap = itemInfo.getPgData().getRelationList();

									if (navigableMap.get('B') != null)
										totalBrandRelation = navigableMap.get('B').size();

									for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap
											.entrySet()) {
										// logger.debug("Guideline Type in PG relation - " + entry.getKey());

										// Don't apply brand/size if it is excluded in the Strategy
										if (entry.getKey() == PRConstants.SIZE_RELATION && guidelineTypeId == 4) {
											continue;
										} else if (entry.getKey() == PRConstants.BRAND_RELATION
												&& guidelineTypeId == 5) {
											continue;
										}

										// Sorts related items by brand precedence so that brand relation
										// with precedence 1 will be first in the list
										Collections.sort(entry.getValue());
										// Loop Related Items
										for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
											if (!isIgnoreFurtherProcessing) {
												//logger.debug("Precedence " + relatedItem.getBrandPrecedence());

												ItemKey relateItemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
												// logger.info("Related Item key : - " + relateItemKey);

												if (itemDataMap.get(relateItemKey) != null) {
													
													// Apply Price Group relations only when related item is processed and if it has a recommended price
													/*
													 * if(itemDataMap.get( relateItemKey). isProcessed() &&
													 * itemDataMap.get( relateItemKey). getRecommendedRegPrice() !=
													 * null){
													 */
													if (itemDataMap.get(relateItemKey).isProcessed()) {
														// If the related item is not recommended because of error
														if (itemDataMap.get(relateItemKey)
																.getRecommendedRegPrice() != null) {
															//logger.debug("Applying price group guideline " + itemCode);
															PRRange newRange = new PRRange();
															// relatedItem.setRelatedItemPrice(itemDataMap.get(relateItemKey).getRecommendedRegPrice());
															//															relatedItem.setRelatedItemPrice(
															//																	itemDataMap.get(relateItemKey).getRecommendedRegPrice().price);

															// 29th Dec 2016, when related item is in multiple, its not
															// taking unit price
															// while applying brand/size relation
															relatedItem.setRelatedItemPrice(itemDataMap
																	.get(relateItemKey).getRecommendedRegPrice());
															relatedItem.setListCost(
																	itemDataMap.get(relateItemKey).getListCost());

															if (relatedItem.getRelatedItemSize() == 0) {
																if (itemDataMap.get(relateItemKey) != null)
																	relatedItem.setRelatedItemSize(itemDataMap
																			.get(relateItemKey).getItemSize());
																else
																	logger.warn("No Item Size found for "
																			+ relatedItem.getRelatedItemCode());
															}

															if (relatedItem.getPriceRelation() != null && relatedItem
																	.getPriceRelation().getOperatorText() != null) {
																// Keep the input price range, it will be if there is no cost
																PRRange inputRange = new PRRange();
																inputRange.copy(priceRange);

																PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
																// Apply Price Group relation
																PRRange pgPriceRange = applyPriceGroupGuideline(
																		relatedItem, itemInfo.getPgData().getItemSize(),
																		entry.getKey(), sizeShelfPCT);

																if (isUnitCostOverride(item) && !isCompOverride(item)) {
																	// Apply Price Group relation
																	pgPriceRange = applyPriceGroupGuidelineWithUnitCostOverride(
																			relatedItem,
																			itemInfo.getPgData().getItemSize(),
																			entry.getKey(), sizeShelfPCT,
																			item.getListCost(), item, newRange);

																} else if (isCompOverride(item) || (isCompOverride(item)
																		&& isUnitCostOverride(item))) {
																	boolean isAUCEnabled = isUnitCostOverride(item);
																	item.setCompOverride(false);
																	// Apply Price Group relation

																	pgPriceRange = applyPriceGroupGuidelineWithCompOverride(
																			relatedItem,
																			itemInfo.getPgData().getItemSize(),
																			entry.getKey(), compRange, sizeShelfPCT,
																			item.getListCost(), item, isAUCEnabled,
																			newRange);

																} else if (!isCompOverride(item)
																		&& !isUnitCostOverride(item)) {
																	// Apply Price Group relation
																	pgPriceRange = applyPriceGroupGuideline(relatedItem,
																			itemInfo.getPgData().getItemSize(),
																			entry.getKey(), sizeShelfPCT);
																}

																// Adjust if there is negative range
																filterPriceRange.handleNegativeRange(pgPriceRange);
																//logger.debug("pgPriceRange: " + pgPriceRange.getStartVal() + "end:"+ pgPriceRange.getEndVal());

																// For (Ahold), In (cost changes) for (National brand), give
																// imp to margin, mark conflicts as usual and ignore other guidelines
																if (itemInfo.getCostChangeBehavior()
																		.equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)
																		&& itemInfo.getPgData()
																		.getBrandClassId() == BrandClassLookup.NATIONAL
																		.getBrandClassId()) {
																	// If this is the first guideline
																	if (priceRange.getStartVal() == Constants.DEFAULT_NA
																			&& priceRange
																			.getEndVal() == Constants.DEFAULT_NA) {
																		priceRange = pgPriceRange;
																	} else {
																		if (itemInfo.getIsMarginGuidelineApplied()) {
																			PRRange tempForConflict = filterPriceRange
																					.filterRange(priceRange,
																							pgPriceRange);
																			priceRange.setConflict(
																					tempForConflict.isConflict());
																		} else {
																			priceRange = filterPriceRange.filterRange(
																					priceRange, pgPriceRange);
																		}
																	}

																}
																// For (Ahold), In (Cost Change) for (Store Brand) in Brand
																// relation, give imp to brand use the range as next range
																else if (itemInfo.getCostChangeBehavior().equals(
																		PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)) {

																	if (entry.getKey() == PRConstants.BRAND_RELATION) {
																		// Handle multi brand relation, take relation operator
																		// of first brand Check if atleast one brand relation is applied
																		if (itemInfo
																				.getBrandRelationAppliedCount() == 0) {
																			priceRange = pgPriceRange;
																			itemInfo.setStoreBrandRelationOperator(
																					relatedItem.getPriceRelation()
																					.getOperatorText());
																		} else {
																			priceRange = filterPriceRange.filterRange(
																					priceRange, pgPriceRange);
																		}

																		itemInfo.setBrandRelationAppliedCount(
																				itemInfo.getBrandRelationAppliedCount()
																				+ 1);

																		// When there are multiple brand relation, the
																		// margin
																		// needs to be considered only to the last
																		// brand.
																		// for e.g. if there are 2 reation, apply 1st
																		// relation, then apply 2nd relation and
																		// include margin guideline here. There are
																		// no more brand relation to be applied and
																		// margin
																		// guideline is already applied
																		if (itemInfo
																				.getBrandRelationAppliedCount() == totalBrandRelation
																				&& itemInfo
																				.getIsMarginGuidelineApplied()) {
																			// Consider margin
																			PRRange tempPriceRange;
																			tempPriceRange = filterPriceRange
																					.filterRange(priceRange, itemInfo
																							.getMarginGuidelineRange());
																			// If margin not in conflict
																			if (!tempPriceRange.isConflict()) {
																				// Set max range as brand relation range
																				tempPriceRange.setEndVal(
																						priceRange.getEndVal());
																			}
																			// Set brand's conflict
																			tempPriceRange.setConflict(
																					priceRange.isConflict());
																			priceRange = tempPriceRange;
																		}

																		// Set Store Brand as applied, only when all brand relation are set
																		if (itemInfo
																				.getBrandRelationAppliedCount() == totalBrandRelation)
																			itemInfo.setIsStoreBrandRelationApplied(
																					true);
																		itemInfo.setStoreBrandRelationRange(priceRange);

																		if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(
																				itemInfo.getStoreBrandRelationOperator())
																				|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM
																				.equals(itemInfo
																						.getStoreBrandRelationOperator())
																				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM
																				.equals(itemInfo
																						.getStoreBrandRelationOperator())) {
																			itemInfo.setRoundingLogic(
																					PRConstants.ROUND_UP);
																		} else {
																			itemInfo.setRoundingLogic(
																					PRConstants.ROUND_DOWN);
																		}
																	} else {
																		// Don't apply size relation, pass input as output If
																		// this is the first guideline
																		if (priceRange
																				.getStartVal() == Constants.DEFAULT_NA
																				&& priceRange
																				.getEndVal() == Constants.DEFAULT_NA) {
																			priceRange = pgPriceRange;
																		} else {
																			// Update conflict alone and let the input price
																			// range passed as output price range
																			boolean conflict = filterPriceRange
																					.filterRange(priceRange,
																							pgPriceRange)
																					.isConflict();
																			priceRange.setConflict(conflict);
																		}
																	}
																} else {

																	if (isCompOverride(item) && compRange != null) {
																		ArrayList<PRGuidelineBrand> brandguide = item
																				.getStrategyDTO().getGuidelines()
																				.getBrandGuideline();
																		PRGuidelineBrand brandGuidlineSample = brandguide
																				.get(0);
																		int compOverrideId = brandGuidlineSample
																				.getCompOverrideId();

																		PRRange outputRange = filterCompRange(
																				item.getOperatorText(), compOverrideId,
																				pgPriceRange, priceRange);
																		priceRange = outputRange;
																	}


																	PRRange inputPriceRange = new PRRange();
																	inputPriceRange.copy(priceRange);
																	priceRange = filterPriceRange
																			.filterRange(priceRange, pgPriceRange);

																	if (!itemInfo.getCostChangeBehavior().equals(
																			PRConstants.COST_CHANGE_AHOLD_MARGIN)) {
																		filterPriceRange.updateRoundingLogic(itemInfo,
																				inputPriceRange, pgPriceRange,
																				priceRange,
																				relatedItem.getPriceRelation()
																				.getOperatorText());
																	}
																}

																if (item.isCompOverride() && compRange != null) {

																	// code to get the multicomp guidline Range

																	PRRange multiCompRange = item.getMultiCompRange();

																	if (multiCompRange != null && multiCompRange
																			.getStartVal() != Constants.DEFAULT_NA
																			&& compRange
																			.getStartVal() != Constants.DEFAULT_NA
																			&& multiCompRange
																			.getEndVal() != Constants.DEFAULT_NA
																			&& compRange
																			.getEndVal() != Constants.DEFAULT_NA) {
																		if (multiCompRange.getStartVal() != compRange
																				.getStartVal()
																				&& multiCompRange
																				.getEndVal() != compRange
																				.getEndVal()) {
																			item.setCompOverride(false);
																		}
																	}

																}

																if (isUnitCostOverride(item) && !isCompOverride(item)) {

																	if (item.isAucOverride()) {

																		item.setAucOverride(false);

																		if (priceRange
																				.getStartVal() != Constants.DEFAULT_NA
																				&& pgPriceRange
																				.getStartVal() != Constants.DEFAULT_NA)

																		{
																			if (priceRange.getStartVal() == pgPriceRange
																					.getStartVal()) {
																				item.setAucOverride(true);
																				/** PROM-2274 changes Issue with message display was identified in testing , fix is added  **/
																				item.setBrandGuidelineApplied(false);
																				/** end **/
																			}

																		}
																		if (priceRange
																				.getEndVal() != Constants.DEFAULT_NA
																				&& pgPriceRange
																				.getEndVal() != Constants.DEFAULT_NA) {
																			if (priceRange.getEndVal() == pgPriceRange
																					.getEndVal())

																				item.setAucOverride(true);
																			/** PROM-2274 changes Issue with message display was identified in testing , fix is added  **/
																			item.setBrandGuidelineApplied(false);
																			/** end **/
																		}
																	}

																	else if (!item.isAucOverride()) {

																		checkBrandGuidelineApplied(item, priceRange,
																				pgPriceRange);

																	}

																} else if (isCompOverride(item) || (isCompOverride(item)
																		&& isUnitCostOverride(item))) {
																	if (!item.isCompOverride()
																			&& !item.isAucOverride()) {
																		checkBrandGuidelineApplied(item, priceRange,
																				pgPriceRange);
																	}

																}

																if (entry.getKey() == PRConstants.BRAND_RELATION) {
																	guidelineAndConstraintLog.setGuidelineTypeId(
																			GuidelineTypeLookup.BRAND
																			.getGuidelineTypeId());
																} else {
																	guidelineAndConstraintLog
																	.setGuidelineTypeId(GuidelineTypeLookup.SIZE
																			.getGuidelineTypeId());
																}

																if (itemDataMap.get(relateItemKey).isLir()) {
																	guidelineAndConstraintLog.setIsLirInd(true);
																}
																if (entry.getKey() == PRConstants.BRAND_RELATION) {
																	guidelineAndConstraintLog.setRelationText(
																			relatedItem.getPriceRelation()
																			.formRelationText(entry.getKey()));
																} else {
																	guidelineAndConstraintLog.setRelationText(
																			itemInfo.getPgData().getSizeRelationText());
																	guidelineAndConstraintLog.setRelatedItemSize(String
																			.valueOf(relatedItem.getRelatedItemSize()));
																	guidelineAndConstraintLog.setRelatedUOMName(
																			relatedItem.getRelatedUOMName());
																}

																//logger.debug("Operator Text: " + relatedItem.getPriceRelation().getOperatorText());
																guidelineAndConstraintLog.setOperatorText(relatedItem
																		.getPriceRelation().getOperatorText().trim());
																guidelineAndConstraintLog
																.setIsConflict(priceRange.isConflict());
																guidelineAndConstraintLog
																.setIsGuidelineOrConstraintApplied(true);
																guidelineAndConstraintLog
																.setGuidelineOrConstraintPriceRange1(
																		pgPriceRange);
																guidelineAndConstraintLog.setRelationItemCode(
																		relatedItem.getRelatedItemCode());

																// If Cost is not available pass input range as output range
																if ((itemInfo.getListCost() == null
																		|| itemInfo.getListCost() <= 0)
																		&& (itemInfo.getRegPrice() != null
																		&& itemInfo.getRegPrice() > 0)) {
																	priceRange = new PRRange();
																	priceRange.copy(inputRange);
																}
																guidelineAndConstraintLog
																.setOutputPriceRange(priceRange);
																explainLog.getGuidelineAndConstraintLogs()
																.add(guidelineAndConstraintLog);

																//logger.debug("Brand/Size Guideline -- " + "Is Applied: " + guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()
																//				+ ",Input Range: " + priceRange.toString()+ ",Brand/Size Range: "
																//				+ pgPriceRange.toString() + ",Output Range: "
																//				+ priceRange.toString());

															}
															// Turn off flag since brand/size guideline is applied
															noGuidelineApplied = false;
														} else {
															// logger.debug("Related Item doesn't have recommended retail");
															// item can be marked as processed, but may not have
															// recommended in
															// few cases When the related item is // processed, and
															// doesn't have
															// recommended price mark the item as not processed, so that
															// it is
															// considered in next iteration

															// If the recommended price is not there because of error
															// flag, then
															// no point in waiting till the related item price
															// is determined, as its is never going to be recommended
															// Not marked as error or error but going to be recommend
															PRItemDTO relatedItemTemp = itemDataMap.get(relateItemKey);
															if (!relatedItemTemp.getIsRecError()
																	|| relatedItemTemp.getErrorButRecommend()) {
																isIgnoreFurtherProcessing = true;
															}
														}
													} else {
														logger.debug(relatedItem.getRelatedItemCode() + " is not processed");
														// Do not process this item further since a related item is not processed
														isIgnoreFurtherProcessing = true;

													}
												} else {
													logger.debug("Related Item Code Not retrieved in master item list");
												}
											}
										}

										if (!isIgnoreFurtherProcessing) {
											// If the price group relations are applied set flags accordingly
											if (entry.getKey() == PRConstants.BRAND_RELATION) {
												isBrandGuidelineApplied = true;
												// logger.debug("isBrandGuidelineApplied is set to true");
											}
											if (entry.getKey() == PRConstants.SIZE_RELATION) {
												isSizeGuidelineApplied = true;
												// logger.debug("isSizeGuidelineApplied is set to true");
											}
										}
									}
								} else {
									logger.info("no guideline applied");
									noGuidelineApplied = true;
								}
							}
						}
					}
				}

				/*
				 * If no guideline was applied on an item and there is no constraint, mark the
				 * item as processed, assign recommended price as current price and add item to
				 * the final list. *
				 */
				if (!isIgnoreFurtherProcessing && noGuidelineApplied
						&& itemInfo.getStrategyDTO().getConstriants() == null) {
					itemDataMap.get(itemKey).setProcessed(true);
					if (itemInfo.getRegPrice() != null)
						itemInfo.setPriceRange(new Double[] { itemInfo.getRegPrice() });
					itemInfo.setExplainLog(explainLog);
					prItemList.add(itemInfo);
					isIgnoreFurtherProcessing = true;
				}

				if (!isIgnoreFurtherProcessing) {
					// Mark an item as processed since all guidelines are applied
					itemDataMap.get(itemKey).setProcessed(true);
					PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;

					if (itemInfo.getStrategyDTO().getConstriants() != null) {
						//commented on 05/20/2022 
						//changes done to apply min max constraint after all guidelines and constarints are applied  

						/** Apply min/max Constraint **/
						/*
						 * MinMaxConstraint minMaxConstraint = new MinMaxConstraint(itemInfo,
						 * priceRange, explainLog); guidelineAndConstraintOutputLocal =
						 * minMaxConstraint.applyMinMaxConstraint(); priceRange =
						 * guidelineAndConstraintOutputLocal.outputPriceRange;
						 *//** Apply min/max Constraint **/

						/** Apply Threshold Constraint **/
						if (strategyDTO.getConstriants().getThresholdConstraint() != null) {
							guidelineAndConstraintOutputLocal = strategyDTO.getConstriants().getThresholdConstraint()
									.applyThreshold(itemInfo, priceRange, explainLog);
							priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
						}
						/** Apply Threshold Constraint **/

						/** Apply Guardrail Constraint **/
						if (strategyDTO.getConstriants().getGuardrailConstraint() != null) {
							GuardrailConstraint guardrailConstraint = new GuardrailConstraint(item, priceRange,
									explainLog, curWeekStartDate, compRange);
							guidelineAndConstraintOutputLocal = guardrailConstraint.applyGuardrailConstraint();
							priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
						}
						/** Apply Guardrail Constraint **/

						/** Apply Cost Constraint **/
						// Is there Cost Constraint
						CostConstraint costConstraint = new CostConstraint(itemInfo, priceRange, explainLog);
						guidelineAndConstraintOutputLocal = costConstraint.applyCostConstraint();
						priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
						/** Apply Cost Constraint **/

						/** Apply Lower/Higher Constraint **/
						LowerHigherConstraint lhConstraint = new LowerHigherConstraint(itemInfo, priceRange,
								explainLog);
						guidelineAndConstraintOutputLocal = lhConstraint.applyLowerHigherConstraint();
						priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
						/** Apply Lower/Higher Constraint **/

						/** Apply Freight Charge Constraint **/
						// Commenting this on 11/17/21 as there is new enhancement for AZ related to freight charge
						/*
						 * if (strategyDTO.getConstriants().getFreightChargeConstraint() != null) {
						 * FreightChargeConstraint freightchargeConstraint = new
						 * FreightChargeConstraint(itemInfo, priceRange, explainLog);
						 * guidelineAndConstraintOutputLocal =
						 * freightchargeConstraint.applyFreightChargeConstraint(); priceRange =
						 * guidelineAndConstraintOutputLocal.outputPriceRange;
						 *//** Apply Freight Charge Constraint **//*
						 * }
						 */
						/** Apply MAP Constraint always if there Is there map price **/
						// Change done for AZ by Karishma on 01/11/2021 to apply Map constraint by
						// default to ensure
						// that no price is recommended below the MAP
						/*
						 * if (applyMapConstraint.equalsIgnoreCase("TRUE")) { MapConstraint
						 * mapConstraint = new MapConstraint(itemInfo, priceRange, explainLog);
						 * guidelineAndConstraintOutputLocal = mapConstraint.applyMapConstraint();
						 * priceRange = guidelineAndConstraintOutputLocal.outputPriceRange; }
						 */

						/** Apply min/max Constraint **/
						MinMaxConstraint minMaxConstraint = new MinMaxConstraint(itemInfo, priceRange, explainLog);
						guidelineAndConstraintOutputLocal = minMaxConstraint.applyMinMaxConstraint();
						priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
						/** Apply min/max Constraint **/

						Double[] priceRangeArr = null;
						/** Find Rounding Digits **/
						priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(itemInfo,
								priceRange, explainLog);
						/** Find Rounding Digits **/

						// Added for handling AutoZone 90% threshold
						// priceRangeArr = filterPricePointsXNos(itemInfo, priceRangeArr);

						itemInfo.setPriceRange(priceRangeArr);
					}
					itemInfo.setExplainLog(explainLog);
					pricingEngineService.updateConflicts(itemInfo);
					prItemList.add(itemInfo);
				}
			}

		} else {

			itemDataMap.get(itemKey).setProcessed(true);
			prItemList.add(itemInfo);
		}
	}

	// Find what logic to be applied whenever there is cost change, For ahold, when
	// there is no relation the importance
	// is given to margin,
	// when there is store brand relation the importance is given to Store Brand
	// along with margin
	private void findCostChangeBehaviorLogic(PRItemDTO itemInfo) {
		boolean isBrandGuidelinePresent = true;
		// When a brand guideline is excluded, the store brand logic need not to work
		if (itemInfo.getStrategyDTO().getGuidelines().getBrandGuideline() == null) {
			isBrandGuidelinePresent = false;
		} else if (itemInfo.getStrategyDTO().getGuidelines().getBrandGuideline() != null) {
			if (itemInfo.getStrategyDTO().getGuidelines().getBrandGuideline().size() == 0) {
				isBrandGuidelinePresent = false;
			}
		}

		// Set to Default
		itemInfo.setCostChangeBehavior(PRConstants.COST_CHANGE_GENERIC);
		if ((costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)
				&& itemInfo.getCostChgIndicator() != PRConstants.COST_NO_CHANGE)) {
			// Mark only if it has margin guideline
			if (itemInfo.getStrategyDTO().getGuidelines().getMarginGuideline() != null)
				itemInfo.setCostChangeBehavior(PRConstants.COST_CHANGE_AHOLD_MARGIN);
			// Check if price group has any store vs national brand relation
			if (itemInfo.getPgData() != null) {
				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = null;
				navigableMap = itemInfo.getPgData().getRelationList();
				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
					if (entry.getKey() != PRConstants.SIZE_RELATION) {
						for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
							if (itemInfo.getPgData().getBrandClassId() == BrandClassLookup.STORE.getBrandClassId()
									&& isBrandGuidelinePresent && relatedItem.getPriceRelation() != null
									&& relatedItem.getPriceRelation().getOperatorText() != null) {
								itemInfo.setCostChangeBehavior(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Applies Price Group Guideline
	 * 
	 * @param recommendedRegPrice
	 * @param priceRange
	 * @param itemInfo
	 * @return
	 */
	private PRRange applyPriceGroupGuideline(PRPriceGroupRelatedItemDTO itemInfo, double itemSize, char relationType,
			double sizeShelfPCT) {
		PRRange range = itemInfo.getPriceRelation().getPriceRange(itemInfo.getRelatedItemPrice(),
				itemInfo.getRelatedItemSize(), itemSize, relationType, sizeShelfPCT);
		return range;
	}

	public TreeMap<Integer, ArrayList<Integer>> reOrderSizeAndBrand(
			TreeMap<Integer, ArrayList<Integer>> orgExecOrderMap, PRItemDTO itemInfo,
			HashMap<Integer, Integer> guidelineIdMap) {
		TreeMap<Integer, ArrayList<Integer>> reOrderedExecOrderMap = null;

		// Find the current position/index of size and brand in order map and guildeline map
		int sizePositionInMap = -1, brandPositionInMap = -1, tempMapPosition = -1;
		int sizePositionInGuideline = -1, brandPositionInGuideline = -1, tempGuidelinePosition = -1;
		int brandGuidelineId = -1, sizeGuidelineId = -1;
		for (ArrayList<Integer> guidelineList : orgExecOrderMap.values()) {
			tempGuidelinePosition = -1;
			tempMapPosition = tempMapPosition + 1;
			for (Integer guidelineId : guidelineList) {
				tempGuidelinePosition = tempGuidelinePosition + 1;
				int guidelineTypeId = guidelineIdMap.get(guidelineId);
				// Brand
				if (guidelineTypeId == 4) {
					brandPositionInMap = tempMapPosition;
					brandPositionInGuideline = tempGuidelinePosition;
					brandGuidelineId = guidelineId;
				} else if (guidelineTypeId == 5) {
					// Size
					sizePositionInMap = tempMapPosition;
					sizePositionInGuideline = tempGuidelinePosition;
					sizeGuidelineId = guidelineId;
				}
			}
		}
		// if both size and brand is present in guideline
		if (sizePositionInMap > -1 && brandPositionInMap > -1) {
			char brandSizePrecedence = PRConstants.BRAND_SIZE_PRECECENDENCE_NA;

			if (itemInfo.getPgData() != null) {
				if (itemInfo.getPgData().getRelationList() != null) {
					brandSizePrecedence = itemInfo.getPgData().getBrandSizePrecedence();
				}
			}

			// If brand/size precedence is defined in price group
			if (brandSizePrecedence != PRConstants.BRAND_SIZE_PRECECENDENCE_NA) {
				boolean isPrecedenceSameAsStrategy = false;
				// If brand/size precedence is same as in price group
				if (brandSizePrecedence == PRConstants.SIZE_RELATION) {
					// If size is already have precedence in strategy
					if ((sizePositionInMap < brandPositionInMap)) {
						isPrecedenceSameAsStrategy = true;
					} else if (brandPositionInMap == sizePositionInMap) {
						// if both give same precedence in strategy
						if (sizePositionInGuideline < brandPositionInGuideline)
							isPrecedenceSameAsStrategy = true;
					}
				} else if (brandSizePrecedence == PRConstants.BRAND_RELATION) {
					// If brand is already have precedence in strategy
					if ((brandPositionInMap < sizePositionInMap)) {
						isPrecedenceSameAsStrategy = true;
					} else if (brandPositionInMap == sizePositionInMap) {
						// if both give same precedence in strategy
						if (brandPositionInGuideline < sizePositionInGuideline)
							isPrecedenceSameAsStrategy = true;
					}
				}

				// if precedence is different from price group
				if (!isPrecedenceSameAsStrategy) {
					tempMapPosition = -1;
					// Exchange the position of size & brand, if the order is different from price group
					reOrderedExecOrderMap = new TreeMap<Integer, ArrayList<Integer>>();

					for (Entry<Integer, ArrayList<Integer>> orgGuidelineListInp : orgExecOrderMap.entrySet()) {
						ArrayList<Integer> reOrderedGuidelineList = new ArrayList<Integer>();
						tempGuidelinePosition = -1;
						tempMapPosition = tempMapPosition + 1;
						ArrayList<Integer> guidelineList = orgGuidelineListInp.getValue();
						for (Integer guidelineId : guidelineList) {
							reOrderedGuidelineList.add(guidelineId);
							tempGuidelinePosition = tempGuidelinePosition + 1;
							// Size is precedence in price group
							if (brandSizePrecedence == PRConstants.SIZE_RELATION) {
								//
								if (tempMapPosition == sizePositionInMap
										&& tempGuidelinePosition == sizePositionInGuideline)
									reOrderedGuidelineList.set(tempGuidelinePosition, brandGuidelineId);

								if (tempMapPosition == brandPositionInMap
										&& tempGuidelinePosition == brandPositionInGuideline)
									reOrderedGuidelineList.set(tempGuidelinePosition, sizeGuidelineId);

							} else if (brandSizePrecedence == PRConstants.BRAND_RELATION) {
								if (tempMapPosition == sizePositionInMap
										&& tempGuidelinePosition == sizePositionInGuideline)
									reOrderedGuidelineList.set(tempGuidelinePosition, brandGuidelineId);

								if (tempMapPosition == brandPositionInMap
										&& tempGuidelinePosition == brandPositionInGuideline)
									reOrderedGuidelineList.set(tempGuidelinePosition, sizeGuidelineId);
							}
						}
						reOrderedExecOrderMap.put(orgGuidelineListInp.getKey(), reOrderedGuidelineList);
					}
				}
			}
		}

		// If order is not changed, then pass the org treemap
		if (reOrderedExecOrderMap == null)
			reOrderedExecOrderMap = orgExecOrderMap;

		return reOrderedExecOrderMap;
	}

	/**
	 * Applies Price Group Guideline
	 * 
	 * @param recommendedRegPrice
	 * @param priceRange
	 * @param itemInfo
	 * @param newRange
	 * @return
	 */
	private PRRange applyPriceGroupGuidelineWithUnitCostOverride(PRPriceGroupRelatedItemDTO itemInfo, double itemSize,
			char relationType, double sizeShelfPCT, Double listCost, PRItemDTO itemDTO, PRRange newRange) {
		PRRange range = itemInfo.getPriceRelation().getPriceRangeUnitCost(itemInfo.getRelatedItemPrice(),
				itemInfo.getRelatedItemSize(), itemSize, relationType, sizeShelfPCT, listCost, itemInfo.getListCost(),
				itemDTO, newRange);
		return range;
	}

	private boolean isUnitCostOverride(PRItemDTO itemDTO) {
		boolean isUnitCostOverride = false;
		if (itemDTO.getStrategyDTO() != null) {
			if (itemDTO.getStrategyDTO().getGuidelines() != null) {
				if (itemDTO.getStrategyDTO().getGuidelines().getBrandGuideline() != null) {
					ArrayList<PRGuidelineBrand> brandguide = itemDTO.getStrategyDTO().getGuidelines()
							.getBrandGuideline();
					PRGuidelineBrand brandGuidlineSample = brandguide.get(0);
					if (brandGuidlineSample.isAUCOverrideEnabled()) {
						isUnitCostOverride = true;
					}
				}
			}
		}

		return isUnitCostOverride;
	}

	private boolean isCompOverride(PRItemDTO itemDTO) {
		boolean isCompOverride = false;
		if (itemDTO.getStrategyDTO() != null) {
			if (itemDTO.getStrategyDTO().getGuidelines() != null) {
				if (itemDTO.getStrategyDTO().getGuidelines().getBrandGuideline() != null) {
					ArrayList<PRGuidelineBrand> brandguide = itemDTO.getStrategyDTO().getGuidelines()
							.getBrandGuideline();
					PRGuidelineBrand brandGuidlineSample = brandguide.get(0);
					if (brandGuidlineSample.isCompOverrideEnabled()) {
						isCompOverride = true;
					}
				}
			}
		}

		return isCompOverride;
	}

	private PRRange applyPriceGroupGuidelineWithCompOverride(PRPriceGroupRelatedItemDTO itemInfo, double itemSize,
			char relationType, PRRange compRange, Double sizeShelfPCT, Double listcost, PRItemDTO item,
			boolean isAUCEnabled, PRRange newRange) {

		ArrayList<PRGuidelineBrand> brandguide = item.getStrategyDTO().getGuidelines().getBrandGuideline();
		PRGuidelineBrand brandGuidlineSample = brandguide.get(0);
		int compOverrideId = brandGuidlineSample.getCompOverrideId();

		PRRange priceRange = itemInfo.getPriceRelation().getPriceRangeWithCompOverride(itemInfo.getRelatedItemPrice(),
				itemInfo.getRelatedItemSize(), itemSize, sizeShelfPCT, relationType, compRange, listcost,
				itemInfo.getListCost(), item, compOverrideId, isAUCEnabled, newRange);

		return priceRange;
	}

	/**
	 * Added by Karishma on 05/31/2021 This function will decide the final output
	 * range after CompOverride is applied in brand guideline by comparing with comp
	 * guideline output range
	 * 
	 * @param operatorText
	 * @param compOverrideId
	 * @param pgPriceRange
	 * @param compGuidelineRange
	 * @return
	 */
	private PRRange filterCompRange(String operatorText, int compOverrideId, PRRange pgPriceRange,
			PRRange compGuidelineRange) {

		PRRange finalRange = new PRRange();

		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {

			if (compOverrideId == PRConstants.MAX_RET_HT_MIN_RET_LT
					|| compOverrideId == PRConstants.MAX_RET_HT_MAX_RET_LT) {
				if (pgPriceRange.getEndVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getEndVal() != Constants.DEFAULT_NA) {

					if (compGuidelineRange.getEndVal() >= pgPriceRange.getEndVal()) {
						finalRange.setEndVal(compGuidelineRange.getEndVal());

					} else {
						finalRange.setEndVal(pgPriceRange.getEndVal());
					}
				}
				if (pgPriceRange.getStartVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getStartVal() != Constants.DEFAULT_NA) {
					
					if (compGuidelineRange.getStartVal() >= pgPriceRange.getStartVal()) {
						finalRange.setStartVal(compGuidelineRange.getStartVal());

					} else {
						finalRange.setStartVal(pgPriceRange.getStartVal());
					}
				}
			} else if (compOverrideId == PRConstants.MIN_RET_HT_MIN_RET_LT
					|| compOverrideId == PRConstants.MIN_RET_HT_MAX_RET_LT) {

				if (pgPriceRange.getEndVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getEndVal() != Constants.DEFAULT_NA) {

					if (compGuidelineRange.getEndVal() <= pgPriceRange.getEndVal()) {
						finalRange.setEndVal(compGuidelineRange.getEndVal());
					} else {
						finalRange.setEndVal(pgPriceRange.getEndVal());
					}
				}
				if (pgPriceRange.getStartVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getStartVal() != Constants.DEFAULT_NA) {
					if (compGuidelineRange.getStartVal() < pgPriceRange.getStartVal()) {
						finalRange.setStartVal(compGuidelineRange.getStartVal());
					} else {
						finalRange.setStartVal(pgPriceRange.getStartVal());
					}
				}
			}
		}
		else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {

			if (compOverrideId == PRConstants.MAX_RET_HT_MIN_RET_LT
					|| compOverrideId == PRConstants.MIN_RET_HT_MIN_RET_LT) {
				if (pgPriceRange.getEndVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getEndVal() != Constants.DEFAULT_NA) {
					if (compGuidelineRange.getEndVal() <= pgPriceRange.getEndVal()) {
						finalRange.setEndVal(compGuidelineRange.getEndVal());
					} else {
						finalRange.setEndVal(pgPriceRange.getEndVal());
					}
				}
				if (pgPriceRange.getStartVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getStartVal() != Constants.DEFAULT_NA) {
					if (compGuidelineRange.getEndVal() <= pgPriceRange.getStartVal()) {
						finalRange.setStartVal(compGuidelineRange.getStartVal());
					} else {
						finalRange.setStartVal(pgPriceRange.getStartVal());
					}
				}
			}
			else if (compOverrideId == PRConstants.MAX_RET_HT_MAX_RET_LT
					|| compOverrideId == PRConstants.MIN_RET_HT_MAX_RET_LT) {

				if (pgPriceRange.getEndVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getEndVal() != Constants.DEFAULT_NA) {

					if (compGuidelineRange.getEndVal() >= pgPriceRange.getEndVal()) {
						finalRange.setEndVal(compGuidelineRange.getEndVal());
					} else {
						finalRange.setEndVal(pgPriceRange.getEndVal());
					}
				}
				if (pgPriceRange.getStartVal() != Constants.DEFAULT_NA
						&& compGuidelineRange.getStartVal() != Constants.DEFAULT_NA) {
					if (compGuidelineRange.getStartVal() > pgPriceRange.getStartVal()) {
						finalRange.setStartVal(compGuidelineRange.getStartVal());
					} else {
						finalRange.setStartVal(pgPriceRange.getStartVal());
					}
				}
			}
		}
		return finalRange;
	}

	public void checkBrandGuidelineApplied(PRItemDTO item, PRRange priceRange, PRRange pgPriceRange) {

		item.setBrandGuidelineApplied(false);

		if (priceRange != null && pgPriceRange != null) {

			if (priceRange.getStartVal() != Constants.DEFAULT_NA && pgPriceRange.getStartVal() != Constants.DEFAULT_NA)	{
				if (priceRange.getStartVal() == pgPriceRange.getStartVal()) {
					item.setBrandGuidelineApplied(true);
				}
			}
			if (priceRange.getEndVal() != Constants.DEFAULT_NA && pgPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				if (priceRange.getEndVal() == pgPriceRange.getEndVal()) {
					item.setBrandGuidelineApplied(true);
				}
			}
		}
	}
}
