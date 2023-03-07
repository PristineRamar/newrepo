package com.pristine.service.offermgmt.mwr.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.ApplyStrategy;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PricePointFinder {

	ApplyStrategy applyStrategy = new ApplyStrategy();
	PricingEngineWS pricingEngineWS = new PricingEngineWS();
	ObjectiveService objectiveService = new ObjectiveService();
	LIGConstraint ligConstraint = new LIGConstraint();
	PricingEngineService pricingEngineService = new PricingEngineService();
	private int recurseCount = 0;
	private static Logger logger = Logger.getLogger("PricingEngineService");

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param retLirMap
	 * @param retLirConstraintMap
	 * @param finalLigMap
	 * @param compIdMap
	 * @param multiCompLatestPriceMap
	 * @return list of items/LIG with possible price points
	 * @throws Exception
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	public List<PRItemDTO> findPricePoints(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap)
					throws Exception, GeneralException, OfferManagementException {

		List<PRItemDTO> itemList = new ArrayList<>();

		List<PRItemDTO> nonProcessedLigList = new ArrayList<>();

		// 1. Get non related items
		HashMap<ItemKey, PRItemDTO> nonRelatedItemDataMap = getNonRelatedItems(itemDataMap);

		logger.info("findPricePoints() - # of non related items: " + nonRelatedItemDataMap.size());

		// 2. Get lead items
		HashMap<ItemKey, PRItemDTO> leadItemDataMap = getLeadItems(itemDataMap);

		logger.info("findPricePoints() - # of lead items: " + leadItemDataMap.size());

		// 3. Get related items
		HashMap<ItemKey, PRItemDTO> relatedItemDataMap = getRelatedItems(itemDataMap);

		logger.info("findPricePoints() - # of related items: " + relatedItemDataMap.size());

		logger.info("findPricePoints() - applying strategy for non related items...");
		// 4. Process non related items
		applyStrategyToFindPricePoints(recommendationInputDTO, nonRelatedItemDataMap, retLirMap, retLirConstraintMap,
				finalLigMap, compIdMap, multiCompLatestPriceMap, itemList, nonProcessedLigList, itemDataMap);

		logger.info("findPricePoints() - applying strategy for lead items...");
		// 5. Process lead items
		applyStrategyToFindPricePoints(recommendationInputDTO, leadItemDataMap, retLirMap, retLirConstraintMap,
				finalLigMap, compIdMap, multiCompLatestPriceMap, itemList, nonProcessedLigList, itemDataMap);

		logger.info("findPricePoints() - applying strategy for related items...");
		// 6. Process related items
		applyStrategyToFindPricePoints(recommendationInputDTO, relatedItemDataMap, retLirMap, retLirConstraintMap,
				finalLigMap, compIdMap, multiCompLatestPriceMap, itemList, nonProcessedLigList, itemDataMap);
		
		
		for (Map.Entry<Integer, List<PRItemDTO>> outEntry : finalLigMap.entrySet()) {
			// Set conflicts based on recommended price
			for (PRItemDTO item : outEntry.getValue()) {
				// Ignore LIG Item, as it representation any way would have
				// updated the conflicts
				if (!item.isLir()) {
					pricingEngineService.updateConflicts(item);
				}
			}
		}
		
		List<PRItemDTO> finalRecList = new LIGConstraint().applyLIGConstraint(finalLigMap, itemDataMap, retLirConstraintMap);
		
		return removeDuplicates(finalRecList);
	}
	
	private List<PRItemDTO> removeDuplicates(List<PRItemDTO> finalRecList) {
		Set<ItemKey> itemSet = new HashSet<>();
		List<PRItemDTO> items = new ArrayList<>();
		for(PRItemDTO itemDTO: finalRecList) {
			if(!itemSet.contains(PRCommonUtil.getItemKey(itemDTO))) {
				items.add(itemDTO);
				itemSet.add(PRCommonUtil.getItemKey(itemDTO));
			}
		}
		return items;
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param filteredItemDataMap
	 * @param retLirMap
	 * @param retLirConstraintMap
	 * @param finalLigMap
	 * @param compIdMap
	 * @param multiCompLatestPriceMap
	 * @param itemList
	 * @throws Exception
	 * @throws GeneralException
	 */
	public void applyStrategyToFindPricePoints(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> filteredItemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, List<PRItemDTO> itemList,
			List<PRItemDTO> nonProcessedLigList, HashMap<ItemKey, PRItemDTO> itemDataMap)
					throws Exception, GeneralException {

		logger.debug("applyStrategyToFindPricePoints() - processing LIG...");
		// Apply strategy for LIG members first and get the LIG level price points
		applyStrategyForLIG(recommendationInputDTO, filteredItemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
				compIdMap, multiCompLatestPriceMap, itemList, nonProcessedLigList, itemDataMap);

		logger.debug("applyStrategyToFindPricePoints() - LIGs Processed!");

		logger.debug("applyStrategyToFindPricePoints() - processing Non LIG...");
		// Apply strategy for Non lig items
		applyStrategyForItem(recommendationInputDTO, filteredItemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
				compIdMap, multiCompLatestPriceMap, itemList, nonProcessedLigList, itemDataMap);

		logger.debug("applyStrategyToFindPricePoints() - Non LIGs processed!");
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param retLirMap
	 * @param retLirConstraintMap
	 * @param finalLigMap
	 * @param compIdMap
	 * @param multiCompLatestPriceMap
	 * @param itemList
	 * @throws Exception
	 * @throws GeneralException
	 */
	private void applyStrategyForLIG(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> filteredItemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, List<PRItemDTO> itemList,
			List<PRItemDTO> nonProcessedLigList, HashMap<ItemKey, PRItemDTO> itemDataMap)
					throws Exception, GeneralException {

		for (Map.Entry<ItemKey, PRItemDTO> itemEntry : filteredItemDataMap.entrySet()) {
			PRItemDTO prItemDTO = itemEntry.getValue();
			if (!prItemDTO.isProcessed() && prItemDTO.isLir()) {
				List<PRItemDTO> ligMembers = getLIGMembers(prItemDTO, filteredItemDataMap, retLirMap);
				for (PRItemDTO ligMember : ligMembers) {
					applyStrategy(recommendationInputDTO, itemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
							compIdMap, multiCompLatestPriceMap, itemList, ligMember, nonProcessedLigList, false);
				}
				pricingEngineWS.setLigProcessedStatus(itemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
						itemList, nonProcessedLigList);
			}
		}
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param retLirMap
	 * @param retLirConstraintMap
	 * @param finalLigMap
	 * @param compIdMap
	 * @param multiCompLatestPriceMap
	 * @param itemList
	 * @throws Exception
	 * @throws GeneralException
	 */
	private void applyStrategyForItem(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> filteredItemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, List<PRItemDTO> itemList,
			List<PRItemDTO> nonProcessedLigList, HashMap<ItemKey, PRItemDTO> itemDataMap)
					throws Exception, GeneralException {

		for (Map.Entry<ItemKey, PRItemDTO> itemEntry : filteredItemDataMap.entrySet()) {
			PRItemDTO prItemDTO = itemEntry.getValue();
			if (!prItemDTO.isLir() && !prItemDTO.isProcessed()) {
				applyStrategy(recommendationInputDTO, itemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
						compIdMap, multiCompLatestPriceMap, itemList, prItemDTO, nonProcessedLigList, false);
			}
		}
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param retLirMap
	 * @param retLirConstraintMap
	 * @param finalLigMap
	 * @param compIdMap
	 * @param multiCompLatestPriceMap
	 * @param itemList
	 * @param prItemDTO
	 * @throws Exception
	 * @throws GeneralException
	 */
	public void applyStrategy(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, List<PRItemDTO> itemList,
			PRItemDTO prItemDTO, List<PRItemDTO> nonProcessedLigList, boolean isRecurse)
					throws Exception, GeneralException {

		String methodTag = "applyStrategy()";
		if (isRecurse) {
			methodTag = "applyStrategyRecursed()";
		}else {
			recurseCount = 0;
		}
		// Get related item list
		List<PRItemDTO> relatedItemList = getLeadItemList(prItemDTO, itemDataMap);
		// Check whether the current item has any related item
		if (relatedItemList.size() > 0) {
			logger.debug(methodTag + " - Found lead items for item: " + prItemDTO.getItemCode());

			boolean isProcessRecursed = processLeadItemsIfNotProcessed(recommendationInputDTO, itemDataMap, retLirMap, retLirConstraintMap,
					finalLigMap, compIdMap, multiCompLatestPriceMap, itemList, prItemDTO, nonProcessedLigList);

			logger.debug(methodTag + " - Applying strategy for dependant item...");
			// Apply strategy after applying price points combination for each related item
			applyStrategy.applyStrategies(prItemDTO, itemList, itemDataMap, compIdMap, /* TODO run id */0,
					retLirConstraintMap, multiCompLatestPriceMap, recommendationInputDTO.getBaseWeek(),
					/* lead zone details */null, false, /* recommendation run header */null);

			// Set recommended price by following rules
			objectiveService.applyFollowRulesAndSetRecPrice(itemList);

			logger.debug(methodTag + " - Strategy applied!");
			
			if(isProcessRecursed) {
				logger.info(methodTag + " - # of levels for item " + prItemDTO.getItemCode() + ": " + recurseCount);
			}
			
			logger.debug(methodTag + " - Finding additional price points...");
			
			// Add additional price points by considering the all the price points of lead items
			// Only when the recommendation mode is multi week prediction
			if(MultiWeekRecConfigSettings.getMwrRecommendationMode().equals(PRConstants.RECOMMEND_BY_MULTI_WEEK_PRED)) {
				addAdditionalPricePonitsByLeadItems(prItemDTO, itemDataMap, retLirMap);	
			}
		} else {

			logger.debug(methodTag + " - No lead items for item: " + prItemDTO.getItemCode());

			logger.debug(methodTag + " - Applying strategy for item...");
			// Apply strategy
			applyStrategy.applyStrategies(prItemDTO, itemList, itemDataMap, compIdMap, /* TODO run id */0,
					retLirConstraintMap, multiCompLatestPriceMap, recommendationInputDTO.getBaseWeek(),
					/* lead zone details */null, false, /* recommendation run header */null);

			// Set recommended price by following rules
			objectiveService.applyFollowRulesAndSetRecPrice(itemList);

			logger.debug(methodTag + " - Strategy applied!");
		}
	}

	private boolean processLeadItemsIfNotProcessed(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, List<PRItemDTO> itemList,
			PRItemDTO prItemDTO, List<PRItemDTO> nonProcessedLigList) throws Exception, GeneralException {
		// Get all related items
		List<PRItemDTO> relatedItemList = getLeadItemList(prItemDTO, itemDataMap);

		List<PRItemDTO> notProcessedRelatedItems = getNotProcessedRelatedItems(relatedItemList);
		boolean isProcessRecursed = false;
		
		if(notProcessedRelatedItems.size() > 0) {
			recurseCount++;
		}
		// Apply strategy for not processed related items
		for (PRItemDTO notProcessedRelatedItem : notProcessedRelatedItems) {
			isProcessRecursed = true;
			// If the related item is an lir, process all the members to find all possible price points at LIG level
			if (notProcessedRelatedItem.isLir()) {

				logger.debug("processLeadItemsIfNotProcessed() - Processing lead LIG: "
						+ notProcessedRelatedItem.getRetLirId());

				Set<Double> distinctPricePoints = new HashSet<>();

				List<PRItemDTO> ligMembers = getLIGMembers(notProcessedRelatedItem, itemDataMap, retLirMap);

				for (PRItemDTO ligMember : ligMembers) {

					logger.debug("processLeadItemsIfNotProcessed() - Processing LIG members...");

					logger.debug("processLeadItemsIfNotProcessed() - LIG Member: " + ligMember.getItemCode());
					// Recurse this method for not processed lead items
					applyStrategy(recommendationInputDTO, itemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
							compIdMap, multiCompLatestPriceMap, itemList, ligMember, nonProcessedLigList, true);

					logger.debug("processLeadItemsIfNotProcessed() - Strategy Applied for member!");
					if(ligMember.getPriceRange() != null){
						for (double price : ligMember.getPriceRange()) {
							distinctPricePoints.add(price);
						}	
					}
				}

				logger.debug("processLeadItemsIfNotProcessed() - Lead LIG processed!");

				// Set all possible price points for related lig level
				Double[] distinctPriceArr = new Double[distinctPricePoints.size()];
				int counter = 0;
				for (Double price : distinctPricePoints) {
					distinctPriceArr[counter] = price;
					counter++;
				}
				notProcessedRelatedItem.setPriceRange(distinctPriceArr);

				logger.debug("processLeadItemsIfNotProcessed() - Updating LIG processed status...");
				// Set LIG processed status
				pricingEngineWS.setLigProcessedStatus(itemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
						itemList, nonProcessedLigList);

			} else {

				logger.debug("processLeadItemsIfNotProcessed() - Processing lead item: "
						+ notProcessedRelatedItem.getItemCode());
				// Recurse this method for not processed lead items
				applyStrategy(recommendationInputDTO, itemDataMap, retLirMap, retLirConstraintMap, finalLigMap,
						compIdMap, multiCompLatestPriceMap, itemList, notProcessedRelatedItem, nonProcessedLigList,
						true);

				logger.debug("processLeadItemsIfNotProcessed() - Lead item processed!");

			}
		}
		
		return isProcessRecursed;
	}

	/**
	 * 
	 * @param relatedItemList
	 * @return not processed related items
	 */
	private List<PRItemDTO> getNotProcessedRelatedItems(List<PRItemDTO> relatedItemList) {
		logger.debug("getNotProcessedRelatedItems() - # of lead items: " + relatedItemList.size());

		List<PRItemDTO> notProcessedRelatedItems = new ArrayList<>();

		// Filter only not processed items
		relatedItemList.stream().filter(p -> !p.isProcessed()).forEach(relatedItem -> {
			notProcessedRelatedItems.add(relatedItem);
		});

		logger.debug(
				"getNotProcessedRelatedItems() - # of lead items not processed: " + notProcessedRelatedItems.size());

		return notProcessedRelatedItems;
	}

	/**
	 * 
	 * @param item
	 * @param itemDataMap
	 * @param retLirMap
	 */
	public void addAdditionalPricePonitsByLeadItems(PRItemDTO item, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		// check if there is related item for this item
		if (pricingEngineService.hasRelatedItems(item)) {
			Set<Double> possiblePricePoints = new HashSet<Double>();

			// find all possible related price points
			if (item.getPgData() != null && item.getPgData().getRelationList() != null) {

				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = item.getPgData()
						.getRelationList();

				PRStrategyDTO strategyDTO = item.getStrategyDTO();
				List<PRGuidelineSize> sizeGuidelines = null;
				double sizeShelfPCT = Constants.DEFAULT_NA;
				if (strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getSizeGuideline() != null) {
					sizeGuidelines = strategyDTO.getGuidelines().getSizeGuideline();
					if (sizeGuidelines.size() > 0)
						sizeShelfPCT = sizeGuidelines.get(0).getShelfValue();
				}

				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
					// Loop all related items
					for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
						ItemKey relatedItemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
						if (itemDataMap.get(relatedItemKey) != null) {
							// If the related item is LIG, then pick all members
							PRItemDTO prItemDTO = itemDataMap.get(relatedItemKey);
							List<PRItemDTO> ligMembersOrNonLig = new ArrayList<PRItemDTO>();
							if (relatedItemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
								ligMembersOrNonLig = getLIGMembers(prItemDTO, itemDataMap, retLirMap);
							} else {
								ligMembersOrNonLig.add(itemDataMap.get(relatedItemKey));
							}

							for (PRItemDTO itemDTO : ligMembersOrNonLig) {
								ItemKey itemKey = new ItemKey(itemDTO.getItemCode(),
										PRConstants.NON_LIG_ITEM_INDICATOR);
								PRItemDTO tempRelatedItem = itemDataMap.get(itemKey);
								Double[] tempPricePoints = itemDataMap.get(itemKey).getPriceRange();
								Double[] pricePoints = null;

								// NU:: 16th Feb 2017, when items are retained with current price,
								// those range are not covered here. Added current price also in the range

								MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(tempRelatedItem.getRegMPack(),
										tempRelatedItem.getRegPrice(), tempRelatedItem.getRegMPrice());
								double curRegUnitPrice = PRCommonUtil.getUnitPrice(curRegPrice, true);

								if (curRegUnitPrice > 0) {
									if (tempPricePoints != null) {
										Double[] curPrice = new Double[] { curRegUnitPrice };
										pricePoints = new Double[tempPricePoints.length + 1];

										System.arraycopy(tempPricePoints, 0, pricePoints, 0, tempPricePoints.length);
										System.arraycopy(curPrice, 0, pricePoints, tempPricePoints.length,
												curPrice.length);
									} else {
										pricePoints = new Double[] { curRegUnitPrice };
									}
								} else {
									pricePoints = tempPricePoints;
								}

								// logger.debug("relateItemKey " + itemKey.toString() + " price points:"
								// + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));
								// get price points of related item
								PredictionComponent predictionComponent = new PredictionComponent();
								if (tempRelatedItem.isRecProcessCompleted()) {
									if (tempRelatedItem.getRecommendedRegPrice() != null) {
										double[] priceRangeArr = predictionComponent.getNewRange(
												tempRelatedItem.getRecommendedRegPrice().multiple,
												tempRelatedItem.getRecommendedRegPrice().price, relatedItem,
												item, entry.getKey(), sizeShelfPCT, strategyDTO);
										Set<Double> priceRangeSet = new HashSet<>();
										for (Double pp : priceRangeArr) {
											priceRangeSet.add(pp);
										}

										PRRange newRange = new PRRange();
										double minPr = Collections.min(priceRangeSet);
										double maxPr = Collections.max(priceRangeSet);
										newRange.setStartVal(minPr);
										newRange.setEndVal(maxPr);

										Double[] priceRangeArrNew = predictionComponent.applyConstraints(item, newRange,
												strategyDTO);

										for (Double pp : priceRangeArrNew) {
											possiblePricePoints.add(pp);
										}
									}
								} else {
									if (pricePoints != null && pricePoints.length > 0) {
										Set<Double> priceRangeSet = new HashSet<>();
										for (Double pricePoint : pricePoints) {
											double[] priceRangeArr = predictionComponent.getNewRange(1, pricePoint, relatedItem,
													item, entry.getKey(), sizeShelfPCT, strategyDTO);
											for (Double pp : priceRangeArr) {
												priceRangeSet.add(pp);
											}
										}

										PRRange newRange = new PRRange();
										double minPr = Collections.min(priceRangeSet);
										double maxPr = Collections.max(priceRangeSet);
										newRange.setStartVal(minPr);
										newRange.setEndVal(maxPr);

										Double[] priceRangeArr = predictionComponent.applyConstraints(item, newRange, strategyDTO);

										for (Double pp : priceRangeArr) {
											possiblePricePoints.add(pp);
										}
									}
								}
							}
						}
					}
				}
			}

			Double[] priceArr = new Double[possiblePricePoints.size()];
			int counter = 0;
			for (Double price : possiblePricePoints) {
				priceArr[counter] = price;
				counter++;
			}
			item.setPriceRange(priceArr);
		}
	}

	/**
	 * 
	 * @param prItemDTO
	 * @param itemDataMap
	 * @param retLirMap
	 * @return
	 */
	private List<PRItemDTO> getLIGMembers(PRItemDTO prItemDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		List<PRItemDTO> ligMembers = new ArrayList<>();
		if (retLirMap.containsKey(prItemDTO.getRetLirId())) {
			List<PRItemDTO> members = retLirMap.get(prItemDTO.getRetLirId());
			for (PRItemDTO ligMember : members) {
				ItemKey itemKey = PRCommonUtil.getItemKey(ligMember);
				if (itemDataMap.get(itemKey) != null) {
					PRItemDTO prItemMember = itemDataMap.get(itemKey);
					ligMembers.add(prItemMember);
				}
			}
		}
		return ligMembers;
	}

	/**
	 * 
	 * @param relationMap
	 * @param itemDataMap
	 * @return related item list
	 */
	private List<PRItemDTO> getLeadItemList(PRItemDTO itemDTO, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		List<PRItemDTO> relatedItemList = new ArrayList<>();
		if (itemDTO.getPgData() != null && itemDTO.getPgData().getRelationList() != null) {
			TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationMap = itemDTO.getPgData()
					.getRelationList();
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationEntry : relationMap.entrySet()) {
				for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relationEntry.getValue()) {
					ItemKey relateItemKey = PRCommonUtil.getRelatedItemKey(priceGroupRelatedItemDTO);
					if (itemDataMap.get(relateItemKey) != null) {
						PRItemDTO relatedItem = itemDataMap.get(relateItemKey);
						relatedItemList.add(relatedItem);
					}
				}
			}
		}

		return relatedItemList;
	}

	/**
	 * 
	 * @param itemDataMap
	 * @return non related items
	 */
	public HashMap<ItemKey, PRItemDTO> getNonRelatedItems(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<ItemKey, PRItemDTO> nonRelatedItems = new HashMap<>();

		itemDataMap.forEach((key, value) -> {
			if (value.getPgData() == null) {
				nonRelatedItems.put(key, value);
			}
		});

		return nonRelatedItems;
	}

	/**
	 * 
	 * 
	 * @param itemDataMap
	 * @return lead items
	 */
	public HashMap<ItemKey, PRItemDTO> getLeadItems(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<ItemKey, PRItemDTO> leadItems = new HashMap<>();

		itemDataMap.forEach((key, value) -> {
			if (value.getPgData() != null && value.getPgData().getRelationList() != null) {
				List<PRItemDTO> itemsFiltered = getLeadItemList(value, itemDataMap);
				if (itemsFiltered.isEmpty()) {
					leadItems.put(key, value);
				}
			}
		});

		return leadItems;
	}

	/**
	 * 
	 * 
	 * @param itemDataMap
	 * @return related items
	 */
	public HashMap<ItemKey, PRItemDTO> getRelatedItems(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<ItemKey, PRItemDTO> relatedItems = new HashMap<>();

		itemDataMap.forEach((key, value) -> {
			if (value.getPgData() != null && value.getPgData().getRelationList() != null) {
				List<PRItemDTO> itemsFiltered = getLeadItemList(value, itemDataMap);
				if (itemsFiltered.size() > 0) {
					relatedItems.put(key, value);
				}
			}
		});

		return relatedItems;
	}
	
	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 * @return items classified by depedency
	 */
	public HashMap<String, HashMap<ItemKey, PRItemDTO>> getItemsClassified(HashMap<ItemKey, PRItemDTO> itemDataMap, 
			HashMap<Integer, List<PRItemDTO>> retLirMap){
		HashMap<String, HashMap<ItemKey, PRItemDTO>> itemMap = new HashMap<>();
		HashMap<ItemKey, PRItemDTO> independentItems = new HashMap<>();
		itemDataMap.forEach((key, value) -> {
			if(value.isLir()) {
				if(isAllMembersIndependent(value, itemDataMap, retLirMap)) {
					independentItems.put(key, value);
					addMembers(value, retLirMap, independentItems);
				}
			}else if(value.getRetLirId() == 0){
				if(!isDependentItem(value, itemDataMap)) {
					independentItems.put(key, value);
				}
			}
		});
		
		itemMap.put(PRConstants.INDEPENDENT_ITEMS, independentItems);
		return itemMap;
	}
	
	private boolean isDependentItem(PRItemDTO itemDTO, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		boolean isDependentItem = false;
		if (itemDTO.getPgData() != null && itemDTO.getPgData().getRelationList() != null) {
			List<PRItemDTO> itemsFiltered = getLeadItemList(itemDTO, itemDataMap);
			if (itemsFiltered.size() == 0) {
				isDependentItem = false;
			}else {
				isDependentItem = true;
			}
		}else {
			isDependentItem = true;
		}
		return isDependentItem;
	}
	
	private boolean isAllMembersIndependent(PRItemDTO lig, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		boolean isAllMembersIndependent = true;
		if(retLirMap.get(lig.getRetLirId()) != null) {
			List<PRItemDTO> members = retLirMap.get(lig.getRetLirId());
			for(PRItemDTO prItemDTO: members) {
				boolean isDependentItem = isDependentItem(prItemDTO, itemDataMap);
				if(isDependentItem) {
					isAllMembersIndependent = false;
					break;
				}
			}
		}
		return isAllMembersIndependent;
	}
	
	private void addMembers(PRItemDTO lig, HashMap<Integer, List<PRItemDTO>> retLirMap,HashMap<ItemKey, PRItemDTO> itemDataMap) {
		List<PRItemDTO> members = retLirMap.get(lig.getRetLirId());
		for(PRItemDTO member: members) {
			ItemKey itemKey = PRCommonUtil.getItemKey(member);
			itemDataMap.put(itemKey, member);
		}
	}
}
