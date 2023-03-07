package com.pristine.service.offermgmt.constraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRConstraintsDTO;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.SecondaryZoneRecDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;


public class LIGConstraint {
	private static Logger logger = Logger.getLogger("LIGConstraint");
	
	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));
	private static final boolean checkForClearanceStores = Boolean
			.parseBoolean(PropertyManager.getProperty("FETCH_CLEARANCE_STORES", "FALSE"));

	/**
	 * Applies LIG constraint
	 * @param itemPriceRangeMap		Map containing Child Location Id as key and list of items for that location as value
	 * @param itemDataMap			Map containing all items under input product
	 * @param retLirMap				Map containing RET_LIR_ID as key and map containing RET_LIR_ITEM_CODE as key and items under that group as its value
	 * @param retLirConstraintMap	Map containing Child Location Id as key and map containing RET_LIR_ID and S-Same/D-Different LIG constraint as its value 
	 * @return
	 */
	/*
	 * Child Location Id was added as part of processing for prediction from
	 * strategy definition screen at location list level.
	 */

	public List<PRItemDTO> applyLIGConstraint(HashMap<Integer, List<PRItemDTO>> itemPriceRangeMap,
			HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap) {
		List<PRItemDTO> finalItemList = new ArrayList<PRItemDTO>();
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		PricingEngineService pricingEngineService = new PricingEngineService();

		// Loop each location
		for (Map.Entry<Integer, List<PRItemDTO>> outEntry : itemPriceRangeMap.entrySet()) {
			// logger.debug("LIG input count " + outEntry.getValue().size());

			/*
			 * Map containing RET_LIR_ID as key and map containing ITEM_CODE as key and item
			 * detail as its value HashMap<RET_LIR_ID, HashMap<ITEM_CODE, ItemDetail>
			 */
			HashMap<Integer, HashMap<Integer, PRItemDTO>> retLirItemPriceMap = new HashMap<Integer, HashMap<Integer, PRItemDTO>>();
			List<PRItemDTO> nonLirItems = new ArrayList<PRItemDTO>();

			// Keep lig and its members
			// Iterate through all items under the child location id (Includes
			// both LIR and non LIR Items)
			for (PRItemDTO prItem : outEntry.getValue()) {
				int retLirId = prItem.getRetLirId();
				if (retLirId > 0 && !prItem.isLir()) {
					if (retLirItemPriceMap.get(retLirId) != null) {
						HashMap<Integer, PRItemDTO> tMap = retLirItemPriceMap.get(retLirId);
						tMap.put(prItem.getItemCode(), prItem);
						retLirItemPriceMap.put(retLirId, tMap);
					} else {
						HashMap<Integer, PRItemDTO> tMap = new HashMap<Integer, PRItemDTO>();
						tMap.put(prItem.getItemCode(), prItem);
						retLirItemPriceMap.put(retLirId, tMap);
					}
				} else {
					// Add non lir items to a separate list. This will be added
					// to final list being returned later in the method.
					nonLirItems.add(prItem);
				}
			}

			/**
			 * 1.Ignore all the shipper items and items without cost or current retail from
			 * the LIG 2.If there is an item level size or brand relation from the above
			 * filtered items, then pick the most common recommended price within these
			 * price group items. If there are more than one recommended price, then pick
			 * the recommended price with highest collective item level 13 weeks average
			 * movement 3.If there is no item level size or brand relation from the above
			 * filtered items, then pick the most common recommended price within the LIG
			 * members. If there are more than one recommended price, then pick the
			 * recommended price with highest collective item level movement. 4.Apply the
			 * final recommended price to all the LIG members
			 */
			// Loop each lig
			for (Map.Entry<Integer, HashMap<Integer, PRItemDTO>> entry : retLirItemPriceMap.entrySet()) {

				int retLirId = entry.getKey();
				int itemCode = -1;
				double price = Constants.DEFAULT_NA;
				int multiple = 1;
				Collection<PRItemDTO> commonOccrItemList = new ArrayList<PRItemDTO>();
				Collection<PRItemDTO> allItemList = entry.getValue().values();
				ItemKey ligItemKey = PRCommonUtil.getItemKey(entry.getKey(), true);

				// 26 Oct 2017 Dinesh:: Changed logic to avoid Shipper item while considering
				// Max occurrence Price within LIG
				// Member item
				// Give preference to Non Shipper items. In an LIG only shipper items were
				// present, then get Max occurrence price
				// with exiting logic

				// If all the items in an LIG is shipper items
				PRItemDTO maxOccurenceObject = new PRItemDTO();

				HashMap<Integer, PRItemDTO> ligMembers = new HashMap<Integer, PRItemDTO>();

				// Filter Shipper items and item without cost or current retail.

				if (ligMembers.size() == 0) {
					for (Map.Entry<Integer, PRItemDTO> prItemDTOMap : entry.getValue().entrySet()) {
						PRItemDTO prItemDTO = prItemDTOMap.getValue();
						if (!prItemDTO.isShipperItem() && prItemDTO.hasValidCost()
								&& prItemDTO.hasValidCurrentRetail()) {
							// Filter items with locked strategy.
							// Added by Karishma on 02/22/2022
							if (prItemDTO.getStrategyDTO().getConstriants() != null
									&& prItemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
								ligMembers.put(prItemDTOMap.getKey(), prItemDTO);
							}
						}
					}
				}

				// If no filtered items, then filter items which has no cost or current retail
				if (ligMembers.size() == 0) {
					for (Map.Entry<Integer, PRItemDTO> prItemDTOMap : entry.getValue().entrySet()) {
						PRItemDTO prItemDTO = prItemDTOMap.getValue();
						if (prItemDTO.hasValidCost() && prItemDTO.hasValidCurrentRetail()) {
							ligMembers.put(prItemDTOMap.getKey(), prItemDTO);
						}
					}
				}

				// If none of the items were available after filtering, consider all items
				if (ligMembers.size() == 0) {
					ligMembers = entry.getValue();
				}

				PRItemDTO ligItemSample = itemDataMap.get(ligItemKey);
				/**/
				PRStrategyDTO strategyDTO = ligItemSample.getStrategyDTO();
				if (strategyDTO.getConstriants() != null) {
					PRConstraintsDTO prConstraints = strategyDTO.getConstriants();
					if (prConstraints.getLigConstraint() != null
							&& prConstraints.getLigConstraint().getLigConstraintText() != null) {
						if (prConstraints.getLigConstraint().getLigConstraintText().trim()
								.equalsIgnoreCase(PRConstants.LIG_MIN_PRICE)) {
							maxOccurenceObject = getRepItemByMinOrMaxPrice(ligMembers, entry.getKey(),
									mostOccurrenceData, pricingEngineService, commonOccrItemList, itemDataMap, "min");

							//logger.debug("***Using Min Logic");
						} else if (prConstraints.getLigConstraint().getLigConstraintText().trim()
								.equalsIgnoreCase(PRConstants.LIG_MAX_PRICE)) {

							maxOccurenceObject = getRepItemByMinOrMaxPrice(ligMembers, entry.getKey(),
									mostOccurrenceData, pricingEngineService, commonOccrItemList, itemDataMap, "max");
							//logger.debug("***Using Max Logic");
						} else if (prConstraints.getLigConstraint().getLigConstraintText().trim()
								.equalsIgnoreCase(PRConstants.LIG_MODE_PRICE)) {

							maxOccurenceObject = getRepItemByModePrice(ligMembers, entry.getKey(), mostOccurrenceData,
									pricingEngineService, commonOccrItemList, itemDataMap);

							//logger.debug("***Using Mode Logic");
						} else if (prConstraints.getLigConstraint().getLigConstraintText().trim()
								.equalsIgnoreCase(PRConstants.LIG_HIGH_MOVER_PRICE)) {

							maxOccurenceObject = getRepItemUsingHighMover(ligMembers, entry.getKey(),
									mostOccurrenceData, pricingEngineService, commonOccrItemList, itemDataMap);
							//logger.debug("***Using high mover Logic");
						} else {

							
							maxOccurenceObject = getRepItemUsingHighMover(ligMembers, entry.getKey(),
									mostOccurrenceData, pricingEngineService, commonOccrItemList, itemDataMap);
						}
					} else {
						
						
						
						maxOccurenceObject = getRepItemUsingHighMover(ligMembers, entry.getKey(), mostOccurrenceData,
								pricingEngineService, commonOccrItemList, itemDataMap);
					}
				} else {


					maxOccurenceObject = getRepItemUsingHighMover(ligMembers, entry.getKey(), mostOccurrenceData,
							pricingEngineService, commonOccrItemList, itemDataMap);
				}

				itemCode = maxOccurenceObject.getItemCode();
				price = maxOccurenceObject.getRegPrice();
				multiple = maxOccurenceObject.getRegMPack();
				// logger.debug("Item whose property is going to represent at
				// item level: " + itemCode);

				PRItemDTO chosenItemDTO = null;
				PRItemDTO ligItem = null;
				// Item with most common occurring price
				chosenItemDTO = entry.getValue().get(itemCode);
//				logger.debug("Chosen Item DTO: " + chosenItemDTO);
				// DTO that stores LIR representing item information
				// Get from itemmap
				ligItem = itemDataMap.get(ligItemKey);
				// logger.debug("Lig Item DTO: " + ligItem);
				ligItem.copy(chosenItemDTO);
				boolean futurepricePresent=false;
				boolean ispromoStartsWithinXWeeks=false;
				
				for (PRItemDTO item : allItemList) {
					if (item.isFuturePricePresent()) {
						futurepricePresent=true;
						break;
					}
				}
				// set promostarts withing x weeks of recc week for lig rep row even if one
				// member has promotionstarting
				// withinx weeks
				for (PRItemDTO item : allItemList) {
					if (item.isPromoStartsWithinXWeeks()) {
						ispromoStartsWithinXWeeks = true;
						break;
					}
				}
				//commented as its not requied to copy the price points to all the LIG members 
				//boolean ligSameConstraintPresent = false;

				// 3rd March 2015, don't apply lig constraint even if any one of
				// the item is pre-priced item (assumed here
				// if one item is pre-price all items will be pre-price
				if (price != Constants.DEFAULT_NA && retLirConstraintMap.get(chosenItemDTO.getChildLocationId()) != null
						&& retLirConstraintMap.get(chosenItemDTO.getChildLocationId()).get(retLirId) != null
						&& PRConstants.LIG_GUIDELINE_SAME == retLirConstraintMap.get(chosenItemDTO.getChildLocationId())
								.get(retLirId)
						&& chosenItemDTO.getIsPrePriced() == 0) {
					//commented as its not requied to copy the price points to all the LIG members 
					//ligSameConstraintPresent = true;
					for (PRItemDTO itemDTO : allItemList) {
						int tItemCode = itemDTO.getItemCode();
						// Apply LIG constraint only when  final
						// Objective is set
						//Condition added on 05/11 for not applying LIG same rule to memebrs if they have pending retail
						if (itemDTO.isFinalObjectiveApplied() && itemDTO.getPendingRetail()==null && 
								!futurepricePresent && !ispromoStartsWithinXWeeks) {
							if (entry.getValue().get(tItemCode) != null) {
								PRItemDTO tDTO = entry.getValue().get(tItemCode);

								PRGuidelineAndConstraintLog guidelineAndConstraintLog = null;
								// Check if lig log is already present
								for (PRGuidelineAndConstraintLog gAndCLog : tDTO.getExplainLog()
										.getGuidelineAndConstraintLogs()) {
									if (gAndCLog.getConstraintTypeId() == ConstraintTypeLookup.LIG
											.getConstraintTypeId()) {
										guidelineAndConstraintLog = gAndCLog;
									}
								}

								if (guidelineAndConstraintLog == null) {
									guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
									guidelineAndConstraintLog
											.setConstraintTypeId(ConstraintTypeLookup.LIG.getConstraintTypeId());

									// Added by Karishma
									// 22 Feb, 2022 don't apply LIGConstraint, if any one of member is on lock
									if (tDTO.getRecommendedRegPrice() != null
											&& price != tDTO.getRecommendedRegPrice().price
											&& tDTO.getStrategyDTO().getConstriants() != null
											&& tDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
										// Added on 02/22/22
										// Condition to check if the rep item's price is getting applied to members
										// then it should not go below map
										if (price >= tDTO.getMapRetail()) {
											guidelineAndConstraintLog
													.setRecommendedRegMultiple(tDTO.getRecommendedRegPrice().multiple);
											guidelineAndConstraintLog
													.setRecommendedRegPrice(tDTO.getRecommendedRegPrice().price);
												guidelineAndConstraintLog.setIsSameForAllLigMember(true);
											tDTO.getExplainLog().getGuidelineAndConstraintLogs()
													.add(guidelineAndConstraintLog);
										}

									} else if (tDTO.getRecommendedRegPrice() == null) {
										guidelineAndConstraintLog.setIsSameForAllLigMember(true);
										
										
										tDTO.getExplainLog().getGuidelineAndConstraintLogs()
												.add(guidelineAndConstraintLog);
									}
								}

								MultiplePrice mp = new MultiplePrice(multiple, price);

								if (tDTO.getStrategyDTO().getConstriants() != null
										&& tDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
									if (price >= tDTO.getMapRetail()) {
										tDTO.setRecommendedRegPrice(mp);
										tDTO.setIsLocPriced(chosenItemDTO.getIsLocPriced());
										tDTO.setIsPrePriced(chosenItemDTO.getIsPrePriced());
									}
								}

								// Set the system Overridden details if present to all the members.
								// Added by Karishma on 06/29 to solve the issue with systemOverride for LIG
								// members
								tDTO.setSystemOverrideFlag(chosenItemDTO.isSystemOverrideFlag());
								tDTO.setOverrideRegPrice(chosenItemDTO.getOverrideRegPrice());
								tDTO.setOverrideRegMultiple(chosenItemDTO.getOverrideRegMultiple());

								if (tDTO.getStrategyId() <= 0) {
									tDTO.setStrategyId(chosenItemDTO.getStrategyId());
								}
								finalItemList.add(tDTO);
							}
						} else {
							if (entry.getValue().get(tItemCode) != null) {
								PRItemDTO tDTO = entry.getValue().get(tItemCode);
								finalItemList.add(tDTO);
							}
						}
					}

				} else {

					for (PRItemDTO itemDTO : allItemList) {
						int tItemCode = itemDTO.getItemCode();
						if (entry.getValue().get(tItemCode) != null) {
							PRItemDTO tDTO = entry.getValue().get(tItemCode);
							finalItemList.add(tDTO);
						}
					}
				}

			
					updateAttributesFromRepItem(ligItem, commonOccrItemList, allItemList, mostOccurrenceData,
							chosenItemDTO);
				

				if (ligItem != null) {
					// Add LIR representing item to the final list. This
					// will now carry the attributes of item which was
					// choosen with most common recommended price
					finalItemList.add(ligItem);
					// Mark the LIR representing item as processed.
					/*
					 * This is being used when applying Price Group relations. If an item(A) is
					 * related to another item(B) by brand/size and if the related item(B) is a LIR,
					 * Item A will be processed only when Item B is processed and has a recommended
					 * price.
					 */
					ItemKey itemKey = PRCommonUtil.getItemKey(ligItem);
					if (itemDataMap.get(itemKey) != null) {
						itemDataMap.get(itemKey).setProcessed(true);
						itemDataMap.get(itemKey).setRecommendedRegPrice(ligItem.getRecommendedRegPrice());
					}

				}
			}

			// Add all nor lir items to final list
			finalItemList.addAll(nonLirItems);
//			logger.debug("finalItemList size:" + finalItemList.size());
		}
		return finalItemList;
	}

	private void updatePromotedRealtedInfo(PRItemDTO ligItem, Collection<PRItemDTO> allItemList,
			MostOccurrenceData mostOccurrenceData) {

		// Cur sale details
//		ligItem.getCurSaleInfo().setSalePrice((MultiplePrice) mostOccurrenceData.getMaxOccurance(allItemList, "CurSaleMultiplePrice"));
//		if((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "CurSalePromoTypeId") != null) {
//			ligItem.getCurSaleInfo().setSalePromoTypeLookup(PromoTypeLookup.get((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "CurSalePromoTypeId")));
//		} 
//		ligItem.getCurSaleInfo().setSaleStartDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "CurSaleStartDate"));
//		ligItem.getCurSaleInfo().setSaleEndDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "CurSaleEndDate"));
//		ligItem.getCurSaleInfo().setSaleCost((Double) mostOccurrenceData.getMaxOccurance(allItemList, "CurSaleCost"));

		// Rec week sale details
		ligItem.getRecWeekSaleInfo().setSalePrice(
				(MultiplePrice) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSaleMultiplePrice"));
		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSalePromoTypeId") != null) {
			ligItem.getRecWeekSaleInfo().setPromoTypeId(
					(Integer) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSalePromoTypeId"));
		}
		ligItem.getRecWeekSaleInfo()
				.setSaleStartDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSaleStartDate"));
		ligItem.getRecWeekSaleInfo().setSaleWeekStartDate(
				(String) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSaleWeekStartDate"));
		ligItem.getRecWeekSaleInfo()
				.setSaleEndDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSaleEndDate"));

		// Fut week sale details
		ligItem.getFutWeekSaleInfo().setSalePrice(
				(MultiplePrice) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSaleMultiplePrice"));
		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSalePromoTypeId") != null) {
			ligItem.getFutWeekSaleInfo().setPromoTypeId(
					(Integer) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSalePromoTypeId"));
		}
		ligItem.getFutWeekSaleInfo()
				.setSaleStartDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSaleStartDate"));
		ligItem.getFutWeekSaleInfo().setSaleWeekStartDate(
				(String) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSaleWeekStartDate"));
		ligItem.getFutWeekSaleInfo()
				.setSaleEndDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSaleEndDate"));
//		ligItem.getFutWeekSaleInfo().setSaleCost((Double) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekSaleCost"));

		// Cur ad
//		ligItem.getCurAdInfo().setWeeklyAdStartDate((String)mostOccurrenceData.getMaxOccurance(allItemList, "CurAdStartDate"));
//		ligItem.getCurAdInfo().setAdPageNo((int)mostOccurrenceData.getMaxOccurance(allItemList, "CurAdPageNo"));
//		ligItem.getCurAdInfo().setAdBlockNo((int)mostOccurrenceData.getMaxOccurance(allItemList, "CurAdBlockNo"));

		// Rec week ad
		ligItem.getRecWeekAdInfo()
				.setWeeklyAdStartDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekAdStartDate"));

		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekAdPageNo") != null) {
			ligItem.getRecWeekAdInfo()
					.setAdPageNo((int) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekAdPageNo"));
		}

		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekAdBlockNo") != null) {
			ligItem.getRecWeekAdInfo()
					.setAdBlockNo((int) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekAdBlockNo"));
		}

		// Fut week ad
		ligItem.getFutWeekAdInfo()
				.setWeeklyAdStartDate((String) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekAdStartDate"));

		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekAdPageNo") != null) {
			ligItem.getFutWeekAdInfo()
					.setAdPageNo((int) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekAdPageNo"));
		}

		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekAdBlockNo") != null) {
			ligItem.getFutWeekAdInfo()
					.setAdBlockNo((int) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekAdBlockNo"));
		}

		// Cur display
//		ligItem.getCurDisplayInfo().setDisplayWeekStartDate((String)mostOccurrenceData.getMaxOccurance(allItemList, "CurDisplayStartDate"));
//		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "CurDisplayTypeId") != null) {
//			ligItem.getCurDisplayInfo()
//					.setDisplayTypeLookup(DisplayTypeLookup.get((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "CurDisplayTypeId")));
//		}
		// Rec week display
		ligItem.getRecWeekDisplayInfo().setDisplayWeekStartDate(
				(String) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekDisplayStartDate"));
		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekDisplayTypeId") != null) {
			ligItem.getRecWeekDisplayInfo().setDisplayTypeLookup(DisplayTypeLookup
					.get((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekDisplayTypeId")));
		}

		// Fut week display
		ligItem.getFutWeekDisplayInfo().setDisplayWeekStartDate(
				(String) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekDisplayStartDate"));
		if ((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekDisplayTypeId") != null) {
			ligItem.getFutWeekDisplayInfo().setDisplayTypeLookup(DisplayTypeLookup
					.get((Integer) mostOccurrenceData.getMaxOccurance(allItemList, "FutWeekDisplayTypeId")));
		}

		ligItem.setRecWeekSaleCost((Double) mostOccurrenceData.getMaxOccurance(allItemList, "RecWeekSaleCost"));

		// if any one item is tpr, then mark lig as tpr
		ligItem.setIsTPR(isAnyOneTrue(allItemList, "TPR"));

		// if any one item is sale, then mark lig as sale
		ligItem.setIsOnSale(isAnyOneTrue(allItemList, "SALE"));

		// if any one item is ad, then mark lig as ad
		ligItem.setIsOnAd(isAnyOneTrue(allItemList, "AD"));

		// if any one item is in on going promotion, then mark it as true
		ligItem.setOnGoingPromotion(getPromotionStatus(allItemList, "OnGoingPromo"));

		// If anyone item is in Future promotion, then mark it as true
		ligItem.setFuturePromotion(getPromotionStatus(allItemList, "FuturePromo"));

		// If anyone item promotion is not ends within X weeks, then mark it as False
		ligItem.setPromoEndsWithinXWeeks(getPromotionStatus(allItemList, "PromoEndsInXWeeks"));

		ligItem = sumRecWeekSalePredAtCurRegForLIG(ligItem, allItemList);
		ligItem = sumRecWeekSalePredAtRecRegForLIG(ligItem, allItemList);

		ligItem = updateRecWeekSalePredStatusAtCurReg(ligItem, allItemList);
		ligItem = updateRecWeekSalePredStatusAtRecReg(ligItem, allItemList);
					
		//if any one of lig  memeber's promotion is starting within x weeks from recc week
		ligItem.setPromoStartsWithinXWeeks(getFlagStatusForLig(allItemList,"PromoStartsWithinXWeeks"));
		
	}

	public int isAnyOneTrue(Collection<PRItemDTO> allItemList, String type) {
		Boolean isAnyOneTrue = false;
		int output = 0;
		// Update conflict if any of the item in conflict
		for (PRItemDTO item : allItemList) {
			int propertyValue = 0;
			if (type.equals("TPR")) {
				propertyValue = item.getIsTPR();
			} else if (type.equals("SALE")) {
				propertyValue = item.getIsOnSale();
			} else if (type.equals("AD")) {
				propertyValue = item.getIsOnAd();
			}
			if (propertyValue == 1) {
				isAnyOneTrue = true;
				break;
			}
		}
		output = isAnyOneTrue ? 1 : 0;

		return output;
	}

	public PRItemDTO updateSamePriceAcrossStore(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Boolean isAnyOneItemHasDiffStorePrice = false;
		for (PRItemDTO item : allItemList) {
			// item's prices are different in stores
			if (!item.getIsCurRetailSameAcrossStores()) {
				isAnyOneItemHasDiffStorePrice = true;
				break;
			}
		}
		if (isAnyOneItemHasDiffStorePrice) {
			retLirItemCodeDTO.setIsCurRetailSameAcrossStores(false);
		} else {
			retLirItemCodeDTO.setIsCurRetailSameAcrossStores(true);
		}

		return retLirItemCodeDTO;
	}

	public PRItemDTO updateErrorCode(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Boolean isAnyOneItemIsInError = false;
		Set<Integer> distinctErrorCodes = new HashSet<Integer>();
		for (PRItemDTO item : allItemList) {
			if (item.getIsRecError()) {
				isAnyOneItemIsInError = true;
				break;
			}
		}

		if (isAnyOneItemIsInError) {
			List<Integer> errorCodes = new ArrayList<Integer>();
			for (PRItemDTO item : allItemList) {
				for (Integer errorCode : item.getRecErrorCodes()) {
					distinctErrorCodes.add(errorCode);
				}
			}
			for (Integer errorCode : distinctErrorCodes) {
				errorCodes.add(errorCode);
			}
			retLirItemCodeDTO.setIsRecError(true);
			retLirItemCodeDTO.setRecErrorCodes(errorCodes);
		} else {
			retLirItemCodeDTO.setIsRecError(false);
			retLirItemCodeDTO.setRecErrorCodes(new ArrayList<Integer>());
		}

		return retLirItemCodeDTO;
	}

	public PRItemDTO updateLIGConflict(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Boolean isAnyOneItemInConflict = false;
		// Update conflict if any of the item in conflict
		for (PRItemDTO item : allItemList) {
			if (item.getIsConflict() == 1) {
				isAnyOneItemInConflict = true;
				break;
			}
		}
		if (isAnyOneItemInConflict) {
			retLirItemCodeDTO.setIsConflict(1);
		} else {
			retLirItemCodeDTO.setIsConflict(0);
		}

		return retLirItemCodeDTO;
	}

	public PRItemDTO updateMarkForReview(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Boolean isAnyOneItemIsMarkedForReview = false;
		// Update Mark for review if any of the item in conflict
		for (PRItemDTO item : allItemList) {
			if (item.getIsMarkedForReview().equals(String.valueOf(Constants.YES))) {
				isAnyOneItemIsMarkedForReview = true;
				break;
			}
		}
		if (isAnyOneItemIsMarkedForReview) {
			retLirItemCodeDTO.setIsMarkedForReview(String.valueOf(Constants.YES));
		} else {
			retLirItemCodeDTO.setIsMarkedForReview(String.valueOf(Constants.NO));
		}

		return retLirItemCodeDTO;
	}

	public PRItemDTO updatePredictionStatus(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		retLirItemCodeDTO.setPredictionStatus(findPredictionStatus(allItemList, "RECOMMENDED_RETAIL"));
		return retLirItemCodeDTO;
	}

	public PRItemDTO updateOverridePredictionStatus(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		retLirItemCodeDTO.setOverridePredictionStatus(findPredictionStatus(allItemList, "OVERRIDE_RETAIL"));
		return retLirItemCodeDTO;
	}

	public PRItemDTO updateRecWeekSalePredStatusAtCurReg(PRItemDTO retLirItemCodeDTO,
			Collection<PRItemDTO> allItemList) {
		if (findPredictionStatus(allItemList, "REC_WEEK_SALE_CUR_REG") != null) {
			retLirItemCodeDTO.getRecWeekSaleInfo().setSalePredStatusAtCurReg(
					PredictionStatus.get(findPredictionStatus(allItemList, "REC_WEEK_SALE_CUR_REG")));
		}
		return retLirItemCodeDTO;
	}

	public PRItemDTO updateRecWeekSalePredStatusAtRecReg(PRItemDTO retLirItemCodeDTO,
			Collection<PRItemDTO> allItemList) {
		if (findPredictionStatus(allItemList, "REC_WEEK_SALE_REC_REG") != null) {
			retLirItemCodeDTO.getRecWeekSaleInfo().setSalePredStatusAtRecReg(
					PredictionStatus.get(findPredictionStatus(allItemList, "REC_WEEK_SALE_REC_REG")));
		}
		return retLirItemCodeDTO;
	}

	private Integer findPredictionStatus(Collection<PRItemDTO> allItemList, String operType) {
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		Boolean isPredictionStatusIsErrorForAllItems = null;
		Integer ligPredictionStatus = null;
		for (PRItemDTO item : allItemList) {
			Integer predStatus = null;
			if (operType.equals("RECOMMENDED_RETAIL")) {
				predStatus = item.getPredictionStatus();
			} else if (operType.equals("OVERRIDE_RETAIL")) {
				predStatus = item.getOverridePredictionStatus();
			} else if (operType.equals("REC_WEEK_SALE_CUR_REG")) {
				predStatus = item.getRecWeekSaleInfo().getSalePredStatusAtCurReg() != null
						? item.getRecWeekSaleInfo().getSalePredStatusAtCurReg().getStatusCode()
						: null;
			} else if (operType.equals("REC_WEEK_SALE_REC_REG")) {
				predStatus = item.getRecWeekSaleInfo().getSalePredStatusAtRecReg() != null
						? item.getRecWeekSaleInfo().getSalePredStatusAtRecReg().getStatusCode()
						: null;
			}

			if (predStatus != null && predStatus == PredictionStatus.SUCCESS.getStatusCode()) {
				isPredictionStatusIsErrorForAllItems = false;
			} else {
				// if already marked as success don't mark as failure
				if (isPredictionStatusIsErrorForAllItems == null || isPredictionStatusIsErrorForAllItems)
					isPredictionStatusIsErrorForAllItems = true;
			}
		}
		// If all lig member prediction status is error
		if (isPredictionStatusIsErrorForAllItems != null && isPredictionStatusIsErrorForAllItems) {
			// pick the common error status
			Object mostCommonPredictionStatus = null;
			if (operType.equals("RECOMMENDED_RETAIL")) {
				mostCommonPredictionStatus = mostOccurrenceData.getMaxOccurance(allItemList, "PredictionStatus");
			} else if (operType.equals("OVERRIDE_RETAIL")) {
				mostCommonPredictionStatus = mostOccurrenceData.getMaxOccurance(allItemList,
						"OverridePredictionStatus");
			} else if (operType.equals("CUR_SALE")) {
				mostCommonPredictionStatus = mostOccurrenceData.getMaxOccurance(allItemList, "CurSalePredStatus");
			} else if (operType.equals("REC_WEEK_SALE_CUR_REG")) {
				mostCommonPredictionStatus = mostOccurrenceData.getMaxOccurance(allItemList,
						"RecWeekSalePredStatusAtCurReg");
			} else if (operType.equals("REC_WEEK_SALE_REC_REG")) {
				mostCommonPredictionStatus = mostOccurrenceData.getMaxOccurance(allItemList,
						"RecWeekSalePredStatusAtRecReg");
			}

			if (mostCommonPredictionStatus != null)
				ligPredictionStatus = (int) mostCommonPredictionStatus;
		} else {
			ligPredictionStatus = PredictionStatus.SUCCESS.getStatusCode();
		}
		return ligPredictionStatus;
	}

	public PRItemDTO sumCurRetailMarginDollarForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double curRegMarginDollar = sumMarginDollar(retLirItemCodeDTO, allItemList, "CURRENT_RETAIL");
		retLirItemCodeDTO.setCurRetailMarginDollar(curRegMarginDollar);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumCurRetailSalesDollarForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double curRegSalesDollar = sumSalesDollar(retLirItemCodeDTO, allItemList, "CURRENT_RETAIL");
		retLirItemCodeDTO.setCurRetailSalesDollar(curRegSalesDollar);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumRecRetailMarginDollarForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double recRegMarginDollar = sumMarginDollar(retLirItemCodeDTO, allItemList, "RECOMMENDED_RETAIL");
		retLirItemCodeDTO.setRecRetailMarginDollar(recRegMarginDollar);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumRecRetailSalesDollarForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double recRegSalesDollar = sumSalesDollar(retLirItemCodeDTO, allItemList, "RECOMMENDED_RETAIL");
		retLirItemCodeDTO.setRecRetailSalesDollar(recRegSalesDollar);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumOverrideRetailMarginDollarForLIG(PRItemDTO retLirItemCodeDTO,
			Collection<PRItemDTO> allItemList) {
		double overrideMarginDollar = sumMarginDollar(retLirItemCodeDTO, allItemList, "OVERRIDE_RETAIL");
		retLirItemCodeDTO.setOverrideRetailMarginDollar(overrideMarginDollar);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumOverrideRetailSalesDollarForLIG(PRItemDTO retLirItemCodeDTO,
			Collection<PRItemDTO> allItemList) {
		double overrideSalesDollar = sumSalesDollar(retLirItemCodeDTO, allItemList, "OVERRIDE_RETAIL");
		retLirItemCodeDTO.setOverrideRetailSalesDollar(overrideSalesDollar);
		return retLirItemCodeDTO;
	}

	private double sumSalesDollar(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList, String operType) {
		double salesDollar = 0;
		for (PRItemDTO item : allItemList) {
			MultiplePrice curRegMultiplePrice = null;
			curRegMultiplePrice = PRCommonUtil.getMultiplePrice(item.getRegMPack(), item.getRegPrice(),
					item.getRegMPrice());
			if (PRCommonUtil.canConsiderItemForCalculation(curRegMultiplePrice, item.getCost(),
					item.getPredictionStatus())) {
				if (operType.equals("RECOMMENDED_RETAIL"))
					salesDollar = salesDollar + item.getRecRetailSalesDollar();
				else if (operType.equals("CURRENT_RETAIL"))
					salesDollar = salesDollar + item.getCurRetailSalesDollar();
				else if (operType.equals("OVERRIDE_RETAIL"))
					salesDollar = salesDollar + item.getOverrideRetailSalesDollar();
			}
		}
		return salesDollar;
	}

	private double sumMarginDollar(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList, String operType) {
		double marginDollar = 0;
		for (PRItemDTO item : allItemList) {
			MultiplePrice curRegMultiplePrice = null;
			curRegMultiplePrice = PRCommonUtil.getMultiplePrice(item.getRegMPack(), item.getRegPrice(),
					item.getRegMPrice());
			if (PRCommonUtil.canConsiderItemForCalculation(curRegMultiplePrice, item.getCost(),
					item.getPredictionStatus())) {
				if (operType.equals("RECOMMENDED_RETAIL"))
					marginDollar = marginDollar + item.getRecRetailMarginDollar();
				else if (operType.equals("CURRENT_RETAIL"))
					marginDollar = marginDollar + item.getCurRetailMarginDollar();
				else if (operType.equals("OVERRIDE_RETAIL"))
					marginDollar = marginDollar + item.getOverrideRetailMarginDollar();
			}
		}
		return marginDollar;
	}

	public PRItemDTO sumCurRetailPredictionForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double predictedMovement = sumPrediction(retLirItemCodeDTO, allItemList, "CURRENT_RETAIL_PREDICTION");
		retLirItemCodeDTO.setCurRegPricePredictedMovement(predictedMovement);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumOverrideRetailPredictionForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double predictedMovement = sumPrediction(retLirItemCodeDTO, allItemList, "OVERRIDE_RETAIL_PREDICTION");
		retLirItemCodeDTO.setOverridePredictedMovement(predictedMovement);
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumPredictionForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double predictedMovement = sumPrediction(retLirItemCodeDTO, allItemList, "RECOMMENDED_RETAIL_PREDICTION");
		retLirItemCodeDTO.setPredictedMovement(predictedMovement);
		return retLirItemCodeDTO;
	}

	private double sumPrediction(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList, String predType) {
		double aggregatedMovement = 0;
		Double predictedMovement = null;

		for (PRItemDTO item : allItemList) {
			predictedMovement = null;
			if (predType.equals("RECOMMENDED_RETAIL_PREDICTION")) {
				predictedMovement = item.getPredictedMovement();
			} else if (predType.equals("CURRENT_RETAIL_PREDICTION")) {
				predictedMovement = item.getCurRegPricePredictedMovement();
			} else if (predType.equals("OVERRIDE_RETAIL_PREDICTION")) {
				predictedMovement = item.getOverridePredictedMovement();
			} else if (predType.equals("REC_WEEK_SALE_PRED_CUR_REG")) {
				predictedMovement = item.getRecWeekSaleInfo().getSalePredMovAtCurReg();
			} else if (predType.equals("REC_WEEK_SALE_PRED_REC_REG")) {
				predictedMovement = item.getRecWeekSaleInfo().getSalePredMovAtRecReg();
			}

			MultiplePrice curRegMultiplePrice = null;
			curRegMultiplePrice = PRCommonUtil.getMultiplePrice(item.getRegMPack(), item.getRegPrice(),
					item.getRegMPrice());
			if (predictedMovement != null && predictedMovement > 0 && PRCommonUtil
					.canConsiderItemForCalculation(curRegMultiplePrice, item.getCost(), item.getPredictionStatus()))
				aggregatedMovement = aggregatedMovement + predictedMovement;
		}
		return aggregatedMovement;
	}

	public int getItemCodeWhoseLogIsUsedAsLigLevelLog(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList)
			throws GeneralException {
		ObjectMapper mapper = new ObjectMapper();
		String retLirExplainLog = "", ligMemberExplainLog = "";
		int itemCode = 0;
		try {
			retLirExplainLog = mapper.writeValueAsString(retLirItemCodeDTO.getExplainLog());
			for (PRItemDTO item : allItemList) {
				ligMemberExplainLog = mapper.writeValueAsString(item.getExplainLog());
				if (ligMemberExplainLog.equals(retLirExplainLog)) {
					itemCode = item.getItemCode();
					break;
				}
			}
		} catch (JsonProcessingException e) {
			logger.error("Exception in getItemCodeWhoseLogIsUsedAsLigLevelLog" + e.toString(), e);
			throw new GeneralException(e.toString());
		}
		return itemCode;
	}

	public PRItemDTO updateLIGExplainLog(int itemCode, PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		for (PRItemDTO item : allItemList) {
			if (item.getItemCode() == itemCode)
				retLirItemCodeDTO.setExplainLog(item.getExplainLog());
		}
		return retLirItemCodeDTO;
	}

	/**
	 * Return all the lig members which has item level relation
	 * 
	 * @param ligMembers
	 */
	private List<PRItemDTO> getAllLigMemberWithItemRelation(Collection<PRItemDTO> ligMembers) {
		List<PRItemDTO> ligMemWithItemRelation = new ArrayList<PRItemDTO>();
		for (PRItemDTO ligMember : ligMembers) {
			if (ligMember.isItemLevelRelation()) {
				ligMemWithItemRelation.add(ligMember);
			}
		}
		return ligMemWithItemRelation;
	}

	/**
	 * Find the lig price for lig's whose price group relation is at lig member
	 * level
	 * 
	 * @param ligMembers
	 * @return
	 */
	public MultiplePrice getLigLevelPrice(Collection<PRItemDTO> ligMembers, boolean usePrediction) {
		MultiplePrice ligPrice = null;
		// If there is a relation at lig member level rather than at lig level,
		// then the lig price has to be determined in following way

		List<MultiplePrice> commonPriceOfPriceRange = new ArrayList<MultiplePrice>();
		commonPriceOfPriceRange = getCommonPricePointOfPriceRange(ligMembers);
//		logger.debug("No of common price point:" + commonPriceOfPriceRange.size());
		// There is no common price point
		if (commonPriceOfPriceRange.size() == 0) {
			// Return price point which has highest pred or avg mov
			ligPrice = getPricePointWithHighestMov(ligMembers, usePrediction);
		} else if (commonPriceOfPriceRange.size() == 1) {
			// If only one price point is there, return that as lig price
			ligPrice = commonPriceOfPriceRange.get(0);
		} else {
			// If there is more than one common price point
			List<MultiplePrice> commonPriceOfRecPrice = new ArrayList<MultiplePrice>();
			// Then find common price point using recommended retail (that is the price
			// after applying objective)
			commonPriceOfRecPrice = getCommonPricePointOfRecPrice(ligMembers);
			if (commonPriceOfRecPrice.size() > 0) {
				ligPrice = commonPriceOfRecPrice.get(0);
				// logger.debug("lig level price: " + (ligPrice != null ? ((MultiplePrice)
				// ligPrice).price : ""));
			} else {
				// Otherwise return price point which has highest pred or avg mov
				ligPrice = getPricePointWithHighestMov(ligMembers, usePrediction);
			}
		}
		return ligPrice;
	}

	/**
	 * Get price point which is available in all lig members
	 * 
	 * @param ligMembers
	 * @return
	 */
	private List<MultiplePrice> getCommonPricePointOfPriceRange(Collection<PRItemDTO> ligMembers) {
		List<MultiplePrice> commonPriceOfPriceRange = new ArrayList<MultiplePrice>();
		List<Double> firstOccurancePriceRange = new ArrayList<Double>();

		// Get first occurrence price range of the lig member
		for (PRItemDTO ligMember : ligMembers) {
			// Check if there is at least one price point
			if (ligMember.getPriceRange() != null && ligMember.getPriceRange().length > 0) {
				firstOccurancePriceRange = Arrays.asList(ligMember.getPriceRange());
				break;
			}
		}

		// For logging alone
		/*
		 * for (PRItemDTO ligMember : ligMembers) { if (ligMember.getPriceRange() !=
		 * null) logger.debug("Lig member:" + ligMember.getItemCode() + ", priceRange:"
		 * + Arrays.toString(ligMember.getPriceRange())); }
		 */

		// Check if price point is available in all lig member using the first
		// occurrence price range
		for (Double pricePoint : firstOccurancePriceRange) {
			Boolean isPricePointPresentInAllMem = null;
			// Check all lig members
			for (PRItemDTO ligMember : ligMembers) {
				// Make sure there is price range for the member
				if (ligMember.getPriceRange() != null) {
					boolean isPricePointPresentInPriceRange = false;
					// Loop all price point of lig member
					for (Double pricePointOfPriceRange : ligMember.getPriceRange()) {
						// if price point present in any of the point price range
						if (pricePoint.equals(pricePointOfPriceRange)) {
							isPricePointPresentInPriceRange = true;
							break;
						}
					}
					if (isPricePointPresentInPriceRange) {
						isPricePointPresentInAllMem = true;
					} else {
						// Mark as false even if any one of the lig member price range
						// doesn't have the price point
						isPricePointPresentInAllMem = false;
						break;
					}
				}
			}
			// if the price point is present in all the lig members
			if (isPricePointPresentInAllMem != null && isPricePointPresentInAllMem) {
				MultiplePrice mulitplePrice = new MultiplePrice(PRConstants.DEFAULT_REG_MULTIPLE, pricePoint);
				commonPriceOfPriceRange.add(mulitplePrice);
			}
		}
		return commonPriceOfPriceRange;
	}

	/**
	 * Get rec price which is available in all lig members
	 * 
	 * @param ligMembers
	 * @return
	 */
	private List<MultiplePrice> getCommonPricePointOfRecPrice(Collection<PRItemDTO> ligMembers) {
		List<MultiplePrice> commonPriceOfPriceRange = new ArrayList<MultiplePrice>();
//		Double firstOccuranceRecPrice = null;
		MultiplePrice firstOccuranceRecPrice = null;

		// Get common price point among lig members
		for (PRItemDTO ligMember : ligMembers) {
			// Check if there is at least one price point
			if (ligMember.getRecommendedRegPrice() != null) {
				firstOccuranceRecPrice = ligMember.getRecommendedRegPrice();
				break;
			}
		}

		// Check if rec price is available in all lig member from the first
		// items rec price
		Boolean isPricePointPresent = null;
		if (firstOccuranceRecPrice != null) {
			// Loop all lig members
			for (PRItemDTO ligMember : ligMembers) {
				if (ligMember.getRecommendedRegPrice() != null) {
//					logger.debug("Lig member:" + ligMember.getItemCode() + ", RecommendedRegPrice:" +  PRCommonUtil.getPriceForLog(ligMember.getRecommendedRegPrice()));
					if (ligMember.getRecommendedRegPrice().equals(firstOccuranceRecPrice)) {
						isPricePointPresent = true;
					} else {
						isPricePointPresent = false;
						break;
					}
				}
			}
		}
		// if the price point is present in all the lig members
		if (isPricePointPresent != null && isPricePointPresent) {
//			MultiplePrice mulitplePrice = new MultiplePrice(PRConstants.DEFAULT_REG_MULTIPLE, firstOccuranceRecPrice);
//			commonPriceOfPriceRange.add(mulitplePrice);
			commonPriceOfPriceRange.add(firstOccuranceRecPrice);
		}

		return commonPriceOfPriceRange;
	}

	/**
	 * Get price point with highest predicted or average movement
	 * 
	 * @param ligMembers
	 * @return
	 */
	private MultiplePrice getPricePointWithHighestMov(Collection<PRItemDTO> ligMembers, boolean usePrediction) {
		MultiplePrice ligPrice = null;
		Double priceWithHighestMov = null;
		Double highestMov = 0d;

		// Loop all lig member
		for (PRItemDTO ligMember : ligMembers) {
			// Loop all price point of lig member
			for (Double pricePointOfPriceRange : ligMember.getPriceRange()) {
				Double predOrAvgMov = 0d;
				if (usePrediction) {
					MultiplePrice multiplePrice = new MultiplePrice(1, pricePointOfPriceRange);
					PricePointDTO pricePointDTO = ligMember.getRegPricePredictionMap().get(multiplePrice);
					predOrAvgMov = ((pricePointDTO != null && pricePointDTO.getPredictedMovement() > 0)
							? pricePointDTO.getPredictedMovement()
							: 0);
				} else {
					predOrAvgMov = ligMember.getAvgMovement();
				}
//				logger.debug("Lig member:" + ligMember.getItemCode() + ", pricePoint: " + pricePointOfPriceRange + ", predOrAvgMov:" + predOrAvgMov);
				// first occurrence
				if (priceWithHighestMov == null) {
					highestMov = predOrAvgMov;
					priceWithHighestMov = pricePointOfPriceRange;
				} else if (predOrAvgMov > highestMov) {
					highestMov = predOrAvgMov;
					priceWithHighestMov = pricePointOfPriceRange;
				}
//				logger.debug("highestMov:" + highestMov + ", priceWithHighestMov: " + priceWithHighestMov);
			}
		}
		ligPrice = new MultiplePrice(PRConstants.DEFAULT_REG_MULTIPLE, priceWithHighestMov);
		return ligPrice;
	}

	public PRItemDTO sumRecWeekSalePredAtCurRegForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double predictedMovement = sumPrediction(retLirItemCodeDTO, allItemList, "REC_WEEK_SALE_PRED_CUR_REG");
		if (predictedMovement > 0) {
			retLirItemCodeDTO.getRecWeekSaleInfo().setSalePredMovAtCurReg(predictedMovement);
		}
		return retLirItemCodeDTO;
	}

	public PRItemDTO sumRecWeekSalePredAtRecRegForLIG(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		double predictedMovement = sumPrediction(retLirItemCodeDTO, allItemList, "REC_WEEK_SALE_PRED_REC_REG");
		if (predictedMovement > 0) {
			retLirItemCodeDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(predictedMovement);
		}
		return retLirItemCodeDTO;
	}

	private PRItemDTO sumAvgMovement(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Double avgMovement = 0d;

		for (PRItemDTO item : allItemList) {
			avgMovement = avgMovement + item.getAvgMovement();
		}
		retLirItemCodeDTO.setAvgMovement(avgMovement);
		return retLirItemCodeDTO;
	}

	private PRItemDTO sumAvgRevenue(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Double avgRevenue = 0d;

		for (PRItemDTO item : allItemList) {
			avgRevenue = avgRevenue + item.getAvgRevenue();
		}
		retLirItemCodeDTO.setAvgRevenue(avgRevenue);
		return retLirItemCodeDTO;
	}

	public PRItemDTO updateQuestionablePred(PRItemDTO retLirItemCodeDTO, Collection<PRItemDTO> allItemList) {
		Set<String> distinctQuestionablePred = new HashSet<String>();
		for (PRItemDTO item : allItemList) {
			// pick only successful prediction
			if (item.getRegPricePredReasons() != null && item.getRegPricePredReasons() != ""
					&& item.getPredictionStatus() != null
					&& item.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()) {
				for (String predReason : item.getRegPricePredReasons().split(",")) {
					distinctQuestionablePred.add(predReason);
				}
			}
		}
		retLirItemCodeDTO.setRegPricePredReasons(String.join(",", distinctQuestionablePred));

		return retLirItemCodeDTO;
	}

	private PRItemDTO getRepItemByModePrice(HashMap<Integer, PRItemDTO> ligMembersItemMap, int key,
			MostOccurrenceData mostOccurrenceData, PricingEngineService pricingEngineService,
			Collection<PRItemDTO> commonOccrItemList, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		PRItemDTO maxOccurenceObj = new PRItemDTO();
		MultiplePrice multiplePrice = null;
		int itemCode = -1;
		double price = Constants.DEFAULT_NA;
		int multiple = 1;
		Object object = null;
		ItemKey ligItemKey = PRCommonUtil.getItemKey(key, true);

		PRItemDTO ligItem = itemDataMap.get(ligItemKey);
		// NU: 21st Oct 2015
		// Changes related to applying price group at item level inside a lig
		List<PRItemDTO> ligMemberWithItemRelation = getAllLigMemberWithItemRelation(ligMembersItemMap.values());
		List<PRItemDTO> relatedRepItem = new ArrayList<>();
		// if the lig has item level relation ship, then lig price has to be calculated
		// using item in relation
		if (ligMemberWithItemRelation.size() > 0) {
//			logger.debug("RetLirId: " + key + ":NoOfMembers:" + ligMembersItemMap.size() + " whose LIG price is based on lig mem with price relation");
			object = mostOccurrenceData.getMaxOccurance(ligMemberWithItemRelation, "RecRegPrice");
//			logger.debug("lig level price: " + (object != null ? ((MultiplePrice) object).price : ""));

			// Added by Karishma on 26/03/2020 for AZ
			// once the object is populated with min/max Price from list
			// loop the list and get all items matching the object and pass this list to get
			// the item having highestavgMovement
			// That item will be selected as LigRep
			if (object != null) {
				multiplePrice = (MultiplePrice) object;
				for (PRItemDTO prItem : ligMemberWithItemRelation) {
					if (prItem.getRecommendedRegPrice() != null) {
						if (prItem.getRecommendedRegPrice().price.equals(multiplePrice.price)) {
							relatedRepItem.add(prItem);
						}
					}
				}
				if (relatedRepItem.size() == 1) {
					itemCode = relatedRepItem.get(0).getItemCode();
				} else {
					if (relatedRepItem.size() == 0) {
						relatedRepItem = ligMemberWithItemRelation;
					}
					itemCode = getItemBasedOnMaxMovement(relatedRepItem);
				}
			}
		} else {
			object = mostOccurrenceData.getMaxOccurance(ligMembersItemMap.values(), "RecRegPrice");
			// Added by Karishma on 26/03/2020 for AZ
			// once the object is populated with min/max Price from list
			// loop the list and get all items matching the object and pass this list to get
			// the item having highestavgMovement
			// That item will be selected as LigRep
			if (object != null) {
				multiplePrice = (MultiplePrice) object;
				for (PRItemDTO prItem : ligMembersItemMap.values()) {
					if (prItem.getRecommendedRegPrice() != null) {
						if (prItem.getRecommendedRegPrice().price.equals(multiplePrice.price)) {
							relatedRepItem.add(prItem);
						}
					}
				}
				if (relatedRepItem.size() == 1) {
					itemCode = relatedRepItem.get(0).getItemCode();
				} else {
					if (relatedRepItem.size() == 0) {
						for (PRItemDTO member : ligMembersItemMap.values()) {
							relatedRepItem.add(member);
						}
					}
					itemCode = getItemBasedOnMaxMovement(relatedRepItem);
				}
			}
		}

		// if none of the lig has recommendation due to error
		if (object != null) {
			multiplePrice = (MultiplePrice) object;
			price = multiplePrice.price;
			multiple = multiplePrice.multiple;

			for (PRItemDTO prItem : ligMembersItemMap.values()) {

				if (prItem.getRecommendedRegPrice() != null && prItem.getRecommendedRegPrice().price == price) {
					// Take last occurrence
					// itemCode = prItem.getItemCode();
					commonOccrItemList.add(prItem);
				}
			}
		}

		/*
		 * If none of the lig member has recommended price, then first one
		 */
		if (itemCode == -1) {
			itemCode = new ArrayList<Integer>(ligMembersItemMap.keySet()).get(0);
		}

		if (ligItem.getLigRepItemCode() > 0) {
			itemCode = ligItem.getLigRepItemCode();
		}

		maxOccurenceObj.setItemCode(itemCode);
		maxOccurenceObj.setRegPrice(price);
		maxOccurenceObj.setRegMPack(multiple);
		return maxOccurenceObj;
	}

	private PRItemDTO getRepItemByMinOrMaxPrice(HashMap<Integer, PRItemDTO> ligMembersItemMap, int key,
			MostOccurrenceData mostOccurrenceData, PricingEngineService pricingEngineService,
			Collection<PRItemDTO> commonOccrItemList, HashMap<ItemKey, PRItemDTO> itemDataMap, String minOrMax) {
		PRItemDTO maxOccurenceObj = new PRItemDTO();
		MultiplePrice multiplePrice = null;
		int itemCode = -1;
		double price = Constants.DEFAULT_NA;
		int multiple = 1;
		Object object = null;
		ItemKey ligItemKey = PRCommonUtil.getItemKey(key, true);

		PRItemDTO ligItem = itemDataMap.get(ligItemKey);
		// NU: 21st Oct 2015
		// Changes related to applying price group at item level inside a lig
		List<PRItemDTO> ligMemberWithItemRelation = getAllLigMemberWithItemRelation(ligMembersItemMap.values());
		List<PRItemDTO> relatedRepItem = new ArrayList<>();
		// if the lig has item level relation ship, then lig price has to be calculated
		// using item in relation
		if (ligMemberWithItemRelation.size() > 0) {
//			logger.debug("RetLirId: " + key + ":NoOfMembers:" + ligMembersItemMap + " whose LIG price is based on lig mem with price relation");
			// object = mostOccurrenceData.getMaxOccurance(ligMemberWithItemRelation,
			// "RecRegPrice");

			if (minOrMax.equals("min")) {
				object = getMinMultiPrice(ligMemberWithItemRelation);
			} else if (minOrMax.equals("max")) {
				object = getMaxMultiPrice(ligMemberWithItemRelation);
			}
			// Added by Karishma on 26/03/2020 for AZ
			// once the object is populated with min/max Price from list
			// loop the list and get all items matching the object and pass this list to get
			// the item having highestavgMovement
			// That item will be selected as LigRep
			if (object != null) {
				multiplePrice = (MultiplePrice) object;
				for (PRItemDTO prItem : ligMemberWithItemRelation) {
					if (prItem.getRecommendedRegPrice() != null) {
						if (prItem.getRecommendedRegPrice().price.equals(multiplePrice.price)) {
							relatedRepItem.add(prItem);
						}
					}
				}
			}
			if (relatedRepItem.size() == 1) {
				itemCode = relatedRepItem.get(0).getItemCode();
			} else {
				if (relatedRepItem.size() == 0) {
					relatedRepItem = ligMemberWithItemRelation;
				}
				itemCode = getItemBasedOnMaxMovement(relatedRepItem);
			}

		} else {
			if (minOrMax.equals("min")) {
				object = getMinMultiPrice(ligMembersItemMap.values());
			} else if (minOrMax.equals("max")) {
				object = getMaxMultiPrice(ligMembersItemMap.values());
			}
			// Added by Karishma on 26/03/2020 for AZ
			// once the object is populated with min/max Price from list
			// loop the list and get all items matching the object and pass this list to get
			// the item having highestavgMovement
			// That item will be selected as LigRep
			if (object != null) {
				multiplePrice = (MultiplePrice) object;
				for (PRItemDTO prItem : ligMembersItemMap.values()) {
					if (prItem.getRecommendedRegPrice() != null) {
						if (prItem.getRecommendedRegPrice().price.equals(multiplePrice.price)) {
							relatedRepItem.add(prItem);
						}
					}
				}
				if (relatedRepItem.size() == 1) {
					itemCode = relatedRepItem.get(0).getItemCode();
				} else {
					if (relatedRepItem.size() == 0) {
						for (PRItemDTO member : ligMembersItemMap.values()) {
							relatedRepItem.add(member);
						}
					}
					itemCode = getItemBasedOnMaxMovement(relatedRepItem);
				}
			}
		}

		// if none of the lig has recommendation due to error
		if (object != null) {
			multiplePrice = (MultiplePrice) object;
			price = multiplePrice.price;
			multiple = multiplePrice.multiple;

			for (PRItemDTO prItem : ligMembersItemMap.values()) {
				// logger.debug("Lig Members: " + prItem.getItemCode() +
				// ",Vendor Id:" + prItem.getVendorId());
				// if (prItem.getRecommendedRegPrice() != null &&
				// prItem.getRecommendedRegPrice() == price) {
				if (prItem.getRecommendedRegPrice() != null && prItem.getRecommendedRegPrice().price == price) {
					// Take last occurrence
					// itemCode = prItem.getItemCode();
					commonOccrItemList.add(prItem);
				}
			}
		}

		/*
		 * If none of the lig member has recommended price, then first one
		 */
		if (itemCode == -1) {
			itemCode = new ArrayList<Integer>(ligMembersItemMap.keySet()).get(0);
		}

		if (ligItem.getLigRepItemCode() > 0) {
			itemCode = ligItem.getLigRepItemCode();
		}

		maxOccurenceObj.setItemCode(itemCode);
		maxOccurenceObj.setRegPrice(price);
		maxOccurenceObj.setRegMPack(multiple);
		return maxOccurenceObj;
	}

	public boolean getPromotionStatus(Collection<PRItemDTO> allItemList, String type) {

		Boolean isAnyOneTrue = null;
		if (type.equals("OnGoingPromo") || type.equals("FuturePromo")) {
			isAnyOneTrue = false;
		} else if (type.equals("PromoEndsInXWeeks")) {
			isAnyOneTrue = true;
		}

		// Update conflict if any of the item in conflict
		for (PRItemDTO item : allItemList) {
			if (type.equals("OnGoingPromo") && item.isOnGoingPromotion()) {
				isAnyOneTrue = true;
				break;
			} else if (type.equals("FuturePromo") && item.isFuturePromotion()) {
				isAnyOneTrue = true;
				break;
			} else if (type.equals("PromoEndsInXWeeks") && !item.isPromoEndsWithinXWeeks()) {
				isAnyOneTrue = false;
				break;
			}
		}
		return isAnyOneTrue;
	}

	/**
	 * 
	 * @param ligItem
	 * @param commonOccrItemList1
	 * @param allItemList
	 * @param mostOccurrenceData
	 * @param ligRepItem
	 * @param ligSameConstraintPresent
	 */
	private void updateAttributesFromRepItem(PRItemDTO ligItem, Collection<PRItemDTO> commonOccrItemList1,
			Collection<PRItemDTO> allItemList, MostOccurrenceData mostOccurrenceData, PRItemDTO ligRepItem) {

		// Rec Reg Multiple and Price
		ligItem.setRecommendedRegPrice(ligRepItem.getRecommendedRegPrice());

		// Cur Reg Price
		MultiplePrice currentPrice = PRCommonUtil.getCurRegPrice(ligRepItem);

		if (currentPrice != null) {
			ligItem.setRegMPack(currentPrice.multiple);
			if (currentPrice.multiple > 1) {
				ligItem.setRegMPrice(currentPrice.price);
			} else {
				ligItem.setRegPrice(currentPrice.price);
			}
		}

		// Pre Reg Price
		ligItem.setPreRegPrice(ligRepItem.getPreRegPrice());

		// Current Reg Eff Date
		ligItem.setCurRegPriceEffDate(ligRepItem.getCurRegPriceEffDate());

		// Cur List Cost
		ligItem.setListCost(ligRepItem.getListCost());

		if (PropertyManager.getProperty("USE_WEIGHTED_AVG_COST", "FALSE").equalsIgnoreCase("TRUE")) {
			// Original list cost
			ligItem.setOriginalListCost(ligRepItem.getOriginalListCost());
		}

		// Pre List Cost
		ligItem.setPreListCost(ligRepItem.getPreListCost());

		// Cur Vip Cost
		ligItem.setVipCost(ligRepItem.getVipCost());

		// Pre Vip Cost
		ligItem.setPreVipCost(ligRepItem.getPreVipCost());

		// Comp price
		ligItem.setCompPrice(ligRepItem.getCompPrice());

		// Comp previous price
		ligItem.setCompPreviousPrice(ligRepItem.getCompPreviousPrice());

		// Comp All price.Added By karishma
		ligItem.setAllCompPrice(ligRepItem.getAllCompPrice());

		// copy all previous compPrice for RA.Added by Karishma
		ligItem.setAllCompPreviousPrice(ligRepItem.getAllCompPreviousPrice());

		// Multi Comp Copy.Added By karishma
		CopyMultiCompDetails(ligItem, ligRepItem);

		// Comp Check Date
		ligItem.setCompPriceCheckDate(ligRepItem.getCompPriceCheckDate());

		// Vendor Id
		ligItem.setVendorId(ligRepItem.getVendorId());

		// Pre Price
		ligItem.setIsPrePriced(ligRepItem.getIsPrePriced());

		// Loc Price
		ligItem.setIsLocPriced(ligRepItem.getIsLocPriced());

		// cost effective date
		ligItem.setListCostEffDate(ligRepItem.getListCostEffDate());

		// DistFlag
		ligItem.setDistFlag(ligRepItem.getDistFlag());

		// Item size
		ligItem.setItemSize(ligRepItem.getItemSize());

		// Update UOM Id
		ligItem.setUOMId(ligRepItem.getUOMId());

		// Update UOM Name
		ligItem.setUOMName(ligRepItem.getUOMName());

		ligItem = updatePredictionStatus(ligItem, allItemList);
		// Update prediction status

		ligItem = updateLIGConflict(ligItem, allItemList);

		// Find cost difference
		if (ligItem.getListCost() != null && ligItem.getPreListCost() != null && ligItem.getListCost() > 0
				&& ligItem.getPreListCost() > 0) {
			if (ligItem.getPreListCost() != ligItem.getListCost()) {
				if (ligItem.getListCost() < ligItem.getPreListCost()) {
					ligItem.setCostChgIndicator(-1);
				} else if (ligItem.getListCost() > ligItem.getPreListCost()) {
					ligItem.setCostChgIndicator(+1);
				} else {
					ligItem.setCostChgIndicator(0);
				}
			}
		}

		// Find vip cost difference
		if (ligItem.getVipCost() != null && ligItem.getPreVipCost() != null && ligItem.getVipCost() > 0
				&& ligItem.getPreVipCost() > 0) {
			if (ligItem.getPreVipCost() != ligItem.getVipCost()) {
				if (ligItem.getVipCost() < ligItem.getPreVipCost()) {
					ligItem.setVipCostChgIndicator(-1);
				} else if (ligItem.getVipCost() > ligItem.getPreVipCost()) {
					ligItem.setVipCostChgIndicator(+1);
				} else {
					ligItem.setVipCostChgIndicator(0);
				}
			}
		}

		// Find comp price difference
		if (ligItem.getCompPreviousPrice() != null && ligItem.getCompPrice() != null && ligItem.getCompPrice().price > 0
				&& ligItem.getCompPreviousPrice().price > 0) {
			// compare against unit price
			double curCompPrice = PRCommonUtil.getUnitPrice(ligItem.getCompPrice(), true);
			double preCompPrice = PRCommonUtil.getUnitPrice(ligItem.getCompPreviousPrice(), true);
			if (curCompPrice != preCompPrice) {
				if (curCompPrice < preCompPrice) {
					ligItem.setCompPriceChgIndicator(-1);
				} else if (curCompPrice > preCompPrice) {
					ligItem.setCompPriceChgIndicator(+1);
				} else {
					ligItem.setCompPriceChgIndicator(0);
				}
			}
		}

		// Update error codes (update as error even one of the item is in error)
		ligItem = updateErrorCode(ligItem, allItemList);

		// Update if price is same across all stores (even if one item's prices are not
		// identical in stores, then mark that lig
		// as store has diff prices
		ligItem = updateSamePriceAcrossStore(ligItem, allItemList);

		// Update mark for review as yes, even if any one of the item is marked for
		// review
		// 19th May 2016, Mark as review when there is cost change for CVS
		ligItem = updateMarkForReview(ligItem, allItemList);

		// update predicted movement
		ligItem = sumPredictionForLIG(ligItem, allItemList);
		ligItem = sumCurRetailPredictionForLIG(ligItem, allItemList);
		ligItem = sumCurRetailSalesDollarForLIG(ligItem, allItemList);
		ligItem = sumCurRetailMarginDollarForLIG(ligItem, allItemList);
		ligItem = sumRecRetailSalesDollarForLIG(ligItem, allItemList);
		ligItem = sumRecRetailMarginDollarForLIG(ligItem, allItemList);

		/*
		 * multiplePrice = (MultiplePrice)
		 * mostOccurrenceData.getMaxOccurance(commonOccrItemList, "CurCompSalePrice");
		 * if(multiplePrice == null) multiplePrice = (MultiplePrice)
		 * mostOccurrenceData.getMaxOccurance(allItemList, "CurCompSalePrice");
		 * ligItem.setCompCurSalePrice(multiplePrice);
		 */

		ligItem.setCompCurSalePrice(
				(MultiplePrice) mostOccurrenceData.getMaxOccurance(allItemList, "CurCompSalePrice"));

		updatePromotedRealtedInfo(ligItem, allItemList, mostOccurrenceData);

		ligItem = sumAvgRevenue(ligItem, allItemList);
		ligItem = sumAvgMovement(ligItem, allItemList);

		// To get all vendor Id's from LIG Members. Changes Done by Dinesh(10/20/2017)
		// Code commented since the requirement is not finalized(Dinesh)
		// ligItem = getVendorIDsForLIGItem(ligItem, allItemList);

		updateQuestionablePred(ligItem, allItemList);
		ligItem.setRecProcessCompleted(ligRepItem.isRecProcessCompleted());

		ligItem.setRecPriceEffectiveDate(
				(String) mostOccurrenceData.getMaxOccurance(allItemList, "RecPriceEffectiveDate"));

		ligItem.setFutureRecRetail((MultiplePrice) mostOccurrenceData.getMaxOccurance(allItemList, "FutureRecRetail"));

		ligItem.setCwacCoreCost((Double) mostOccurrenceData.getMaxOccurance(allItemList, "CoreCost"));
		ligItem.setCoreRetail((Double) mostOccurrenceData.getMaxOccurance(allItemList, "CoreRetail"));
		/*
		 * double priceChangeImpact =
		 * allItemList.stream().mapToDouble(PRItemDTO::getPriceChangeImpact).sum();
		 * ligItem.setPriceChangeImpact(priceChangeImpact);
		 */
		ligItem.setSecondaryZoneRecPresent(ligRepItem.isSecondaryZoneRecPresent());
		setSecondaryZoneRecsForLig(ligItem, ligRepItem);

		//commented as it creates propbelm for lig rep item selection
		// Copy same price points to all LIG members when the lig constraint is enabled
		// and lock and pre-price are not set.
		/*
		 * allItemList.forEach(item -> { if (item.getRetLirId() == 1081279){
		 * logger.info("price Range original for item" + item.getItemCode() +"; "+
		 * PRCommonUtil.getCommaSeperatedStringFromDouble(item.getPriceRange())); }
		 * 
		 * if (item.getIsPrePriced() == 0 &&
		 * item.getStrategyDTO().getConstriants().getLocPriceConstraint() == null &&
		 * ligSameConstraintPresent) { item.setPriceRange(ligRepItem.getPriceRange()); }
		 * 
		 * });
		 */
		// update the cost change indicator

		ligItem.setCostChgIndicator(ligRepItem.getCostChgIndicator());

		// Set the most common future cost from the members to the rep item
		HashMap<String, Integer> futureCostMap = new HashMap<>();
		
		for (PRItemDTO item : allItemList) {
			//Added existing property check to the function because future cost functionality is applicable for RA only and not FF
			///For FF future cost is for display purpose
			if (item.getFutureListCost() != null && chkFutureCost) {
				int count = 0;
				String costKey = item.getFutureListCost() + "-" + item.getFutureCostEffDate();
				if (futureCostMap.containsKey(costKey)) {
					count = futureCostMap.get(costKey);
				}
				futureCostMap.put(costKey, count + 1);
			}
		}

		if (futureCostMap.size() > 0) {
			int maxCount = 0;
			String mostCommonCostKey = "";
			for (Map.Entry<String, Integer> futureCost : futureCostMap.entrySet()) {

				if (futureCost.getValue() > maxCount) {
					maxCount = futureCost.getValue();
					mostCommonCostKey = futureCost.getKey();
				}
			}
			ligItem.setFutureListCost(Double.parseDouble(mostCommonCostKey.split("-")[0]));
			ligItem.setFutureCostEffDate(mostCommonCostKey.split("-")[1]);
		}

		// Added by Karishma for AZ on 08/11 for setting the nipo_cost at LIG rep Item
		ligItem.setNipoBaseCost(ligRepItem.getNipoBaseCost());

		/** PROM-2214 changes **/
		ligItem.setNoOfStoresItemAuthorized(ligRepItem.getNoOfStoresItemAuthorized());
		/** PROM-2214 changes **/

		// Added by Karishma on 11/22/21 for setting the Freight charge selected flag
		// for LIG rep item
		ligItem.setFreightChargeIncluded(ligRepItem.getFreightChargeIncluded());

		/** PRES-213 Change **/
		// Added by Karishma on 01/10/22 for setting the MAP retail for the rep item
		ligItem.setMapRetail(ligRepItem.getMapRetail());

		ligItem.setCwagBaseCost(ligRepItem.getCwagBaseCost());
		
		ligItem.setClearanceItem(ligRepItem.isClearanceItem());
		
		// set is clearance flag for lig rep row when any member in lig has clearance
		// retail
		if (checkForClearanceStores) {
			for (PRItemDTO item : allItemList) {
				if (item.isClearanceItem()) {
					ligItem.setClearanceItem(true);
					ligItem.setClearanceRetail(item.getClearanceRetail());
					ligItem.setClearanceRetailEffDate(item.getClearanceRetailEffDate());
					break;
				}
			}
			// if lig rep item has clearnace price then set it for lig rep row
			if (ligRepItem.isClearanceItem()) {
				ligItem.setClearanceItem(true);
				ligItem.setClearanceRetail(ligRepItem.getClearanceRetail());
				ligItem.setClearanceRetailEffDate(ligRepItem.getClearanceRetailEffDate());
			}
		}
		// set is future price present flag for lig rep row when any member in lig has future
		// retail
		for (PRItemDTO item : allItemList) {
			if (item.isFuturePricePresent()) {
				ligItem.setFuturePricePresent(item.isFuturePricePresent());
				ligItem.setFuturePriceEffDate(item.getFuturePriceEffDate());
				ligItem.setFutureUnitPrice(item.getFutureUnitPrice());
				break;
			}

			if (ligRepItem.isFuturePricePresent()) {
				ligItem.setFuturePricePresent(ligRepItem.isFuturePricePresent());
				ligItem.setFuturePriceEffDate(ligRepItem.getFuturePriceEffDate());
				ligItem.setFutureUnitPrice(ligRepItem.getFutureUnitPrice());
			}
		}
		// set promostarts withing x weeks of recc week for lig rep row even if one
		// member has promotionstarting
		// withinx weeks
		for (PRItemDTO item : allItemList) {
			if (item.isPromoStartsWithinXWeeks()) {
				ligItem.setPromoStartsWithinXWeeks(true);
				break;
			}
		}

		//copy S/H FLAG for LIG  row from rep item for AZ
		//Added to set the postdated date at lig row same as rep item selected
		ligItem.setUserAttr6(ligRepItem.getUserAttr6());
	}

	private Object getMinMultiPrice(Collection<PRItemDTO> ligMembers) {
		MultiplePrice multiplePrice = null;
		double minUnitPrice = 0;
		for (PRItemDTO itemDTO : ligMembers) {
			double unitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRecommendedRegPrice(), true);
			if (minUnitPrice == 0 || minUnitPrice > unitPrice) {
				minUnitPrice = unitPrice;
				multiplePrice = itemDTO.getRecommendedRegPrice();
			}
		}
		return multiplePrice;
	}

	private Object getMaxMultiPrice(Collection<PRItemDTO> ligMembers) {
		MultiplePrice multiplePrice = null;
		double maxUnitPrice = 0;
		for (PRItemDTO itemDTO : ligMembers) {
			double unitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRecommendedRegPrice(), true);
			if (maxUnitPrice == 0 || maxUnitPrice < unitPrice) {
				maxUnitPrice = unitPrice;
				multiplePrice = itemDTO.getRecommendedRegPrice();
			}
		}
		return multiplePrice;
	}

	/**
	 * 
	 * @param ligDTO
	 * @param ligRepItem
	 */
	private void setSecondaryZoneRecsForLig(PRItemDTO ligDTO, PRItemDTO ligRepItem) {
		if (ligRepItem.getSecondaryZones() != null && ligRepItem.getSecondaryZones().size() > 0) {
			List<SecondaryZoneRecDTO> secondaryZoneRecsForLig = new ArrayList<>();
			ligRepItem.getSecondaryZones().forEach(secZoneRecRep -> {
				try {
					SecondaryZoneRecDTO SecondaryZoneRecLig = (SecondaryZoneRecDTO) secZoneRecRep.clone();
					SecondaryZoneRecLig.setProductLevelId(Constants.PRODUCT_LEVEL_ID_LIG);
					SecondaryZoneRecLig.setProductId(ligDTO.getItemCode());
					secondaryZoneRecsForLig.add(SecondaryZoneRecLig);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			ligDTO.setSecondaryZones(secondaryZoneRecsForLig);
		}
	}

	/**
	 * 
	 * @param ligItem
	 * @param ligRepItem
	 */
	private void CopyMultiCompDetails(PRItemDTO ligItem, PRItemDTO ligRepItem) {

		if (ligRepItem.getComp1StrId() != null && ligRepItem.getComp1StrId() > 0) {
			ligItem.setComp1StrId(ligRepItem.getComp1StrId());
			ligItem.setComp1Retail(ligRepItem.getComp1Retail());
		}
		if (ligRepItem.getComp2StrId() != null && ligRepItem.getComp2StrId() > 0) {
			ligItem.setComp2StrId(ligRepItem.getComp2StrId());
			ligItem.setComp2Retail(ligRepItem.getComp2Retail());
		}
		if (ligRepItem.getComp3StrId() != null && ligRepItem.getComp3StrId() > 0) {
			ligItem.setComp3StrId(ligRepItem.getComp3StrId());
			ligItem.setComp3Retail(ligRepItem.getComp3Retail());
		}
		if (ligRepItem.getComp4StrId() != null && ligRepItem.getComp4StrId() > 0) {
			ligItem.setComp4StrId(ligRepItem.getComp4StrId());
			ligItem.setComp4Retail(ligRepItem.getComp4Retail());
		}
		if (ligRepItem.getComp5StrId() != null && ligRepItem.getComp5StrId() > 0) {
			ligItem.setComp5StrId(ligRepItem.getComp5StrId());
			ligItem.setComp5Retail(ligRepItem.getComp5Retail());
		}
	}

	/**
	 * Added by Karishma to first get the item having max movement and then use the
	 * proce of this item
	 * 
	 * @param ligMembersItemMap
	 * @param key
	 * @param mostOccurrenceData
	 * @param pricingEngineService
	 * @param commonOccrItemList
	 * @param itemDataMap
	 * @return
	 */
	public PRItemDTO getRepItemUsingHighMover(HashMap<Integer, PRItemDTO> ligMembersItemMap, int key,
			MostOccurrenceData mostOccurrenceData, PricingEngineService pricingEngineService,
			Collection<PRItemDTO> commonOccrItemList, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		PRItemDTO maxOccurenceObj = new PRItemDTO();

		int itemCode = -1;
		double price = Constants.DEFAULT_NA;
		int multiple = 1;

		// Get the rep item by finding the highest mover

		List<PRItemDTO> relatedRepItem = new ArrayList<>();
		Set<Integer> itemCodeSet = new HashSet<Integer>();

		for (PRItemDTO prItem : ligMembersItemMap.values()) {
			if (!prItem.isLir()) {
				itemCodeSet.add(prItem.getItemCode());
				relatedRepItem.add(prItem);
			}
		}

		itemCode = getItemBasedOnMaxMovement(relatedRepItem);

		Map<Integer, List<PRItemDTO>> itemCodeMap = ligMembersItemMap.values().stream()
				.collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		// If none of the item has movement then select item with min prestoItemCode
		if (itemCode == -1 && itemCodeSet.size() > 0) {
			itemCode = Collections.min(itemCodeSet);
		}

		if (itemCode != -1) {

			if (itemCodeMap.get(itemCode) != null) {

				if (itemCodeMap.get(itemCode).get(0).getRecommendedRegPrice() != null) {
					price = itemCodeMap.get(itemCode).get(0).getRecommendedRegPrice().price;
					multiple = itemCodeMap.get(itemCode).get(0).getRecommendedRegPrice().multiple;
				}
			}

			maxOccurenceObj.setItemCode(itemCode);
			maxOccurenceObj.setRegPrice(price);
			maxOccurenceObj.setRegMPack(multiple);

		} else {
			logger.warn("No representative Item is selected for LIG : " + key);
		}

		return maxOccurenceObj;
	}

	// Added by Karishma on 02/14/2022
	// This function will select the member with max XweekMovForLIGRepItem from
	// members and set it as LIG representative
	// If multiple items have same movement then iteM with min prestoItemCode will
	// be selected as Representative
	// If none of the items have movement ,then select the item with min Presto
	// itemcode
	public int getItemBasedOnMaxMovement(List<PRItemDTO> ligMembers) {
		int itemCode = -1;
		HashMap<Double, Integer> movementMap = new HashMap<>();

		ligMembers.forEach(item -> {

			if (movementMap.containsKey(item.getXweekMovForLIGRepItem())) {
				int prestoItemCode = movementMap.get(item.getXweekMovForLIGRepItem());
				movementMap.put(item.getXweekMovForLIGRepItem(), Math.min(prestoItemCode, item.getItemCode()));
			} else {
				movementMap.put(item.getXweekMovForLIGRepItem(), item.getItemCode());
			}

		});

		Double maxMovement = Collections.max(movementMap.keySet());
		itemCode = movementMap.get(maxMovement);
		return itemCode;
	}
	
	public boolean getFlagStatusForLig(Collection<PRItemDTO> allItemList, String type) {

		Boolean isAnyOneTrue = false;
	
		// Update isPromoStartsWithinXWeeks if any of the item has promotion starting in x weeks
		for (PRItemDTO item : allItemList) {
		  if (type.equals("PromoStartsWithinXWeeks") && item.isPromoStartsWithinXWeeks()) {
				isAnyOneTrue = true;
				break;
			} 
		}
		return isAnyOneTrue;
	}

}
