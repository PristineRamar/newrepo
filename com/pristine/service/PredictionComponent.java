package com.pristine.service;

import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
//import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
//import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
//import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

//import com.pristine.dto.ProductMetricsDataDTO;
//import com.pristine.dto.RetailPriceDTO;
//import com.pristine.dto.RetailCalendarDTO;
//import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
//import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PricingEngineHelper;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.constraint.CostConstraint;
import com.pristine.service.offermgmt.constraint.FreightChargeConstraint;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.constraint.LowerHigherConstraint;
import com.pristine.service.offermgmt.constraint.MinMaxConstraint;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * @author NAGARAJ
 *
 */
public class PredictionComponent {
	private static Logger logger = Logger.getLogger("PredictionComponent");
	final DecimalFormat twoDigitFormat = new DecimalFormat("0.00");

	// public void predictPrice(Connection conn, List<PRItemDTO> prItemList, PRStrategyDTO inputDTO, List<PRProductGroupProperty>
	// productProperties,
	// List<ExecutionTimeLog> executionTimeLogs, PRRecommendationRunHeader recommendationRunHeader, boolean isOnline,
	// HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
	// RetailCalendarDTO curCalDTO) throws GeneralException {
	// // Invoke prediction engine
	// invokePredictionEngine(conn, prItemList, inputDTO, "PRICE_RANGE", productProperties, executionTimeLogs,
	// recommendationRunHeader, isOnline,
	// retLirMap);
	//// applyObjective(prItemList, itemZonePriceHistory, curCalDTO);
	// }

	public void predictRegPricePoints(Connection conn, HashMap<ItemKey, PRItemDTO> itemDataMap, List<PRItemDTO> itemList, PRStrategyDTO inputDTO,
			List<PRProductGroupProperty> productProperties, PRRecommendationRunHeader recommendationRunHeader,
			List<ExecutionTimeLog> executionTimeLogs, boolean isOnline, HashMap<Integer, List<PRItemDTO>> retLirMap, 
			PredictionService predictionService) throws Exception {
		
		//TODO::SRIC make sure all the lead item price points also assigned to the other items of the group 
		logger.debug("predictRegPricePoints() :: Start ");
		boolean usePrediction = true;
		int noOfLowerPoints = 0, noOfHigherPoints = 0;
		PredictionInputDTO predictionInput = new PredictionInputDTO();
		PredictionOutputDTO predictionOutput = new PredictionOutputDTO();
		ArrayList<PredictionItemDTO> predictionItems = new ArrayList<PredictionItemDTO>();
		// This hashmap is used in making sure all lig members are send with same number of price points
		HashMap<ItemKey, PredictionItemDTO> itemsForPrediction = new HashMap<ItemKey, PredictionItemDTO>();
		PricingEngineService pricingEngineService = new PricingEngineService();

		// Decide between avg movement or prediction. (configuration from table PR_PRODUCT_GROUP_PROPERTY)
		// For store always use avg movement
		usePrediction = PricingEngineHelper.isUsePrediction(productProperties, inputDTO.getProductLevelId(), inputDTO.getProductId(),
				inputDTO.getLocationLevelId(), inputDTO.getLocationId());

		logger.debug("usePrediction:" + usePrediction + ",inputDTO.getLocationLevelId():" + inputDTO.getLocationLevelId() + 
				",inputDTO.getLocationId():" + inputDTO.getLocationId());
		
		// 26th Mar 2016, In order to improve strategy what-if performance, no of price point for prediction
		// is reduced by not finding the next and previous price points
		if (!recommendationRunHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
			noOfLowerPoints = Integer.parseInt(PropertyManager.getProperty("MAR_OPP_NO_OF_LOWER_PRICE_POINTS", "1"));
			noOfHigherPoints = Integer.parseInt(PropertyManager.getProperty("MAR_OPP_NO_OF_HIGHER_PRICE_POINTS", "1"));
		}

		fillBasicInfoOfPredictionInputDTO(predictionInput, inputDTO, recommendationRunHeader);
	//	int count=0;
		// Gather price point of all the items
		for (PRItemDTO item : itemList) {
			if (!item.isLir()) {
				PredictionItemDTO pItem = new PredictionItemDTO();
				pItem.upc = item.getUpc();
				pItem.itemCodeOrLirId = item.getItemCode();
				pItem.setSendToPrediction(item.isSendToPrediction());
				pItem.setAvgMovement(item.getAvgMovement());
				/*
				 * if (item.isNoRecentWeeksMov()) { pItem.setNorecentWeeksMov(true); }
				 * 
				 * if (item.isNoRecentWeeksMov() && !item.isSendToPrediction()) { count++; }
				 */
					
				List<PricePointDTO> pricePointList = new ArrayList<PricePointDTO>();

				// check if there is related item for this item
				if (pricingEngineService.hasRelatedItems(item) && !item.isCurrentPriceRetained()) {
					List<Double> possiblePricePoints = new ArrayList<Double>();

					// find all possible related price points
					if (item.getPgData() != null && item.getPgData().getRelationList() != null) {
//						logger.debug("Adding possible price points for dependent item:" + item.getItemCode());
						NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = item.getPgData().getRelationList();

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
//								logger.debug("relateItemKey for dependent item:" + item.getItemCode() + " is " + relatedItemKey.toString());
								if (itemDataMap.get(relatedItemKey) != null) {
									// If the related item is LIG, then pick all members
									List<PRItemDTO> ligMembersOrNonLig = new ArrayList<PRItemDTO>();
									if (relatedItemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
										ligMembersOrNonLig = pricingEngineService.getLigMembers(relatedItemKey.getItemCodeOrRetLirId(), itemList);
									} else {
										ligMembersOrNonLig.add(itemDataMap.get(relatedItemKey));
									}

									for (PRItemDTO itemDTO : ligMembersOrNonLig) {
										ItemKey itemKey = new ItemKey(itemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
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
												int arrLength = tempPricePoints.length + 1;
												// Past overridden retail
												if(tempRelatedItem.getOverriddenRegularPrice() != null) {
													double overrideUnitPrice = PRCommonUtil.getUnitPrice(tempRelatedItem.getOverriddenRegularPrice(), true);
													if(overrideUnitPrice != curRegUnitPrice) {
														curPrice = new Double[] { curRegUnitPrice, overrideUnitPrice };
														arrLength = tempPricePoints.length + 2;
													}
												}
												
												pricePoints = new Double[arrLength];
												 
												System.arraycopy(tempPricePoints, 0, pricePoints, 0, tempPricePoints.length);
												System.arraycopy(curPrice, 0, pricePoints, tempPricePoints.length, curPrice.length);
											} else {
												pricePoints = new Double[] { curRegUnitPrice };
											}
										} else {
											pricePoints = tempPricePoints;
										}
										

//										logger.debug("relateItemKey " + itemKey.toString() + " price points:"
//												+ PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));
										// get price points of related item
										if(relatedItem.getPriceRelation() != null) {
											if (tempRelatedItem.isRecProcessCompleted()) {
												if (tempRelatedItem.getRecommendedRegPrice() != null) {
													double[] priceRangeArr = getNewRange(
															tempRelatedItem.getRecommendedRegPrice().multiple,
															tempRelatedItem.getRecommendedRegPrice().price, relatedItem,
															item, entry.getKey(), sizeShelfPCT, strategyDTO);
													Set<Double> priceRangeSet = new HashSet<>();
													for (Double pp : priceRangeArr) {
														priceRangeSet.add(pp);
													}

													if (priceRangeSet.size() > 0) {
														PRRange newRange = new PRRange();
														double minPr = Collections.min(priceRangeSet);
														double maxPr = Collections.max(priceRangeSet);
														newRange.setStartVal(minPr);
														newRange.setEndVal(maxPr);

														Double[] priceRangeArrNew = applyConstraints(item, newRange,
																strategyDTO);

														for (Double pp : priceRangeArrNew) {
															possiblePricePoints.add(pp);
														}

													}
												}
											} else {
												if (pricePoints != null && pricePoints.length > 0) {
													Set<Double> priceRangeSet = new HashSet<>();
													for (Double pricePoint : pricePoints) {
														double[] priceRangeArr = getNewRange(1, pricePoint, relatedItem,
																item, entry.getKey(), sizeShelfPCT, strategyDTO);
														for (Double pp : priceRangeArr) {
															priceRangeSet.add(pp);
														}
													}

													if(priceRangeSet.size() > 0) {
														PRRange newRange = new PRRange();
														double minPr = Collections.min(priceRangeSet);
														double maxPr = Collections.max(priceRangeSet);
														newRange.setStartVal(minPr);
														newRange.setEndVal(maxPr);

														Double[] priceRangeArr = applyConstraints(item, newRange, strategyDTO);

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
						}
					}

					// add to pricePointList
					pricePointList.addAll(addPricePoints(possiblePricePoints));

					// add prev and next previous price points of first and last price point
					if (possiblePricePoints.size() > 0 && !item.isCurrentPriceRetained()) {
						List<Double> preAndNextPricePoint = pricingEngineService.addPreviousAndNextPricePoints(possiblePricePoints.get(0),
								possiblePricePoints.get(possiblePricePoints.size() - 1), item.getStrategyDTO(), noOfLowerPoints, noOfHigherPoints);
						pricePointList.addAll(addPricePoints(preAndNextPricePoint));
					}

				}

				// add price points
				if (item.getPriceRange() != null) {
					for (Double pricePoint : item.getPriceRange()) {
						if (pricePoint > 0) {
							PricePointDTO pp = new PricePointDTO();
							pp.setRegPrice(pricePoint);
							pp.setRegQuantity(1);
							pricePointList.add(pp);
						}
					}

					if(!item.isCurrentPriceRetained()) {
						// add prev and next previous price points of first and last price point
						List<Double> preAndNextPricePoint = pricingEngineService.addPreviousAndNextPricePoints(item, noOfLowerPoints, noOfHigherPoints);
						pricePointList.addAll(addPricePoints(preAndNextPricePoint));	
					}
				}

				// add current reg price
				addCurRegPriceToPrediction(item, pricePointList);

				// add override price point
				addOverridePricePoint(item, pricePointList);

				// Remove duplicate price points -- added on 24th Aug 2015
				List<PricePointDTO> distinctPricePointList = new ArrayList<PricePointDTO>();
				for (PricePointDTO pricePoint : pricePointList) {
					if (!isPricePointExists(distinctPricePointList, pricePoint)) {
						distinctPricePointList.add(pricePoint);
					}
				}
				pItem.pricePoints = distinctPricePointList;

				if (!usePrediction || !pItem.isSendToPrediction()) {
					if (pItem.pricePoints != null) {
						for (PricePointDTO pricePointDTO : pItem.pricePoints) {
							pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
							pricePointDTO.setPredictedMovement(item.getAvgMovement());
						}
					}
				}
				ItemKey itemKey = PRCommonUtil.getItemKey(item.getItemCode(), item.isLir());
				itemsForPrediction.put(itemKey, pItem);

				// for debugging alone
				String itemsPricePoints = "";
				for (PricePointDTO pricePoint : distinctPricePointList) {
					itemsPricePoints = itemsPricePoints + ",(" + pricePoint.getRegQuantity() + "/" + pricePoint.getRegPrice() + ")";
				}
				//logger.debug("Item code: " + itemKey.toString() + " Price points:" + itemsPricePoints);
			}
		}
		//logger.info("# of itemw with no mov and less Revenue count:"+ count);

		// 18th Mar 2015 -- Make sure each price points has all the lig members, if not add it
		addMissingPricePointToLigMembers(retLirMap, itemsForPrediction, usePrediction);

		// Copy list which has missing price points
		predictionItems.addAll(itemsForPrediction.values());

		// Filter items which can be passed to prediction
		// This filter is added to improve performance and ignore passing items not moved in last X weeks
		List<PredictionItemDTO> itemsToBePassedToPrediction = predictionItems.stream()
				.filter(p ->  p.isSendToPrediction()).collect(Collectors.toList());
		/*
		 * List<PredictionItemDTO> itemsToBePassedToPrediction =
		 * predictionItems.stream() .filter(p -> ! p.isNorecentWeeksMov() &&
		 * p.isSendToPrediction()).collect(Collectors.toList());
		 */
		
		predictionInput.predictionItems = itemsToBePassedToPrediction;
		
		// Changes for performance improvement
		// Added for performance improvement. Added on 03/13/2020 by Pradeep
		// populate prediction status for items not moved in last X weeks
		/*
		 * List<PredictionItemDTO> itemsToBeUpdatedWithNoMovPredictionStatus =
		 * predictionItems.stream() .filter(p ->
		 * p.isNorecentWeeksMov()).collect(Collectors.toList());
		 */
		
	
		logger.info("predictRegPricePoints() - Total # of Items: " + predictionItems.size());
		
		/*
		 * logger.
		 * info("predictRegPricePoints() - # of Items with no recent weeks movement: " +
		 * itemsToBeUpdatedWithNoMovPredictionStatus.size());
		 */
		
		// Changes for performance improvement
		// Added for performance improvement. Added on 04/07/2020 by Karishma
		// populate prediction status for items which has less per Store Revenue
		List<PredictionItemDTO> itemsToBeUpdatedWithLessAvgRevenue = predictionItems.stream()
				.filter(p -> !p.isSendToPrediction()).collect(Collectors.toList());
		
		logger.info("predictRegPricePoints() - # of Items contributing less Revenue:"
				+ itemsToBeUpdatedWithLessAvgRevenue.size());
		
		logger.info("predictRegPricePoints() - # of Items to be passed to prediction: " + itemsToBePassedToPrediction.size());


		// Invoke prediction service only when there are items to predict
		if (predictionInput.predictionItems.size() > 0) {
//			PredictionService service = new PredictionService(movementData, itemPriceHistory );
			try {
				// If it is average movement, it is already set above
				if (!usePrediction) {
					logger.info("Using Avg Mov for Location: " + inputDTO.getLocationLevelId() + "-" + inputDTO.getLocationId() + " , Product: "
							+ inputDTO.getProductLevelId() + "-" + inputDTO.getProductId());
					predictionService.convertPredictionInputToOutput(predictionInput, predictionOutput);
				} else {
					logger.info("Using Prediction for Location: " + inputDTO.getLocationLevelId() + "-" + inputDTO.getLocationId() + " , Product: "
							+ inputDTO.getProductLevelId() + "-" + inputDTO.getProductId());
					predictionOutput = predictionService.predictMovement(conn, predictionInput, executionTimeLogs, "PREDICTION_PRICE_REC", isOnline);
				}
			} catch (GeneralException e) {
				predictionOutput = new PredictionOutputDTO();
				logger.error("Error while predicting movement");
			}
		}

		
		//updateNoRecentWeeksMovPredStatus(itemsToBeUpdatedWithNoMovPredictionStatus);
		updateLessAvgRevenuePredStatus(itemsToBeUpdatedWithLessAvgRevenue);
		// Merge items not passed to prediction in prediction output
		if(predictionOutput.predictionItems != null) {
			//predictionOutput.predictionItems.addAll(itemsToBeUpdatedWithNoMovPredictionStatus);
			predictionOutput.predictionItems.addAll(itemsToBeUpdatedWithLessAvgRevenue);	

		}
		
		// Changes ends
		
		
		// put/update all items's price points prediction in the hashmap
		updatePredMov(itemList, predictionOutput);
	}


	private List<PricePointDTO> addPricePoints(List<Double> pricePointList) {
		List<PricePointDTO> finalPricePoints = new ArrayList<PricePointDTO>();
		for (Double price : pricePointList) {
			if (price > 0) {
				PricePointDTO pricePoint = new PricePointDTO();
				pricePoint.setRegPrice(price);
				pricePoint.setRegQuantity(1);
				finalPricePoints.add(pricePoint);
			}
		}
		return finalPricePoints;
	}

	/***
	 * Predicted movement is updated to item's predicted movement collection. Items all price point's predicted movement will be
	 * saved here
	 * 
	 * @param prItemList
	 * @param predictionOutputDto
	 */
	private void updatePredMov(List<PRItemDTO> prItemList, PredictionOutputDTO predictionOutputDto) {
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = convertPredictionOutputDTOToPredictionOutputMap(predictionOutputDto);

		// If prediction output returned any result
		if (predictionOutputMap != null && predictionOutputMap.size() > 0) {
			// Update each price points of items with predicted movement
			// Loop all items
			for (PRItemDTO itemDto : prItemList) {
				//if (!itemDto.isLir() && itemDto.getPriceRange() != null && itemDto.getPriceRange().length > 0) {
				//10th Mar 2017, commented as store recommended items predictions are not updated, as priceRange 
				//would be null for this items, because those prices found on common store prices
				if (!itemDto.isLir()) {
					List<PricePointDTO> pricePointListFromPrediction = predictionOutputMap.get(itemDto.getItemCode());
					// Loop all price points of the item
					if (pricePointListFromPrediction != null) {
						// logger.debug("Item Code in predict Price " + itemDto.getItemCode());
						// Loop price point of prediction
						for (PricePointDTO pricePointFromPrediction : pricePointListFromPrediction) {
							MultiplePrice multiplePrice = new MultiplePrice(pricePointFromPrediction.getRegQuantity(),
									pricePointFromPrediction.getRegPrice());
							// itemDto.addPriceMovementPrediction(pricePointFromPrediction.getRegPrice(),
							// pricePointFromPrediction);
							itemDto.addRegPricePrediction(multiplePrice, pricePointFromPrediction);
							
							// Set override predicted movement
							// Changes for including past overrides 
							// Changes done by Pradeep on 03/06/2019
							if(itemDto.getOverriddenRegularPrice()!= null) {
								if(itemDto.getOverriddenRegularPrice().equals(multiplePrice)) {
									itemDto.setOverridePredictedMovement(pricePointFromPrediction.getPredictedMovement());
									itemDto.setOverridePredictionStatus(pricePointFromPrediction.getPredictionStatus().getStatusCode());
								}
							}
						}
						// Save Current Reg Price Predicted Movement
						// setCurRegPricePredictedMovement(itemDto, pricePointListFromPrediction);
					} else {
						logger.warn("No output from prediction engine for item code - " + itemDto.getItemCode());
					}
				}
			}
		} else {
			logger.warn("No output from prediction service");
		}
	}

	/**
	 * Set Predicted Movement for Current Reg Price
	 * 
	 * @param itemDto
	 * @param pricePointListFromPrediction
	 */
	public void setCurRegPricePredictedMovement(PRItemDTO itemDto, List<PricePointDTO> pricePointListFromPrediction) {
		Integer curRegMPack = 1;
		Double curRegPrice = itemDto.getRegPrice();
		Double formattedCurRegPrice, formattedCurRegPrice1;

		if (itemDto.getRegMPack() != null) {
			curRegMPack = itemDto.getRegMPack() > 0 ? itemDto.getRegMPack() : 1;
		}
		curRegPrice = curRegMPack > 1 ? itemDto.getRegMPrice() : itemDto.getRegPrice();

		if (itemDto.getCurRegPricePredictedMovement() <= 0 && curRegPrice != null) {
			for (PricePointDTO pricePointFromPrediction : pricePointListFromPrediction) {
				formattedCurRegPrice1 = new Double(twoDigitFormat.format(curRegPrice));
				formattedCurRegPrice = new Double(twoDigitFormat.format(pricePointFromPrediction.getRegPrice()));
				// if(pricePointFromPrediction.getRegPrice().equals(curRegPrice)
				if ((formattedCurRegPrice1.equals(formattedCurRegPrice)) && pricePointFromPrediction.getRegQuantity() == curRegMPack) {
					itemDto.setCurRegPricePredictedMovement(pricePointFromPrediction.getPredictedMovement());
					itemDto.setCurRegPricePredictionStatus(pricePointFromPrediction.getPredictionStatus().getStatusCode());
					// logger.debug("Current Reg Price for item code: " + itemDto.getItemCode() + ", Predicted Movement is:"
					// + curRegMPack + "/" + curRegPrice + " -- " + itemDto.getCurRegPricePredictedMovement());
					break;
				}
			}
		} else {
			// logger.debug("Current Reg Price is null for item code: " + itemDto.getItemCode() );
		}
	}

	/**
	 * Invoke prediction engine with price points
	 * 
	 * @param prItemList
	 *            Item List
	 * @param recInputDTO
	 *            Strategy DTO return
	 */
//	public PredictionOutputDTO invokePredictionEngine(Connection conn, List<PRItemDTO> prItemList, PRStrategyDTO inputDTO, String sourceType,
//			List<PRProductGroupProperty> productProperties, List<ExecutionTimeLog> executionTimeLogs,
//			PRRecommendationRunHeader recommendationRunHeader, boolean isOnline, HashMap<Integer, List<PRItemDTO>> retLirMap) {
//		PredictionInputDTO predictionInput = new PredictionInputDTO();
//		PredictionOutputDTO predictionOutput = new PredictionOutputDTO();
//		ArrayList<PredictionItemDTO> predictionItems = new ArrayList<PredictionItemDTO>();
//		// This hashmap is used in making sure all lig members are send with same number of price points
//		HashMap<ItemKey, PredictionItemDTO> itemsForPrediction = new HashMap<ItemKey, PredictionItemDTO>();
//		PricingEngineService pricingEngineService = new PricingEngineService();
//		Boolean skipItem = false;
//		boolean usePrediction = true;
//		String predictionType;
//		// Decide between avg movement or prediction. (configuration from table PR_PRODUCT_GROUP_PROPERTY)
//		// For store always use avg movement
//		usePrediction = PricingEngineHelper.isUsePrediction(productProperties, inputDTO.getProductLevelId(), inputDTO.getProductId(),
//				inputDTO.getLocationLevelId(), inputDTO.getLocationId());
//		if (sourceType == "MARGIN_OPPURTUNITY")
//			predictionType = "PREDICTION_MAR_OPP";
//		else
//			predictionType = "PREDICTION_PRICE_REC";
//
//		logger.debug("Prediction Type for Location: " + inputDTO.getLocationLevelId() + "-" + inputDTO.getLocationId() + " , Product: "
//				+ inputDTO.getProductLevelId() + "-" + inputDTO.getProductId() + " " + (usePrediction ? "Prediction" : "8 Week's Avg Movement"));
//
//		fillBasicInfoOfPredictionInputDTO(predictionInput, inputDTO, recommendationRunHeader);
//
//		int noOfLowerPoints = 0, noOfHigherPoints = 0;
//
//		// 26th Mar 2016, In order to improve strategy what-if performance, no of price point for prediction
//		// is reduced by not finding the next and previous price points
//		if (!recommendationRunHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
//			noOfLowerPoints = Integer.parseInt(PropertyManager.getProperty("MAR_OPP_NO_OF_LOWER_PRICE_POINTS", "1"));
//			noOfHigherPoints = Integer.parseInt(PropertyManager.getProperty("MAR_OPP_NO_OF_HIGHER_PRICE_POINTS", "1"));
//		}
//
//		// pick which ever is higher, so that price points are predicted here itself
//		// if(noOfLowerPoints > noOfHigherPoints)
//		// noOfHigherPoints = noOfLowerPoints;
//		// else
//		// noOfLowerPoints = noOfHigherPoints;
//
//		// Populate items to send for prediction
//		for (PRItemDTO item : prItemList) {
//			skipItem = false;
//
//			if (sourceType == "MARGIN_OPPURTUNITY") {
//				// Skip LIG Items
//				if (!item.isLir()) {
//					skipItem = false;
//				} else {
//					skipItem = true;
//					// logger.debug("Not sending item " + item.getItemCode() + " isLir " + item.isLir() + " to prediction
//					// engine");
//					if (item.getOppurtunities() != null) {
//					}
//					// logger.debug("Opp price points: " + item.getOppurtunities().size());
//				}
//			} else if (sourceType == "PRICE_RANGE") {
//				// Skip Price Group Items, Skip item with no Price Range
//				logger.debug("Item Code:" + item.getItemCode() + "pg id:" + (item.getPgData() != null ? item.getPgData().getPriceGroupId() : 0));
//
//				// Bug Fix (29th Apr 2016): if the prediction is average movement, then assign
//				// avg movement as prediction even for price group items as avg mov is not going to change
//				// based on the price. Predicted movement were not shown for price group related item,
//				// as those items predictions are updated during margin oppor and margin oppor will be
//				// done when avg mov is used as prediction (issue id: PROM-1012)
//				if (!usePrediction && item.getPriceRange() != null) {
//					skipItem = false;
//				}
//				// non price-group items
//				else if (item.getPgData() == null && item.getPriceRange() != null) {
//					skipItem = false;
//				}
//				// Uncomment this block to send price group items with no related item to prediction engine
//				else if (item.getPgData() != null && item.getPriceRange() != null) {
//					if (item.getPgData().getRelationList() == null || item.getPgData().getRelationList().size() == 0)
//						skipItem = false;
//					else {
//						boolean isRelatedItemPresent = false;
//						TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = item.getPgData().getRelationList();
//						for (Collection<PRPriceGroupRelatedItemDTO> relatedItems : relationList.values()) {
//							for (PRPriceGroupRelatedItemDTO relatedItem : relatedItems) {
//								if (relatedItem.getRelatedItemCode() > 0) {
//									isRelatedItemPresent = true;
//								}
//							}
//						}
//						if (isRelatedItemPresent) {
//							skipItem = true;
//							if (item.getPriceRange() != null)
//								logger.debug("Item Price Range Size: " + item.getPriceRange().length);
//							if (item.getPgData() != null)
//								logger.debug("Price Group Id: " + item.getPgData().getPriceGroupId());
//						}
//					}
//					if (!skipItem) {
//						logger.debug("Non related Price Group item is included for prediction: " + item.getItemCode());
//					}
//				}
//				// Uncomment this block to send price group items with no related item to prediction engine - Ends
//				else {
//					skipItem = true;
//					// logger.debug("Item excluded for prediction as it is part of Price Group or it has no Price Range : " +
//					// item.getItemCode());
//					if (item.getPriceRange() != null)
//						logger.debug("Item Price Range Size: " + item.getPriceRange().length);
//					if (item.getPgData() != null)
//						logger.debug("Price Group Id: " + item.getPgData().getPriceGroupId());
//				}
//			}
//
//			if (!skipItem) {
//				PredictionItemDTO pItem = new PredictionItemDTO();
//				List<PricePointDTO> pricePointList = new ArrayList<PricePointDTO>();
//
//				pItem.upc = item.getUpc();
//				// Lir Id will be populated if item is LIR representation. Item Code will be populated if not
//				if (item.isLir())
//					pItem.itemCodeOrLirId = item.getRetLirId();
//				else
//					pItem.itemCodeOrLirId = item.getItemCode();
//
//				pItem.lirInd = item.isLir();
//
//				String pricePoints = "";
//				if (sourceType == "MARGIN_OPPURTUNITY") {
//					if (item.getOppurtunities() != null) {
//						for (PricePointDTO pricePoint : item.getOppurtunities()) {
//							pricePointList.add(pricePoint);
//						}
//						// Commented on 24th Aug 2015 extra price points are added
//						// to opportunity and those are considered during margin oppor,
//						// which is not supposed to be considered
//						// pricePointList = item.getOppurtunities();
//					}
//					// Add more lower and one higher price point to prediction, but these
//					// price point will not be considered while marking margin opportunity.
//					// this is done not to call the prediction for objective Highest Margin $,
//					// where X lower and Y higher price point are used in determining the final price
//
//					String additionalPricePoint = "";
//					if (item.getRecommendedRegPrice() != null) {
//						// List<Double> preAndNextPricePoint = pricingEngineService.addPreviousAndNextPricePoints(
//						// item.getRecommendedRegPrice(), item.getRecommendedRegPrice(), item.getStrategyDTO(),
//						// noOfLowerPoints, noOfHigherPoints);
//						List<Double> preAndNextPricePoint = pricingEngineService.addPreviousAndNextPricePoints(item.getRecommendedRegPrice().price,
//								item.getRecommendedRegPrice().price, item.getStrategyDTO(), noOfLowerPoints, noOfHigherPoints);
//						for (Double price : preAndNextPricePoint) {
//							if (price > 0) {
//								PricePointDTO pricePoint = new PricePointDTO();
//								pricePoint.setRegPrice(price);
//								pricePoint.setRegQuantity(1);
//								pricePointList.add(pricePoint);
//								additionalPricePoint = additionalPricePoint + "," + price;
//							}
//						}
//					}
//					// logger.debug("Additional price point during margin oppor prediciton:" + additionalPricePoint);
//				} else if (sourceType == "PRICE_RANGE") {
//					pricePoints = "Actual Price Points:";
//					for (double price : item.getPriceRange()) {
//						PricePointDTO pricePoint = new PricePointDTO();
//						pricePoint.setRegPrice(price);
//						pricePoint.setRegQuantity(1);
//						pricePointList.add(pricePoint);
//						pricePoints = pricePoints + "," + price;
//					}
//					List<Double> preAndNextPricePoint = pricingEngineService.addPreviousAndNextPricePoints(item, noOfLowerPoints, noOfHigherPoints);
//					pricePoints = pricePoints + "; Previous And Next Price Points: ";
//					for (Double price : preAndNextPricePoint) {
//						if (price > 0) {
//							PricePointDTO pricePoint = new PricePointDTO();
//							pricePoint.setRegPrice(price);
//							pricePoint.setRegQuantity(1);
//							pricePointList.add(pricePoint);
//							pricePoints = pricePoints + "," + price;
//						}
//					}
//					// logger.debug("Price Points for item :" + item.getItemCode() + " sent for Prediction: " + pricePoints);
//				}
//				addCurRegPriceToPrediction(item, pricePointList);
//
//				// Remove duplicate price points -- added on 24th Aug 2015
//				List<PricePointDTO> distinctPricePointList = new ArrayList<PricePointDTO>();
//				for (PricePointDTO pricePoint : pricePointList) {
//					if (!isPricePointExists(distinctPricePointList, pricePoint)) {
//						distinctPricePointList.add(pricePoint);
//					}
//				}
//				pItem.pricePoints = distinctPricePointList;
//
//				// pItem.pricePoints = pricePointList;
//				// logger.debug("No of price points for item " + item.getItemCode()
//				// + " are " + pricePointList.size() + " price points: " + pricePoints);
//				if (item.isLir()) {
//					logger.warn("Item " + item.getItemCode() + " should not be reaching here");
//				}
//				if (pItem.pricePoints.size() > 0) {
//					// Assign 8week's average movement if it is store, as Prediction engine doesn't support prediction at store
//					// level
//					// For certain categories, 8'weeks avg needs to be used, this is controlled through the configuration in the
//					// table
//
//					if (!usePrediction) {
//						// logger.debug("8 Week Average Movement for Store : "
//						// + predictionInput.locationId + ",Item :" + item.getItemCode() + ",Movement: " + item.getAvgMovement());
//
//						for (PricePointDTO pricePoint : pricePointList) {
//							pricePoint.setPredictionStatus(PredictionStatus.SUCCESS);
//							// 8th Apr 2016, don't round predicted movement
//							// pricePoint.setPredictedMovement(Double.valueOf(Math.round(item.getAvgMovement())));
//							pricePoint.setPredictedMovement(item.getAvgMovement());
//						}
//					}
//					// No need to find margin opportunity for zone items, for which there is store level recommendation
//					ItemKey itemKey = PRCommonUtil.getItemKey(item.getItemCode(), item.isLir());
//					if (sourceType == "MARGIN_OPPURTUNITY") {
//						if (!item.getIsMostCommonStorePriceRecAsZonePrice()) {
//							// predictionItems.add(pItem);
//							itemsForPrediction.put(itemKey, pItem);
//						}
//					} else {
//						// predictionItems.add(pItem);
//						itemsForPrediction.put(itemKey, pItem);
//					}
//				}
//
//			}
//		}
//		// 18th Mar 2015 -- Make sure each price points has all the lig members, if not add it
//		addMissingPricePointToLigMembers(retLirMap, itemsForPrediction, usePrediction);
//
//		// 20th Jul 2016 -- Change as part of "Handling multiple presto items with same retailer item codes"
//		// Check if any same retailer item code available in itemsForPrediction
//		// Get distinct price points of all such items, for which there is no prediction
//
//		// Copy list which has missing price points
//		predictionItems.addAll(itemsForPrediction.values());
//		// Loop each lig and its member add price points for items which is not present
//		predictionInput.predictionItems = predictionItems;
//		logger.debug("Number of items for prediction " + predictionItems.size());
//
//		// Invoke prediction service only when there are items to predict
//		if (predictionInput.predictionItems.size() > 0) {
//			PredictionService service = new PredictionService();
//			try {
//				// The 8 weeks average movement is already added for each price point,
//				// just convert Prediction Input to Prediction Output
//				if (!usePrediction) {
//					logger.info("Using Avg Mov for Location: " + inputDTO.getLocationLevelId() + "-" + inputDTO.getLocationId() + " , Product: "
//							+ inputDTO.getProductLevelId() + "-" + inputDTO.getProductId());
//					service.convertPredictionInputToOutput(predictionInput, predictionOutput);
//				} else {
//					logger.info("Using Prediction for Location: " + inputDTO.getLocationLevelId() + "-" + inputDTO.getLocationId() + " , Product: "
//							+ inputDTO.getProductLevelId() + "-" + inputDTO.getProductId());
//					predictionOutput = service.predictMovement(conn, predictionInput, executionTimeLogs, predictionType, isOnline);
//				}
//			} catch (GeneralException e) {
//				predictionOutput = new PredictionOutputDTO();
//				logger.error("Error while predicting movement");
//			}
//		}
//
//		// put/update all items's price points prediction in the hashmap
//		updatePredMov(prItemList, predictionOutput);
//
//		return predictionOutput;
//	}

	private boolean isPricePointExists(List<PricePointDTO> pricePointList, PricePointDTO pricePointDTO) {
		boolean isPricePointExists = false;
		Double formattedCurRegPrice, formattedCurRegPrice1;

		formattedCurRegPrice1 = new Double(twoDigitFormat.format(pricePointDTO.getRegPrice()));
		MultiplePrice multiplePriceToCheck = new MultiplePrice(pricePointDTO.getRegQuantity(), formattedCurRegPrice1);
		// logger.debug("multiplePriceToCheck:" + multiplePriceToCheck.toString());
		for (PricePointDTO pricePoint : pricePointList) {
			formattedCurRegPrice = new Double(twoDigitFormat.format(pricePoint.getRegPrice()));
			MultiplePrice multiplePrice = new MultiplePrice(pricePoint.getRegQuantity(), formattedCurRegPrice);
			// logger.debug("multiplePrice:" + multiplePrice.toString());
			if (multiplePrice.equals(multiplePriceToCheck)) {
				isPricePointExists = true;
				break;
			}
		}
		return isPricePointExists;
	}

	private void addMissingPricePointToLigMembers(HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<ItemKey, PredictionItemDTO> itemsForPrediction,
			boolean usePrediction) {
		// keep ret lir id and its distinct price points
		HashMap<Integer, Set<MultiplePrice>> distinctPricePoints = new HashMap<Integer, Set<MultiplePrice>>();
		// Loop each lig and its members
		for (Entry<Integer, List<PRItemDTO>> ligAndItsMembers : retLirMap.entrySet()) {
			int retLirId = ligAndItsMembers.getKey();
			List<PRItemDTO> ligMembers = ligAndItsMembers.getValue();
			Set<MultiplePrice> distPricePoints;

			if (distinctPricePoints.get(retLirId) != null)
				distPricePoints = distinctPricePoints.get(retLirId);
			else
				distPricePoints = new HashSet<MultiplePrice>();

			// Loop each lig members
			for (PRItemDTO ligMember : ligMembers) {
				ItemKey itemKey = PRCommonUtil.getItemKey(ligMember.getItemCode(), false);
				// If item present in prediction item list
				if (itemsForPrediction.get(itemKey) != null) {
					// get all its price point and put it
					PredictionItemDTO predictionItemDTO = itemsForPrediction.get(itemKey);
					if (predictionItemDTO.pricePoints != null) {
						for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
							MultiplePrice multiplePrice = new MultiplePrice(pricePointDTO.getRegQuantity(), pricePointDTO.getRegPrice());
							distPricePoints.add(multiplePrice);
						}
					}
				}
			}
			// String disPricePoints = "";
			// for (Double pp : distPricePoints) {
			// disPricePoints = disPricePoints + "," + pp;
			// }
			// logger.debug("Distinct Price Points for Ret Lir Id:" + retLirId + "--" + disPricePoints);
			distinctPricePoints.put(retLirId, distPricePoints);
		}

		// Loop each lig
		for (Entry<Integer, List<PRItemDTO>> ligAndItsMembers : retLirMap.entrySet()) {
			int retLirId = ligAndItsMembers.getKey();
			List<PRItemDTO> ligMembers = ligAndItsMembers.getValue();
			Set<MultiplePrice> ligDistPricePts = distinctPricePoints.get(retLirId);
			// Loop each lig members
			for (PRItemDTO ligMember : ligMembers) {
				ItemKey itemKey = PRCommonUtil.getItemKey(ligMember.getItemCode(), false);
				// If lig member present in prediction item list
				if (itemsForPrediction.get(itemKey) != null) {
					PredictionItemDTO predictionItemDTO = itemsForPrediction.get(itemKey);
					if (predictionItemDTO.pricePoints != null && predictionItemDTO.pricePoints.size() > 0) {
						// check if lig member has all price points
						for (MultiplePrice ligDistPricePoint : ligDistPricePts) {
							boolean pricePointAlreadyPresent = false;
							for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
								if (ligDistPricePoint.price.equals(pricePointDTO.getRegPrice())
										&& ligDistPricePoint.multiple.equals(pricePointDTO.getRegQuantity())) {
									pricePointAlreadyPresent = true;
									break;
								}
							}

							// If that price point is not set for the item, then
							// add it
							if (!pricePointAlreadyPresent) {
								//logger.debug("Price Point: " + ligDistPricePoint + " is added for item: " + itemKey.getItemCodeOrRetLirId());
								PricePointDTO pricePointDTO = new PricePointDTO();
								pricePointDTO.setRegPrice(ligDistPricePoint.price);
								pricePointDTO.setRegQuantity(ligDistPricePoint.multiple);
								if (!usePrediction) {
									pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
									// take first price point movement as all price point will have same movement
									// (because its not predicted movement,but actual item's average movement)
									pricePointDTO.setPredictedMovement(predictionItemDTO.pricePoints.get(0).getPredictedMovement());
								}
								predictionItemDTO.pricePoints.add(pricePointDTO);
							}
						}
					}
				}
			}
		}
	}

	private void addCurRegPriceToPrediction(PRItemDTO item, List<PricePointDTO> pricePointList) {
		Boolean isCurRegPriceIsInPriceRange = false; // Cur reg price prediction is always required, even if it is not part of
														// price range
		Integer curRegMPack = 1;
		Double formattedCurRegPrice, formattedCurRegPrice1;
		Double curRegPrice;

		// Check if Cur Reg Price movement is already predicted during the Price Range Prediction
		if (item.getCurRegPricePredictedMovement() == null || item.getCurRegPricePredictedMovement() <= 0) {
			curRegPrice = item.getRegPrice();
			if (item.getRegMPack() != null) {
				curRegMPack = item.getRegMPack() > 0 ? item.getRegMPack() : 1;
			} else {
				curRegMPack = 1;
			}
			curRegPrice = curRegMPack > 1 ? item.getRegMPrice() : item.getRegPrice();

			for (PricePointDTO pricePoint : pricePointList) {
				// Check if cur price is already added
				// If the cur reg mpack is greater than 1 then send it separately
				if (curRegMPack == 1 && curRegPrice != null) {
					formattedCurRegPrice1 = new Double(twoDigitFormat.format(curRegPrice));
					formattedCurRegPrice = new Double(twoDigitFormat.format(pricePoint.getRegPrice()));
					if (formattedCurRegPrice.equals(formattedCurRegPrice1)) {
						// logger.debug("Current Reg Price is part of Price Range, Cur Reg Price: " + formattedCurRegPrice1 +
						// "Cur Reg Price in Price Point: " + formattedCurRegPrice);
						pricePoint.setIsCurrentPrice(true);
						isCurRegPriceIsInPriceRange = true;
						break;
					}
				}
			}

			// If current reg price is not already added
			// 16th Jun 2016, bug fix: if current reg price is 0, it is also send to prediction
			// don't send to prediction, if it is 0
			// if (!isCurRegPriceIsInPriceRange && item.getRegPrice() != null) {
			if (!isCurRegPriceIsInPriceRange && curRegPrice != null && curRegPrice > 0) {
				PricePointDTO pricePoint = new PricePointDTO();
				pricePoint.setRegQuantity(curRegMPack);
				pricePoint.setRegPrice(curRegPrice);
				pricePoint.setIsCurrentPrice(true);
				pricePointList.add(pricePoint);
				// logger.debug("Current Reg Price is not part of Price Range/ Opportunity and it is passed only for Prediction:"
				// + item.getItemCode() + "--" + pricePoint.getRegQuantity() + "/" + pricePoint.getRegPrice());
			}
		}
	}

	private void fillBasicInfoOfPredictionInputDTO(PredictionInputDTO predictionInput, PRStrategyDTO inputDTO,
			PRRecommendationRunHeader recommendationRunHeader) {
		predictionInput.recommendationRunId = recommendationRunHeader.getRunId();
		predictionInput.locationLevelId = inputDTO.getLocationLevelId();
		predictionInput.locationId = inputDTO.getLocationId();
		predictionInput.productLevelId = inputDTO.getProductLevelId();
		predictionInput.productId = inputDTO.getProductId();
		predictionInput.startCalendarId = inputDTO.getStartCalendarId();
		predictionInput.endCalendarId = inputDTO.getEndCalendarId();
		predictionInput.startWeekDate = inputDTO.getStartDate();
		predictionInput.endWeekDate = inputDTO.getEndDate();
	}

	private HashMap<Integer, List<PricePointDTO>> convertPredictionOutputDTOToPredictionOutputMap(PredictionOutputDTO predictionOutput) {
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = null;
		if (predictionOutput.predictionItems != null) {
			predictionOutputMap = new HashMap<Integer, List<PricePointDTO>>();
			for (PredictionItemDTO pItem : predictionOutput.predictionItems) {
				// logger.debug("Item Placed in predictionOutputMap: " + pItem.itemCodeOrLirId);
				predictionOutputMap.put(pItem.itemCodeOrLirId, pItem.pricePoints);
			}
		} else {
			logger.warn("No output from prediction service");
		}
		return predictionOutputMap;
	}

	// public void applyObjective(List<PRItemDTO> prItemList, HashMap<Integer, HashMap<String, RetailPriceDTO>>
	// itemZonePriceHistory,
	// RetailCalendarDTO curCalDTO) throws GeneralException {
	// ObjectiveService objectiveService = new ObjectiveService();
	//
	// for(PRItemDTO itemDTO : prItemList){
	// if (itemDTO.getIsPrePriced() == 1 || itemDTO.getIsLocPriced() == 1) {
	// //TODO:: NU: why predicted movement is assigned here?
	// //this function supposed to apply only the objective
	// if (itemDTO.getPriceRange() != null && itemDTO.getPriceRange().length > 0) {
	// if (itemDTO.getRegPricePredictionMap() != null && itemDTO.getRegPricePredictionMap().size() > 0
	// &&
	// // itemDTO.getPriceMovementPrediction().get(itemDTO.getPriceRange()[0]).getPredictionStatus()
	// // != null) {
	// itemDTO.getRegPricePredictionMap().get(itemDTO.getRecommendedRegPrice().price)
	// .getPredictionStatus() != null) {
	// PricePointDTO pricePointDTO = itemDTO.getRegPricePredictionMap().get(
	// itemDTO.getRecommendedRegPrice().price);
	// // Set predicted movement only when prediction status is
	// // success
	// if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
	// .getStatusCode()) {
	// itemDTO.setPredictedMovement(pricePointDTO.getPredictedMovement());
	// }
	// // Set prediction status
	// itemDTO.setPredictionStatus(pricePointDTO.getPredictionStatus().getStatusCode());
	// } else {
	// logger.debug("Prediction status is null for item " + itemDTO.getItemCode());
	// }
	// } else {
	// logger.debug("Prediction status is null for item " + itemDTO.getItemCode());
	// }
	// }
	// //20th Dec 2016, check if the price point has prediction, this is needed
	// //when price group related items are re-recommended again
	// else if (itemDTO.getRegPricePredictionMap() != null && itemDTO.getRegPricePredictionMap().size() > 0
	// && hasAtleastOnePrediction(itemDTO)) {
	// Double cost = itemDTO.getCost();
	//
	// //NU:: 26th Nov 2016, filter price points further
	// logger.debug("------Filtering price range for item:" + itemDTO.getItemCode() + "--------");
	// Double[] filteredRange = objectiveService.filterFinalPriceRanges(itemZonePriceHistory, itemDTO, curCalDTO);
	// logger.debug("---------------------------------------------------------------------------");
	// //for debugging
	// /*logger.debug("itemDTO.getPriceMovementPrediction().size():" + itemDTO.getPriceMovementPrediction().size());
	// for (Map.Entry<Double, PricePointDTO> outEntry : itemDTO.getPriceMovementPrediction().entrySet()) {
	// logger.debug("Key:" + outEntry.getKey().toString() + ",values: " + (outEntry.getValue() != null ?
	// outEntry.getValue().toString() : ""));
	// }*/
	//
	//// Double price = applyObjective(itemDTO.getObjectiveTypeId(), itemDTO.getPriceRange(),
	// itemDTO.getPriceMovementPrediction(), cost,
	//// itemDTO.getRegPrice());
	// Double price = objectiveService.applyObjective(itemDTO, filteredRange);
	// // itemDTO.setRecommendedRegMultiple(1);
	// // itemDTO.setRecommendedRegPrice(price);
	// itemDTO.setRecommendedRegPrice(new MultiplePrice(1, price));
	// logger.debug("1. Price for item " + itemDTO.getItemCode() + " after applying objective " +
	// PRCommonUtil.getPriceForLog(itemDTO.getRecommendedRegPrice()));
	//
	// if (itemDTO.getRegPricePredictionMap().get(price) != null) {
	// if (itemDTO.getRegPricePredictionMap().get(price).getPredictionStatus() != null) {
	// // Set predicted movement only when prediction status is success
	// if (itemDTO.getRegPricePredictionMap().get(price).getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
	// .getStatusCode()) {
	// itemDTO.setPredictedMovement(itemDTO.getRegPricePredictionMap().get(price).getPredictedMovement());
	// }
	// // Set prediction status
	// itemDTO.setPredictionStatus(itemDTO.getRegPricePredictionMap().get(price).getPredictionStatus().getStatusCode());
	// } else {
	// logger.debug("Prediction status is null for item " + itemDTO.getItemCode());
	// }
	// } else {
	// logger.debug("No Prediction for price " + price);
	// }
	//
	// }
	// // To handle scenario when there are no results from prediction engine for an item
	// // no prediction for all price point
	// else if (itemDTO.getRegPricePredictionMap() == null
	// || (itemDTO.getRegPricePredictionMap() != null && itemDTO.getRegPricePredictionMap().size() == 0)
	// || !hasAtleastOnePrediction(itemDTO)) {
	// //NU:: 24th Nov 2016, retain current price, when prediction is not available for the item
	// //Roll backed as price group relation items are retained with cur retail, even though there is valid prediction
	//// itemDTO.setRecommendedRegPrice(PRCommonUtil.getCurRegPrice(itemDTO));
	//// itemDTO.setExplainRetailNoteTypeLookupId(ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_AS_NO_VALID_PRED.getNoteTypeLookupId());
	//// logger.debug("2.Cur retained is retail as there is no valid prediction for item code:" + itemDTO.getItemCode());
	//
	// if(itemDTO.getPriceRange() != null && itemDTO.getPriceRange().length > 0){
	// for(double price : itemDTO.getPriceRange()){
	//// itemDTO.addPriceMovementPrediction(price, null);
	// itemDTO.addRegPricePrediction(new MultiplePrice(1, price), null);
	// }
	// //Double price = applyObjective13(itemDTO.getPriceMovementPrediction(), itemDTO.getRegPrice());
	// //Double price = applyObjective13(itemDTO.getPriceRange(), itemDTO.getRegPrice());
	//
	// logger.debug("------2.Filtering price range for item:" + itemDTO.getItemCode() + "--------");
	// Double[] filteredRange = objectiveService.filterFinalPriceRanges(itemZonePriceHistory, itemDTO, curCalDTO);
	// logger.debug("---------------------------------------------------------------------------");
	//
	// Double price = null;
	// if (filteredRange == null || filteredRange.length == 0) {
	// price = PRCommonUtil.getUnitPrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice(), true);
	// logger.debug("2.Cur reg price is retained for item code: " + itemDTO.getItemCode() + " as filtered price ranges have no
	// price points");
	// } else {
	// price = objectiveService.applyFollowRules(filteredRange, itemDTO.getRegPrice());
	// }
	//
	// itemDTO.setRecommendedRegPrice(new MultiplePrice(1,price));
	//
	//// itemDTO.setRecommendedRegMultiple(1);
	//// itemDTO.setRecommendedRegPrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(price)));
	//
	// logger.debug("2. Price for item " + itemDTO.getItemCode() + " after applying objective " +
	// itemDTO.getRecommendedRegPrice());
	// }else{
	// //logger.debug("Item Code - " + itemDTO.getItemCode() + "; " + "Log - " + itemDTO.getLog() + "; Recommended price not
	// available");
	// logger.debug("Item Code - " + itemDTO.getItemCode() + "; Recommended price not available");
	// }
	// }
	// }
	// }

	// Call prediction engine to find prediction for promotion items (sale or ad or display)
	// for current and recommended week
	public void predictPromoItems(Connection conn, List<ExecutionTimeLog> executionTimeLogs, PRRecommendationRunHeader recommendationRunHeader,
			List<PRItemDTO> items, boolean isOnline, PredictionService predictionService) throws GeneralException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		LIGConstraint ligConstraint = new LIGConstraint();

		// Form input for the prediction
		PredictionInputDTO predictionInputDTO = formSalePredictionInput(recommendationRunHeader, items);

		// Call prediction engine
		PredictionOutputDTO predictionOutput = predictionService.predictMovement(conn, predictionInputDTO, executionTimeLogs, "PREDICTION_SALE",
				isOnline);

		// Update the sale prediction in the object
		updateSalePrediction(recommendationRunHeader, items, predictionOutput);

		// Sum cur sale pred and rec week sale pred
		for (PRItemDTO itemDTO : items) {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(itemDTO.getRetLirId(), items);
				PRItemDTO ligRepresentingItem = pricingEngineService.getLigRepresentingItem(itemDTO.getRetLirId(), items);
				if (ligRepresentingItem != null) {
					// Sum pred movement
					ligRepresentingItem = ligConstraint.sumRecWeekSalePredAtCurRegForLIG(ligRepresentingItem, ligMembers);
					ligRepresentingItem = ligConstraint.sumRecWeekSalePredAtRecRegForLIG(ligRepresentingItem, ligMembers);
					// logger.debug("LIG Sale at Cur Reg: " + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtCurReg());
					// logger.debug("LIG Sale at Rec Reg: " + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtRecReg());
					// Update status also
					ligRepresentingItem = ligConstraint.updateRecWeekSalePredStatusAtCurReg(ligRepresentingItem, ligMembers);
					ligRepresentingItem = ligConstraint.updateRecWeekSalePredStatusAtRecReg(ligRepresentingItem, ligMembers);
					// logger.debug("Cur Reg LIG Pred Status: " +
					// ligRepresentingItem.getRecWeekSaleInfo().getSalePredStatusAtCurReg());
					// logger.debug("Rec Reg LIG Pred Status: " +
					// ligRepresentingItem.getRecWeekSaleInfo().getSalePredStatusAtRecReg());
				}
			}
		}
	}

	private PredictionInputDTO formSalePredictionInput(PRRecommendationRunHeader recommendationRunHeader, List<PRItemDTO> items) {
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.locationId = recommendationRunHeader.getLocationId();
		predictionInputDTO.locationLevelId = recommendationRunHeader.getLocationLevelId();
		predictionInputDTO.productId = recommendationRunHeader.getProductId();
		predictionInputDTO.productLevelId = recommendationRunHeader.getProductLevelId();
		predictionInputDTO.startCalendarId = recommendationRunHeader.getCalendarId();
		predictionInputDTO.endCalendarId = recommendationRunHeader.getCalendarId();
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();

		for (PRItemDTO itemDTO : items) {
			if (!itemDTO.isLir()) {

				//NU:: 7th Feb 2018, send only Standard or BOGO, as prediction doesn't support other promotion types
				// Add only if it is promoted item
				if (itemDTO.isItemPromotedForRecWeek() && (itemDTO.getRecWeekSaleInfo() != null && 
						itemDTO.getRecWeekSaleInfo().getPromoTypeId() == PromoTypeLookup.STANDARD.getPromoTypeId() ||
						itemDTO.getRecWeekSaleInfo().getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId())) {

					PredictionItemDTO predictionItemDTO = new PredictionItemDTO();
					predictionItemDTO.lirInd = false;
					predictionItemDTO.itemCodeOrLirId = itemDTO.getItemCode();
					predictionItemDTO.upc = itemDTO.getUpc();
					predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();

					MultiplePrice curReg = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
					// MultiplePrice recReg = new MultiplePrice(itemDTO.getRecommendedRegMultiple(),
					// itemDTO.getRecommendedRegPrice());
					MultiplePrice recReg = itemDTO.getRecommendedRegPrice();

					// Add sale pred with cur retail
					if (curReg != null) {
						PricePointDTO pricePointDTO = getPricePoint(itemDTO, curReg);
						predictionItemDTO.pricePoints.add(pricePointDTO);
//						logger.debug("Item Code: " + itemDTO.getItemCode() + ", Cur Reg price points:" + pricePointDTO.toString());
					}

					// Add sale pred with rec retail and if different from cur reg
					if (recReg != null) {
						if (curReg == null || (curReg != null && !curReg.equals(recReg))) {
							// if item effective date is future, then it goes with curren price
							// for the recommendation week
							if (!itemDTO.isFutureRetailRecommended()) {
								PricePointDTO pricePointDTO = getPricePoint(itemDTO, recReg);
								predictionItemDTO.pricePoints.add(pricePointDTO);
//								logger.debug("Item Code: " + itemDTO.getItemCode() + ", Rec Reg price points:" + pricePointDTO.toString());
							}
						}
					}

					predictionInputDTO.predictionItems.add(predictionItemDTO);

				}

			}
		}
		return predictionInputDTO;
	}

	private PricePointDTO getPricePoint(PRItemDTO itemDTO, MultiplePrice regPrice) {
		PricePointDTO pricePointDTO = new PricePointDTO();

		pricePointDTO.setRegPrice(regPrice.price);
		pricePointDTO.setRegQuantity(regPrice.multiple);

		if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null) {
			pricePointDTO.setSaleQuantity(itemDTO.getRecWeekSaleInfo().getSalePrice().multiple);
			pricePointDTO.setSalePrice(itemDTO.getRecWeekSaleInfo().getSalePrice().price);
			pricePointDTO.setPromoTypeId(itemDTO.getRecWeekSaleInfo().getPromoTypeId());
		}

		pricePointDTO.setAdPageNo(itemDTO.getRecWeekAdInfo().getAdPageNo());
		pricePointDTO.setAdBlockNo(itemDTO.getRecWeekAdInfo().getAdBlockNo());

		if (itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null) {
			pricePointDTO.setDisplayTypeId(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId());
		}
		return pricePointDTO;
	}

	private void updateSalePrediction(PRRecommendationRunHeader recommendationRunHeader, List<PRItemDTO> items,
			PredictionOutputDTO predictionOutput) {
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = convertPredictionOutputDTOToPredictionOutputMap(predictionOutput);
		if (predictionOutputMap != null && predictionOutputMap.size() > 0) {
			for (PRItemDTO itemDTO : items) {

				// NU:: 7th Feb 2018, if reward promotion, then update reg price prediction as sales prediction as
				if (!itemDTO.isLir() && itemDTO.getRecWeekSaleInfo() != null
						&& itemDTO.getRecWeekSaleInfo().getPromoTypeId() == PromoTypeLookup.REWARD_PROMO.getPromoTypeId()) {
					logger.debug("Reward promo. itemCode:" + itemDTO.getItemCode() + "getCurRegPricePredictedMovement" + itemDTO.getCurRegPricePredictedMovement());
					if (itemDTO.getCurRegPricePredictionStatus() != null) {
						itemDTO.getRecWeekSaleInfo().setSalePredMovAtCurReg(itemDTO.getCurRegPricePredictedMovement());
						itemDTO.getRecWeekSaleInfo().setSalePredStatusAtCurReg(PredictionStatus.get(itemDTO.getCurRegPricePredictionStatus()));
					}
					
					if (itemDTO.getPredictionStatus() != null) {
						itemDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(itemDTO.getPredictedMovement());
						itemDTO.getRecWeekSaleInfo().setSalePredStatusAtRecReg(PredictionStatus.get(itemDTO.getPredictionStatus()));
					}
					
				} else {

					// prediction engine can't predict
					List<PricePointDTO> pricePointListFromPrediction = predictionOutputMap.get(itemDTO.getItemCode());
					if (pricePointListFromPrediction != null) {

						MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
								itemDTO.getRegMPrice());

						// logger.debug("itemCode:" + (itemDTO != null ? itemDTO.getItemCode() : "") + ",curRegPrice:"
						// + (curRegPrice != null ? curRegPrice.toString() : ""));

						PredictionDetailKey pdkSaleAtCurReg = new PredictionDetailKey(recommendationRunHeader.getLocationLevelId(),
								recommendationRunHeader.getLocationId(), itemDTO.getItemCode(), (curRegPrice != null ? curRegPrice.multiple : 0),
								(curRegPrice != null ? curRegPrice.price : 0),
								(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
								(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
								itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
								(itemDTO.getRecWeekSaleInfo() != null
										? itemDTO.getRecWeekSaleInfo().getPromoTypeId()
										: 0),
								(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
										? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId()
										: 0));

						PredictionDetailKey pdkSaleAtRecReg = new PredictionDetailKey(recommendationRunHeader.getLocationLevelId(),
								recommendationRunHeader.getLocationId(), itemDTO.getItemCode(),
								(itemDTO.getRecommendedRegPrice() != null ? itemDTO.getRecommendedRegPrice().multiple : 0),
								(itemDTO.getRecommendedRegPrice() != null ? itemDTO.getRecommendedRegPrice().price : 0),
								(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
								(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
								itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
								(itemDTO.getRecWeekSaleInfo() != null
										? itemDTO.getRecWeekSaleInfo().getPromoTypeId()
										: 0),
								(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
										? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId()
										: 0));

						for (PricePointDTO pricePointFromPrediction : pricePointListFromPrediction) {
							PredictionDetailKey pdkPricePoint = new PredictionDetailKey(recommendationRunHeader.getLocationLevelId(),
									recommendationRunHeader.getLocationId(), itemDTO.getItemCode(), pricePointFromPrediction.getRegQuantity(),
									pricePointFromPrediction.getRegPrice(), pricePointFromPrediction.getSaleQuantity(),
									pricePointFromPrediction.getSalePrice(), pricePointFromPrediction.getAdPageNo(),
									pricePointFromPrediction.getAdBlockNo(), pricePointFromPrediction.getPromoTypeId(),
									pricePointFromPrediction.getDisplayTypeId());

							// update current sale prediction
							if (pdkSaleAtCurReg.equals(pdkPricePoint)) {
								itemDTO.getRecWeekSaleInfo().setSalePredMovAtCurReg(pricePointFromPrediction.getPredictedMovement());
								itemDTO.getRecWeekSaleInfo().setSalePredStatusAtCurReg(pricePointFromPrediction.getPredictionStatus());

								// if future retail is recommended, it means cur price is retained, so predict with cur price
								if (itemDTO.isFutureRetailRecommended()) {
									itemDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(pricePointFromPrediction.getPredictedMovement());
									itemDTO.getRecWeekSaleInfo().setSalePredStatusAtRecReg(pricePointFromPrediction.getPredictionStatus());
								}
							}
							// update rec week sale prediction
							if (pdkSaleAtRecReg.equals(pdkPricePoint)) {
								if (!itemDTO.isFutureRetailRecommended()) {
									itemDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(pricePointFromPrediction.getPredictedMovement());
									itemDTO.getRecWeekSaleInfo().setSalePredStatusAtRecReg(pricePointFromPrediction.getPredictionStatus());
								}
							}
						}
					}
				}
			}
		}
	}
	
	public double[] getNewRange(int qty, double price, PRPriceGroupRelatedItemDTO relatedItem, PRItemDTO item,
			Character key, double sizeShelfPCT, PRStrategyDTO strategyDTO) {
		double[] priceRangeArr = null;
		MultiplePrice multiplePrice = new MultiplePrice(qty, price);
		// Apply guideline with that price and find start and end range
		// logger.debug("relatedItem.getPriceRelation(): " +
		// relatedItem.getPriceRelation());
		// NU:: 10th Feb 2017, bug fix: when there a brand relation and there is no
		// brand guideline defined for that tier
		if (relatedItem.getPriceRelation() != null) {
			PRRange range = relatedItem.getPriceRelation().getPriceRange(multiplePrice,
					relatedItem.getRelatedItemSize(), item.getPgData().getItemSize(), key, sizeShelfPCT);
			// logger.debug("range for price point:" + pricePoint + " is " + range.toString());

			priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint().getRange(range.getStartVal(),
					range.getEndVal());
		}

		return priceRangeArr;
	}
	
	public Double[] applyConstraints(PRItemDTO item, PRRange range, PRStrategyDTO strategyDTO) {
		Double[] priceRangeArrNew = null;
		if (item.getStrategyDTO().getConstriants() != null) {
			/** Apply min/max Constraint **/
			MinMaxConstraint minMaxConstraint = new MinMaxConstraint(item, range, item.getExplainLog());
			PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal = minMaxConstraint
					.applyMinMaxConstraint();
			range = guidelineAndConstraintOutputLocal.outputPriceRange;
			/** Apply min/max Constraint **/

			/** Apply Threshold Constraint **/
			if (strategyDTO.getConstriants().getThresholdConstraint() != null) {
				guidelineAndConstraintOutputLocal = strategyDTO.getConstriants().getThresholdConstraint()
						.applyThreshold(item, range, item.getExplainLog());
				range = guidelineAndConstraintOutputLocal.outputPriceRange;
			}
			/** Apply Threshold Constraint **/

			/** Apply Cost Constraint **/
			// Is there Cost Constraint
			CostConstraint costConstraint = new CostConstraint(item, range, item.getExplainLog());
			guidelineAndConstraintOutputLocal = costConstraint.applyCostConstraint();
			range = guidelineAndConstraintOutputLocal.outputPriceRange;
			/** Apply Cost Constraint **/

			/** Apply Lower/Higher Constraint **/
			LowerHigherConstraint lhConstraint = new LowerHigherConstraint(item, range, item.getExplainLog());
			guidelineAndConstraintOutputLocal = lhConstraint.applyLowerHigherConstraint();
			range = guidelineAndConstraintOutputLocal.outputPriceRange;
			/** Apply Lower/Higher Constraint **/

			/** Apply Lower/Higher Constraint **/
			if (strategyDTO.getConstriants().getFreightChargeConstraint() != null) {
			FreightChargeConstraint  freightchargeConstraint = new FreightChargeConstraint(item, range, item.getExplainLog());
			guidelineAndConstraintOutputLocal = freightchargeConstraint.applyFreightChargeConstraint();
			range = guidelineAndConstraintOutputLocal.outputPriceRange;
	
			}
			/** Apply Lower/Higher Constraint **/
			
			
			
			/** Find Rounding Digits **/
			
			priceRangeArrNew = strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(item, range, item.getExplainLog());
			
			/** Find Rounding Digits **/
		}

		/*
		 * logger.debug("After Threshold " + item.getItemCode() +
		 * ": rounding digits for above range:" +
		 * PRCommonUtil.getCommaSeperatedStringFromDouble(priceRangeArrNew));
		 */
		return priceRangeArrNew;
	}

	/**
	 * 	
	 * @param item
	 * @param pricePointsList
	 */
	private void addOverridePricePoint(PRItemDTO item, List<PricePointDTO> pricePointsList) {
		if(item.getOverriddenRegularPrice() != null) {
			PricePointDTO pricePointDTO = new PricePointDTO();
			pricePointDTO.setRegQuantity(item.getOverriddenRegularPrice().multiple);
			pricePointDTO.setRegPrice(item.getOverriddenRegularPrice().price);
			pricePointsList.add(pricePointDTO);
		}
	}
	
	
	/**
	 * Populates status code and movement as -1 for items with no recent weeks movement
	 * @param itemsWithNoRecentWeeksMov
	 */
	/*
	 * private void updateNoRecentWeeksMovPredStatus(List<PredictionItemDTO>
	 * itemsWithNoRecentWeeksMov) { itemsWithNoRecentWeeksMov.forEach(pItem -> {
	 * if(pItem.pricePoints != null && pItem.pricePoints.size() > 0) {
	 * pItem.pricePoints.forEach(pricePoint -> {
	 * pricePoint.setPredictedMovement(-1D);
	 * pricePoint.setPredictionStatus(PredictionStatus.NO_RECENT_MOVEMENT); }); }
	 * }); }
	 */

	
	private void updateLessAvgRevenuePredStatus(List<PredictionItemDTO> itemsToBeUpdatedWithLessAvgRevenue) {
		itemsToBeUpdatedWithLessAvgRevenue.forEach(pItem -> {
			if (pItem.pricePoints != null && pItem.pricePoints.size() > 0) {
				pItem.pricePoints.forEach(pricePoint -> {
					pricePoint.setPredictionStatus(PredictionStatus.SUCCESS);
					pricePoint.setPredictedMovement(pItem.getAvgMovement());
				});
			}
		});

	}

	
//	private boolean hasAtleastOnePrediction(PRItemDTO itemDTO) {
//		boolean hasAtleastOnePrediction = false;
//		if (itemDTO.getRegPricePredictionMap() != null && itemDTO.getRegPricePredictionMap().size() > 0) {
//			for (Map.Entry<MultiplePrice, PricePointDTO> entry : itemDTO.getRegPricePredictionMap().entrySet()) {
//				if (entry.getValue() != null) {
//					hasAtleastOnePrediction = true;
//					break;
//				}
//			}
//		}
//		return hasAtleastOnePrediction;
//	}
}
