package com.pristine.service.offermgmt;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.constraint.CostConstraint;
import com.pristine.service.offermgmt.constraint.LowerHigherConstraint;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.service.offermgmt.recommendationrule.CheckNextDollarCrossingPPRule;
import com.pristine.service.offermgmt.recommendationrule.CheckPPWithinXPctThanHighestPrevPriceRule;
import com.pristine.service.offermgmt.recommendationrule.FilterPricePointsRule;
import com.pristine.service.offermgmt.recommendationrule.RecommendationRuleHelper;
import com.pristine.service.offermgmt.recommendationrule.SameRetailInLastXWeeksRule;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class ObjectiveService {
	private static Logger logger = Logger.getLogger("ObjectiveService");
	
	/*
	private int regRetailIncreaseInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_INCREASED_IN_LAST_X_WEEKS"));
	private int regRetailDecreaseInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_DECREASED_IN_LAST_X_WEEKS"));
	private int maxRegRetailIncreasePct = Integer.parseInt(PropertyManager.getProperty("MAX_REG_RETAIL_INCREASE_PCT"));
	private int maxRegRetailIncreasePctForUnchangedItem = Integer.parseInt(PropertyManager.getProperty("MAX_REG_RETAIL_INCREASE_PCT_FOR_UNCHANGED_ITEM"));
	private int maxRegRetailNotToCrossNextRange = Integer.parseInt(PropertyManager.getProperty("MAX_REG_RETAIL_NOT_TO_CROSS_NEXT_RANGE"));
	private int noSaleItemDecreasePct = Integer.parseInt(PropertyManager.getProperty("NO_SALE_ITEM_DECREASE_PCT"));
	private int maxSalePriceInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("MAX_SALE_PRICE_IN_LAST_X_WEEKS"));
	*/
	
	private int maxRegRetailInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("MAX_REG_RETAIL_IN_LAST_X_WEEKS"));
	private int regRetailUnchangedInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_UNCHANGED_IN_LAST_X_WEEKS"));
	private int noSaleInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("NO_SALE_IN_LAST_X_WEEKS"));
	 
	public void applyHighestMar$UsingCurRetail(PRItemDTO inputItem, PRExplainLog explainLog) {
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		PRRange priceRange = new PRRange();
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		Double[] priceRangeArr = null;
		PricingEngineService pricingEngineService = new PricingEngineService();		
		int noOfLowerPoints, noOfHigherPoints;
		noOfLowerPoints = Integer.parseInt(PropertyManager.getProperty("MAR_OPP_NO_OF_LOWER_PRICE_POINTS"));
		noOfHigherPoints = Integer.parseInt(PropertyManager.getProperty("MAR_OPP_NO_OF_HIGHER_PRICE_POINTS"));
		
		//If Current price is available
		if(inputItem.getRecommendedRegPrice() != null){
			priceRangeArr = new Double[] { inputItem.getRecommendedRegPrice().price };
			inputItem.setPriceRange(priceRangeArr);
			List<Double> pricePoints = pricingEngineService.addPreviousAndNextPricePoints(inputItem, noOfLowerPoints, noOfHigherPoints);
			
			if (pricePoints.size() > 0) {
				if (pricePoints.get(0) > 0)
					priceRange.setStartVal(pricePoints.get(0));
				else
					priceRange.setStartVal(inputItem.getRecommendedRegPrice().price);

				if (pricePoints.get(pricePoints.size() - 1) > 0)
					priceRange.setEndVal(pricePoints.get(pricePoints.size() - 1));
				else
					priceRange.setEndVal(inputItem.getRecommendedRegPrice().price);
			} else {
				priceRange.setStartVal(inputItem.getRecommendedRegPrice().price);
				priceRange.setEndVal(inputItem.getRecommendedRegPrice().price);
			}

			//Just Show Cost Range and don't apply it
			CostConstraint costConstraint = new CostConstraint(inputItem, priceRange, explainLog);
			guidelineAndConstraintOutputLocal = costConstraint.getCostRange();
			priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
			
			//Apply lower/higher retail
			LowerHigherConstraint lhConstraint = new LowerHigherConstraint(inputItem, priceRange, explainLog);
			guidelineAndConstraintOutputLocal = lhConstraint.applyLowerHigherConstraint();
			priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;		
			
			//Rounding
			priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint()
					.getRoundingDigits(inputItem, priceRange, explainLog);
			 
			inputItem.setPriceRange(priceRangeArr);
			inputItem.setExplainLog(explainLog);
		}
	}
	
	public void applyFollowRulesAndSetRecPrice(List<PRItemDTO> itemList)
			throws GeneralException {

		for (PRItemDTO itemDTO : itemList) {
			MultiplePrice multiplePrice = null;
			Double regPrice = itemDTO.getRegPrice();

			if (itemDTO.getPriceRange() == null || itemDTO.getPriceRange().length == 0) {
				// current reg unit price
				multiplePrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
				logger.debug("Cur reg price is retained for item code: " + itemDTO.getItemCode() + " as there is no price points");
			} else {
				multiplePrice = applyFollowRules(itemDTO.getPriceRange(), regPrice);
			}
			itemDTO.setRecommendedRegPrice(multiplePrice);
		}
	}
	
	public void applyObjectiveAndSetRecPrice(List<PRItemDTO> itemList, HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			RetailCalendarDTO curCalDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws GeneralException, Exception {
		for (PRItemDTO itemDTO : itemList) {
			if(!itemDTO.isLir()) {
				applyObjective(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);	
			}
		}
	}
	
	public void applyObjectiveAndSetRecPrice(PRItemDTO itemDTO, HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			RetailCalendarDTO curCalDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws GeneralException, Exception {
		if (!itemDTO.isLir()) {
			applyObjective(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		}
	}

	/**
	 * @param itemDTO
	 * @param itemZonePriceHistory
	 * @param curCalDTO
	 * @param recommendationRuleMap
	 * @throws GeneralException
	 * @throws Exception
	 */
	private void applyObjective(PRItemDTO itemDTO, HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			RetailCalendarDTO curCalDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws GeneralException, Exception {
		MultiplePrice multiplePrice = null;
		Double cost = itemDTO.getCost();
		Double regPrice = itemDTO.getRegPrice();
		int objectiveTypeId = itemDTO.getObjectiveTypeId();
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();

//		logger.debug("Applying objective type: " + objectiveTypeId +  ", for item code:" + itemDTO.getItemCode());
		
		 if (objectiveTypeId == ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL.getObjectiveTypeId()) {
			 applyHighestMar$UsingCurRetail(itemDTO, itemDTO.getExplainLog());
		 }
		 
//		 Filter price range further
		 Double[] filteredPricePoints = filterFinalPricePoints(itemDTO, itemZonePriceHistory,
					curCalDTO, recommendationRuleMap);

		// Price point would have been filtered future due to additional conditions
		if(itemDTO.getIsLocPriced() == 1) {
			if(filteredPricePoints != null && filteredPricePoints.length > 0) {
				multiplePrice = new MultiplePrice(1, filteredPricePoints[0]);
//				logger.debug("Locked retail found for: " + itemDTO.getItemCode() + ", Lock retail: " + multiplePrice );
			} else {
				multiplePrice = PRCommonUtil.getCurRegPrice(itemDTO);	
//				logger.debug("Locked retail found for: " + itemDTO.getItemCode() + ", Lock retail: " + multiplePrice );
			}
			
		} else if (filteredPricePoints == null || filteredPricePoints.length == 0) {
//			current reg unit price
			multiplePrice = PRCommonUtil.getCurRegPrice(itemDTO);
//			logger.debug("Cur reg price is retained for item code: " + itemDTO.getItemCode() + " as filtered price ranges have no price points");
		} else {
			if (objectiveTypeId == ObjectiveTypeLookup.USE_GUIDELINES_AND_CONSTRAINTS.getObjectiveTypeId()) {
//				Use guidelines and constraints only
				multiplePrice = applyFollowRules(filteredPricePoints, regPrice);
			} else if (objectiveTypeId == ObjectiveTypeLookup.LOWEST_PRICE_TO_CUSTOMER.getObjectiveTypeId()) {
//				Lowest price to customer
				multiplePrice = applyLowestPriceToCustomer(filteredPricePoints);
			} else if (objectiveTypeId == ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR.getObjectiveTypeId()
					|| objectiveTypeId == ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL.getObjectiveTypeId()) {
//				Highest margin $ or Highest margin $ using current retail
				multiplePrice = applyMaximizeMarginDollar(itemDTO, filteredPricePoints);
//				multiplePrice = applyMaximizeMarginDollar(filteredPricePoints, priceMovementMap, cost, regPrice);
			} else if (objectiveTypeId == ObjectiveTypeLookup.HIGHEST_REVENUE.getObjectiveTypeId()) {
//				 Highest revenue
				multiplePrice = applyMaximizeSalesDollar(itemDTO, filteredPricePoints);
			} else if (objectiveTypeId == ObjectiveTypeLookup.MINIMUM_REVENUE_DOLLAR.getObjectiveTypeId()) {
//				 Minimum Revenue $
				multiplePrice = applyMinRevenueDollar(filteredPricePoints, priceMovementMap, regPrice);
			} else if (objectiveTypeId == ObjectiveTypeLookup.MINIMUM_MARGIN_DOLLAR.getObjectiveTypeId()) {
//				 Minimum Margin $
				multiplePrice = applyMinMarginDollar(filteredPricePoints, priceMovementMap, cost, regPrice);
			} else if (objectiveTypeId == ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_MAINTAIN_CUR_SALE.getObjectiveTypeId()) {
//				 Maximize Margin while maintaining sale dollar
				multiplePrice = applyMaximizeMarginByMaintainingSaleDollar(itemDTO, filteredPricePoints);
			} else if (objectiveTypeId == ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_MAINTAIN_CUR_MOV.getObjectiveTypeId()) {
//				 Maximize Margin while maintaining item movement (units)
				multiplePrice = applyMaximizeMarginByMaintainingCurMov(itemDTO, filteredPricePoints);
			} else if(objectiveTypeId == ObjectiveTypeLookup.HIGHEST_MOV_MAINTAIN_CUR_MARGIN_DOLLAR.getObjectiveTypeId()) {
//				Maximize item movement (units) while maintaining Margin
				multiplePrice = applyMaximizeMovByMaintainingCurMargin(itemDTO, filteredPricePoints);
			}
		}
//		Added By Karishma to set this field which will be used to set the LIg Rep
//		Removed this condition as its not proper place
//		itemDTO.setFinalObjectiveApplied(true);
	
		itemDTO.setRecommendedRegPrice(multiplePrice);
		itemDTO.setRecProcessCompleted(true);
		
	}
	
	/**
	 * Returns lowest price to customer
	 * 
	 * @param priceSet
	 * @return
	 */
	private MultiplePrice applyLowestPriceToCustomer(Double[] pricePoints) {
		Arrays.sort(pricePoints);
		return new MultiplePrice(1, pricePoints[0]);
	}

	/*
	private MultiplePrice applyMaximizeMarginDollar(Double[] pricePoints, HashMap<MultiplePrice, PricePointDTO> priceMovementMap, Double cost, Double regPrice) {
		Double margin = null;
		Double finalPrice = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;

		logger.debug("1.Available price points:" + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));
		
		if (cost != null) {
			boolean isValidPredictionFound = false;
			for (Double price : pricePoints) {
//				PricePointDTO pricePointDTO = priceMovementMap.get(price);
//				price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
//				 Use only price points with prediction status success and predicted movement > 0
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					double tPrice = price;
					predictedMovement = pricePointDTO.getPredictedMovement();

					double tMargin = ((tPrice * predictedMovement) - (cost * predictedMovement));
					logger.debug("1.price point:" + price + ",margin:"  + tMargin);

					if (margin == null) {
						margin = tMargin;
						finalPrice = tPrice;
					} else if (tMargin > margin) {
						margin = tMargin;
						finalPrice = tPrice;
					}
					isValidPredictionFound = true;
				} else {
//					 logger.debug("Ignoring price point" + entry.getKey() + " with prediction status " +
//					 entry.getValue().getPredictionStatus().getStatusCode() + " and predicted movement " +
//					 entry.getValue().getPredictedMovement());
				}
			}
			
//			 Apply "Use guidelines and constraints objective when the prediction status is error or prediction status is sucess
//			 but predicted movement is 0 for all price points
			 
			if (!isValidPredictionFound) {
				recPrice = applyFollowRules(pricePoints, regPrice);
			} else {
				recPrice = new MultiplePrice(1, finalPrice);
			}
		} else {
			// When there is no list cost, pick the price closer to current price
			recPrice = applyFollowRules(pricePoints, regPrice);
		}
		logger.debug("1.Picked price:" + recPrice);
		return recPrice;
	}
	*/
	
	private MultiplePrice applyMaximizeMarginDollar(PRItemDTO itemDTO, Double[] pricePoints) {
		Double margin = null;
		Double finalPrice = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;
		Double cost = itemDTO.getCost();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
		boolean hasPredForOneOfThePricePoint = false;

//		logger.debug("1.Available price points:" + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));
		hasPredForOneOfThePricePoint = hasPredForOneOfThePricePoint(priceMovementMap, pricePoints);

		if (cost != null && hasPredForOneOfThePricePoint) {
			for (Double price : pricePoints) {
				// PricePointDTO pricePointDTO = priceMovementMap.get(price);
				// price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
				// Use only price points with prediction status success and predicted movement > 0
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					double tPrice = price;
					predictedMovement = pricePointDTO.getPredictedMovement();

					double tMargin = ((tPrice * predictedMovement) - (cost * predictedMovement));
//					logger.debug("1.price point:" + price + ",margin:" + tMargin);

					if (margin == null) {
						margin = tMargin;
						finalPrice = tPrice;
					} else if (tMargin > margin) {
						margin = tMargin;
						finalPrice = tPrice;
					}
				}
			}
			recPrice = new MultiplePrice(1, finalPrice);
		} else {
			PricingEngineService pricingEngineService = new PricingEngineService();
			List<String> additionalDetails = new ArrayList<>();
			if(!hasPredForOneOfThePricePoint) {
				additionalDetails.add("None of the price points has valid prediction");
			}
			pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_WHILE_MAXIMIZE_MARGIN_DOLLAR,
					additionalDetails);
			recPrice = curRegPrice;
		}
//		logger.debug("1.Picked price:" + recPrice);
		return recPrice;
	}
	
	/*
	private MultiplePrice applyMaximizeMarginByMaintainingSaleDollar(PRItemDTO itemDTO, Double[] pricePoints) {
		Double margin = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;
		Double cost = itemDTO.getCost();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
				
		logger.debug(itemDTO.getItemCode() + " - 2. Available price points:" + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));

		boolean retainCurrentPrice = true;
		
		//There is cost, cur price and cur price prediction
		if (cost != null && curRegPrice != null && priceMovementMap.get(curRegPrice) != null
				&& priceMovementMap.get(curRegPrice).getPredictedMovement() > 0) {
			Double curRetailSalesDollar = PRCommonUtil.getSalesDollar(curRegPrice, priceMovementMap.get(curRegPrice).getPredictedMovement());
			logger.debug("curRetailSalesDollar:" + curRetailSalesDollar);
			for (Double price : pricePoints) {
				// price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
				// Use only price points with prediction status success and predicted movement > 0
				// price point with higher prediction
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					// && newPriceSalesDollar >= curRetailSalesDollar) {
					Double newPriceSalesDollar = PRCommonUtil.getSalesDollar(new MultiplePrice(1, price), pricePointDTO.getPredictedMovement());
					logger.debug("newPriceSalesDollar:" + newPriceSalesDollar);
					if (newPriceSalesDollar >= curRetailSalesDollar) {
						double tPrice = price;
						predictedMovement = pricePointDTO.getPredictedMovement();

						double tMargin = ((tPrice * predictedMovement) - (cost * predictedMovement));
						logger.debug("2. price point:" + price + ",margin:" + tMargin);

						if (margin == null) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						} else if (tMargin > margin) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						}
						retainCurrentPrice = false;
					}
				}
			}
		}
		
		if(retainCurrentPrice) {
			recPrice = curRegPrice;
			logger.debug("2. Current price is retained");
		}
		
		logger.debug("2. Picked price:" + recPrice);
		return recPrice;
	}
	*/
	
	private MultiplePrice applyMaximizeMarginByMaintainingSaleDollar(PRItemDTO itemDTO, Double[] pricePoints) {
		Double margin = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;
		Double cost = itemDTO.getCost();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
		HashMap<MultiplePrice, Double> pricePointsWithValidPred = new HashMap<MultiplePrice, Double>();
				
//		logger.debug(itemDTO.getItemCode() + " - 2. Available price points:" + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));

		boolean hasBetterSalesPrice = false;
		boolean hasPredForOneOfThePricePoint = false;
		boolean isRetainCurrentPrice = false;

		hasPredForOneOfThePricePoint = hasPredForOneOfThePricePoint(priceMovementMap, pricePoints);
		
		if (cost != null && curRegPrice != null && priceMovementMap.get(curRegPrice) != null
				&& priceMovementMap.get(curRegPrice).getPredictedMovement() > 0 && hasPredForOneOfThePricePoint) {
			Double curRetailSalesDollar = PRCommonUtil.getSalesDollar(curRegPrice, priceMovementMap.get(curRegPrice).getPredictedMovement());
//			logger.debug("curRetailSalesDollar:" + curRetailSalesDollar);
			for (Double price : pricePoints) {
				// price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
				// Use only price points with prediction status success and predicted movement > 0
				// price point with higher prediction
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					// && newPriceSalesDollar >= curRetailSalesDollar) {
					Double newPriceSalesDollar = PRCommonUtil.getSalesDollar(new MultiplePrice(1, price), pricePointDTO.getPredictedMovement());
//					logger.debug("newPriceSalesDollar:" + newPriceSalesDollar);
					if (newPriceSalesDollar >= curRetailSalesDollar) {
						double tPrice = price;
						predictedMovement = pricePointDTO.getPredictedMovement();

						double tMargin = ((tPrice * predictedMovement) - (cost * predictedMovement));
//						logger.debug("2. price point:" + price + ",margin:" + tMargin);

						if (margin == null) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						} else if (tMargin > margin) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						}
						hasBetterSalesPrice = true;
					}
					
					pricePointsWithValidPred.put(new MultiplePrice(1, price), newPriceSalesDollar);
				}
			}
		} else {
			//There is cost or cur price or cur price prediction or none of the price has prediction
			isRetainCurrentPrice = true;
			if(curRegPrice != null) {
				recPrice = applyFollowRules(pricePoints, curRegPrice.getUnitPrice());	
			}
		}
		
		if(isRetainCurrentPrice) {
			RecommendationRuleHelper recHelper = new RecommendationRuleHelper();
			PricingEngineService pricingEngineService = new PricingEngineService();
			List<String> additionalDetails = recHelper.getAddtDetails(priceMovementMap.get(curRegPrice), hasPredForOneOfThePricePoint);
			pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.RECOMMENDED_PRICE_USING_RULES,
					additionalDetails);
//			recPrice = curRegPrice;
//			logger.debug("2. Current price is retained");
		} else if (!hasBetterSalesPrice) {
//			 If none of the price point maintains the current sales
			LinkedHashMap<MultiplePrice, Double> result = pricePointsWithValidPred.entrySet().stream()
					.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
			recPrice = result.entrySet().iterator().next().getKey();
		}
		 
		
//		logger.debug("2. Picked price:" + recPrice);
		return recPrice;
	}
	
	/**
	 * PP: 30-Jun-2017 : Function Added for enabling new Objective - Maximize margin by maintaining
	 * current movement 
	 * @param itemDTO
	 * @param pricePoints
	 * @return PricePoints (Multiple Price)
	 */
	/*
	public MultiplePrice applyMaximizeMarginByMaintainingCurMov(PRItemDTO itemDTO, Double[] pricePoints) {
		Double margin = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;
		Double cost = itemDTO.getCost();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
				
		logger.debug(itemDTO.getItemCode() + " - 2. Available price points:" + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));

		boolean retainCurrentPrice = true;
		
		//There is cost, cur price and cur price prediction
		if (cost != null && curRegPrice != null && priceMovementMap.get(curRegPrice) != null
				&& priceMovementMap.get(curRegPrice).getPredictedMovement() > 0) {
			
			Double curRetailMovement = priceMovementMap.get(curRegPrice).getPredictedMovement();
			
			logger.debug("curRetailMovement:" + curRetailMovement);
			
			for (Double price : pricePoints) {
				// price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
				// Use only price points with prediction status success and predicted movement > 0
				// price point with higher prediction
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					// && newPriceSalesDollar >= curRetailSalesDollar) {

					Double newPriceMovement = pricePointDTO.getPredictedMovement(); 
					
					logger.debug("newPriceMovement:" + newPriceMovement);
					
					if (newPriceMovement >= curRetailMovement) {
						logger.debug("Inside now" + newPriceMovement);
						double tPrice = price;
						predictedMovement = pricePointDTO.getPredictedMovement();

						double tMargin = ((tPrice * predictedMovement) - (cost * predictedMovement));
						logger.debug("2. price point:" + price + ",new margin:" + tMargin + ",current margin:" + margin );

						if (margin == null) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						} else if (tMargin > margin) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						}
						retainCurrentPrice = false;
					}
				}
			}
		}
		
		if(retainCurrentPrice) {
			recPrice = curRegPrice;
			logger.debug("2. Current price is retained");
		}
		
		logger.debug("2. Picked price:" + recPrice);
		return recPrice;
	}
	*/
	
	public MultiplePrice applyMaximizeMarginByMaintainingCurMov(PRItemDTO itemDTO, Double[] pricePoints) {
		Double margin = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;
		Double cost = itemDTO.getCost();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
		HashMap<MultiplePrice, Double> pricePointsWithValidPred = new HashMap<MultiplePrice, Double>();
		
//		logger.debug(itemDTO.getItemCode() + " - 2. Available price points:" + PRCommonUtil.getCommaSeperatedStringFromDouble(pricePoints));

		boolean hasBetterUnitsPrice = false;
		boolean hasPredForOneOfThePricePoint = false;
		boolean isRetainCurrentPrice = false;

		hasPredForOneOfThePricePoint = hasPredForOneOfThePricePoint(priceMovementMap, pricePoints);
		 
		
//		There is cost, cur price and cur price prediction
		if (cost != null && curRegPrice != null && priceMovementMap.get(curRegPrice) != null
				&& priceMovementMap.get(curRegPrice).getPredictedMovement() > 0 && hasPredForOneOfThePricePoint) {
			
			Double curRetailMovement = priceMovementMap.get(curRegPrice).getPredictedMovement();
			
//			logger.debug("curRetailMovement:" + curRetailMovement);
			
			for (Double price : pricePoints) {
//				 price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
//				Use only price points with prediction status success and predicted movement > 0
//				price point with higher prediction
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
//					 && newPriceSalesDollar >= curRetailSalesDollar) {

					Double newPriceMovement = pricePointDTO.getPredictedMovement(); 
					
//					logger.debug("newPriceMovement:" + newPriceMovement);
					
					if (newPriceMovement >= curRetailMovement) {
//						logger.debug("Inside now" + newPriceMovement);
						double tPrice = price;
						predictedMovement = pricePointDTO.getPredictedMovement();

						double tMargin = ((tPrice * predictedMovement) - (cost * predictedMovement));
//						logger.debug("2. price point:" + price + ",new margin:" + tMargin + ",current margin:" + margin );

						if (margin == null) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						} else if (tMargin > margin) {
							margin = tMargin;
							recPrice = new MultiplePrice(1, tPrice);
						}
						hasBetterUnitsPrice = true;
					}
					pricePointsWithValidPred.put(new MultiplePrice(1, price), pricePointDTO.getPredictedMovement());
				}
			}
		} else {
			isRetainCurrentPrice = true;
			if(curRegPrice != null) {
				recPrice = applyFollowRules(pricePoints, curRegPrice.getUnitPrice());	
			}
		}
		
		if(isRetainCurrentPrice) {
			RecommendationRuleHelper recHelper = new RecommendationRuleHelper();
			PricingEngineService pricingEngineService = new PricingEngineService();
			List<String> additionalDetails = recHelper.getAddtDetails(priceMovementMap.get(curRegPrice), hasPredForOneOfThePricePoint);
			pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.RECOMMENDED_PRICE_USING_RULES,
					additionalDetails);
//			recPrice = curRegPrice;
//			logger.debug("2. Current price is retained");
		} else if (!hasBetterUnitsPrice) {
			boolean currentPrPresent = false;

//			If none of the price point maintains the current movement check if we have
//			the current Regprice in price points
			for (Double points : pricePoints) {
				MultiplePrice pricePt = new MultiplePrice(1, points);
				if (pricePt.equals(curRegPrice)) {
//					logger.info("retaining currentprice for :- " + itemDTO.getItemCode() + "Price is  " + curRegPrice);
					recPrice = curRegPrice;
					currentPrPresent = true;
				}
			}
			if (!currentPrPresent) {

				LinkedHashMap<MultiplePrice, Double> result = pricePointsWithValidPred.entrySet().stream()
						.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
								(oldValue, newValue) -> oldValue, LinkedHashMap::new));
				recPrice = result.entrySet().iterator().next().getKey();
			}
		}
		 
//		logger.debug("2. Picked price:" + recPrice);
		return recPrice;
	}
	
	/**
	 * If the item has a valid cost, price and predicted units, then apply this objective 
	 * else return a price point which is closest to the current retail. 
	 * To apply this objective, find the price point which provides the max movement 
	 * form among such price points that have a valid sales and yield a margin >= the margin as per current price. 
	 * If no valid price point yields a margin >= the margin as per the current price, then return the current retail 
	 * provided the current retail is equal to one of the price points, else return the valid price point that yields the max margin. 
	 * Price points are considered valid iff they have a successful prediction status and have positive movement.
	 * 
	 * @param itemDTO A PRItemDTO object representing the item which this objective is being applied to.
	 * @param pricePoints An array of Doubles representing the possible prices for itemDTO
	 * @return A MultiplePrice object representing the price/s that satisfy this objective for the specified item.
	 */
	public MultiplePrice applyMaximizeMovByMaintainingCurMargin(PRItemDTO itemDTO, Double[] pricePoints) {
		MultiplePrice result = null;
		
		Double cost = itemDTO.getCost();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
		boolean hasPredForOneOfThePricePoints = hasPredForOneOfThePricePoint(priceMovementMap, pricePoints);
		
		if (null != cost 
					&& null != curRegPrice
					&& hasPredForOneOfThePricePoints) {
			double movementForCurrentPrice = priceMovementMap.get(curRegPrice).getPredictedMovement(); 
			double marginForCurrentPrice = (curRegPrice.getUnitPrice() - cost) * movementForCurrentPrice; 
			
			double maxMargin = Double.MIN_VALUE, pricePointWithMaxMargin = Double.MIN_VALUE, 
					maxMovement = Double.MIN_VALUE, pricePointWithMaxMovement = Double.MIN_VALUE;
			Double marginForPricePoint;
			Double movementForPricePoint;
			PricePointDTO pricePointDTO;
			boolean currentPricePresentInPricePoints = false;
			for(Double pricePoint: pricePoints) {
				
//				Note if the current retail is one of the price points. This may be needed later.
				if(curRegPrice.getUnitPrice() == pricePoint)
					currentPricePresentInPricePoints = true;
				
//				price point will be always at unit level. Hence, the hard-coded 1
				pricePointDTO = priceMovementMap.get(new MultiplePrice(1, pricePoint));
				if(null == pricePointDTO 
						|| null == pricePointDTO.getPredictionStatus()
						|| pricePointDTO.getPredictionStatus().getStatusCode() != PredictionStatus.SUCCESS.getStatusCode()) {
//					Invalid price point.
					continue;
				}
				movementForPricePoint = pricePointDTO.getPredictedMovement();
				if(null == movementForPricePoint || 0 > movementForPricePoint) {
//					Invalid movement (sales) for this price point.
					continue;
				}
				marginForPricePoint = (pricePoint - cost) * movementForPricePoint;
				if( marginForPricePoint >= marginForCurrentPrice && maxMovement <= movementForPricePoint) {
					maxMovement = movementForPricePoint;
					pricePointWithMaxMovement = pricePoint;
				}
//				Note the valid price point that yields the max margin. This may be needed later.
				if(maxMargin <= marginForPricePoint) {
					maxMargin = marginForPricePoint;
					pricePointWithMaxMargin = pricePoint;
				}
			}
//			If no valid price point satisfies the condition margin >= margin as per current price,
			if(Double.MIN_VALUE == pricePointWithMaxMovement) {
//				If the current retail is one of the price points
				if(currentPricePresentInPricePoints) {
//					return the current retail
					result = curRegPrice;
				} else if (Double.MIN_VALUE == pricePointWithMaxMargin ){
					logger.warn("None of the price points had a successful prediction and a positive movement! Retaining the current price.");
					result = curRegPrice;
				} else {
//				Return the price point that yields the max margin.
					result = new MultiplePrice(1, pricePointWithMaxMargin);
				}
			} else {
//				Movement maximized by a valid price point while maintaining current margin. Return this price point.
				result = new MultiplePrice(1, pricePointWithMaxMovement);
			}
		}else {
//			Invalid cost, price, or predicted units for this item. Return current price.
			result = null == curRegPrice ? null : applyFollowRules(pricePoints, curRegPrice.getUnitPrice());
			
			RecommendationRuleHelper recHelper = new RecommendationRuleHelper();
			List<String> additionalDetails = recHelper.getAddtDetails(priceMovementMap.get(curRegPrice), hasPredForOneOfThePricePoints);
			
			PricingEngineService pricingEngineService = new PricingEngineService();
			pricingEngineService.writeAdditionalExplainLog(itemDTO, 
					ExplainRetailNoteTypeLookup.RECOMMENDED_PRICE_USING_RULES,
					additionalDetails);
			
		}
		
		return result;
		
	}

	private MultiplePrice applyMaximizeSalesDollar(PRItemDTO itemDTO, Double[] pricePoints) {
		Double revenue = null;
		Double finalPrice = null;
		Double predictedMovement = null;
		MultiplePrice recPrice = null;
		boolean hasPredForOneOfThePricePoint = false;
		HashMap<MultiplePrice, PricePointDTO> priceMovementMap = itemDTO.getRegPricePredictionMap();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		
		hasPredForOneOfThePricePoint = hasPredForOneOfThePricePoint(priceMovementMap, pricePoints);
		
		if (hasPredForOneOfThePricePoint) {
			for (Double price : pricePoints) {
				// PricePointDTO pricePointDTO = priceMovementMap.get(price);
				// price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
				// Use only price points with prediction status success and predicted movement > 0
				if (pricePointDTO != null && pricePointDTO.getPredictionStatus() != null
						&& pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					double tPrice = price;
					predictedMovement = pricePointDTO.getPredictedMovement();

					double tRevenue = tPrice * predictedMovement;
					if (revenue == null) {
						revenue = tRevenue;
						finalPrice = tPrice;
					} else if (tRevenue > revenue) {
						revenue = tRevenue;
						finalPrice = tPrice;
					}
				} else {
					logger.debug("Ignoring price point" + price);
				}
			}
		}

		/*
		 * Apply "Use guidelines and constraints objective when the prediction status is error or prediction status is success but
		 * predicted movement is 0 for all price points
		 */
		if (!hasPredForOneOfThePricePoint) {
			recPrice = curRegPrice;
		} else {
			recPrice = new MultiplePrice(1, finalPrice);
		}

		return recPrice;
	}

	private MultiplePrice applyMinRevenueDollar(Double[] pricePoints, HashMap<MultiplePrice, PricePointDTO> priceMovementMap, Double regPrice) {
		Double revenue = null;
		Double finalPrice = null;
		boolean isValidPredictionFound = false;
		MultiplePrice recPrice = null;
		
		for (Double price : pricePoints) {
//			PricePointDTO pricePointDTO = priceMovementMap.get(price);
			//price point will be always at unit level
			PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
			// Use only price points with prediction status success and
			// predicted movement > 0
			if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
					&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
				double tPrice = price;
				double tRevenue = price * pricePointDTO.getPredictedMovement();
				if (revenue == null) {
					revenue = tRevenue;
					finalPrice = tPrice;
				} else if (tRevenue < revenue) {
					revenue = tRevenue;
					finalPrice = tPrice;
				}
			} else {
				logger.debug("Ignoring price point" + price + " with prediction status " + pricePointDTO.getPredictionStatus().getStatusCode()
						+ " and predicted movement " + pricePointDTO.getPredictedMovement());
			}
			isValidPredictionFound = true;
		}

		/*
		 * Apply "Use guidelines and constraints objective when the prediction status is error or prediction status is sucess but
		 * predicted movement is 0 for all price points
		 */
		if (!isValidPredictionFound) {
			recPrice = applyFollowRules(pricePoints, regPrice);
		} else {
			recPrice = new MultiplePrice(1, finalPrice);
		}
		return recPrice;
	}

	private MultiplePrice applyMinMarginDollar(Double[] pricePoints, HashMap<MultiplePrice, PricePointDTO> priceMovementMap, Double cost, Double regPrice) {
		Double margin = null;
		Double finalPrice = null;
		MultiplePrice recPrice = null;
		
		if (cost != null) {
			boolean isValidPredictionFound = false;
			for (Double price : pricePoints) {
//				PricePointDTO pricePointDTO = priceMovementMap.get(price);
				//price point will be always at unit level
				PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
				// Use only price points with prediction status success and
				// predicted movement > 0
				if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
						&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
					double tPrice = price;
					double tMargin = (price - cost) / price;
					if (margin == null) {
						margin = tMargin;
						finalPrice = tPrice;
					} else if (tMargin < margin) {
						margin = tMargin;
						finalPrice = tPrice;
					}
				} else {
					logger.debug("Ignoring price point" + price + " with prediction status " + pricePointDTO.getPredictionStatus().getStatusCode()
							+ " and predicted movement " + pricePointDTO.getPredictedMovement());
				}
				isValidPredictionFound = true;
			}

			/*
			 * Apply "Use guidelines and constraints objective when the prediction status is error or prediction status is sucess
			 * but predicted movement is 0 for all price points
			 */
			if (!isValidPredictionFound) {
				recPrice = applyFollowRules(pricePoints, regPrice);
			} else {
				recPrice = new MultiplePrice(1, finalPrice);
			}
		} else {
			// When there is no list cost, pick the price closer to current
			// price
			recPrice = applyFollowRules(pricePoints, regPrice);
		}
		return recPrice;
	}

	/**
	 * Returns a price point from the given array of price points that is closest to the given price.
	 * @param pricePoints An array of Double values denoting various prices for an item
	 * @param regPrice A Double value denoting the current price of an item
	 * @return A MultiplePrice object for the value from pricePoints that has the minimum absolute difference from the regPrice.
	 */
	private MultiplePrice applyFollowRules(Double[] pricePoints, Double regPrice) {
		Double finalPrice = null;
		double closestDiff = 0;
		double closestPrice = 0;
		boolean first = true;
		// Use all price points
		for (Double price : pricePoints) {
			if (first) {
				closestPrice = price;
				if (regPrice != null) {
					closestDiff = Math.abs(regPrice - price);
				}
				first = false;
			} else {
				if (regPrice != null) {
					double diff = Math.abs(regPrice - price);
					if (diff < closestDiff) {
						closestPrice = price;
						closestDiff = diff;
					}
				}
			}
		}
		finalPrice = closestPrice;
		return new MultiplePrice(1, finalPrice);
	}
	
	/**
	 * return -1 - decreased, 0 - unchanged, 1 - increased
	 * @param itemPriceHistory
	 * @param itemCode
	 * @param inpWeekStartDate
	 * @param noOfDays
	 * @return
	 * @throws GeneralException
	 */
	private int itemPriceChangeStatus(HashMap<String, RetailPriceDTO> itemPriceHistory, LocalDate baseWeekStartDate, int noOfWeeks)
			throws GeneralException {
		int itemPriceChangeStatus = 0;

		LocalDate baseWeekEndDate = baseWeekStartDate.plusDays(6);
		LocalDate inpStartDateRange = baseWeekStartDate.minusWeeks(noOfWeeks - 1);
		
		// Convert to weeks so that price history
		// is checked within these weeks
//		long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(inpStartDateRange, baseWeekEndDate);
//		int noOfWeeks = (int) Math.ceil(daysBetween / 7.0);
		Double lastRegUnitPrice = 0d;
		for (int i = 0; i < noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)), Constants.APP_DATE_FORMAT);

			// Get that week price
			if (itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				Double regUnitPrice = PRCommonUtil.getUnitPrice(itemPrice.getRegMPack(), Double.valueOf(itemPrice.getRegPrice()),
						Double.valueOf(itemPrice.getRegMPrice()), true);

				if (regUnitPrice > 0 && itemPrice.getRegEffectiveDate() != null) {
					LocalDate regEffectiveDate = DateUtil.toDateAsLocalDate(itemPrice.getRegEffectiveDate());

					// Check if there is price within the date range
					if (regEffectiveDate.isAfter(inpStartDateRange) && regEffectiveDate.isBefore(baseWeekEndDate) && lastRegUnitPrice == 0) {
						lastRegUnitPrice = regUnitPrice;
					//} else if (regEffectiveDate.isAfter(inpStartDateRange) && regEffectiveDate.isBefore(baseWeekEndDate)) {
					} else {

						if (lastRegUnitPrice > 0 && lastRegUnitPrice - regUnitPrice > 0) {
							itemPriceChangeStatus = 1;
							break;
						} else if (lastRegUnitPrice > 0 && lastRegUnitPrice - regUnitPrice < 0) {
							itemPriceChangeStatus = -1;
							break;
						}
					}
				}
			}
		}

		return itemPriceChangeStatus;
	}
	
	private MultiplePrice getMaxPriceInLastXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate, int noOfWeeks, String priceType)
			throws GeneralException {
		MultiplePrice maxPrice = null;
		Double lastUnitPrice = 0d;
		RetailPriceDTO retailPriceDTO = null;
		
		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(inpBaseWeekStartDate);
		
		for (int i = 0; i < noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)), Constants.APP_DATE_FORMAT);

			// Get that week price
			if (itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				
				Double unitPrice = null;
				if(priceType.equals("REG")) {
					unitPrice = PRCommonUtil.getUnitPrice(itemPrice.getRegMPack(), Double.valueOf(itemPrice.getRegPrice()),
							Double.valueOf(itemPrice.getRegMPrice()), true);
				} else {
					unitPrice = PRCommonUtil.getUnitPrice(itemPrice.getSaleMPack(), Double.valueOf(itemPrice.getSalePrice()),
							Double.valueOf(itemPrice.getSaleMPrice()), true);
				}
//				logger.debug("unit price for week " + tempWeekStartDate + ":" + unitPrice);

				if (unitPrice != null && unitPrice > 0) {
					if (lastUnitPrice == 0) {
						retailPriceDTO = itemPrice;
						lastUnitPrice = unitPrice;
					} else if (unitPrice > lastUnitPrice) {
						retailPriceDTO = itemPrice;
						lastUnitPrice = unitPrice;
					}
				}
			}
		}

		if (retailPriceDTO != null) {
			if (priceType.equals("REG")) {
				maxPrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getRegMPack(), Double.valueOf(retailPriceDTO.getRegPrice()),
						Double.valueOf(retailPriceDTO.getRegMPrice()));
			} else {
				maxPrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getSaleMPack(), Double.valueOf(retailPriceDTO.getSalePrice()),
						Double.valueOf(retailPriceDTO.getSaleMPrice()));
			}
		}
		return maxPrice;
	}
	
	private boolean isRegPriceDecreasedInLastXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory, String baseWeekStartDate, int noOfWeeks)
			throws GeneralException {

		int priceChangeStatus = itemPriceChangeStatus(itemPriceHistory, DateUtil.toDateAsLocalDate(baseWeekStartDate), noOfWeeks);

		boolean isPriceDecreased = (priceChangeStatus == -1 ? true : false);

		return isPriceDecreased;
	}
	
	private boolean isRegPriceIncreasedInLastXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory, String baseWeekStartDate, int noOfWeeks)
			throws GeneralException {

		int priceChangeStatus = itemPriceChangeStatus(itemPriceHistory, DateUtil.toDateAsLocalDate(baseWeekStartDate), noOfWeeks);

		boolean isPriceIncreased = (priceChangeStatus == 1 ? true : false);

		return isPriceIncreased;
	}
		
	private boolean isItemInSaleXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate, int noOfWeeks)
			throws GeneralException {
		
		boolean isItemInSale = false;
		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(inpBaseWeekStartDate);

		for (int i = 0; i < noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)), Constants.APP_DATE_FORMAT);

			if (itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				Double unitPrice = PRCommonUtil.getUnitPrice(itemPrice.getSaleMPack(), Double.valueOf(itemPrice.getSalePrice()),
						Double.valueOf(itemPrice.getSaleMPrice()), true);
				if(unitPrice != null && unitPrice > 0) {
					isItemInSale = true;
					break;
				}
			}
		}
		return isItemInSale;
	}
	
//	7-May-2017 : Changes to Enable/Disable a filter from configuration : the new method has been formed
	/*
	public Double[] filterFinalPricePointsOld(PRItemDTO itemDTO, HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			RetailCalendarDTO curCalDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws GeneralException {

		PricingEngineService pricingEngineService = new PricingEngineService();
//		Double[] actualPriceRange = itemDTO.getPriceRange();
		List<Double> filteredPriceRange = new ArrayList<Double>();
		Double[] finalPricepoints = null;

		HashMap<String, RetailPriceDTO> itemPrices = itemZonePriceHistory.get(itemDTO.getItemCode());
		Double curUnitRegPrice = PRCommonUtil.getUnitPrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice(), true);

		// There is price history and current retail
		if (itemPrices != null && curUnitRegPrice != null && itemDTO.getPriceRange() != null && itemDTO.getPriceRange().length > 0) {
			
			boolean isFirstFilterApplied = false;
			
			boolean isPriceIncreased = isRegPriceIncreasedInLastXWeeks(itemPrices, curCalDTO.getStartDate(), regRetailIncreaseInLastXWeeks);
			boolean isPriceDecreased = isRegPriceDecreasedInLastXWeeks(itemPrices, curCalDTO.getStartDate(), regRetailDecreaseInLastXWeeks);
			boolean isPriceUnchanged = pricingEngineService.isRegPriceUnchangedInLastXWeeks(itemPrices, curCalDTO.getStartDate(), regRetailUnchangedInLastXWeeks);
			MultiplePrice maxRegRetail = getMaxPriceInLastXWeeks(itemPrices, curCalDTO.getStartDate(), maxRegRetailInLastXWeeks, "REG");
			MultiplePrice maxSalePrice = getMaxPriceInLastXWeeks(itemPrices, curCalDTO.getStartDate(), maxSalePriceInLastXWeeks, "SALE");
			boolean isItemNotOnSale = isItemInSaleXWeeks(itemPrices, curCalDTO.getStartDate(), noSaleInLastXWeeks);
			
			List<Double> actualPricePointsList = new ArrayList<Double>();
			for (Double pricePoint : itemDTO.getPriceRange()) {
				actualPricePointsList.add(pricePoint);
			}
			
			if((isPriceIncreased || isPriceDecreased) && itemDTO.getCostChgIndicator() <= 0) {
				// If Reg retail was increased during last 13 weeks, then the retail will not be 
				// increased again unless such increase is warranted by a cost increase that took 
				// place within last 13 weeks or due to a cost increase that’s scheduled to take 
				// place in the next 2 weeks.
				
				// If Reg retail was decreased during last 13 weeks, 
				// then the retail will not be increased, unless such increase was 
				// necessitated by a cost increase.
				
				// Cost is not increased, remove all price points above current price point
				isFirstFilterApplied = true;
				if(pricingEngineService.isRuleEnabled(recommendationRuleMap, "R1", itemDTO.getObjectiveTypeId()) || 
						pricingEngineService.isRuleEnabled(recommendationRuleMap, "R2", itemDTO.getObjectiveTypeId())){
					
					if(pricingEngineService.isRuleEnabled(recommendationRuleMap, "R1", itemDTO.getObjectiveTypeId())){
						logger.debug("filterFinalPricePoints() :: Pricing Rule - R1 is enabled and applied to item :" + itemDTO.getItemCode());
					} else{
						logger.debug("filterFinalPricePoints() :: Pricing Rule - R2 is enabled and applied to item :" + itemDTO.getItemCode());
					}
					
					for (Double pricePoint : actualPricePointsList) {
						// price points is less than current price
						if (pricePoint <= curUnitRegPrice) {
							filteredPriceRange.add(pricePoint);
						}
					}
				

				logger.debug("Reg retail was " + (isPriceIncreased ? "raised" : "decreased") + " during last "
						+ (isPriceIncreased ? regRetailIncreaseInLastXWeeks : regRetailDecreaseInLastXWeeks)
						+ " weeks. Do not raise further, unless cost increased. " + "Cur Reg Unit Price:" + curUnitRegPrice + ",Actual price points: "
						+ PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePointsList) + ",Filtered price points: "
						+ PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange));
				} else{
					filteredPriceRange.addAll(actualPricePointsList);
				}
				// If Reg retail was decreased during last 13 weeks, then
				// the retail will not be decreased, unless such decrease 
				// was supported by a cost decrease or to meet
				// the Index/Competition guideline
				
				// price decreased, cost unchanged
				if(isPriceDecreased && itemDTO.getCostChgIndicator() == 0 && 
						pricingEngineService.isRuleEnabled(recommendationRuleMap, "R5", itemDTO.getObjectiveTypeId()) ) {

					logger.debug("filterFinalPricePoints() :: Pricing Rule - R5 is enabled and applied to item :" + itemDTO.getItemCode());
					boolean isIndexGuidelinePresent = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.PRICE_INDEX.getGuidelineTypeId());
					boolean isMultiCompGuidelinePresent = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.COMPETITION.getGuidelineTypeId());
					//There is index or competition guideline
					if(isIndexGuidelinePresent || isMultiCompGuidelinePresent) {
					} else {
						//Stay with current price, when cost didn't change and no index/comp guideline
						filteredPriceRange.clear();
					}
					
					logger.debug("Reg retail was decreased during last " + regRetailDecreaseInLastXWeeks
							+ " weeks. Do not change further, as cost is unchanged and no index/comp guideline. " + "Cur Reg Unit Price:"
							+ curUnitRegPrice + ",Actual price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePointsList)
							+ ",Filtered price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange));
				}
				
				maintainBrandAndSizeRelation(itemDTO, actualPricePointsList, filteredPriceRange);
				
				//update explain log
				if (filteredPriceRange.size() == 0) {
					logger.debug("1. Addition log");
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(itemDTO.getCurRegPriceEffDate());
					if (isPriceDecreased) {
						if(pricingEngineService.isRuleEnabled(recommendationRuleMap, "R2", itemDTO.getObjectiveTypeId()) ||
								pricingEngineService.isRuleEnabled(recommendationRuleMap, "R5", itemDTO.getObjectiveTypeId())){
							logger.debug("filterFinalPricePoints() :: Update explain Log set against - R2 & R5");
							pricingEngineService.writeAdditionalExplainLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_DECREASED, additionalDetails);
						}
					} else {
						if(pricingEngineService.isRuleEnabled(recommendationRuleMap, "R1", itemDTO.getObjectiveTypeId())){
							logger.debug("filterFinalPricePoints() :: Update explain Log set against - R1 ");
							pricingEngineService.writeAdditionalExplainLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_INCREASED, additionalDetails);
						}
					}
				} else {
					String pricePoints = getIgnoredPricePoints(actualPricePointsList, filteredPriceRange);
					if(pricePoints != "") {
						logger.debug("1-1. Addition log");
						List<String> additionalDetails = new ArrayList<String>();
						additionalDetails.add(pricePoints);
						additionalDetails.add(itemDTO.getCurRegPriceEffDate());
						if (isPriceDecreased) {
							if(pricingEngineService.isRuleEnabled(recommendationRuleMap, "R2", itemDTO.getObjectiveTypeId()) ||
									pricingEngineService.isRuleEnabled(recommendationRuleMap, "R5", itemDTO.getObjectiveTypeId())){
								logger.debug("filterFinalPricePoints() :: Update explain Log set against - R2 & R5");
								pricingEngineService.writeAdditionalExplainLog(itemDTO,
									ExplainRetailNoteTypeLookup.PP_IGNORED_RECENTLY_DECREASED, additionalDetails);
							}
						} else {
							if(pricingEngineService.isRuleEnabled(recommendationRuleMap, "R1", itemDTO.getObjectiveTypeId())){
								logger.debug("filterFinalPricePoints() :: Update explain Log set against - R1");
								pricingEngineService.writeAdditionalExplainLog(itemDTO,
									ExplainRetailNoteTypeLookup.PP_IGNORED_RECENTLY_INCREASED, additionalDetails);
							}
						}
					}
				}
				
			} else if (isPriceUnchanged && pricingEngineService.isRuleEnabled(recommendationRuleMap, "R4", itemDTO.getObjectiveTypeId())) {
				
				logger.debug("filterFinalPricePoints() :: Pricing Rule - R4 is enabled and applied to item :" + itemDTO.getItemCode());
				List<Double> tempPriceRange = null;
				// If item hasn’t gone through a Reg retail change during last 52 weeks
				// Don’t increase retail > 5%, unless warranted by a cost
				// increase.
				if (itemDTO.getCostChgIndicator() <= 0 && 
						pricingEngineService.isRuleEnabled(recommendationRuleMap, "R4A", itemDTO.getObjectiveTypeId())) {
					logger.debug("filterFinalPricePoints() :: Pricing Rule - R4A is enabled and applied to item :" + itemDTO.getItemCode());
					isFirstFilterApplied = true;
					
					Double maxIncreaseRetail = (curUnitRegPrice + (curUnitRegPrice * (maxRegRetailIncreasePctForUnchangedItem / 100.0)));
					maxIncreaseRetail = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(maxIncreaseRetail));
					for (Double pricePoint : actualPricePointsList) {
						if (pricePoint <= maxIncreaseRetail) {
							filteredPriceRange.add(pricePoint);
						}
					}
					logger.debug("Item hasn't gone through reg retail change during " + regRetailUnchangedInLastXWeeks + " weeks "
							+ ".Do not increase retail > " + maxIncreaseRetail + "(" + maxRegRetailIncreasePctForUnchangedItem
							+ "%), unless cost increased" + ". Cur Reg Unit Price:" + curUnitRegPrice + ",Actual price points: "
							+ PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePointsList) + ",Filtered price points: "
							+ PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange));
					
					maintainBrandAndSizeRelation(itemDTO, actualPricePointsList, filteredPriceRange);
					
					//update explain log
					if (filteredPriceRange.size() == 0) {
						logger.debug("2. Addition log");
						List<String> additionalDetails = new ArrayList<String>();
						additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
						additionalDetails.add(maxRegRetailIncreasePctForUnchangedItem + "%");
						pricingEngineService.writeAdditionalExplainLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_INCREASE, additionalDetails);
					} else {
						String pricePoints = getIgnoredPricePoints(actualPricePointsList, filteredPriceRange);
						if (pricePoints != "") {
							logger.debug("2-1. Addition log");
							List<String> additionalDetails = new ArrayList<String>();
							additionalDetails.add(pricePoints);
							additionalDetails.add(maxRegRetailIncreasePctForUnchangedItem + "%");
							additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));

							pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_CANNOT_INCREASE,
									additionalDetails);

						}
					}
				} else {
					filteredPriceRange.clear();
					for (Double pricePoint : actualPricePointsList) {	
						filteredPriceRange.add(pricePoint);
					}
				}
				
				// For items priced < $15.00, 
				// While raising the retail, if new retail crosses the next $ range 
				// (e.g. going from $2.89 to $3.09) and there is another retail available
				// without crossing the next $ range (e.g. $2.99), use the retail within 
				// the existing $ range, unless such retail increase is necessitated to 
				// maintain the per unit margin $ mandated by the Margin Guideline
				boolean isMarginGuidelinePresent = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
				if (curUnitRegPrice < maxRegRetailNotToCrossNextRange && !isMarginGuidelinePresent && 
						pricingEngineService.isRuleEnabled(recommendationRuleMap, "R4D-1", itemDTO.getObjectiveTypeId()) ) {
						
					logger.debug("filterFinalPricePoints() :: Pricing Rule - R4D-1 is enabled and applied to item :" + itemDTO.getItemCode());
						
					tempPriceRange = new ArrayList<Double>();
					isFirstFilterApplied = true;
					int curUnitRegPriceWholeNumber = (int) Math.floor(curUnitRegPrice);
					for (Double pricePoint : filteredPriceRange) {
						if (curUnitRegPriceWholeNumber == (int) Math.floor(pricePoint) || pricePoint <= curUnitRegPrice) {
							tempPriceRange.add(pricePoint);
						}
					}
					
					logger.debug("Item price < $" + maxRegRetailNotToCrossNextRange + " don't cross to next range " + ".Cur Reg Unit Price:"
							+ curUnitRegPrice + ",Actual price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange)
							+ ",Filtered price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(tempPriceRange));
					
					maintainBrandAndSizeRelation(itemDTO, filteredPriceRange, tempPriceRange);
					
					//update explain log
					if (filteredPriceRange.size() > 0 && tempPriceRange.size() == 0) {
						logger.debug("3. Addition log");
						List<String> additionalDetails = new ArrayList<String>();
						additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
						additionalDetails.add(PRFormatHelper.doubleToTwoDigitString(maxRegRetailNotToCrossNextRange));
						pricingEngineService.writeAdditionalExplainLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_NEXT_RANGE, additionalDetails);
					} else {
						String pricePoints = getIgnoredPricePoints(filteredPriceRange, tempPriceRange);
						if (pricePoints != "") {
							logger.debug("3-1. Addition log");
							List<String> additionalDetails = new ArrayList<String>();
							additionalDetails.add(pricePoints);
							additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
							additionalDetails.add(PRFormatHelper.doubleToTwoDigitString(maxRegRetailNotToCrossNextRange));

							pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_NO_NEXT_RANGE,
									additionalDetails);

						}
					}
					
					filteredPriceRange.clear();
					filteredPriceRange.addAll(tempPriceRange);
					tempPriceRange.clear();
					
				}
				
				// Retail will not be reduced below the highest sale/TPR 
				// price observed in last 52 weeks, unless there is a cost decrease 
				if (itemDTO.getCostChgIndicator() >= 0 && 
						pricingEngineService.isRuleEnabled(recommendationRuleMap, "R4B", itemDTO.getObjectiveTypeId())) {
					
					logger.debug("filterFinalPricePoints() :: Pricing Rule - R4B is enabled and applied to item :" + itemDTO.getItemCode());
					
					if (maxSalePrice != null) {
						Double maxSaleUnitPrice = PRCommonUtil.getUnitPrice(maxSalePrice, true);
						
						tempPriceRange = new ArrayList<Double>();
						isFirstFilterApplied = true;
						
						// Remove price points below the highest sale unit price
						for (Double pricePoint : filteredPriceRange) {
							if (pricePoint >= maxSaleUnitPrice) {
								tempPriceRange.add(pricePoint);
							}
						}
						
						logger.debug("Don't reduce below highest sale price: " + maxSaleUnitPrice + " in last " + maxSalePriceInLastXWeeks
								+ " weeks, unless cost decrease. " + "Cur Reg Unit Price:" + curUnitRegPrice + ",Actual price points: "
								+ PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange) + ",Filtered price points: "
								+ PRCommonUtil.getCommaSeperatedStringFromDouble(tempPriceRange));
						
						maintainBrandAndSizeRelation(itemDTO, filteredPriceRange, tempPriceRange);
						
						if (filteredPriceRange.size() > 0 && tempPriceRange.size() == 0) {
							logger.debug("4. Addition log");
							List<String> additionalDetails = new ArrayList<String>();
							additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
							additionalDetails.add(String.valueOf(maxSalePriceInLastXWeeks));
							pricingEngineService.writeAdditionalExplainLog(itemDTO,
									ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_DECREASE, additionalDetails);
						} else {
							String pricePoints = getIgnoredPricePoints(filteredPriceRange, tempPriceRange);
							if (pricePoints != "") {
								logger.debug("4-1. Addition log");
								List<String> additionalDetails = new ArrayList<String>();
								additionalDetails.add(pricePoints);
								additionalDetails.add(String.valueOf(maxSalePriceInLastXWeeks));
								additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));

								pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_CANNOT_DECREASE,
										additionalDetails);

							}
						}
						
						filteredPriceRange.clear();
						filteredPriceRange.addAll(tempPriceRange);
						tempPriceRange.clear();
						
						
					}
					
				}
				
				
				// If Item was never on sale during last 52 weeks, Retail will not be reduced by > 5%, 
				// unless there is a cost decrease or comp price decrease
				if ((itemDTO.getCostChgIndicator() >= 0 && itemDTO.getCompPriceChgIndicator() >= 0) && 
						pricingEngineService.isRuleEnabled(recommendationRuleMap, "R4C", itemDTO.getObjectiveTypeId())) {
					
					logger.debug("filterFinalPricePoints() :: Pricing Rule - R4C is enabled and applied to item :" + itemDTO.getItemCode());	
					tempPriceRange = new ArrayList<Double>();
					
					if(!isItemNotOnSale) {
						isFirstFilterApplied = true;
						
						// Don't go below 5% of cur price
						Double minPrice = (curUnitRegPrice - (curUnitRegPrice * (noSaleItemDecreasePct / 100.0)));
						minPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(minPrice));
						
						for (Double pricePoint : filteredPriceRange) {
							if (pricePoint >= minPrice) {
								tempPriceRange.add(pricePoint);
							}
						}

						logger.debug("Item was never on sale in last " + noSaleInLastXWeeks + " weeks, don't reduce > " + noSaleItemDecreasePct
								+ "% (" + minPrice + ")unless cost or comp decrease. " + "Cur Reg Unit Price:" + curUnitRegPrice
								+ ",Actual price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange)
								+ ",Filtered price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(tempPriceRange));

						maintainBrandAndSizeRelation(itemDTO, filteredPriceRange, tempPriceRange);

						if (filteredPriceRange.size() > 0 && tempPriceRange.size() == 0) {
							logger.debug("5. Addition log");
							List<String> additionalDetails = new ArrayList<String>();
							additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
							additionalDetails.add(String.valueOf(noSaleItemDecreasePct) + "%");
							pricingEngineService.writeAdditionalExplainLog(itemDTO,
									ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_SALE, additionalDetails);
						} else {
							String pricePoints = getIgnoredPricePoints(filteredPriceRange, tempPriceRange);
							if (pricePoints != "") {
								logger.debug("5-1. Addition log");
								List<String> additionalDetails = new ArrayList<String>();
								additionalDetails.add(pricePoints);
								additionalDetails.add(String.valueOf(noSaleItemDecreasePct) + "%");
								additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));

								pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_NO_SALE,
										additionalDetails);

							}
						}
						
						filteredPriceRange.clear();
						filteredPriceRange.addAll(tempPriceRange);
						tempPriceRange.clear();
					}
				}
			}
			
			// if above filters are not applied
			if (!isFirstFilterApplied) {
				filteredPriceRange.clear();
				for (Double pricePoint : actualPricePointsList) {
					filteredPriceRange.add(pricePoint);
				}
			}
						
			if (pricingEngineService.isRuleEnabled(recommendationRuleMap, "R15", itemDTO.getObjectiveTypeId()) ) {
					
				logger.debug("filterFinalPricePoints() :: Pricing Rule - R15 is enabled and applied to item :" + itemDTO.getItemCode());
					
				List<Double> tempPriceRange = new ArrayList<Double>();
				isFirstFilterApplied = true;
				int curUnitRegPriceWholeNumber = (int) Math.floor(curUnitRegPrice);
				for (Double pricePoint : filteredPriceRange) {
					// Price point <= current price and price points in the same digit as current price
					if (curUnitRegPriceWholeNumber == (int) Math.floor(pricePoint) || pricePoint <= curUnitRegPrice) {
						tempPriceRange.add(pricePoint);
					}
				}
				
				logger.debug("Don't cross to next range " + ".Cur Reg Unit Price:"
						+ curUnitRegPrice + ",Actual price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange)
						+ ",Filtered price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(tempPriceRange));
				
				maintainBrandAndSizeRelation(itemDTO, filteredPriceRange, tempPriceRange);
				
				//update explain log
				if (filteredPriceRange.size() > 0 && tempPriceRange.size() == 0) {
					//If there is no option, stay with all price points
					tempPriceRange.addAll(filteredPriceRange);
				} else {
					String pricePoints = getIgnoredPricePoints(filteredPriceRange, tempPriceRange);
					if (pricePoints != "") {
						logger.debug("3-1. Addition log");
						List<String> additionalDetails = new ArrayList<String>();
						additionalDetails.add(pricePoints);

						pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE,
								additionalDetails);

					}
				}
				
				filteredPriceRange.clear();
				filteredPriceRange.addAll(tempPriceRange);
				tempPriceRange.clear();
			}

			// if above filters are not applied
			if (!isFirstFilterApplied) {
				filteredPriceRange.clear();
				for (Double pricePoint : actualPricePointsList) {
					filteredPriceRange.add(pricePoint);
				}
			}
			
			// Retail is not increased greater than 5% than the highest 
			// Reg price point in last 52 weeks, unless there was a cost increase.
			
			List<Double> finalFilteredPriceRange = new ArrayList<Double>();
			if (maxRegRetail != null && itemDTO.getCostChgIndicator() <= 0 &&  
					pricingEngineService.isRuleEnabled(recommendationRuleMap, "R3", itemDTO.getObjectiveTypeId())) {
				
				logger.debug("filterFinalPricePoints() :: Pricing Rule - R3 is enabled and applied to item :" + itemDTO.getItemCode());
				Double maxRegUnitPrice = PRCommonUtil.getUnitPrice(maxRegRetail, true);
				Double maxIncreaseRetail = (maxRegUnitPrice + (maxRegUnitPrice * (maxRegRetailIncreasePct / 100.0)));
				maxIncreaseRetail = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(maxIncreaseRetail));
				for (Double pricePoint : filteredPriceRange) {
					if (pricePoint <= maxIncreaseRetail) {
						finalFilteredPriceRange.add(pricePoint);
					}
				}

				logger.debug("max price point: " + maxRegRetail.toString() + " in last " + maxRegRetailInLastXWeeks + " weeks. Do not raise more than "
						+ maxIncreaseRetail + " (" + maxRegRetailIncreasePct + "%) from max price point." + " First filter price points:"
						+ PRCommonUtil.getCommaSeperatedStringFromDouble(filteredPriceRange) + ",Final filtered price points: "
						+ PRCommonUtil.getCommaSeperatedStringFromDoubleArray(finalFilteredPriceRange));
				
				maintainBrandAndSizeRelation(itemDTO, filteredPriceRange, finalFilteredPriceRange);
				
				if (filteredPriceRange.size() > 0 && finalFilteredPriceRange.size() == 0) {
					logger.debug("6. Addition log");
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(String.valueOf(maxRegRetailIncreasePct) + "%");
					additionalDetails.add(String.valueOf(maxRegRetailInLastXWeeks));
					pricingEngineService.writeAdditionalExplainLog(itemDTO,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				}  else {
					String pricePoints = getIgnoredPricePoints(filteredPriceRange, finalFilteredPriceRange);
					if (pricePoints != "") {
						logger.debug("6-1. Addition log");
						List<String> additionalDetails = new ArrayList<String>();
						additionalDetails.add(pricePoints);
						additionalDetails.add(String.valueOf(maxRegRetailIncreasePct) + "%");
						additionalDetails.add(String.valueOf(maxRegRetailInLastXWeeks));

						pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_EXCEED_HIGHEST_RETAIL,
								additionalDetails);

					}
				}
				
			} else {
				finalFilteredPriceRange = filteredPriceRange;
			}


			finalPricepoints = new Double[finalFilteredPriceRange.size()];

			for (int i = 0; i < finalFilteredPriceRange.size(); ++i) {
				finalPricepoints[i] = finalFilteredPriceRange.get(i);
			}

		} else {
			finalPricepoints = itemDTO.getPriceRange();
		}

		return finalPricepoints;
	}
	*/
	
//	 If there is brand or size relation, don't break it
//	 If the filtering of price point is not going to give any
//	 price point it would probably break the brand or size relation, 
//	 retain the price points as those price points
//	 would have already passed brand and size relation
	private void maintainBrandAndSizeRelation(PRItemDTO itemDTO, List<Double> actualPricePoints, List<Double> filteredPricePoints) {
		PricingEngineService pricingEngineService = new PricingEngineService();
		boolean isBrandGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		boolean isSizeGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO, GuidelineTypeLookup.SIZE.getGuidelineTypeId());
		if ((isBrandGuidelineApplied || isSizeGuidelineApplied) && filteredPricePoints.size() == 0) {
			for (Double pricePoint : actualPricePoints) {
				filteredPricePoints.add(pricePoint);
			}
			logger.debug("No price point available after filtering, so price points are retained as it may break the brand and size relation");
		}
	}
	
	private String getIgnoredPricePoints(List<Double> actualPricePoints, List<Double> filteredPricePoints) {
		String ignoredPricePoints = "";
		List<Double> tempList = new ArrayList<>(actualPricePoints);
		tempList.removeAll(filteredPricePoints);
		
		for (Double d : tempList) {
			ignoredPricePoints = ignoredPricePoints + "," + PRFormatHelper.doubleToTwoDigitString(d);
		}
		
		if (ignoredPricePoints != "") {
			ignoredPricePoints = ignoredPricePoints.substring(1);
		}

		return ignoredPricePoints;
	}

	private boolean hasPredForOneOfThePricePoint(HashMap<MultiplePrice, PricePointDTO> priceMovementMap, Double[] pricePoints) {
		boolean hasPredForOneOfThePricePoint = false;
		for (Double price : pricePoints) {

			PricePointDTO pricePointDTO = priceMovementMap.get(new MultiplePrice(1, price));
			if (pricePointDTO != null && pricePointDTO.getPredictionStatus() != null
					&& pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()
					&& pricePointDTO.getPredictedMovement() != null && pricePointDTO.getPredictedMovement() > 0) {
				hasPredForOneOfThePricePoint = true;
				break;
			}
		}
		
		return hasPredForOneOfThePricePoint;
	}
	
	public Double[] filterFinalPricePoints(PRItemDTO itemDTO, HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			RetailCalendarDTO curCalDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws GeneralException, Exception {
		PricingEngineService pricingEngineService = new PricingEngineService();
		HashMap<String, RetailPriceDTO> itemPrices = itemZonePriceHistory.get(itemDTO.getItemCode());
		
		Double curUnitRegPrice = PRCommonUtil.getUnitPrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice(), true);
		List<Double> actualPricePointsList = new ArrayList<Double>();
		
		if (itemPrices != null && curUnitRegPrice != null && itemDTO.getPriceRange() != null && itemDTO.getPriceRange().length > 0) {
			for (Double pricePoint : itemDTO.getPriceRange()) {
				actualPricePointsList.add(pricePoint);
			}
			// A retail is not increased greater than 5% than the highest Reg price point in the last 52 weeks, 
			// unless there is a cost increase or brand/size violation.   Price points which are more than 5% will be ignored.  
			// If all the price points are more than 5%, then the price point which is closer to 5% will be chosen.
			FilterPricePointsRule recommendationRuleR3 = new CheckPPWithinXPctThanHighestPrevPriceRule(recommendationRuleMap,
					itemDTO, actualPricePointsList, itemPrices, curCalDTO.getStartDate(), maxRegRetailInLastXWeeks, "REG");
			actualPricePointsList = recommendationRuleR3.applyRule();
			
			// If an item hasn’t gone through a Reg retail change during the last 52 weeks
			boolean isPriceUnchanged = pricingEngineService.isRegPriceUnchangedInLastXWeeks(itemPrices, curCalDTO.getStartDate(),
					regRetailUnchangedInLastXWeeks);
			FilterPricePointsRule recRuleR4 = new SameRetailInLastXWeeksRule(recommendationRuleMap, itemDTO, actualPricePointsList, itemPrices,
					curCalDTO.getStartDate(), noSaleInLastXWeeks, "SALE", curUnitRegPrice, isPriceUnchanged);
			actualPricePointsList = recRuleR4.applyRule();

			// Ignore all price points which crosses to next dollar range of current price.
			// If all the price points are crossing to next dollar range, then stay with those price points
			FilterPricePointsRule recommendationRuleR15 = new CheckNextDollarCrossingPPRule(recommendationRuleMap, itemDTO, actualPricePointsList,
					curUnitRegPrice);
			actualPricePointsList = recommendationRuleR15.applyRule();
			return actualPricePointsList.stream().toArray(Double[]::new);
		}else {
			return itemDTO.getPriceRange();
		}
		
		
	}

}
