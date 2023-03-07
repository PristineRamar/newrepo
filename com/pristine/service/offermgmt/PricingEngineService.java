package com.pristine.service.offermgmt;

//import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
//import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.pristine.dao.CompetitiveDataDAO;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.DashboardDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
//import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dao.pricingalert.PricingAlertDAO;
import com.pristine.dataload.offermgmt.Dashboard;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.DashboardDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainAdditionalDetail;
//import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
//import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineLeadZoneDTO;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRGuidelinePI;
//import com.pristine.dto.offermgmt.PRGuidelinePIDetail;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStorePrice;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
//import com.pristine.dto.offermgmt.PRSubstituteGroup;
//import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionRunHeaderDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
//import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.lookup.offermgmt.PriceReactionTimeLookup;
import com.pristine.service.RetailPriceServiceOptimized;
//import com.pristine.service.offermgmt.constraint.CostConstraint;
//import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
//import com.pristine.service.offermgmt.prediction.PredictionStatus;
//import com.pristine.service.offermgmt.prediction.PredictionSubstituteItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

@SuppressWarnings("unused")
public class PricingEngineService {
	private static Logger logger = Logger.getLogger("PricingEngineService");
	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	
	//config is added for RA as when price increase or decrease is elected as immediate they do not want the reccs to be postdated
	//for others it will postdate to date when future cost is effective
	Boolean useCostEffDateToPostDate = Boolean.parseBoolean(PropertyManager.getProperty("POST_DATE_USING_FUTURE_COST_DATE", "TRUE"));

	/***
	 * Add all distinct comp store id defined in price index guideline
	 * @param itemDataMap
	 * @param compIdMap
	 */
	public void addDistinctCompStrId(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, LocationKey> compIdMap) {
		Set<LocationKey> distinctPICompStrId = new HashSet<LocationKey>();
		HashMap<LocationKey, LocationKey> distinctActualCompStrId =  new HashMap<LocationKey, LocationKey>();
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			if (!itemDTO.isLir()) {
				// If price index guideline is defined
				if (itemDTO.getStrategyDTO() != null && itemDTO.getStrategyDTO().getGuidelines() != null
						&& itemDTO.getStrategyDTO().getGuidelines().getPiGuideline() != null
						&& itemDTO.getStrategyDTO().getGuidelines().getPiGuideline().size() > 0) {
					for (PRGuidelinePI guidelinePI : itemDTO.getStrategyDTO().getGuidelines().getPiGuideline()) {
						if (guidelinePI.getCompStrId() > 0) {
							LocationKey lk = new LocationKey(Constants.STORE_LEVEL_ID, guidelinePI.getCompStrId());
							distinctPICompStrId.add(lk);	
						}
					}
				}
			}
		}
		for (LocationKey locationKey : compIdMap.values()) {
			distinctActualCompStrId.put(locationKey, locationKey);
		}

		int dummyKey = 1;
		// Add to compIdMap
		for (LocationKey locationKey : distinctPICompStrId) {
			if (distinctActualCompStrId.get(locationKey) == null) {
				compIdMap.put(dummyKey, locationKey);
				dummyKey = dummyKey + 1;
			}
		}
	}

	/***
	 * Set item's comp current price, check date, price change indicator and comp
	 * previous price
	 * 
	 * @param itemInfo
	 * @param compStrId
	 */
	public void setCompPriceOfItem(PRItemDTO itemInfo, HashMap<Integer, LocationKey> compIdMap) {
		LocationKey compKey = null;
		if (itemInfo.getStrategyDTO() != null && itemInfo.getStrategyDTO().getGuidelines() != null
				&& itemInfo.getStrategyDTO().getGuidelines().getPiGuideline() != null
				&& itemInfo.getStrategyDTO().getGuidelines().getPiGuideline().size() > 0) {
			for (PRGuidelinePI guidelinePI : itemInfo.getStrategyDTO().getGuidelines().getPiGuideline()) {
				if (guidelinePI.getCompStrId() > 0)
					compKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelinePI.getCompStrId());
			}
		}
		if (compKey == null && compIdMap != null && compIdMap.get(PRConstants.COMP_TYPE_1) != null)
			compKey = compIdMap.get(PRConstants.COMP_TYPE_1);

		itemInfo.setCompStrId(compKey);
		itemInfo.setCompTypeId(1);
		// itemInfo.getCompPrice() - Com Str Id as Key, Reg Price as value
		if (itemInfo.getAllCompPrice() != null && itemInfo.getAllCompPrice().size() > 0
				&& itemInfo.getAllCompPrice().get(compKey) != null) {
			itemInfo.setCompPrice(itemInfo.getAllCompPrice().get(compKey));
			itemInfo.setCompCurSalePrice(itemInfo.getAllCompSalePrice().get(compKey));
			if (itemInfo.getAllCompPriceChgIndicator() != null
					&& itemInfo.getAllCompPriceChgIndicator().get(compKey) != null)
				itemInfo.setCompPriceChgIndicator(itemInfo.getAllCompPriceChgIndicator().get(compKey));
			if (itemInfo.getAllCompPriceCheckDate() != null && itemInfo.getAllCompPriceCheckDate().get(compKey) != null)
				itemInfo.setCompPriceCheckDate(itemInfo.getAllCompPriceCheckDate().get(compKey));
		}
		if (itemInfo.getAllCompPreviousPrice().get(compKey) != null) {
			itemInfo.setCompPreviousPrice(itemInfo.getAllCompPreviousPrice().get(compKey));
		}
//		logger.debug("Comp Str Id - " + (compKey != null ? compKey.getLocationId() : "") + "\t"
//				+ (itemInfo.getAllCompPrice() != null ? itemInfo.getAllCompPrice().get(compKey) : ""));
	}

	
	public List<PRStorePrice> getStorePrices(Connection conn, long recommendationRunId, List<Integer> itemCodes)
			throws OfferManagementException {
		List<PRStorePrice> storePrices = new ArrayList<PRStorePrice>();
		this.conn = conn;
		try {
			storePrices = getStorePrices(recommendationRunId, itemCodes);
		} catch (OfferManagementException ex) {
			throw new OfferManagementException("Error in getStorePrices() - " + ex, 
					RecommendationErrorCode.GENERAL_EXCEPTION);	
		} finally {
			PristineDBUtil.close(this.conn);
		}
		return storePrices;
	}

	public List<PRStorePrice> getStorePrices(long recommendationRunId, List<Integer> itemCodes)
			throws OfferManagementException {
		List<PRStorePrice> storePrices = new ArrayList<PRStorePrice>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

		if (this.conn == null) {
			initializeForWS();
			this.conn = getConnection();
		}

		try {
			storePrices = pricingEngineDAO.getStorePrices(this.conn, recommendationRunId, itemCodes);
		} catch (OfferManagementException ex) {
			throw new OfferManagementException("Error in getStorePrices() - " + ex,
					RecommendationErrorCode.GENERAL_EXCEPTION);
		} finally {
			PristineDBUtil.close(this.conn);
		}

		return storePrices;
	}

	public List<Double> addPreviousAndNextPricePoints(PRItemDTO item, int noOfLowerPoints, int noOfHigherPoints) {
		List<Double> pricePoints = new ArrayList<Double>();
		double firstPrice = 0d, lastPrice = 0d;
		int pricePointCnt = 0;
		// Find the first and last price point
		for (double price : item.getPriceRange()) {
			if (pricePointCnt == 0)
				firstPrice = price;
			if ((pricePointCnt + 1) == item.getPriceRange().length)
				lastPrice = price;
			pricePointCnt = pricePointCnt + 1;
		}

		pricePoints = addPreviousAndNextPricePoints(firstPrice, lastPrice, item.getStrategyDTO(), noOfLowerPoints,
				noOfHigherPoints);
		return pricePoints;
	}

	// Added on 13th Jan 2015, to add previous price point of first price point and
	// next price point of last price point
	// of a item(the price points are found after applying the guidelines and
	// constraints). This is done to avoid number of
	// price points send during margin opportunity so that less number of price
	// points would be send there
	/***
	 * Add two lower price point from first price point and two higher price point
	 * from last price point
	 * 
	 * @param item
	 * @return
	 */
	public List<Double> addPreviousAndNextPricePoints(double firstPrice, double lastPrice, PRStrategyDTO itemStrategy,
			int noOfLowerPoints, int noOfHigherPoints) {
		List<Double> preAndNextRoundingDigit = new ArrayList<Double>();
		//double preRoundingDigit1 = 0d, nextRoundingDigit1 = 0d, preRoundingDigit2 = 0d, nextRoundingDigit2 = 0d;
		
		firstPrice = (firstPrice == 0 ? lastPrice : firstPrice);
		if (firstPrice > 0) {
			double lastPricePoint = firstPrice;
			for (int i = 0; i < noOfLowerPoints; i++) {
				double newLowerPricePoint = itemStrategy.getConstriants().getRoundingConstraint()
						.roundupPriceToPreviousRoundingDigit(lastPricePoint - 0.01);
				if (newLowerPricePoint > 0)
					preAndNextRoundingDigit.add(newLowerPricePoint);
				lastPricePoint = newLowerPricePoint;
			}
		}
		
		lastPrice = (lastPrice == 0 ? firstPrice : lastPrice);
		if (lastPrice > 0) {
			double lastPricePoint = lastPrice;
			for (int i = 0; i < noOfHigherPoints; i++) {
				double newHigherPricePoint = itemStrategy.getConstriants().getRoundingConstraint()
						.roundupPriceToNextRoundingDigit(lastPricePoint + 0.01);
				if (newHigherPricePoint > 0)
					preAndNextRoundingDigit.add(newHigherPricePoint);
				lastPricePoint = newHigherPricePoint;
			}
		}
		
		// If both are same
		// Subtract 1 cents, so that current price is not returned while finding
		// pre and next price point
//		if (firstPrice == lastPrice) {
//			if (firstPrice > 0) {
//				preRoundingDigit1 = itemStrategy.getConstriants().getRoundingConstraint()
//						.roundupPriceToPreviousRoundingDigit(firstPrice - 0.01);
//				nextRoundingDigit1 = itemStrategy.getConstriants().getRoundingConstraint()
//						.roundupPriceToNextRoundingDigit(firstPrice + 0.01);
//			}
//		} else {
//			if (firstPrice > 0) {
//				// Get the Previous Price Point
//				preRoundingDigit1 = itemStrategy.getConstriants().getRoundingConstraint()
//						.roundupPriceToPreviousRoundingDigit(firstPrice - 0.01);
//			}
//
//			if (lastPrice > 0) {
//				// Get the Next Price Point
//				nextRoundingDigit1 = itemStrategy.getConstriants().getRoundingConstraint()
//						.roundupPriceToNextRoundingDigit(lastPrice + 0.01);
//			}
//		}
//
//		if (preRoundingDigit1 > 0)
//			preRoundingDigit2 = itemStrategy.getConstriants().getRoundingConstraint()
//					.roundupPriceToPreviousRoundingDigit(preRoundingDigit1 - 0.01);
//
//		if (nextRoundingDigit1 > 0)
//			nextRoundingDigit2 = itemStrategy.getConstriants().getRoundingConstraint()
//					.roundupPriceToNextRoundingDigit(nextRoundingDigit1 + 0.01);
//
//		preAndNextRoundingDigit.add(preRoundingDigit2);
//		preAndNextRoundingDigit.add(preRoundingDigit1);
//		preAndNextRoundingDigit.add(nextRoundingDigit1);
//		preAndNextRoundingDigit.add(nextRoundingDigit2);
		
		Collections.sort(preAndNextRoundingDigit);
		
		return preAndNextRoundingDigit;
	}
	
	
	/***
	 * Get already predicted movement / average movement for the passed items
	 * @param conn
	 * @param items
	 * @param recommendationRunHeader
	 * @param isUsePrediction
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<PredictionDetailKey, PredictionDetailDTO> getAlreadyPredictedValues(Connection conn,
			List<PRItemDTO> items, PRRecommendationRunHeader recommendationRunHeader,
			boolean isUsePrediction) throws GeneralException {
		PredictionService predictionService = new PredictionService();
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
				
		if (isUsePrediction) {
			predictions = predictionService.getAlreadyPredictedMovementOfItems(conn,
					recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(),
					recommendationRunHeader.getCalendarId(), recommendationRunHeader.getCalendarId(), items, true);
		} else {
			predictions = predictionService.getAvgMovementFromItem(recommendationRunHeader.getLocationLevelId(),
					recommendationRunHeader.getLocationId(), items);
		}

		return predictions;
	}
	

	public Long getPredictedMov(long itemCode, int locationLevelId, int locationId, MultiplePrice multiplePrice,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction){
		Long predictedMov = null;
		PredictionDetailKey predictionDetailKey;
		//below variable is added to get the default values for parameter which is not applicable
		//here e.g. sale, page no, block no, display id, promo type id
		PredictionDetailDTO tempPredictionDetailDTO = new PredictionDetailDTO();
		
		if (isUsePrediction) {
			predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, itemCode, multiplePrice.multiple, multiplePrice.price,
					tempPredictionDetailDTO.getSaleQuantity(), tempPredictionDetailDTO.getSalePrice(),
					tempPredictionDetailDTO.getAdPageNo(), tempPredictionDetailDTO.getBlockNo(),
					tempPredictionDetailDTO.getPromoTypeId(), tempPredictionDetailDTO.getDisplayTypeId());
		} else {
			predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, itemCode, 0, 0d,
					tempPredictionDetailDTO.getSaleQuantity(), tempPredictionDetailDTO.getSalePrice(),
					tempPredictionDetailDTO.getAdPageNo(), tempPredictionDetailDTO.getBlockNo(),
					tempPredictionDetailDTO.getPromoTypeId(), tempPredictionDetailDTO.getDisplayTypeId());
		}
		
		PredictionDetailDTO predictionDetailDTO = predictions.get(predictionDetailKey);


		if (predictionDetailDTO != null) {
			predictedMov =  Math.round(predictionDetailDTO.getPredictedMovement());
		}
		return predictedMov;
	}
	
	public void updateConflictsExceptRounding(PRItemDTO itemInfo){
		updateConflicts(itemInfo, true);
	}
	
	public void updateConflicts(PRItemDTO itemInfo) {
		updateConflicts(itemInfo, false);
	}
	
	private void updateConflicts(PRItemDTO itemInfo, boolean ignoreRounding){
		//Conflicts has to be updated based on different scenarios
		//1. Ahold - Cost Change, No Relation -- no guideline/constraint is marked as conflict
		//2. Ahold - Cost Change, National Brand and/or Size Relation - Mark conflicts against the recommended price except the margin guideline
		//3. Others - Mark conflicts against the recommended price (Check if the recommended price is following the guidelines/constraints)		
		
		//Reset all conflicts
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemInfo.getExplainLog().getGuidelineAndConstraintLogs()) {
			guidelineAndConstraintLog.setIsConflict(false);
		}
		
		markConflictAgainstRecommendedPrice(itemInfo, ignoreRounding);

		boolean isItemInConflict = false;
		//Mark item as conflict, if any of the guideline/constraint in conflict
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemInfo.getExplainLog().getGuidelineAndConstraintLogs()) {
			if(guidelineAndConstraintLog.getIsConflict()){
				itemInfo.setIsConflict(1);
				isItemInConflict = true;
				break;
			}
		}	
		if(!isItemInConflict){
			itemInfo.setIsConflict(0);
		}
	}
	
	private void markConflictAgainstRecommendedPrice(PRItemDTO itemInfo, boolean ignoreRounding) {
//		boolean isPriceWithinRange1 = false, isPriceWithinRange2 = false;
		PRRange marginRange = new PRRange();
		boolean markConflicts = true;
		boolean isNationalBrandOrSizeRelationAvailable = checkIfAnyNationalOrSizeBrandAvailable(itemInfo);
		boolean ignoreMarginAsConflict = false;
		

		// Added condition to avoid marking conflicts when the current price is retained
		// Changes done by Pradeep on 02/11/2019 as part of price points optimization
		
		
		//Check if the reg price is already recommended
		// if (itemInfo.getRecommendedRegPrice() != null && itemInfo.getRecommendedRegPrice() > 0) {
		if (itemInfo.getRecommendedRegPrice() != null && itemInfo.getRecommendedRegPrice().price > 0
				&& !itemInfo.isCurPriceRetained()) {
			// locked price changes need to be reflected to user
			if (itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)
					&& itemInfo.getIsLocPriced() == 0) {
				FilterPriceRange filterPriceRange = new FilterPriceRange();
				marginRange = filterPriceRange.getGuidelineRange(itemInfo.getExplainLog(),
						GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
				double nextRoundingDigit = itemInfo.getStrategyDTO().getConstriants().getRoundingConstraint()
						.roundupPriceToNextRoundingDigit(marginRange.getStartVal());
				// If the recommended price as expected(same as margin guideline or next
				// rounding digit)
//				if(itemInfo.getRecommendedRegPrice() == nextRoundingDigit){
				if (itemInfo.getRecommendedRegPrice().price == nextRoundingDigit) {
					if (isNationalBrandOrSizeRelationAvailable) {
						// Don't need to margin as conflict
						markConflicts = true;
						ignoreMarginAsConflict = true;
					} else {
						markConflicts = false;
					}
				}
			} else if (itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)
					&& itemInfo.getIsLocPriced() == 1) {
				ignoreMarginAsConflict = true;
			}
			
			// Mark conflict against recommended price
			for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemInfo.getExplainLog()
					.getGuidelineAndConstraintLogs()) {
				if (markConflicts) {
//					isPriceWithinRange1 = false;
//					isPriceWithinRange2 = false;
					//set the conflict false before and then if there is a conflict mark it as true
					guidelineAndConstraintLog.setIsConflict(false);

					
					if (ignoreMarginAsConflict && guidelineAndConstraintLog
							.getGuidelineTypeId() == GuidelineTypeLookup.MARGIN.getGuidelineTypeId()) {
					} else {
						// Check if the MAP constraint is breaking because of pending retail for items
						// on lock and mak it as conflict.added for AZ
						if (guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.LOCKED_PRICE
								.getConstraintTypeId()) {
							if (itemInfo.getMapRetail() > 0) {
								for (Double roundingDigit : itemInfo.getPriceRange()) {
									if (!roundingDigit.equals(itemInfo.getRecommendedRegPrice().price)) {
										guidelineAndConstraintLog.setIsConflict(true);
										break;
									}
								}
							}

						}
						// Don't mark pre-price/locked price as conflict, when the current price is in
						// multiple & when no cost changed, because if the
						// current price is multiple, we just pass input as output
						else if ((guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId() 
								||	(guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.LOCKED_PRICE.getConstraintTypeId() && 
								itemInfo.getCostChgIndicator() == 0)) && itemInfo.getRecommendedRegPrice().multiple > 1){
							
						}else if (guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.ROUNDING.getConstraintTypeId()
								&& !ignoreRounding) {
							//Check if recommended price is part of rounding. Added to mark rounding as conflict, because when
							//the current cost is not available, we just follow the current price
							boolean isRecPricePartOfRounding = false;
							if(guidelineAndConstraintLog.getRoundingDigits() != null ){
								for(Double roundingDigit : guidelineAndConstraintLog.getRoundingDigits()){
									//if(roundingDigit.equals(itemInfo.getRecommendedRegPrice())){
									if(roundingDigit.equals(itemInfo.getRecommendedRegPrice().price)){
										isRecPricePartOfRounding = true;
										break;
									}
								}
							}
							if(!isRecPricePartOfRounding)
								guidelineAndConstraintLog.setIsConflict(true);
								
						} else {
							
							guidelineAndConstraintLog.setIsConflict(isRecommendedPriceBreaksGuidelinesOrConstraints(
									itemInfo, guidelineAndConstraintLog));
						}
					}
				}
			}
		}
	}
	
	public boolean isRecommendedPriceBreaksGuidelinesOrConstraints(PRItemDTO itemInfo,
			PRGuidelineAndConstraintLog guidelineAndConstraintLog) {
		boolean isPriceWithinRange1 = false, isPriceWithinRange2 = false;
		boolean isBroken = false;
//		isPriceWithinRange1 = checkIfPriceWithinRange(itemInfo.getRecommendedRegMultiple(),
//				itemInfo.getRecommendedRegPrice(), guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1(),
//				guidelineAndConstraintLog);
		isPriceWithinRange1 = checkIfPriceWithinRange(itemInfo.getRecommendedRegPrice().multiple,
				itemInfo.getRecommendedRegPrice().price, guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1(),
				guidelineAndConstraintLog);
		// Check both range for threshold
		if (guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.THRESHOLD.getConstraintTypeId()) {
			PRRange range2 = guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2();
			if (range2.getStartVal() != Constants.DEFAULT_NA && range2.getEndVal() != Constants.DEFAULT_NA) {
//				isPriceWithinRange2 = checkIfPriceWithinRange(itemInfo.getRecommendedRegMultiple(),
//						itemInfo.getRecommendedRegPrice(),
//						guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2(), guidelineAndConstraintLog);
				isPriceWithinRange2 = checkIfPriceWithinRange(itemInfo.getRecommendedRegPrice().multiple,
						itemInfo.getRecommendedRegPrice().price,
						guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange2(), guidelineAndConstraintLog);
			}
		}
		// If recommended price is out of range
		if (isPriceWithinRange1 || isPriceWithinRange2) {
			isBroken = false;
		} else {
			isBroken = true;
		}
		return isBroken;
	}
		
	private boolean checkIfAnyNationalOrSizeBrandAvailable(PRItemDTO itemInfo){
		boolean isNationalBrandOrSizeRelationAvailable = false;
		boolean isStoreBrandRelationAvailable = false;
		boolean isNationalBrandRelationAvailable = false;
		boolean isSizeRelationAvailable = false;
		boolean isBrandGuidelinePresent = true;
		boolean isSizeGuidelinePresent = true;
		
		if(itemInfo.getStrategyDTO() == null){
			return isNationalBrandOrSizeRelationAvailable;
		}else{
			if(itemInfo.getStrategyDTO().getGuidelines() == null) {
				return isNationalBrandOrSizeRelationAvailable;
			}
		}
		
		// When a brand guideline is excluded, need not take it as national
		// brand though it is in price group
		if (itemInfo.getStrategyDTO().getGuidelines().getBrandGuideline() == null) {
			isBrandGuidelinePresent = false;
		}
		else if (itemInfo.getStrategyDTO().getGuidelines().getBrandGuideline() != null) {
			if (itemInfo.getStrategyDTO().getGuidelines().getBrandGuideline().size() == 0) {
				isBrandGuidelinePresent = false;
			}
		}
		
		if (itemInfo.getStrategyDTO().getGuidelines().getSizeGuideline() == null){
			isSizeGuidelinePresent = false;
		}
		else if (itemInfo.getStrategyDTO().getGuidelines().getSizeGuideline() != null) {
			if (itemInfo.getStrategyDTO().getGuidelines().getSizeGuideline().size() == 0) {
				isSizeGuidelinePresent = false;
			}
		}		
		
		if (itemInfo.getPgData() != null) {
			NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = null;
			navigableMap = itemInfo.getPgData().getRelationList();
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				if (entry.getKey() == PRConstants.BRAND_RELATION) {
					for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
						if (itemInfo.getPgData().getBrandClassId() == BrandClassLookup.NATIONAL.getBrandClassId() && 
								isBrandGuidelinePresent && relatedItem.getPriceRelation() != null) {
							isNationalBrandRelationAvailable = true;
						}
						else{
							isStoreBrandRelationAvailable = true;
						}							
					}
				}
			}
			
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				if (entry.getKey() == PRConstants.SIZE_RELATION && isSizeGuidelinePresent) {
					for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
						//Ignore size lead items as size relation will not be applied
						if(relatedItem.getRelatedItemCode() > 0 )
							isSizeRelationAvailable = true;
					}
				}
			}
		}
		
		if(isStoreBrandRelationAvailable){
			isNationalBrandOrSizeRelationAvailable = false;
		}
		else if(isNationalBrandRelationAvailable || isSizeRelationAvailable){
			isNationalBrandOrSizeRelationAvailable = true;
		}

		return isNationalBrandOrSizeRelationAvailable;
	}
	
	public boolean checkIfPriceWithinRange(Integer inpRegMultiple, Double inpRegPrice, PRRange inputRange,
			PRGuidelineAndConstraintLog guidelineAndConstraintLog) {
		boolean isPriceWithinRange = false;
		Double regPrice;
		int regMultiple = 1;
		//double regPrice = Double.valueOf(regPriceInput); 
		if(inpRegMultiple == null)
			regMultiple = 1;
		else if(inpRegMultiple == 0)
			regMultiple = 1;
		else
			regMultiple = inpRegMultiple;
		
		if (regMultiple > 1)
			regPrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(inpRegPrice / regMultiple);
		else
			regPrice = inpRegPrice;
		
		// Both the start and end is not defined
		if (inputRange.getStartVal() == Constants.DEFAULT_NA && inputRange.getEndVal() == Constants.DEFAULT_NA) {
			isPriceWithinRange = true;
		}
		// Both the start and end is defined
		else if (inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA) {
			if (regPrice >= inputRange.getStartVal() && regPrice <= inputRange.getEndVal())
				isPriceWithinRange = true;
			 
			//If Brand or Size Guideline (Brand/size need not to be marked as conflict when there is below/above relation, even if the
			//recommended price is not exactly below or above. e.g. if brand says 0.30 below, then if the rec price is below 0.40, then don't
			//mark as conflict
			if (guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
					|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE.getGuidelineTypeId()) {
				// If both start and end value are same
				if (inputRange.getStartVal() == inputRange.getEndVal()) {
					// If the relation is below
					if (guidelineAndConstraintLog.getOperatorText().equals(PRConstants.PRICE_GROUP_EXPR_BELOW) || 
							guidelineAndConstraintLog.getOperatorText().equals(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM) ||
							guidelineAndConstraintLog.getOperatorText().equals(PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM)) {						 
						// Check if price is less than start val
						if (regPrice <= inputRange.getStartVal()) {
							isPriceWithinRange = true;
						}
					}
					// If the relation is above
					if (guidelineAndConstraintLog.getOperatorText().equals(PRConstants.PRICE_GROUP_EXPR_ABOVE) || 
							guidelineAndConstraintLog.getOperatorText().equals(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM) ||
							guidelineAndConstraintLog.getOperatorText().equals(PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM)) {
						// Check if price is greater than end val
						if (regPrice >= inputRange.getEndVal()) {
							isPriceWithinRange = true;
						}
					}
				}
			}		
		}
		// Only Start is defined
		else if (inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() == Constants.DEFAULT_NA) {
			if (regPrice >= inputRange.getStartVal())
				isPriceWithinRange = true;
		}
		// Only End is defined
		else if (inputRange.getStartVal() == Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA) {
			if (regPrice <= inputRange.getEndVal())
				isPriceWithinRange = true;
		}
		
		return isPriceWithinRange;
	}
	
	/***
	 * check if cost/lower/higher/threshold of the constraint is broken
	 * @param itemInfo
	 * @return
	 */
	public boolean isCostOrLowerHigherOrThresholdBroken(PRItemDTO itemInfo) {
		boolean isConstraintBroken = false;
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemInfo.getExplainLog()
				.getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.COST.getConstraintTypeId()
					|| guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.LOWER_HIGHER
							.getConstraintTypeId()
					|| guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.THRESHOLD
							.getConstraintTypeId()) {
				isConstraintBroken = isRecommendedPriceBreaksGuidelinesOrConstraints(itemInfo,
						guidelineAndConstraintLog);
				if (isConstraintBroken)
					break;
			}
		}
		return isConstraintBroken;
	}
	
	/**
	 * Updates data into dashboard table
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param runId
	 * @throws OfferManagementException 
	 * @throws GeneralException 
	 */
	public void updateDashboard(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId, long runId)
			throws OfferManagementException, GeneralException{
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		DashboardDAO dashboardDAO = new DashboardDAO(conn);
		try{
			//Run id will be -1, if startegy what-if is done, in that case there is no need to update the dashboard
			if(productLevelId == Constants.CATEGORYLEVELID && runId > 0){
				RetailPriceZoneDAO rpzDAO = new RetailPriceZoneDAO();
				Dashboard dashboard = new Dashboard(conn);
				dashboard.setChainId(new RetailPriceDAO().getChainId(conn));
				// Retrieves list of items from PR_RECOMMENDATION table for the run
				List<PRItemDTO> itemList = pricingEngineDAO.getRecommendationItems(conn, runId);
				// Retrieves KVI List Id for zone
				int kviListId = rpzDAO.getKviListForZone(conn, locationId);
				// Retrieves list of KVI items
				ArrayList<Integer> kviList = new PricingAlertDAO().getKVIItems(conn, kviListId);
				// Retrieves parent hierarchy for a category (Major Category, Portfolio). Used in determining strategy applied for a category.
				HashMap<Integer, com.pristine.dto.offermgmt.ProductDTO> categoryHierarchy = dashboardDAO.getCategoryHierarchy(productId);
				// Retrieves division id for input zone
				HashMap<Integer, Integer> zoneDivisionMap = rpzDAO.getZoneDivisionMap(conn, locationId);
				// Retrieves location list ids that containts input zone. Used in determining strategy applied for a category.
				ArrayList<Integer> zoneIdList = dashboardDAO.getLocationListId(locationLevelId, locationId);
				dashboard.setCurrentDate(DateUtil.getWeekStartDate(-1));
				// Retrieves active strategies as on current date
				HashMap<String, HashMap<String, Integer>> strategyMap = dashboardDAO.getStrategies(dashboard.getCurrentDateStr());
				// Retrieves strategy id for the category
				Integer stratId = dashboard.getStrategyId(categoryHierarchy, strategyMap, zoneIdList, zoneDivisionMap.get(locationId), locationLevelId, locationId, productLevelId, productId);
				// Retrieves strategy id at KVI level for the category
				Integer kviStratId = dashboard.getStrategyId(categoryHierarchy, strategyMap, zoneIdList, zoneDivisionMap.get(locationId), locationLevelId, locationId, productLevelId, productId, kviListId);
				DashboardService dashboardService = new DashboardService();
				PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
				
				DashboardDTO dashboardDTO = new DashboardDTO();
				dashboardDTO.setLocationLevelId(locationLevelId);
				dashboardDTO.setLocationId(locationId);
				dashboardDTO.setProductLevelId(productLevelId);
				dashboardDTO.setProductId(productId);
				
				// Invoke method that computes Future PI
				//Double futurePI = getFuturePI(itemList);
//				Double value = null;
//				
//				if(futurePI != null){
//					double avgMovement = 0;
//					for(PRItemDTO item : itemList){
//						avgMovement += (item.getPredictedMovement() == null)? 1 : item.getPredictedMovement();
//					}
//					if(stratId != null){
//						PRStrategyDTO strategy = pricingEngineDAO.getStrategyDefinition(getConnection(), new Long(stratId));
//						if(strategy.getGuidelines() != null)
//							// Invoke method that computes Value (Amount in Dollars customer has to spend to reach target PI)
//							value = getValue(futurePI, avgMovement, strategy.getGuidelines().getPiGuideline());
//					}
//				}
				
				Double futurePI = priceIndexCalculation.getFutureSimpleIndex(itemList, true);
				Double currentPI = priceIndexCalculation.getCurrentSimpleIndex(itemList);
				Double value = dashboardService.calculateValue(conn, stratId, futurePI, itemList);
				
				int itemCount = 0;
				int kviItemCount = 0;
				int totConflict = 0;
				int kviTotConflict = 0;
				int reviewCount = 0;
				int kviReviewCount = 0;
				
				int totErrorItem = 0;
				int disTotErrorItem = 0;
				int kviTotErrorItem = 0;
				int disKviTotErrorItem = 0;

				int disTotConflict = 0;
				int disKviTotConflict = 0;
//				Set<Integer> ligTotConflict = new HashSet<Integer>();
//				Set<Integer> ligKviTotConflict = new HashSet<Integer>();
//				
//				Set<Integer> ligTotErrorItem = new HashSet<Integer>();
//				Set<Integer> ligKviTotErrorItem = new HashSet<Integer>();

				if (itemList != null) {
					for (PRItemDTO item : itemList) {
						// Counts number of items for which a new price was recommended
						// lig members and non lig
						if (item.getIsNewPriceRecommended() == 1 && !item.isLir()) {
							itemCount++;
							if (kviList.contains(item.getItemCode())) {
								kviItemCount++;
							}
						}
						// Counts number of items which had conflicts when applying
						// guidelines/constraints
						if (item.getIsConflict() == 1) {
							if (!item.isLir())
								totConflict++;
							if (kviList.contains(item.getItemCode())) {
								if (!item.isLir())
									kviTotConflict++;
								if (item.isLir()) {
									disKviTotConflict++;
								} else if (!item.isLir() && item.getRetLirId() < 1) {
									disKviTotConflict++;
								}
							}

							if (item.isLir()) {
								disTotConflict++;
							} else if (!item.isLir() && item.getRetLirId() < 1) {
								disTotConflict++;
							}
						}

						// No of items with error
						if (item.getIsRecError()) {
							if (!item.isLir())
								totErrorItem++;

							if (item.isLir()) {
								disTotErrorItem++;
							} else if (!item.isLir() && item.getRetLirId() < 1) {
								disTotErrorItem++;
							}

							// Kvi
							if (kviList.contains(item.getItemCode())) {
								if (!item.isLir())
									kviTotErrorItem++;
								if (item.isLir()) {
									disKviTotErrorItem++;
								} else if (!item.isLir() && item.getRetLirId() < 1) {
									disKviTotErrorItem++;
								}
							}
						}
						// no of manager review
						if (item.getIsMarkedForReview().equals(String.valueOf(Constants.YES)) && !item.isLir()) {
							reviewCount++;
							if (kviList.contains(item.getItemCode())) {
								kviReviewCount++;
							}
						}
					}
				}

				dashboardDTO.setFuturePI(futurePI);
				dashboardDTO.setCurrentPI(currentPI);
				dashboardDTO.setValue(value);
				dashboardDTO.setTotConflict(totConflict);
				dashboardDTO.setKviTotConflict(kviTotConflict);
				dashboardDTO.setLigTotConflict(disTotConflict);
				dashboardDTO.setLigKviTotConflict(disKviTotConflict);
				dashboardDTO.setNoOfRecommendation(itemCount);
				dashboardDTO.setKviNoOfRecommendation(kviItemCount);
				dashboardDTO.setStrategyId(stratId);
				dashboardDTO.setKviStrategyId(kviStratId);
				dashboardDTO.setTotErrorItem(totErrorItem);
				dashboardDTO.setLigTotErrorItem(disTotErrorItem);
				dashboardDTO.setKviTotErrorItem(kviTotErrorItem);
				dashboardDTO.setLigKviTotErrorItem(disKviTotErrorItem);

				List<PRItemDTO> kviItems = new ArrayList<PRItemDTO>();
				// Get kvi items
				for (PRItemDTO item : itemList) {
					if (kviList.contains(item.getItemCode())) {
						kviItems.add(item);
					}
				}
				Double kviFuturePI = priceIndexCalculation.getFutureSimpleIndex(kviItems, false);
				Double kviCurrentPI = priceIndexCalculation.getCurrentSimpleIndex(kviItems);
				Double kviValue = dashboardService.calculateValue(conn, kviStratId, kviFuturePI, kviItems);

				dashboardDTO.setKviFuturePI(kviFuturePI);
				dashboardDTO.setKviCurrentPI(kviCurrentPI);
				dashboardDTO.setKviValue(kviValue);
				dashboardDTO.setNoOfOverridden(0);
				dashboardDTO.setNoOfKVIOverridden(0);
				dashboardDTO.setNoOfManagerReview(reviewCount);
				dashboardDTO.setNoOfKVIManagerReview(kviReviewCount);
				dashboardDTO.setRecommendationRunId(runId);

//				logger.info("Updating strategy id - " + stratId + " for product " + productId);
//				logger.info("Updating kvi strategy id - " + kviStratId + " for product " + productId);

				// Update corresponding fields in PR_DASHBOARD table
				pricingEngineDAO.updateDashboardData(conn, dashboardDTO);

				// NU::26th Jul 2016, Dashboard screen will show the primary competitor from
				// recommendation header table
				// This is done as the primary competitor column is empty, but there is index
				// (as comp is specified at guideline level)
				// Note:: this will not work if Competition guideline or multiple competitor is
				// defined in price index guideline
				// as we don't know which competitor store id to be assigned against the item
				RecommendationAnalysis recommendationAnalysis = new RecommendationAnalysis();
				recommendationAnalysis.insertCompDetails(conn, runId, itemList);
			}
		} catch (GeneralException | Exception ge) {
			ge.printStackTrace();
			logger.error("Error while updating dashboard table " + ge);
			throw new GeneralException("Error while updating dashboard table - " + ge.getMessage());
		}

		pricingEngineDAO = null;
		dashboardDAO = null;
		// Changes for inserting Future PI and Value columns in dashboard table - Ends
	}

	/**
	 * Sort the Lig members according to the latest date
	 * 
	 * @param competitiveDataDTOs
	 */
	// Sorting the values based on the check Date
	public void sortByCheckDate(List<CompetitiveDataDTO> competitiveDataDTOs) {
		final SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);

		Collections.sort(competitiveDataDTOs, new Comparator<CompetitiveDataDTO>() {
			public int compare(final CompetitiveDataDTO competitiveDataDTO1,
					final CompetitiveDataDTO competitiveDataDTO2) {
				Date existingDate;
				Date newDate;
				int compare = 0;
				try {
					if (competitiveDataDTO1.checkDate == null && competitiveDataDTO2.checkDate == null) {
						compare = 0;
					} else if (competitiveDataDTO2.checkDate == null && competitiveDataDTO1.checkDate != null) {
						compare = -1;
					} else if (competitiveDataDTO2.checkDate != null && competitiveDataDTO1.checkDate == null) {
						compare = 1;
					} else {
						existingDate = sdf.parse(competitiveDataDTO2.checkDate);
						newDate = sdf.parse(competitiveDataDTO1.checkDate);
						compare = existingDate.compareTo(newDate);
					}
				} catch (Exception ex) {
					logger.error("Error while executing sortByCheckDate() ");
					try {
						throw new OfferManagementException("Error in sortByCheckDate() - " + ex,
								RecommendationErrorCode.GENERAL_EXCEPTION);
					} catch (OfferManagementException e) {
						e.printStackTrace();
					}
				}
				return compare;
			}
		});
	}

	public PRRecommendation getLeadZoneItem(PRItemDTO inputItem,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails) {
		PRRecommendation leadZoneItem = null;

		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRGuidelineLeadZoneDTO leadZoneGuideline = strategyDTO.getGuidelines().getLeadZoneGuideline();
		LocationKey locationKey = new LocationKey(leadZoneGuideline.getLocationLevelId(),
				leadZoneGuideline.getLocationId());
		ItemKey itemKey = PRCommonUtil.getItemKey(inputItem);

		if (leadZoneDetails.get(locationKey) != null && leadZoneDetails.get(locationKey).get(itemKey) != null) {
			leadZoneItem = leadZoneDetails.get(locationKey).get(itemKey);
		}
		return leadZoneItem;
	}

	public boolean isLeadZoneGuidelinePresent(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		boolean isLeadZoneGuidelinePresent = false;

		for (PRItemDTO itemDTO : itemDataMap.values()) {
			if (itemDTO.getStrategyDTO() != null && itemDTO.getStrategyDTO().getGuidelines() != null
					&& itemDTO.getStrategyDTO().getGuidelines().getLeadZoneGuideline() != null) {
				isLeadZoneGuidelinePresent = true;
				break;
			}
		}

		return isLeadZoneGuidelinePresent;
	}

	// Get latest run id items of a zone
	public HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> getLeadZoneDetails(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, PRStrategyDTO inputDTO) throws GeneralException {
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<Long> runIds = new ArrayList<Long>();
		Long latestRunId = 0l;
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<ItemKey, PRRecommendation> items = new HashMap<ItemKey, PRRecommendation>();
		PRGuidelineLeadZoneDTO guidelineLeadZone = new PRGuidelineLeadZoneDTO();

		// Get the first occurrence of strategy which has lead zone guideline
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			if (itemDTO.getStrategyDTO() != null && itemDTO.getStrategyDTO().getGuidelines() != null
					&& itemDTO.getStrategyDTO().getGuidelines().getLeadZoneGuideline() != null) {
				guidelineLeadZone = itemDTO.getStrategyDTO().getGuidelines().getLeadZoneGuideline();
				break;
			}
		}

		LocationKey locationKey = new LocationKey(guidelineLeadZone.getLocationLevelId(),
				guidelineLeadZone.getLocationId());
		latestRunId = pricingEngineDAO.getLatestRecommendationRunId(conn, guidelineLeadZone.getLocationLevelId(),
				guidelineLeadZone.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId());
		if (latestRunId > 0) {
			runIds.add(latestRunId);
			HashMap<Long, List<PRItemDTO>> runAndItItems = pricingEngineDAO.getRecItemsOfRunIds(conn, runIds);
			if (runAndItItems.get(latestRunId) != null) {
				for (PRItemDTO itemDTO : runAndItItems.get(latestRunId)) {
					PRRecommendation prRecommendation = new PRRecommendation();
					prRecommendation.setItemCode(itemDTO.getItemCode());
					if (itemDTO.getRecommendedRegPrice() != null) {
//						MultiplePrice multiplePrice = new MultiplePrice(itemDTO.getRecommendedRegMultiple(), itemDTO.getRecommendedRegPrice());
						MultiplePrice multiplePrice = itemDTO.getRecommendedRegPrice();
						prRecommendation.setRecRegPriceObj(multiplePrice);
					}
					items.put(PRCommonUtil.getItemKey(itemDTO), prRecommendation);
				}
			}
		}
		leadZoneDetails.put(locationKey, items);
		return leadZoneDetails;
	}

	// Fill sale details
	public void fillSaleDetails(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, String curStartDate, String recWeekStartDate,
			int noOfFutureWeeks) throws GeneralException, ParseException {
		// Add future weeks
		List<String> futureWeeks = getFutureWeeks(recWeekStartDate, noOfFutureWeeks);
//		int tprMinWeek = Integer.parseInt(PropertyManager.getProperty("TPR_MIN_WEEKS"));

		// Loop all lig members and non-lig items
		for (Map.Entry<ItemKey, PRItemDTO> items : itemDataMap.entrySet()) {
			ItemKey itemKey = items.getKey();
			PRItemDTO itemDTO = items.getValue();
			// consider only lig members and non-lig items
			if (itemKey.getLirIndicator() != PRConstants.LIG_ITEM_INDICATOR) {
				// Set sale details of the item

				if (saleDetails.get(itemDTO.getItemCode()) != null) {
					List<PRItemSaleInfoDTO> salePrices = saleDetails.get(itemDTO.getItemCode());

					for (PRItemSaleInfoDTO saleInfoDTO : salePrices) {
						// Set cur week sale details
						if (saleInfoDTO.getSaleWeekStartDate().equals(curStartDate)) {
							// pick first occurrence
							if (itemDTO.getCurSaleInfo().getSalePrice() == null) {
								itemDTO.setCurSaleInfo(saleInfoDTO);
							}
						}

						// Set rec week sale details
						if (saleInfoDTO.getSaleWeekStartDate().equals(recWeekStartDate)) {
							// pick first occurrence
							if (itemDTO.getRecWeekSaleInfo().getSalePrice() == null) {
								itemDTO.setRecWeekSaleInfo(saleInfoDTO);
								DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
								long promoDuration = ChronoUnit.DAYS.between(
										LocalDate.parse(saleInfoDTO.getSaleEndDate(), formatter),
										LocalDate.parse(saleInfoDTO.getSaleStartDate(), formatter));

//								long promoDuration = DateUtil.getDateDiff(saleInfoDTO.getSaleEndDate(), saleInfoDTO.getSaleStartDate());
//								if((promoDuration +1 ) > 7 * tprMinWeek) {
								itemDTO.setIsTPR(1);
//								} else {
//									itemDTO.setIsOnSale(1);
//								}
							}
						}
					}
					// Get latest future week sale details
					// Loop weeks
					for (String weekStartDate : futureWeeks) {
						for (PRItemSaleInfoDTO saleInfoDTO : salePrices) {
							if (saleInfoDTO.getSaleWeekStartDate().equals(weekStartDate)) {
								// pick first occurrence
								if (itemDTO.getFutWeekSaleInfo().getSalePrice() == null) {
									itemDTO.setFutWeekSaleInfo(saleInfoDTO);
								}
							}
						}
					}

				}
			}
		}

	}


	// Fill display details
	public void fillDisplayDetails(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails, String curStartDate, String recWeekStartDate,
			int noOfFutureWeeks) throws GeneralException {
		// Add future weeks
		List<String> futureWeeks = getFutureWeeks(recWeekStartDate, noOfFutureWeeks);

		// Loop all lig members and non-lig items
		for (Map.Entry<ItemKey, PRItemDTO> items : itemDataMap.entrySet()) {
			ItemKey itemKey = items.getKey();
			PRItemDTO itemDTO = items.getValue();
			// lig members and non-lig items
			if (itemKey.getLirIndicator() != PRConstants.LIG_ITEM_INDICATOR) {
				// Get all future display details of the item
				if (displayDetails.get(itemDTO.getItemCode()) != null) {
					List<PRItemDisplayInfoDTO> displayInfoList = displayDetails.get(itemDTO.getItemCode());

					for (PRItemDisplayInfoDTO displayInfoDTO : displayInfoList) {
						// Set cur week display details

						// Set rec week ad details
						if (displayInfoDTO.getDisplayWeekStartDate().equals(recWeekStartDate)) {
							// pick first occurrence
							if (itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() == null) {
								itemDTO.setRecWeekDisplayInfo(displayInfoDTO);
							}
						}
					}

					// Get latest future week display details
					// Loop weeks
					for (String weekStartDate : futureWeeks) {
						for (PRItemDisplayInfoDTO displayInfoDTO : displayInfoList) {
							if (displayInfoDTO.getDisplayWeekStartDate().equals(weekStartDate)) {
								// pick first occurrence
								if (itemDTO.getFutWeekDisplayInfo().getDisplayTypeLookup() == null) {
									itemDTO.setFutWeekDisplayInfo(displayInfoDTO);
								}
							}
						}
					}

				}
			}
		}
	}

	/**
	 * To fill Sale, Ad and Display Promotion for Current week, Recommendation week
	 * and Future week If an item has future promotion, then consider future promo
	 * with most recent date to display in UI Set TPR or Sale flag based on Promo
	 * durations
	 * 
	 * @param itemDataMap
	 * @param adDetails
	 * @param displayDetails
	 * @param saleDetails
	 * @param curStartDate
	 * @param recWeekStartDate
	 * @param noOfFutureWeeks
	 * @throws GeneralException
	 * @throws CloneNotSupportedException
	 * @throws ParseException
	 */
	public void fillPromoDetails(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, String curStartDate, String recWeekStartDate,
			int noOfFutureWeeks) throws GeneralException, CloneNotSupportedException, ParseException {

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		int tprMinWeek = Integer.parseInt(PropertyManager.getProperty("LONG_TERM_PROMO_WEEKS"));

		// Add future weeks
		List<String> futureWeeks = getFutureWeeks(recWeekStartDate, noOfFutureWeeks);
		// Sort Future weeks in desc order
		List<String> futureWeekDates = futureWeeks.stream().map(date -> LocalDate.parse(date, formatter))
				.sorted(Comparator.reverseOrder()).map(date -> formatter.format(date)).collect(Collectors.toList());

		List<PRItemDTO> itemList = new ArrayList<>();
		itemDataMap.forEach((key, value) -> {
			itemList.add(value);
		});

		for (Map.Entry<ItemKey, PRItemDTO> items : itemDataMap.entrySet()) {
			ItemKey itemKey = items.getKey();
			PRItemDTO itemDTO = items.getValue();
			// consider only lig members and non-lig items
			if (itemKey.getLirIndicator() != PRConstants.LIG_ITEM_INDICATOR) {

				// Set sale details of the item
				if (saleDetails.get(itemDTO.getItemCode()) != null) {
					List<PRItemSaleInfoDTO> salePrices = saleDetails.get(itemDTO.getItemCode());

					for (PRItemSaleInfoDTO saleInfoDTO : salePrices) {

						if (saleInfoDTO.getSaleStartDate() == null || saleInfoDTO.getSaleStartDate().trim().isEmpty()
								|| !isDateParsable(saleInfoDTO.getSaleStartDate())) {
							saleInfoDTO.setSaleStartDate(recWeekStartDate);
						}

						// Set cur week sale details
						if (saleInfoDTO.getSaleWeekStartDate().equals(curStartDate)) {
							// pick first occurrence
							if (itemDTO.getCurSaleInfo().getSalePrice() == null) {
								itemDTO.setCurSaleInfo(saleInfoDTO);
							}
						}

						// Set rec week sale details
						if (saleInfoDTO.getSaleWeekStartDate().equals(recWeekStartDate)) {
							// pick first occurrence
							if (itemDTO.getRecWeekSaleInfo().getSalePrice() == null) {

								itemDTO.setRecWeekSaleInfo(saleInfoDTO);
								itemDTO.setOnGoingPromotion(true);
								setTPRAndSaleFlag(saleInfoDTO.getSaleStartDate(), saleInfoDTO.getSaleEndDate(), itemDTO,
										formatter, tprMinWeek);
								boolean isConsiderTPRAsRegEnabled = Boolean
										.parseBoolean(PropertyManager.getProperty("IS_TPR_AS_REG_ENABLED", "FALSE"));
								if (isConsiderTPRAsRegEnabled) {
									// Changes for considering TPR as regular price
									// Changes done by Pradeep on 10/03/2018
									if (itemDTO.getIsTPR() == 1) {
										if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null) {
											MultiplePrice salePrice = itemDTO.getRecWeekSaleInfo().getSalePrice();
											double price = salePrice.multiple > 1
													? (salePrice.price / salePrice.multiple)
													: salePrice.price;
											itemDTO.setRegPrice(price);
											itemDTO.setRegMPack((salePrice.multiple == 0 ? 1 : salePrice.multiple));
											itemDTO.setRegMPrice(salePrice.price);
										}
									}
								}
							}
						}
					}

					// Get latest future week sale details
					PRItemSaleInfoDTO futureSaleInfo = getfutureMostPromo(futureWeekDates, salePrices, itemDTO,
							formatter);
					if (futureSaleInfo != null && futureSaleInfo.getSalePrice() != null) {
						itemDTO.setFutWeekSaleInfo(futureSaleInfo);
						itemDTO.setFuturePromotion(true);
						setTPRAndSaleFlag(futureSaleInfo.getSaleStartDate(), futureSaleInfo.getSaleEndDate(), itemDTO,
								formatter, tprMinWeek);

					}

					// Fill Ad and Display details
					fillAdAndDisplayDetails(adDetails, displayDetails, itemDTO, formatter, recWeekStartDate);

					// Set Promo end within given X weeks flag
					PriceRollbackService priceRollbackService = new PriceRollbackService();
					if (!priceRollbackService.isItemPromoEndsWithinXWeeks(itemDTO, itemList, recWeekStartDate)) {
						itemDTO.setPromoEndsWithinXWeeks(false);
					}
				}
			}
		}

	}

	private void fillAdAndDisplayDetails(HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails, PRItemDTO itemDTO, DateTimeFormatter formatter,
			String recWeekStartDate) {
		// Get list of weeks between rec week sale promotion
//			List<String> adAndDispWeekStartDateList = getAdAndDispWeekStartDateList(itemDTO.getRecWeekSaleInfo().getSaleStartDate(), 
//					itemDTO.getRecWeekSaleInfo().getSaleEndDate(), formatter);

		// Ad Details for an item
		if (adDetails.get(itemDTO.getItemCode()) != null) {
			setAdDetail(adDetails.get(itemDTO.getItemCode()), itemDTO, recWeekStartDate);
		}

		// Promo display details
		fillDisplayDetails(displayDetails, itemDTO, recWeekStartDate);
	}

	private List<String> getAdAndDispWeekStartDateList(String startDate, String endDate, DateTimeFormatter formatter) {
		List<String> futureWeeks = new ArrayList<String>();
		long promoDuration = ChronoUnit.DAYS.between(LocalDate.parse(startDate, formatter),
				LocalDate.parse(endDate, formatter));
		int noOfWeeksToProcess = (int) Math.ceil((double) (promoDuration + 1) / 7);

		for (int i = 0; i < noOfWeeksToProcess; i++) {
			String futureDate = formatter
					.format(LocalDate.parse(startDate, formatter).plus(7 * (1 + i), ChronoUnit.DAYS));
			futureWeeks.add(futureDate);
		}
		return futureWeeks;
	}

	public void setTPRAndSaleFlag(String startDate1, String startDate2, PRItemDTO itemDTO, DateTimeFormatter formatter,
			int tprMinWeek) {

		// If END date is null, then consider it as Long term Promotion(TPR)
		if (startDate2 != null && !startDate2.trim().isEmpty() && isDateParsable(startDate2)) {
			long promoDuration = ChronoUnit.DAYS.between(LocalDate.parse(startDate1, formatter),
					LocalDate.parse(startDate2, formatter));
			if (promoDuration > 0 && (promoDuration + 1) >= 7 * tprMinWeek) {
				itemDTO.setIsTPR(1);
				itemDTO.setIsOnAd(0);
			} else {
				itemDTO.setIsTPR(0);
				itemDTO.setIsOnAd(1);
			}
		} else {
			itemDTO.setIsTPR(1);
			itemDTO.setIsOnAd(0);
		}

	}

	private void fillDisplayDetails(HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails, PRItemDTO itemDTO,
			String recWeekStartDate) {
		// Get all future display details of the item
		if (displayDetails.get(itemDTO.getItemCode()) != null) {

			String weekStartDate = null;
//			if (itemDTO.isFuturePromotion()) {
//				weekStartDate = itemDTO.getFutWeekSaleInfo().getSaleWeekStartDate();
//			} else if (itemDTO.isOnGoingPromotion()) {
//				weekStartDate = recWeekStartDate;
//			}
			if (!itemDTO.isFuturePromotion() && itemDTO.isOnGoingPromotion()) {
				weekStartDate = recWeekStartDate;
			}
			// Get latest future week display details
			if (weekStartDate != null) {
				for (PRItemDisplayInfoDTO displayInfoDTO : displayDetails.get(itemDTO.getItemCode())) {
					if (displayInfoDTO.getDisplayWeekStartDate().equals(weekStartDate)) {
						// pick first occurrence
						if (itemDTO.getFutWeekDisplayInfo().getDisplayTypeLookup() == null) {
							itemDTO.setRecWeekDisplayInfo(displayInfoDTO);
						}
					}
				}
			}
		}
	}

//	private void setRecWeekAdDetail(List<String> futureWeekDates, HashMap<Integer, List<PRItemAdInfoDTO>> adDetails, 
//			String recWeekStartDate, PRItemDTO itemDTO) {
//
//		if (adDetails.get(itemDTO.getItemCode()) != null) {
//
//			if (adDetails.get(itemDTO.getItemCode()) != null) {
//				List<PRItemAdInfoDTO> adInfoList = adDetails.get(itemDTO.getItemCode());
//
//				for (PRItemAdInfoDTO adInfoDTO : adInfoList) {
//
//					// Set rec week ad details
//					if (adInfoDTO.getWeeklyAdStartDate().equals(recWeekStartDate)) {
//						// pick first occurrence
//						if (itemDTO.getRecWeekAdInfo().getAdPageNo() == 0) {
//							itemDTO.setRecWeekAdInfo(adInfoDTO);
//						}
//					}
//				}
//			}
//		}
//	}

	private PRItemSaleInfoDTO getfutureMostPromo(List<String> futureWeekDates, List<PRItemSaleInfoDTO> salePrices,
			PRItemDTO itemDTO, DateTimeFormatter formatter) {
		PRItemSaleInfoDTO saleInfo = null;
		// Loop weeks
		for (String weekStartDate : futureWeekDates) {
			for (PRItemSaleInfoDTO saleInfoDTO : salePrices) {
				if (saleInfoDTO.getSaleWeekStartDate().equals(weekStartDate)) {

					if (itemDTO.getRecWeekSaleInfo() != null && itemDTO.getRecWeekSaleInfo().getSaleEndDate() != null
							&& isDateParsable(itemDTO.getRecWeekSaleInfo().getSaleEndDate())
							&& ChronoUnit.DAYS.between(
									LocalDate.parse(itemDTO.getRecWeekSaleInfo().getSaleEndDate(), formatter),
									LocalDate.parse(saleInfoDTO.getSaleStartDate(), formatter)) > 0) {
						saleInfo = saleInfoDTO;
					} else if (itemDTO.getRecWeekSaleInfo() == null || (itemDTO.getRecWeekSaleInfo() != null
							&& itemDTO.getRecWeekSaleInfo().getSaleWeekStartDate() == null)) {
						saleInfo = saleInfoDTO;
					}
					// pick first occurrence
					if (saleInfo != null) {
						break;
					}
				}
			}

			if (saleInfo != null) {
				break;
			}
		}
		return saleInfo;
	}

	private void setAdDetail(List<PRItemAdInfoDTO> adPromos, PRItemDTO itemDTO, String recWeekStartDate) {

		String weekStartDate = null;
//		if(itemDTO.isFuturePromotion()){
//			weekStartDate = itemDTO.getFutWeekSaleInfo().getSaleWeekStartDate();
//		} else if(itemDTO.isOnGoingPromotion()){
//			weekStartDate = recWeekStartDate;
//		}

		if (!itemDTO.isFuturePromotion() && itemDTO.isOnGoingPromotion()) {
			weekStartDate = recWeekStartDate;
		}

		// Loop weeks
		if (weekStartDate != null && !weekStartDate.trim().isEmpty()) {
			for (PRItemAdInfoDTO adInfoDTO : adPromos) {
				if (adInfoDTO.getWeeklyAdStartDate().equals(weekStartDate)) {
					// pick first occurrence
					if (itemDTO.getRecWeekAdInfo().getAdPageNo() == 0) {
						itemDTO.setRecWeekAdInfo(adInfoDTO);
						break;
					}
				}
			}
		}
	}

	public static List<String> getFutureWeeks(String recWeekStartDate, int noOfFutureWeeks) throws GeneralException {
		List<String> futureWeeks = new ArrayList<String>();
		Date recWeekDate = DateUtil.toDate(recWeekStartDate);
		for (int i = 0; i < noOfFutureWeeks; i++) {
			String futureDate = DateUtil.dateToString(DateUtil.incrementDate(recWeekDate, (7 * (i + 1))),
					Constants.APP_DATE_FORMAT);
			futureWeeks.add(futureDate);
		}
		return futureWeeks;
	}

	public List<PRItemDTO> getLigMembers(int retLirId, List<PRItemDTO> allItems) {
		List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
		for (PRItemDTO itemDTO : allItems) {
			if (itemDTO.getRetLirId() == retLirId && !itemDTO.isLir()) {
				ligMembers.add(itemDTO);
			}
		}
		return ligMembers;
	}

	public PRItemDTO getLigRepresentingItem(int retLirId, List<PRItemDTO> allItems) {
		PRItemDTO ligRepresentingItem = null;
		for (PRItemDTO itemDTO : allItems) {
			if (itemDTO.getItemCode() == retLirId && itemDTO.isLir()) {
				ligRepresentingItem = itemDTO;
				break;
			}
		}
		return ligRepresentingItem;
	}

	public void setEffectiveDate(List<PRItemDTO> itemListWithRecPrice,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			PRRecommendationRunHeader recommendationRunHeader,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap)
			throws ParseException, GeneralException {

		// Clear Future effective date and Future Recommended Price
		itemListWithRecPrice.stream().filter(item -> !item.isLir()).forEach(item -> {
			item.setFutureRetailRecommended(false);
		});

		for (PRItemDTO itemDTO : itemListWithRecPrice) {
			if ((itemDTO.isLir() || (!itemDTO.isLir() && itemDTO.getRetLirId() <= 0))) {

				// Short term promo
				Date promoEndDate = null;
				Date promoStartDate = null;

				/*
				 * If an item is in on-going promotion and if it is going to end within 6 weeks
				 * from recommendation week and if there is a recommendation due to any reason,
				 * then set the effective date as next day after the promotion end date
				 */
				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
						itemDTO.getRegMPrice());
				// Added condition on 09/27/2022 to check if item does not having pending retail
				// as items with pending retail should not be post dated
				if (itemDTO.getRecommendedRegPrice() != null && curRegPrice != null && itemDTO.getPendingRetail()==null)  {
					if (!itemDTO.getRecommendedRegPrice().equals(curRegPrice) && !itemDTO.isFutureRetailPresent()) {
						if ((itemDTO.isOnGoingPromotion() && !itemDTO.isFuturePromotion()
								&& itemDTO.isPromoEndsWithinXWeeks()
								&& isRuleEnabled(recommendationRuleMap, "R9", itemDTO.getObjectiveTypeId()))
								|| (itemDTO.isOnGoingPromotion() && itemDTO.isFuturePromotion()
										&& itemDTO.isPromoEndsWithinXWeeks()
										&& !isRuleEnabled(recommendationRuleMap, "R8", itemDTO.getObjectiveTypeId())
										&& isRuleEnabled(recommendationRuleMap, "R9", itemDTO.getObjectiveTypeId()))) {

							// To consider Future promotions End date than On Going promotion if available
							if (itemDTO.isOnGoingPromotion() && itemDTO.isFuturePromotion()) {
								promoEndDate = getPromoEffectiveEndDate(itemDTO, itemListWithRecPrice,
										recommendationRunHeader, true);
								promoStartDate = getPromoEffectiveStartDate(itemDTO, itemListWithRecPrice,
										recommendationRunHeader, true);

							} else {
								promoEndDate = getPromoEffectiveEndDate(itemDTO, itemListWithRecPrice,
										recommendationRunHeader, false);
								promoStartDate = getPromoEffectiveStartDate(itemDTO, itemListWithRecPrice,
										recommendationRunHeader, false);

							}
						}

						/*
						 * If an item is in future promotion and if it is going to end within 6 weeks
						 * from recommendation week and if there is a recommendation due to any reason,
						 * then set the effective date as next day after the promotion end date
						 */
						else if (itemDTO.isFuturePromotion() && itemDTO.isPromoEndsWithinXWeeks()
								&& isRuleEnabled(recommendationRuleMap, "R8", itemDTO.getObjectiveTypeId())) {
							promoEndDate = getPromoEffectiveEndDate(itemDTO, itemListWithRecPrice,
									recommendationRunHeader, true);
							promoStartDate = getPromoEffectiveStartDate(itemDTO, itemListWithRecPrice,
									recommendationRunHeader, true);

						}

						// Set reg effective date
						if (promoEndDate != null) {
							String effectiveDate = "";

							if (itemDTO.getFutureListCost() != null && itemDTO.getCostChgIndicator() != 0) {

								int priceEffDtOnPriceIncrease = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
								int priceEffDtOnPriceDecrease = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();

								PRStrategyDTO strategyDTO = itemDTO.getStrategyDTO();

								if (strategyDTO.getGuidelines() != null
										&& strategyDTO.getGuidelines().getSizeGuideline() != null) {

									List<PRGuidelineMargin> marginGuidelines = strategyDTO.getGuidelines()
											.getMarginGuideline();
									if (marginGuidelines != null && marginGuidelines.size() > 0) {
										// get the value defined from margin guideline else use default values
										for (PRGuidelineMargin guidelineMargin : marginGuidelines) {

											priceEffDtOnPriceIncrease = guidelineMargin.getPriceIncrease();
											priceEffDtOnPriceDecrease = guidelineMargin.getPriceDecrease();
											break;
										}

									}
								}
								double curRegRetail = itemDTO.getRegPrice();
								double priceIncOrDec = 0;
								Date futureCostEffDate = DateUtil.toDate(itemDTO.getFutureCostEffDate());

								if (itemDTO.getRecommendedRegPrice() != null) {
									priceIncOrDec = itemDTO.getRecommendedRegPrice().getUnitPrice() - curRegRetail;
								}

								if (priceIncOrDec > 0 && priceEffDtOnPriceIncrease > 0) {
									effectiveDate = setEffectiveDateForPromoItems(priceEffDtOnPriceIncrease, futureCostEffDate,
											 promoEndDate,promoStartDate,recommendationRunHeader.getStartDate()	,itemDTO);

								} else if (priceIncOrDec < 0 && priceEffDtOnPriceDecrease > 0) {
									effectiveDate = setEffectiveDateForPromoItems(priceEffDtOnPriceDecrease, futureCostEffDate,
											promoEndDate,promoStartDate,recommendationRunHeader.getStartDate()	,itemDTO);
								}

							} else {
								effectiveDate = DateUtil.dateToString(DateUtil.incrementDate(promoEndDate, 1),
										Constants.APP_DATE_FORMAT);
							}

							List<PRItemDTO> ligMembersOrNonLig = getLigMembersOrNonLigItem(itemDTO,
									itemListWithRecPrice);

							for (PRItemDTO ligMemberOrNonLig : ligMembersOrNonLig) {
								// only if there is new retail
								ligMemberOrNonLig.setRecPriceEffectiveDate(effectiveDate);
								ligMemberOrNonLig.setFutureRetailRecommended(true);
								ligMemberOrNonLig.setFutureRecRetail(itemDTO.getRecommendedRegPrice());
							}
						} else if (itemDTO.getFutureListCost() != null && itemDTO.getCostChgIndicator() != 0) {

							int priceEffDtOnPriceIncrease = PriceReactionTimeLookup.IMMEDIATE.getTypeId();
							int priceEffDtOnPriceDecrease = PriceReactionTimeLookup.WEEK_BEFORE.getTypeId();

							PRStrategyDTO strategyDTO = itemDTO.getStrategyDTO();

							if (strategyDTO.getGuidelines() != null
									&& strategyDTO.getGuidelines().getSizeGuideline() != null) {

								List<PRGuidelineMargin> marginGuidelines = strategyDTO.getGuidelines()
										.getMarginGuideline();
								if (marginGuidelines != null && marginGuidelines.size() > 0) {
									// get the value defined from margin guideline else use default values
									for (PRGuidelineMargin guidelineMargin : marginGuidelines) {

										priceEffDtOnPriceIncrease = guidelineMargin.getPriceIncrease();
										priceEffDtOnPriceDecrease = guidelineMargin.getPriceDecrease();
										break;
									}

								}
							}
							if (itemDTO.getRecommendedRegPrice() != null && curRegPrice != null) {
								Date futureCostEffDate = DateUtil.toDate(itemDTO.getFutureCostEffDate());
								double priceIncOrDec = 0;
								String effectiveDate = "";

								if (itemDTO.getRecommendedRegPrice().getUnitPrice() != curRegPrice.getUnitPrice()) {
									priceIncOrDec = itemDTO.getRecommendedRegPrice().getUnitPrice()
											- curRegPrice.getUnitPrice();
								}

								if (priceIncOrDec > 0 && priceEffDtOnPriceIncrease > 0) {

									effectiveDate = setEffectiveDateNonpromo(priceEffDtOnPriceIncrease,
											futureCostEffDate);

								} else if (priceIncOrDec < 0 && priceEffDtOnPriceDecrease > 0) {

									effectiveDate = setEffectiveDateNonpromo(priceEffDtOnPriceDecrease,
											futureCostEffDate);
								}

								if (effectiveDate != "") {
									List<PRItemDTO> ligMembersOrNonLig = getLigMembersOrNonLigItem(itemDTO,
											itemListWithRecPrice);

									for (PRItemDTO ligMemberOrNonLig : ligMembersOrNonLig) {
										// only if there is new retail
										ligMemberOrNonLig.setRecPriceEffectiveDate(effectiveDate);
										ligMemberOrNonLig.setFutureRetailRecommended(true);
										ligMemberOrNonLig.setFutureRecRetail(itemDTO.getRecommendedRegPrice());
									}
								}
							}

						}
					}
				}
			}
		}

		// update lig data
		for (PRItemDTO itemDTO : itemListWithRecPrice) {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = getLigMembers(itemDTO.getRetLirId(), itemListWithRecPrice);
				PRItemDTO ligRepresentingItem = getLigRepresentingItem(itemDTO.getRetLirId(), itemListWithRecPrice);
				if (ligRepresentingItem != null) {
					MostOccurrenceData mod = new MostOccurrenceData();
					Object object = mod.getMaxOccurance(ligMembers, "RecPriceEffectiveDate");
					String recPriceEffectiveDate = (object != null ? (String) object : null);
					itemDTO.setRecPriceEffectiveDate(recPriceEffectiveDate);

					mod = new MostOccurrenceData();
					object = mod.getMaxOccurance(ligMembers, "FutureRecRetail");
					MultiplePrice multiplePrice = (object != null ? (MultiplePrice) object : null);
					itemDTO.setFutureRecRetail(multiplePrice);
				}
			}
		}
	}

	/**
	 * 
	 * @param priceEffDtOnPriceIncrease
	 * @param futureCostEffDate
	 * @param promoStartDate 
	 * @param promoEndDate 
	 * @param startDate
	 * @param itemDTO
	 * @return
	 * @throws GeneralException 
	 */
	public String setEffectiveDateForPromoItems(int priceFlag, Date futureCostEffDate,
			Date promoEndDate, Date promoStartDate, String recWeekStartDate, PRItemDTO itemDTO) throws GeneralException {
		String effectiveDate = "";
		
		long promoDuration = DateUtil.getDateDiff(promoEndDate, promoStartDate);
		if (priceFlag == PriceReactionTimeLookup.IMMEDIATE.getTypeId()) {
			if (useCostEffDateToPostDate) {
				if (futureCostEffDate.after(promoStartDate) && futureCostEffDate.before(promoEndDate)
						|| futureCostEffDate.equals(promoEndDate)) {

					effectiveDate = DateUtil.dateToString(DateUtil.incrementDate(promoEndDate, 1),
							Constants.APP_DATE_FORMAT);
				} else if (futureCostEffDate.before(promoStartDate) || futureCostEffDate.after(promoEndDate)) {
					effectiveDate = DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT);
				} else if (futureCostEffDate.equals(promoEndDate)) {
					effectiveDate = DateUtil.dateToString(DateUtil.incrementDate(promoEndDate, 1),
							Constants.APP_DATE_FORMAT);
				} else if (futureCostEffDate.equals(promoStartDate)) {
					effectiveDate = DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT);
				}
			} else {
				// Changes for RA , if future cost is effective on or after promotion starts
				// then postdate reccs to week after promotion ends.If there is no promotion in
				// immediate week then do not postdate
				Date recWkStartDate = DateUtil.toDate(recWeekStartDate);
				Date recWkEndDate = DateUtil.toDate(DateUtil.localDateToString(LocalDate
						.parse(recWeekStartDate, DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT)).plusDays(6),
						Constants.APP_DATE_FORMAT));

				Date promotionStartDate;
				Date promotionEndDate;
			
				boolean isDateSet = false;
				// if future cost is effective after promotion starts and the promotion has
				// started before the recc week and ends
				// on or after the recc week then postdate it to a day after recc week
				if (itemDTO.getCurSaleInfo() != null && itemDTO.getCurSaleInfo().getSaleStartDate() != null
						&& itemDTO.getCurSaleInfo().getSaleEndDate() != null
						&& !itemDTO.getCurSaleInfo().getSaleEndDate().trim().isEmpty()) {
					promotionStartDate = DateUtil.toDate(itemDTO.getCurSaleInfo().getSaleStartDate());
					promotionEndDate = DateUtil.toDate(itemDTO.getCurSaleInfo().getSaleEndDate());
					if ((futureCostEffDate.after(promotionStartDate) || futureCostEffDate.equals(promotionStartDate))
							&& promotionStartDate.before(recWkStartDate)
							&& ((promotionEndDate.after(recWkEndDate) || promotionEndDate.equals(recWkEndDate))
									|| (promotionEndDate.before(recWkEndDate)
											&& promotionEndDate.after(recWkStartDate)))) {
						isDateSet = true;
						effectiveDate = DateUtil.dateToString(DateUtil.incrementDate(recWkEndDate, 1),
								Constants.APP_DATE_FORMAT);
					}
				}
				// if the promotion is ongoing and future cost is effective after promotion starts 
				if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null
						&& itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null
						&& itemDTO.getRecWeekSaleInfo().getSaleEndDate() != null
						&& !itemDTO.getRecWeekSaleInfo().getSaleEndDate().trim().isEmpty() && !isDateSet) {

					promotionStartDate = DateUtil.toDate(itemDTO.getRecWeekSaleInfo().getSaleStartDate());
					promotionEndDate = DateUtil.toDate(itemDTO.getRecWeekSaleInfo().getSaleEndDate());
					if ((futureCostEffDate.after(promotionStartDate) || futureCostEffDate.equals(promotionStartDate))
							&& promotionStartDate.equals(recWkStartDate)
							|| promotionStartDate.after(recWkStartDate) && promotionStartDate.before(recWkEndDate)) {

						Date oneWeekAfterpromo = DateUtil.toDate(DateUtil.localDateToString(
								LocalDate.parse(DateUtil.dateToString(promotionStartDate, Constants.APP_DATE_FORMAT),
										DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT)).plusDays(6),
								Constants.APP_DATE_FORMAT));
						effectiveDate = DateUtil.dateToString(DateUtil.incrementDate(oneWeekAfterpromo, 1),
								Constants.APP_DATE_FORMAT);
					}

				}

			}
		} else if (priceFlag == PriceReactionTimeLookup.WEEK_BEFORE.getTypeId()) {

			if ((futureCostEffDate.after(promoStartDate) && futureCostEffDate.before(promoEndDate))
					|| futureCostEffDate.equals(promoEndDate)) {

				if (promoDuration == 6) {
					String WeekStartDate = DateUtil.getWeekStartDate(promoStartDate, 0);
					effectiveDate = DateUtil.localDateToString(
							(DateUtil.stringToLocalDate(WeekStartDate, Constants.APP_DATE_FORMAT)).minusDays(4),
							Constants.APP_DATE_FORMAT);

				} else {

					int weekday = DateUtil.getdayofWeek(
							DateUtil.dateToString(DateUtil.incrementDate(promoEndDate, 1), Constants.APP_DATE_FORMAT),
							Constants.APP_DATE_FORMAT);
					String WeekStartDate = null;
					if (weekday == 1) {
						WeekStartDate = DateUtil.getWeekStartDate((DateUtil.incrementDate(promoEndDate, 1)), 0);
						effectiveDate = DateUtil.localDateToString(
								(DateUtil.stringToLocalDate(WeekStartDate, Constants.APP_DATE_FORMAT)).plusDays(3),
								Constants.APP_DATE_FORMAT);

					} else {
						WeekStartDate = DateUtil.getWeekEndDate(DateUtil.incrementDate(promoEndDate, 1),
								Constants.APP_DATE_FORMAT);
						effectiveDate = DateUtil.localDateToString(
								(DateUtil.stringToLocalDate(WeekStartDate, Constants.APP_DATE_FORMAT)).plusDays(4),
								Constants.APP_DATE_FORMAT);
					}
				}

			} else if (futureCostEffDate.before(promoStartDate) || futureCostEffDate.after(promoEndDate)
					|| futureCostEffDate.equals(promoStartDate)) {

				String WeekStartDate = DateUtil.getWeekStartDate(futureCostEffDate, 0);
				effectiveDate = DateUtil.localDateToString(
						(DateUtil.stringToLocalDate(WeekStartDate, Constants.APP_DATE_FORMAT)).minusDays(4),
						Constants.APP_DATE_FORMAT);

			}
		} else if (priceFlag == PriceReactionTimeLookup.WEEK_AFTER.getTypeId()) {
			if ((futureCostEffDate.after(promoStartDate) && futureCostEffDate.before(promoEndDate))
					|| futureCostEffDate.equals(promoEndDate) || futureCostEffDate.equals(promoStartDate)) {

				int weekdayOfPromoEnd = DateUtil.getdayofWeek(
						DateUtil.dateToString(promoEndDate, Constants.APP_DATE_FORMAT), Constants.APP_DATE_FORMAT);

				if (weekdayOfPromoEnd == 1) {
					effectiveDate = DateUtil.localDateToString(
							(DateUtil.stringToLocalDate(DateUtil.dateToString(promoEndDate, Constants.APP_DATE_FORMAT),
									Constants.APP_DATE_FORMAT)).plusDays(3),
							Constants.APP_DATE_FORMAT);
				} else {

					int weekday = DateUtil.getdayofWeek(
							DateUtil.dateToString(DateUtil.incrementDate(promoEndDate, 1), Constants.APP_DATE_FORMAT),
							Constants.APP_DATE_FORMAT);

					if (weekday != 1) {
						String weekEndDate = DateUtil.getWeekEndDate(DateUtil.incrementDate(promoEndDate, 1),
								Constants.APP_DATE_FORMAT);
						effectiveDate = DateUtil.localDateToString(
								(DateUtil.stringToLocalDate(weekEndDate, Constants.APP_DATE_FORMAT)).plusDays(4),
								Constants.APP_DATE_FORMAT);

					} else {
						effectiveDate = DateUtil.localDateToString(
								(DateUtil.stringToLocalDate(
										DateUtil.dateToString(DateUtil.incrementDate(promoEndDate, 1),
												Constants.APP_DATE_FORMAT),
										Constants.APP_DATE_FORMAT)).plusDays(3),
								Constants.APP_DATE_FORMAT);
					}
				}

			} else if (futureCostEffDate.before(promoStartDate) || futureCostEffDate.after(promoEndDate)) {
				int weekday = DateUtil.getdayofWeek(DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT),
						Constants.APP_DATE_FORMAT);
				if (weekday != 1) {
					String weekEndDate = DateUtil.getWeekEndDate(futureCostEffDate, Constants.APP_DATE_FORMAT);
					effectiveDate = DateUtil.localDateToString(
							(DateUtil.stringToLocalDate(weekEndDate, Constants.APP_DATE_FORMAT)).plusDays(4),
							Constants.APP_DATE_FORMAT);

				} else {
					effectiveDate = DateUtil.localDateToString((DateUtil.stringToLocalDate(
							DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT),
							Constants.APP_DATE_FORMAT)).plusDays(3), Constants.APP_DATE_FORMAT);
				}
			}
		}

		return effectiveDate;
	}

	/**
	 * 
	 * @param priceFlag
	 * @param futureCostEffDate
	 * @return
	 * @throws GeneralException
	 */
	public String setEffectiveDateNonpromo(int priceFlag, Date futureCostEffDate) throws GeneralException {

		String effectiveDate = "";
		if (priceFlag == PriceReactionTimeLookup.IMMEDIATE.getTypeId() && useCostEffDateToPostDate) {

			effectiveDate = DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT);

		} else if (priceFlag == PriceReactionTimeLookup.WEEK_BEFORE.getTypeId()) {

			String WeekStartDate = DateUtil.getWeekStartDate(futureCostEffDate, 0);
			effectiveDate = DateUtil.localDateToString(
					(DateUtil.stringToLocalDate(WeekStartDate, Constants.APP_DATE_FORMAT)).minusDays(4),
					Constants.APP_DATE_FORMAT);

		} else if (priceFlag == PriceReactionTimeLookup.WEEK_AFTER.getTypeId()) {

			int weekday = DateUtil.getdayofWeek(DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT),
					Constants.APP_DATE_FORMAT);
			if (weekday != 1) {
				String weekEndDate = DateUtil.getWeekEndDate(futureCostEffDate, Constants.APP_DATE_FORMAT);
				effectiveDate = DateUtil.localDateToString(
						(DateUtil.stringToLocalDate(weekEndDate, Constants.APP_DATE_FORMAT)).plusDays(4),
						Constants.APP_DATE_FORMAT);

			} else {
				effectiveDate = DateUtil.localDateToString(
						(DateUtil.stringToLocalDate(DateUtil.dateToString(futureCostEffDate, Constants.APP_DATE_FORMAT),
								Constants.APP_DATE_FORMAT)).plusDays(3),
						Constants.APP_DATE_FORMAT);
			}

		}
		return effectiveDate;
	}

	private Date getShortPromoEffectiveEndDate(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice,
			PRRecommendationRunHeader recommendationRunHeader) throws ParseException, GeneralException {
		int shortTermPromoWeeks = Integer.parseInt(PropertyManager.getProperty("SHORT_TERM_PROMO_WEEKS"));
		List<PRItemDTO> ligMembersOrNonLig = getLigMembersOrNonLigItem(ligRepOrNonLigItem, itemListWithRecPrice);

		Date promoEndDate = null;
		// Check if short term promo
		boolean isShortTermPromo = false;
		for (PRItemDTO ligMemberOrNonLig : ligMembersOrNonLig) {

			if (ligMemberOrNonLig.getRecWeekAdInfo().getAdPageNo() > 0
					&& ligMemberOrNonLig.getRecWeekSaleInfo().getSalePrice() == null) {
				isShortTermPromo = true;
			} else {
				// If the item is on sale, new price recommended, if it is
				// short term promo
				if (ligMemberOrNonLig.isItemPromotedForRecWeek()
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSalePrice() != null
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSaleStartDate() != null
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate() != null
						&& ligMemberOrNonLig.getIsNewPriceRecommended() == 1) {

					long promoDuration = DateUtil.getDateDiff(ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate(),
							recommendationRunHeader.getStartDate());

					if ((promoDuration + 1) <= 7 * shortTermPromoWeeks) {
						isShortTermPromo = true;
					}
				}
			}

			if (isShortTermPromo) {
				// If there is no sale end date and there is page no,
				// which means its on ad
				if (ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate() == null
						&& ligMemberOrNonLig.getRecWeekAdInfo().getAdPageNo() > 0) {
					promoEndDate = DateUtil.incrementDate(DateUtil.toDate(recommendationRunHeader.getStartDate()), 6);
				} else {
					promoEndDate = DateUtil.toDate(ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate());
				}
				break;
			}
		}

		return promoEndDate;
	}

	private Date getPromoEffectiveEndDate(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice,
			PRRecommendationRunHeader recommendationRunHeader, boolean useFuturePromotion)
			throws ParseException, GeneralException {
		int shortTermPromoWeeks = Integer.parseInt(PropertyManager.getProperty("PROMO_X_WEEKS_FROM_REC_WEEK"));
		List<PRItemDTO> ligMembersOrNonLig = getLigMembersOrNonLigItem(ligRepOrNonLigItem, itemListWithRecPrice);

		Date promoEndDate = null;
		// Check if short term promo
		boolean isShortTermPromo = false;
		for (PRItemDTO ligMemberOrNonLig : ligMembersOrNonLig) {

			if (!useFuturePromotion) {
				if (ligMemberOrNonLig.getRecWeekSaleInfo().getSalePrice() != null
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSaleStartDate() != null
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate() != null
						&& ligMemberOrNonLig.getIsNewPriceRecommended() == 1) {

					long promoDuration = DateUtil.getDateDiff(ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate(),
							recommendationRunHeader.getStartDate());

					if ((promoDuration + 1) <= 7 * shortTermPromoWeeks) {
						isShortTermPromo = true;
					}
				}

				if (isShortTermPromo) {
					promoEndDate = DateUtil.toDate(ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate());
					break;
				}
			} else if (useFuturePromotion) {
				if (ligMemberOrNonLig.getFutWeekSaleInfo().getSalePrice() != null
						&& ligMemberOrNonLig.getFutWeekSaleInfo().getSaleStartDate() != null
						&& ligMemberOrNonLig.getFutWeekSaleInfo().getSaleEndDate() != null
						&& ligMemberOrNonLig.getIsNewPriceRecommended() == 1) {

					long promoDuration = DateUtil.getDateDiff(ligMemberOrNonLig.getFutWeekSaleInfo().getSaleEndDate(),
							recommendationRunHeader.getStartDate());

					if ((promoDuration + 1) <= 7 * shortTermPromoWeeks) {
						isShortTermPromo = true;
					}
				}

				if (isShortTermPromo) {
					promoEndDate = DateUtil.toDate(ligMemberOrNonLig.getFutWeekSaleInfo().getSaleEndDate());
					break;
				}
			}
		}

		return promoEndDate;
	}

	private Date getPromoEffectiveStartDate(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice,
			PRRecommendationRunHeader recommendationRunHeader, boolean useFuturePromotion)
			throws ParseException, GeneralException {
		int shortTermPromoWeeks = Integer.parseInt(PropertyManager.getProperty("PROMO_X_WEEKS_FROM_REC_WEEK"));
		List<PRItemDTO> ligMembersOrNonLig = getLigMembersOrNonLigItem(ligRepOrNonLigItem, itemListWithRecPrice);

		Date promoStartDate = null;
		// Check if short term promo
		boolean isShortTermPromo = false;
		for (PRItemDTO ligMemberOrNonLig : ligMembersOrNonLig) {

			if (!useFuturePromotion) {
				if (ligMemberOrNonLig.getRecWeekSaleInfo().getSalePrice() != null
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSaleStartDate() != null
						&& ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate() != null
						&& ligMemberOrNonLig.getIsNewPriceRecommended() == 1) {

					long promoDuration = DateUtil.getDateDiff(ligMemberOrNonLig.getRecWeekSaleInfo().getSaleEndDate(),
							recommendationRunHeader.getStartDate());

					if ((promoDuration + 1) <= 7 * shortTermPromoWeeks) {
						isShortTermPromo = true;
					}
				}

				if (isShortTermPromo) {
					promoStartDate = DateUtil.toDate(ligMemberOrNonLig.getRecWeekSaleInfo().getSaleStartDate());
					break;
				}
			} else if (useFuturePromotion) {
				if (ligMemberOrNonLig.getFutWeekSaleInfo().getSalePrice() != null
						&& ligMemberOrNonLig.getFutWeekSaleInfo().getSaleStartDate() != null
						&& ligMemberOrNonLig.getFutWeekSaleInfo().getSaleEndDate() != null
						&& ligMemberOrNonLig.getIsNewPriceRecommended() == 1) {

					long promoDuration = DateUtil.getDateDiff(ligMemberOrNonLig.getFutWeekSaleInfo().getSaleEndDate(),
							recommendationRunHeader.getStartDate());

					if ((promoDuration + 1) <= 7 * shortTermPromoWeeks) {
						isShortTermPromo = true;
					}
				}

				if (isShortTermPromo) {
					promoStartDate = DateUtil.toDate(ligMemberOrNonLig.getFutWeekSaleInfo().getSaleStartDate());
					break;
				}
			}
		}

		return promoStartDate;
	}


	public HashMap<Integer, HashMap<String, RetailPriceDTO>> getItemZonePriceHistory(int chainId, int zoneId,
			HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory) {

		RetailPriceServiceOptimized retailPriceServiceOptimized = new RetailPriceServiceOptimized();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, zoneId);

		// Loop items
		for (Entry<Integer, HashMap<String, List<RetailPriceDTO>>> itemWeeklyPrice : itemPriceHistory.entrySet()) {
			HashMap<String, RetailPriceDTO> itemZoneWeeklyPrice = new HashMap<String, RetailPriceDTO>();
			int itemCode = itemWeeklyPrice.getKey();

			for (Entry<String, List<RetailPriceDTO>> tempWeeklyPrice : itemWeeklyPrice.getValue().entrySet()) {
				RetailPriceDTO zonePriceDTO = null;

				HashMap<RetailPriceCostKey, RetailPriceDTO> priceMap = new HashMap<RetailPriceCostKey, RetailPriceDTO>();
				for (RetailPriceDTO retailPriceDTO : tempWeeklyPrice.getValue()) {
					RetailPriceCostKey rpcky = new RetailPriceCostKey(retailPriceDTO.getLevelTypeId(),
							Integer.parseInt(retailPriceDTO.getLevelId()));
					priceMap.put(rpcky, retailPriceDTO);
				}

				zonePriceDTO = retailPriceServiceOptimized.findPriceForZone(priceMap, zoneKey, chainKey);
				// NU:: 19th Jun 2017, bug fix, later in the code getRegMPack and getSaleMPack
				// is used to get the
				// quantity, so if it is assigned properly here, it will work in other places.
				if (zonePriceDTO != null) {
					zonePriceDTO.setRegMPack(zonePriceDTO.getRegQty());
					zonePriceDTO.setSaleMPack(zonePriceDTO.getSaleQty());
				}
				itemZoneWeeklyPrice.put(tempWeeklyPrice.getKey(), zonePriceDTO);
			}

			itemZonePriceHistory.put(itemCode, itemZoneWeeklyPrice);
		}

		return itemZonePriceHistory;
	}

	public boolean isGuidelineApplied(PRItemDTO itemDTO, int guidelineTypeId) {
		boolean isGuidelineApplied = false;
		if (itemDTO.getExplainLog() != null) {
			for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog()
					.getGuidelineAndConstraintLogs()) {
				if (guidelineAndConstraintLog.getGuidelineTypeId() == guidelineTypeId
						&& guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()) {
					isGuidelineApplied = true;
				}
			}
		}
		return isGuidelineApplied;
	}

	public boolean isAllParentItemProcessed(PRItemDTO itemDTO, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		boolean isAllParentItemProcessed = true;

		/*if (itemDTO.getExplainLog() != null) {
			for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog()
					.getGuidelineAndConstraintLogs()) {
				if ((guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
						|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE
								.getGuidelineTypeId())
						&& guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()) {
					if (guidelineAndConstraintLog.getRelationItemCode() > 0) {
						ItemKey relatedItemKey = null;
						if (guidelineAndConstraintLog.getIsLirInd()) {
							relatedItemKey = new ItemKey(guidelineAndConstraintLog.getRelationItemCode(),
									PRConstants.LIG_ITEM_INDICATOR);
						} else {
							relatedItemKey = new ItemKey(guidelineAndConstraintLog.getRelationItemCode(),
									PRConstants.NON_LIG_ITEM_INDICATOR);
						}

						if (itemDataMap.get(relatedItemKey) != null && !itemDataMap.get(relatedItemKey).isProcessed()) {
							logger.info("itemDataMap.get(relatedItemKey):"+itemDataMap.get(relatedItemKey).getItemCode());
							isAllParentItemProcessed = false;
							break;
						}

					}
				}
			}
		}*/

		//Changed on 07/21/2022 to check if all the related item are processed from Item PGData instead of explain log
		if(itemDTO.getPgData()!=null)
		{
			PRPriceGroupDTO pgDTO = itemDTO.getPgData();
			TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = pgDTO.getRelationList();

			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : relationList
					.entrySet()) {
				for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
					ItemKey relatedItemKey = null;
					if (relatedItem.getIsLig()) {
						relatedItemKey = new ItemKey(relatedItem.getRelatedItemCode(),
								PRConstants.LIG_ITEM_INDICATOR);
					} else {
						relatedItemKey = new ItemKey(relatedItem.getRelatedItemCode(),
								PRConstants.NON_LIG_ITEM_INDICATOR);
					}

					if (itemDataMap.get(relatedItemKey) != null && !itemDataMap.get(relatedItemKey).isProcessed()) {
						logger.info("Related Item that is not processed:"+itemDataMap.get(relatedItemKey).getItemCode());
						isAllParentItemProcessed = false;
						break;
					}
				}
			}
		}		
		return isAllParentItemProcessed;
	}

	public List<PRItemDTO> getLigMembersOrNonLigItem(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> allItems) {
		List<PRItemDTO> ligMembersOrNonLig = new ArrayList<PRItemDTO>();

		if (ligRepOrNonLigItem.isLir()) {
			List<PRItemDTO> ligMembers = getLigMembers(ligRepOrNonLigItem.getRetLirId(), allItems);
			ligMembersOrNonLig.addAll(ligMembers);
		} else {
			ligMembersOrNonLig.add(ligRepOrNonLigItem);
		}

		return ligMembersOrNonLig;
	}

	public Date getFutureAdEffectiveEndDate(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice,
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails, PRRecommendationRunHeader recommendationRunHeader)
			throws GeneralException {
		Date futureAdEndDate = null;
		int retainPriceNoOfFutPromoWeeks = Integer
				.parseInt(PropertyManager.getProperty("RETAIN_PRICE_NO_OF_FUT_AD_WEEKS"));
		PricingEngineService pricingEngineService = new PricingEngineService();

		List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(ligRepOrNonLigItem,
				itemListWithRecPrice);

		List<String> futureWeeks = PricingEngineService.getFutureWeeks(recommendationRunHeader.getStartDate(),
				retainPriceNoOfFutPromoWeeks);

		// Even if any one of the item in the LIG is in future ad
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			// Check if ad details are there
			if (adDetails.get(itemDTO.getItemCode()) != null) {
				List<PRItemAdInfoDTO> adInfoList = adDetails.get(itemDTO.getItemCode());

				for (String weekStartDate : futureWeeks) {
					for (PRItemAdInfoDTO adInfoDTO : adInfoList) {
						if (adInfoDTO.getWeeklyAdStartDate().equals(weekStartDate)) {
							// pick first occurrence
							if (itemDTO.getFutWeekAdInfo().getAdPageNo() > 0) {
								futureAdEndDate = DateUtil.incrementDate(DateUtil.toDate(weekStartDate), 6);
								break;
							}
						}
					}
				}

			}
		}

		return futureAdEndDate;
	}

	public Date getFutureSaleEffectiveEndDate(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, PRRecommendationRunHeader recommendationRunHeader)
			throws GeneralException {
		Date futureSaleEndDate = null;
		int retainPriceNoOfFutPromoWeeks = Integer
				.parseInt(PropertyManager.getProperty("RETAIN_PRICE_NO_OF_FUT_SALE_WEEKS"));
		PricingEngineService pricingEngineService = new PricingEngineService();

		List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(ligRepOrNonLigItem,
				itemListWithRecPrice);

		List<String> futureWeeks = PricingEngineService.getFutureWeeks(recommendationRunHeader.getStartDate(),
				retainPriceNoOfFutPromoWeeks);

		// Even if any one of the item in the LIG is in future sale
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			// Check if sale details are there
			if (saleDetails.get(itemDTO.getItemCode()) != null) {
				List<PRItemSaleInfoDTO> saleInfoList = saleDetails.get(itemDTO.getItemCode());

				for (String weekStartDate : futureWeeks) {
					for (PRItemSaleInfoDTO saleInfoDTO : saleInfoList) {
						if (saleInfoDTO.getSaleWeekStartDate().equals(weekStartDate)) {
							// promoted only in future
							if (!itemDTO.isItemPromotedForRecWeek()) {
								// 2nd Jan 2017, bug fix: future sale end date is null some case
								// in that case assign start as end date
								// futureSaleEndDate = DateUtil.toDate(saleInfoDTO.getSaleEndDate());

								if (saleInfoDTO.getSaleEndDate() != null) {
									futureSaleEndDate = DateUtil.toDate(saleInfoDTO.getSaleEndDate());
								} else {
									futureSaleEndDate = DateUtil
											.incrementDate(DateUtil.toDate(saleInfoDTO.getSaleWeekStartDate()), 6);
								}

								break;
							}

						}
					}
				}

			}
		}

		return futureSaleEndDate;
	}

	public void updateItemAttributes(List<PRItemDTO> itemListWithRecPrice,
			PRRecommendationRunHeader recommendationRunHeader) throws GeneralException, OfferManagementException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		List<PRItemDTO> ligMembersOrNonLig = new ArrayList<PRItemDTO>();

		// lig members and non-lig
		for (PRItemDTO itemDTO : itemListWithRecPrice) {
			// process all items
			if (!itemDTO.isLir()) {
				// update prediction status and movement
				pricingEngineService.updateConflicts(itemDTO);
				ligMembersOrNonLig.add(itemDTO);
			}
		}

		// update prediction and status
		pricingEngineService.updatePredictionOfCurAndRecPriceUsingMap(ligMembersOrNonLig, recommendationRunHeader);
	}

	public void updatePredictionOfCurAndRecPriceUsingMap(List<PRItemDTO> ligMembersOrNonLig,
			PRRecommendationRunHeader recommendationRunHeader) throws GeneralException, OfferManagementException {

		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			if (!itemDTO.isLir()) {
				if (itemDTO.getRegPricePredictionMap() != null) {
					MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(),
							itemDTO.getRegPrice(), itemDTO.getRegMPrice());

					// rec price
					if (itemDTO.getRegPricePredictionMap().get(itemDTO.getRecommendedRegPrice()) != null) {
						PricePointDTO pricePointDTO = itemDTO.getRegPricePredictionMap()
								.get(itemDTO.getRecommendedRegPrice());
						itemDTO.setPredictedMovement(pricePointDTO.getPredictedMovement());
						itemDTO.setPredictionStatus(pricePointDTO.getPredictionStatus().getStatusCode());
//						logger.debug(itemDTO.getItemCode() + ",pricePointDTO.getQuestionablePrediction():"
//								+ ",pricePoint:" + pricePointDTO.getRegQuantity() 
//								+ ","
//								+ pricePointDTO.getRegPrice()
//								+ pricePointDTO.getQuestionablePrediction());
						itemDTO.setRegPricePredReasons(pricePointDTO.getQuestionablePrediction());
					} else {
						itemDTO.setPredictedMovement(null);
						itemDTO.setPredictionStatus(null);
						itemDTO.setRegPricePredReasons(null);
					}

					// cur price
					if (itemDTO.getRegPricePredictionMap().get(curRegPrice) != null) {
						PricePointDTO pricePointDTO = itemDTO.getRegPricePredictionMap().get(curRegPrice);
						itemDTO.setCurRegPricePredictedMovement(pricePointDTO.getPredictedMovement());
						itemDTO.setCurRegPricePredictionStatus(pricePointDTO.getPredictionStatus().getStatusCode());
					} else {
						itemDTO.setCurRegPricePredictedMovement(null);
						itemDTO.setCurRegPricePredictionStatus(null);
					}
				} else {
					itemDTO.setCurRegPricePredictedMovement(null);
					itemDTO.setCurRegPricePredictionStatus(null);
					itemDTO.setPredictedMovement(null);
					itemDTO.setPredictionStatus(null);
					itemDTO.setRegPricePredReasons(null);
				}
			}
		}
	}

	public void updatePredictionOfRecPriceUsingDB(Connection conn, List<PRItemDTO> ligMembersOrNonLig,
			PRRecommendationRunHeader recommendationRunHeader) throws GeneralException, OfferManagementException {

		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<ProductDTO> productList = new ArrayList<ProductDTO>();
		PredictionDetailKey predictionDetailKey;

		ProductDTO productDTO = new ProductDTO();
		productDTO.setProductLevelId(recommendationRunHeader.getProductLevelId());
		productDTO.setProductId(recommendationRunHeader.getProductId());
		productList.add(productDTO);

		// Get product group properties
		List<PRProductGroupProperty> productGroupProperties = pricingEngineDAO.getProductGroupProperties(conn,
				productList);

		// Get isUsePrediction flag of all runs
		boolean usePrediction = PricingEngineHelper.isUsePrediction(productGroupProperties,
				recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(),
				recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId());

		// Get predicted results of all items
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = getAlreadyPredictedValues(conn,
				ligMembersOrNonLig, recommendationRunHeader, usePrediction);

		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			if (!itemDTO.isLir()) {
				// below variable is added to get the default values for parameter which is not
				// applicable
				// here e.g. sale, page no, block no, display id, promo type id
				PredictionDetailDTO tempPredictionDetailDTO = new PredictionDetailDTO();

				if (itemDTO.getRecommendedRegPrice() != null) {
					predictionDetailKey = new PredictionDetailKey(recommendationRunHeader.getLocationLevelId(),
							recommendationRunHeader.getLocationId(), itemDTO.getItemCode(),
							itemDTO.getRecommendedRegPrice().multiple, itemDTO.getRecommendedRegPrice().price,
							tempPredictionDetailDTO.getSaleQuantity(), tempPredictionDetailDTO.getSalePrice(),
							tempPredictionDetailDTO.getAdPageNo(), tempPredictionDetailDTO.getBlockNo(),
							tempPredictionDetailDTO.getPromoTypeId(), tempPredictionDetailDTO.getDisplayTypeId());
					PredictionDetailDTO predictionDetailDTO = predictions.get(predictionDetailKey);
					if (predictionDetailDTO != null) {
						// if(predictionDetailDTO.getPredictedMovement() < 0)
						// itemDTO.setPredictedMovement(0d);
						// else
						itemDTO.setPredictedMovement(Double.valueOf(predictionDetailDTO.getPredictedMovement()));
						itemDTO.setPredictionStatus(
								PredictionStatus.get(predictionDetailDTO.getPredictionStatus()).getStatusCode());
						itemDTO.setRegPricePredReasons(predictionDetailDTO.getQuestionablePrediction());

					} else {
						itemDTO.setPredictedMovement(null);
						itemDTO.setPredictionStatus(null);
					}
				} else {
					itemDTO.setPredictedMovement(null);
					itemDTO.setPredictionStatus(null);
				}

			}
		}
	}

//	public void updateLIGDataAsRecPriceIsChanged(List<PRItemDTO> itemListWithRecPrice) throws GeneralException, JsonProcessingException {
//		PricingEngineService pricingEngineService = new PricingEngineService();
//		LIGConstraint ligConstraint = new LIGConstraint();
//		ObjectMapper mapper = new ObjectMapper();
//		
//		for (PRItemDTO itemDTO : itemListWithRecPrice) {
//			if (itemDTO.isLir()) {
//				int retLirId = itemDTO.getRetLirId();
//				
//				List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
//				ligMembers = pricingEngineService.getLigMembers(retLirId, itemListWithRecPrice);
//				PRItemDTO ligRepresentingItem = pricingEngineService.getLigRepresentingItem(retLirId, itemListWithRecPrice);
//				if (ligRepresentingItem != null) {
//					
//					// Rec price
//					MultiplePrice multiplePrice = ligConstraint.getMostCommonRecPrice(ligMembers);
//					ligRepresentingItem.setRecommendedRegPrice(multiplePrice);
//					
//					// Sum prediction at lig level
//					ligRepresentingItem = ligConstraint.sumPredictionForLIG(ligRepresentingItem, ligMembers);
//					
//					// Update prediction status of lig
//					ligRepresentingItem = ligConstraint.updatePredictionStatus(ligRepresentingItem, ligMembers);
//					
//					//Update sales and margin
//					ligRepresentingItem = ligConstraint.sumRecRetailSalesDollarForLIG(ligRepresentingItem, ligMembers);
//					ligRepresentingItem = ligConstraint.sumRecRetailMarginDollarForLIG(ligRepresentingItem, ligMembers);
//					
//					
//					// Representing lig log
//					ligRepresentingItem.setExplainLog(null);
//					logger.debug("1.ligRepresentingItem.getExplainLog():"
//							+ (ligRepresentingItem.getExplainLog() != null ? mapper.writeValueAsString(ligRepresentingItem.getExplainLog()) : ""));
//					ligRepresentingItem = ligConstraint.updateLIGExplainLog(ligRepresentingItem.getLigRepItemCode(), ligRepresentingItem, ligMembers);
//					logger.debug("2.ligRepresentingItem.getExplainLog():"
//							+ (ligRepresentingItem.getExplainLog() != null ? mapper.writeValueAsString(ligRepresentingItem.getExplainLog()) : ""));
//					
//					// Conflict
//					ligRepresentingItem = ligConstraint.updateLIGConflict(ligRepresentingItem, ligMembers);
//					
//				}
//
//			}
//		}
//	}

	// Single item
	public void writeAdditionalExplainLog(PRItemDTO itemDTO, ExplainRetailNoteTypeLookup noteTypeLookup,
			List<String> additionalInfo) {
		List<PRItemDTO> items = new ArrayList<PRItemDTO>();
		items.add(itemDTO);
		writeAdditionalExplainLog(items, noteTypeLookup, additionalInfo);
	}

	// All the lig members or non-lig
	public void writeAdditionalExplainLog(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice,
			ExplainRetailNoteTypeLookup noteTypeLookup, List<String> additionalInfo) {

		List<PRItemDTO> ligMembersOrNonLig = getLigMembersOrNonLigItem(ligRepOrNonLigItem, itemListWithRecPrice);
		writeAdditionalExplainLog(ligMembersOrNonLig, noteTypeLookup, additionalInfo);
	}

	/*
	 * private void writeAdditionalExplainLog(List<PRItemDTO> ligMembersOrNonLig,
	 * ExplainRetailNoteTypeLookup noteTypeLookup, List<String> additionalInfo) {
	 * 
	 * for (PRItemDTO itemDTO : ligMembersOrNonLig) { if (itemDTO.getExplainLog() !=
	 * null) { List<PRExplainAdditionalDetail> explainAdditionalDetail =
	 * (itemDTO.getExplainLog() .getExplainAdditionalDetail() != null ?
	 * itemDTO.getExplainLog().getExplainAdditionalDetail() : new
	 * ArrayList<PRExplainAdditionalDetail>());
	 * 
	 * PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
	 * expDetail.setExplainRetailNoteTypeLookupId(noteTypeLookup.getNoteTypeLookupId
	 * ()); expDetail.setNoteValues(additionalInfo);
	 * explainAdditionalDetail.add(expDetail);
	 * itemDTO.getExplainLog().setExplainAdditionalDetail(explainAdditionalDetail);
	 * } } }
	 */
	
	private void writeAdditionalExplainLog(List<PRItemDTO> ligMembersOrNonLig,
			ExplainRetailNoteTypeLookup noteTypeLookup, List<String> additionalInfo) {

		for (PRItemDTO itemDTO : ligMembersOrNonLig) {

			Map<Integer, List<PRExplainAdditionalDetail>> explainLogMap = null;
			if (itemDTO.getExplainLog() != null) {
				List<PRExplainAdditionalDetail> explainAdditionalDetail = (itemDTO.getExplainLog()
						.getExplainAdditionalDetail() != null ? itemDTO.getExplainLog().getExplainAdditionalDetail()
								: new ArrayList<PRExplainAdditionalDetail>());

				if (explainAdditionalDetail != null) {
					explainLogMap = explainAdditionalDetail.stream().collect(
							Collectors.groupingBy(PRExplainAdditionalDetail::getExplainRetailNoteTypeLookupId));
				}

				// Added by Karishma on 04/19 to not copy the current retail retained logs to
				// memebers of LIG // if individual item's current price is not retained
				if (!itemDTO.isCurPriceRetained() && noteTypeLookup
						.getNoteTypeLookupId() == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS
								.getNoteTypeLookupId()) {
				} else {
					if (explainLogMap != null && !explainLogMap.containsKey(noteTypeLookup.getNoteTypeLookupId())) {

						PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
						expDetail.setExplainRetailNoteTypeLookupId(noteTypeLookup.getNoteTypeLookupId());
						expDetail.setNoteValues(additionalInfo);
						explainAdditionalDetail.add(expDetail);
						itemDTO.getExplainLog().setExplainAdditionalDetail(explainAdditionalDetail);
					}
				}
			}
		}
	}

	// Clear cur retail retained logs
	public void clearAdditionalExplainLog(PRItemDTO itemDTO) {

		if (itemDTO.getExplainLog() != null && itemDTO.getExplainLog().getExplainAdditionalDetail() != null) {
			List<PRExplainAdditionalDetail> explainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();

			for (PRExplainAdditionalDetail expAddDetail : itemDTO.getExplainLog().getExplainAdditionalDetail()) {
				if (!hasCurrentRetailRetainedLog(expAddDetail)) {
					explainAdditionalDetail.add(expAddDetail);
				}
			}

			itemDTO.getExplainLog().setExplainAdditionalDetail(new ArrayList<PRExplainAdditionalDetail>());
		}
	}

	private boolean hasCurrentRetailRetainedLog(List<PRExplainAdditionalDetail> explainAdditionalDetail) {
		boolean isCurrentRetailRetainedLog = false;

		for (PRExplainAdditionalDetail expAddDetail : explainAdditionalDetail) {
			if (isCurrentRetailRetainedId(expAddDetail.getExplainRetailNoteTypeLookupId())) {
				isCurrentRetailRetainedLog = true;
				break;
			}
		}

		return isCurrentRetailRetainedLog;
	}

	private boolean hasCurrentRetailRetainedLog(PRExplainAdditionalDetail explainAdditionalDetail) {
		boolean isCurrentRetailRetainedLog = false;

		if (isCurrentRetailRetainedId(explainAdditionalDetail.getExplainRetailNoteTypeLookupId())) {
			isCurrentRetailRetainedLog = true;
		}

		return isCurrentRetailRetainedLog;
	}

	private boolean hasNoteTypeLookupId(List<PRExplainAdditionalDetail> explainAdditionalDetail,
			ExplainRetailNoteTypeLookup explainRetailNoteTypeLookup) {
		boolean hasNoteTypeLookupId = false;
		// Check if passed id is present
		for (PRExplainAdditionalDetail expAddDetail : explainAdditionalDetail) {
			if (expAddDetail.getExplainRetailNoteTypeLookupId() == explainRetailNoteTypeLookup.getNoteTypeLookupId()) {
				hasNoteTypeLookupId = true;
				break;
			}
		}

		return hasNoteTypeLookupId;
	}

	private boolean isCurrentRetailRetainedId(int noteTypeLookupId) {
		boolean isCurrentRetailRetainedId = false;

		// for readability
		if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_AS_NO_VALID_PRED.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_LONG_TERM_PROMO
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_NO_MARGIN_BENEFIT
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PRICE_LESS_THAN_SALE_PRICE
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_DECREASED
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_INCREASED
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_DECREASE
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_INCREASE
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_NEXT_RANGE
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_SALE
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		} else if (noteTypeLookupId == ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS
				.getNoteTypeLookupId()) {
			isCurrentRetailRetainedId = true;
		}
	
		return isCurrentRetailRetainedId;
	}

	// if there are multiple reason for a retail to be retained,
	// prioritize the reason
	public void prioritizeAdditionalLog(List<PRItemDTO> itemListWithRecPrice) {
		for (PRItemDTO itemDTO : itemListWithRecPrice) {
			// all lig members and non-lig
			if ((!itemDTO.isLir() && itemDTO.getRetLirId() > 0) || (!itemDTO.isLir() && itemDTO.getRetLirId() <= 0)) {
				// is there additional log
				if (itemDTO.getExplainLog() != null && itemDTO.getExplainLog().getExplainAdditionalDetail() != null) {
					List<PRExplainAdditionalDetail> explainAdditionalDetail = itemDTO.getExplainLog()
							.getExplainAdditionalDetail();

					if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_INCREASED)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_INCREASED);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_DECREASED)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_RECENTLY_DECREASED);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_AS_NO_VALID_PRED)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_AS_NO_VALID_PRED);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_NO_MARGIN_BENEFIT)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_NO_MARGIN_BENEFIT);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_LONG_TERM_PROMO)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_LONG_TERM_PROMO);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_DECREASE)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_DECREASE);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_INCREASE)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_INCREASE);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_NEXT_RANGE)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_NEXT_RANGE);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_SALE)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_SALE);
					} else if (hasNoteTypeLookupId(explainAdditionalDetail,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PRICE_LESS_THAN_SALE_PRICE)) {
						removeAllOtherAdditionalLog(itemDTO,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PRICE_LESS_THAN_SALE_PRICE);
					} else if (hasCurrentRetailRetainedLog(explainAdditionalDetail)) {
						keepFirstAdditionalLogAlone(itemDTO);
					}
				}
			}
		}
	}

	private void removeAllOtherAdditionalLog(PRItemDTO itemDTO,
			ExplainRetailNoteTypeLookup explainRetailNoteTypeLookup) {
		List<PRExplainAdditionalDetail> newExplainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();

		if (itemDTO.getExplainLog() != null && itemDTO.getExplainLog().getExplainAdditionalDetail() != null) {
			for (PRExplainAdditionalDetail expAddDetail : itemDTO.getExplainLog().getExplainAdditionalDetail()) {
				if (expAddDetail.getExplainRetailNoteTypeLookupId() == explainRetailNoteTypeLookup
						.getNoteTypeLookupId()) {
					newExplainAdditionalDetail.add(expAddDetail);
				}
			}
		}
		itemDTO.getExplainLog().setExplainAdditionalDetail(newExplainAdditionalDetail);
	}

	// keep first current retail retained log
	private void keepFirstAdditionalLogAlone(PRItemDTO itemDTO) {
		List<PRExplainAdditionalDetail> newExplainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();

		if (itemDTO.getExplainLog() != null && itemDTO.getExplainLog().getExplainAdditionalDetail() != null) {

			Map<Integer, List<PRExplainAdditionalDetail>> additionalLogMapwithLookupId = itemDTO.getExplainLog()
					.getExplainAdditionalDetail().stream()
					.collect(Collectors.groupingBy(PRExplainAdditionalDetail::getExplainRetailNoteTypeLookupId));

			for (PRExplainAdditionalDetail expAddDetail : itemDTO.getExplainLog().getExplainAdditionalDetail()) {
				if (isCurrentRetailRetainedId(expAddDetail.getExplainRetailNoteTypeLookupId())) {
					
					//logger.info("keepFirstAdditionalLogAlone item:"+ itemDTO.getItemCode() );
					// Added by Karishma on 02/11/2022 for AZ USA
					// If current retail is below MAP then Map is retained,hence the log is to be
					// displayed showing message as MAP has overridden current retail and current retail retained log should not be displayed
					if (itemDTO.isCurrentPriceBelowMAP()) {
						List<String> additionalDetails = new ArrayList<String>();
						
						PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
						expDetail.setExplainRetailNoteTypeLookupId(
								ExplainRetailNoteTypeLookup.CURR_RETAIL_OVERRIDEN_BY_MAP.getNoteTypeLookupId());
						newExplainAdditionalDetail.add(expDetail);
						itemDTO.setCurPriceRetained(false);
					} else {
						newExplainAdditionalDetail.add(expAddDetail);
					}

					break;
				}
			}
		}
		itemDTO.getExplainLog().setExplainAdditionalDetail(newExplainAdditionalDetail);
	}

	public HashMap<Integer, List<PRItemDTO>> formLigMap(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<Integer, List<PRItemDTO>> ligMap = new HashMap<Integer, List<PRItemDTO>>();
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			List<PRItemDTO> tList = new ArrayList<PRItemDTO>();
			if (ligMap.get(itemDTO.getChildLocationId()) != null) {
				tList = ligMap.get(itemDTO.getChildLocationId());
			}
			tList.add(itemDTO);
			ligMap.put(itemDTO.getChildLocationId(), tList);
		}
		return ligMap;
	}

	public boolean hasRelatedItems(PRItemDTO itemDTO) {
		boolean hasRelatedItems = false;
		if (itemDTO.getExplainLog() != null) {
			for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog()
					.getGuidelineAndConstraintLogs()) {
				if ((guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
						|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE
								.getGuidelineTypeId())
						&& guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()) {
					hasRelatedItems = true;
				}
			}
		}
		return hasRelatedItems;
	}

	/**
	 * When current retail is in multiples and if the recommended price is unit
	 * price and if the difference between current unit price and recommended unit
	 * price is within <<5>> cents then retain current price
	 * 
	 * @param itemDTO
	 * @param itemListWithRecPrice
	 * @param maxUnitPriceDiff
	 * @return
	 */
	public boolean retainCurrentMultipleRetail(PRItemDTO itemDTO, List<PRItemDTO> itemListWithRecPrice,
			double maxUnitPriceDiff) {
		boolean isCurrentRetailInMultiples = false;

		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);

		// If current reg price is in multiples
		if (curRegPrice != null && curRegPrice.multiple > 1) {
			// if the recommended price is in unit price
			if (itemDTO.getRecommendedRegPrice() != null && itemDTO.getRecommendedRegPrice().price != null
					&& itemDTO.getRecommendedRegPrice().multiple == 1 && maxUnitPriceDiff > 0) {

				double curRegUnitPrice = PRCommonUtil.getUnitPrice(curRegPrice, true);
				double unitPriceDiff = (curRegUnitPrice - itemDTO.getRecommendedRegPrice().price);
				double unitPriceDiffInPositive = (unitPriceDiff < 0 ? unitPriceDiff * -1 : unitPriceDiff);

				// If the difference in unit price within x cents, then retain current retail
				if (unitPriceDiffInPositive <= maxUnitPriceDiff) {
//					logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:") + itemDTO.getItemCode()
//							+ " as recommended unit price difference is within " + maxUnitPriceDiff + " Cents of current unit price ***");

					isCurrentRetailInMultiples = true;

					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(String.valueOf(maxUnitPriceDiff));
					new PricingEngineService().writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_UNIT_PRICE_LESSER_DIFFERENCE,
							additionalDetails);
				}
			}
		}

		return isCurrentRetailInMultiples;
	}

	public boolean isRuleEnabled(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, String ruleCode,
			int objectiveTypeId) {
		boolean isRuleEnabled = false;
		boolean isObjSpecificRuleDefined = false;

		// Rule code available
		if (recommendationRuleMap.get(ruleCode) != null) {
			List<RecommendationRuleMapDTO> recommendationRules = recommendationRuleMap.get(ruleCode);

			// Check if there is objective specific rule
			for (RecommendationRuleMapDTO recommendationRule : recommendationRules) {
				if (recommendationRule.getObjectiveTypeId() == objectiveTypeId) {
					if (recommendationRule.isEnabled()) {
						isRuleEnabled = true;
					}
					isObjSpecificRuleDefined = true;
					break;
				}
			}

			// If not check if defined at higher level
			if (!isObjSpecificRuleDefined) {
				for (RecommendationRuleMapDTO recommendationRule : recommendationRules) {
					if (recommendationRule.getObjectiveTypeId() == 0 && recommendationRule.isEnabled()) {
						isRuleEnabled = true;
						break;
					}
				}
			}
		}

		return isRuleEnabled;
	}

	/**
	 * To check Price range is within the limit for the given TPR item. Check only
	 * Brand and Size relation
	 * 
	 * @param itemDTO
	 * @return
	 */
	public boolean checkPriceRangeUsingRelatedItem(PRItemDTO itemDTO) {
		boolean isPriceWithInRange = true;
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog()
				.getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
					|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE
							.getGuidelineTypeId()) {
				if (!checkIfPriceWithinRange(itemDTO.getRecommendedRegPrice().multiple,
						itemDTO.getRecommendedRegPrice().price, guidelineAndConstraintLog.getOutputPriceRange(),
						guidelineAndConstraintLog)) {
					isPriceWithInRange = false;
				}
			}
		}
		return isPriceWithInRange;
	}

//	public void resetCompStrIdInIndexGuideline(HashMap<ItemKey, PRItemDTO> itemDataMap) {
//		for (PRItemDTO itemDTO : itemDataMap.values()) {
//			if (!itemDTO.isLir()) {
//				// If price index guideline is defined
//				if (itemDTO.getStrategyDTO() != null && itemDTO.getStrategyDTO().getGuidelines() != null
//						&& itemDTO.getStrategyDTO().getGuidelines().getPiGuideline() != null
//						&& itemDTO.getStrategyDTO().getGuidelines().getPiGuideline().size() > 0) {
//					for (PRGuidelinePI guidelinePI : itemDTO.getStrategyDTO().getGuidelines().getPiGuideline()) {
//						guidelinePI.setCompStrId(0); 
//					}
//				}
//			}
//		}
//	}

	public void copyForLeadStrategyDTO(PRStrategyDTO sourceInputDTO, PRStrategyDTO destInputDTO) {
		destInputDTO.setLocationLevelId(sourceInputDTO.getLocationLevelId());
		destInputDTO.setProductLevelId(sourceInputDTO.getProductLevelId());
		destInputDTO.setProductId(sourceInputDTO.getProductId());
		destInputDTO.setStartDate(sourceInputDTO.getStartDate());
		destInputDTO.setEndDate(sourceInputDTO.getEndDate());
	}

	public void writeAddtExpLogForTPRRelLigItems(PRItemDTO itemDTO, List<PRItemDTO> itemListWithRecPrice,
			ExplainRetailNoteTypeLookup noteTypeLookup, List<String> addtLogDetails) {
		List<PRItemDTO> tprItemList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> ligMembers = getLigMembersOrNonLigItem(itemDTO, itemListWithRecPrice);
		ligMembers.forEach(prItemDTO -> {
			if ((prItemDTO.isOnGoingPromotion() || prItemDTO.isFuturePromotion())
					&& !prItemDTO.isPromoEndsWithinXWeeks()) {
				tprItemList.add(prItemDTO);
			}
		});
		writeAdditionalExplainLog(itemDTO, tprItemList, noteTypeLookup, addtLogDetails);

	}

	public LinkedHashMap<Integer, Integer> getZoneStoreCount(List<PRItemDTO> allStoresItem) {
		HashMap<Integer, Integer> zoneStoreCountMap = new HashMap<Integer, Integer>();

		HashMap<Integer, HashSet<Integer>> tempZoneStoreCountMap = new HashMap<Integer, HashSet<Integer>>();

		// Group by zone and its distinct stores
		for (PRItemDTO itemInfo : allStoresItem) {
			HashSet<Integer> tempSet = new HashSet<Integer>();

			if (tempZoneStoreCountMap.get(itemInfo.getPriceZoneId()) != null) {
				tempSet = tempZoneStoreCountMap.get(itemInfo.getPriceZoneId());
			}
			tempSet.add(itemInfo.getChildLocationId());

			tempZoneStoreCountMap.put(itemInfo.getPriceZoneId(), tempSet);

			zoneStoreCountMap.put(itemInfo.getPriceZoneId(), tempSet.size());
		}

		LinkedHashMap<Integer, Integer> finalMap = zoneStoreCountMap.entrySet().stream()
				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toMap(
						Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));

//		logger.debug("Zone and store count:" + finalMap);

		return finalMap;
	}

	/**
	 * Initializes connection. Used when program is accessed through webservice
	 */
	protected void initializeForWS() {
		setConnection(getDSConnection());
	}

	/**
	 * Returns Connection from datasource
	 * 
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
//		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));;
		try {
			if (ds == null) {
				initContext = new InitialContext();
				envContext = (Context) initContext.lookup("java:/comp/env");
				ds = (DataSource) envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		} catch (NamingException exception) {
			logger.error("Error when creating connection from datasource " + exception.toString());
		} catch (SQLException exception) {
			logger.error("Error when creating connection from datasource " + exception.toString());
		}
		return connection;
	}

	protected Connection getConnection() {
		return conn;
	}

	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	private boolean isDateParsable(String inputDate) {
		boolean isDateParsable = true;

		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
			LocalDate.parse(inputDate, formatter);
		} catch (Exception e) {
			isDateParsable = false;
		}

		return isDateParsable;
	}

	/**
	 * return -1 - decreased, 0 - unchanged, 1 - increased
	 * 
	 * @param itemPriceHistory
	 * @param itemCode
	 * @param inpWeekStartDate
	 * @param noOfDays
	 * @return
	 * @throws GeneralException
	 */
	public int itemPriceChangeStatus(HashMap<String, RetailPriceDTO> itemPriceHistory, LocalDate baseWeekStartDate,
			int noOfWeeks) throws GeneralException {
		int itemPriceChangeStatus = 0;

		LocalDate baseWeekEndDate = baseWeekStartDate.plusDays(6);
		LocalDate inpStartDateRange = baseWeekStartDate.minusWeeks(noOfWeeks);

		// Convert to weeks so that price history
		// is checked within these weeks
//		long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(inpStartDateRange, baseWeekEndDate);
//		int noOfWeeks = (int) Math.ceil(daysBetween / 7.0);
		Double lastRegUnitPrice = 0d;
		for (int i = 0; i <= noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)),
					Constants.APP_DATE_FORMAT);

			// Get that week price
			if (itemPriceHistory != null && itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				Double regUnitPrice = PRCommonUtil.getUnitPrice(itemPrice.getRegMPack(),
						Double.valueOf(itemPrice.getRegPrice()), Double.valueOf(itemPrice.getRegMPrice()), true);

				if (regUnitPrice > 0 && itemPrice.getRegEffectiveDate() != null) {
					LocalDate regEffectiveDate = DateUtil.toDateAsLocalDate(itemPrice.getRegEffectiveDate());

					// Check if there is price within the date range
					if (regEffectiveDate.isAfter(inpStartDateRange) && regEffectiveDate.isBefore(baseWeekEndDate)
							&& lastRegUnitPrice == 0) {
						lastRegUnitPrice = regUnitPrice;
						// } else if (regEffectiveDate.isAfter(inpStartDateRange) &&
						// regEffectiveDate.isBefore(baseWeekEndDate)) {
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

	public boolean isRegPriceUnchangedInLastXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory,
			String baseWeekStartDate, int noOfWeeks) throws GeneralException {

		int priceChangeStatus = itemPriceChangeStatus(itemPriceHistory, DateUtil.toDateAsLocalDate(baseWeekStartDate),
				noOfWeeks);

		boolean isPriceUnchanged = (priceChangeStatus == 0 ? true : false);

		return isPriceUnchanged;
	}

	public String getItemPriceChangeEffDate(HashMap<String, RetailPriceDTO> itemPriceHistory,
			LocalDate baseWeekStartDate, int noOfWeeks) throws GeneralException {
		String effDate = null;
		LocalDate baseWeekEndDate = baseWeekStartDate.plusDays(6);
		LocalDate inpStartDateRange = baseWeekStartDate.minusWeeks(noOfWeeks - 1);

		// Convert to weeks so that price history
		// is checked within these weeks
//		long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(inpStartDateRange, baseWeekEndDate);
//		int noOfWeeks = (int) Math.ceil(daysBetween / 7.0);
		Double lastRegUnitPrice = 0d;
		for (int i = 0; i < noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)),
					Constants.APP_DATE_FORMAT);

			// Get that week price
			if (itemPriceHistory != null && itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				Double regUnitPrice = PRCommonUtil.getUnitPrice(itemPrice.getRegMPack(),
						Double.valueOf(itemPrice.getRegPrice()), Double.valueOf(itemPrice.getRegMPrice()), true);

				if (regUnitPrice > 0 && itemPrice.getRegEffectiveDate() != null) {
					LocalDate regEffectiveDate = DateUtil.toDateAsLocalDate(itemPrice.getRegEffectiveDate());

					// Check if there is price within the date range
					if (regEffectiveDate.isAfter(inpStartDateRange) && regEffectiveDate.isBefore(baseWeekEndDate)
							&& lastRegUnitPrice == 0) {
						lastRegUnitPrice = regUnitPrice;
						// } else if (regEffectiveDate.isAfter(inpStartDateRange) &&
						// regEffectiveDate.isBefore(baseWeekEndDate)) {
					} else {

						if (lastRegUnitPrice > 0
								&& (lastRegUnitPrice - regUnitPrice > 0 || lastRegUnitPrice - regUnitPrice < 0)) {
							effDate = tempWeekStartDate;
							break;
						}
					}
				}
			}
		}

		return effDate;
	}

	
	
	
	  // Added by Karishma on 04/19 to set the current retail retained log for 
	// individual LIG members\
	public void writeExplainLogForLIGRepItem(PRItemDTO itemDTO, ExplainRetailNoteTypeLookup noteTypeLookup,
			List<String> additionalInfo) {
		Map<Integer, List<PRExplainAdditionalDetail>> explainLogMap = null;
		if (itemDTO.getExplainLog() != null) {
			List<PRExplainAdditionalDetail> explainAdditionalDetail = (itemDTO.getExplainLog()
					.getExplainAdditionalDetail() != null ? itemDTO.getExplainLog().getExplainAdditionalDetail()
							: new ArrayList<PRExplainAdditionalDetail>());
			if (explainAdditionalDetail != null) {
				explainLogMap = explainAdditionalDetail.stream()
						.collect(Collectors.groupingBy(PRExplainAdditionalDetail::getExplainRetailNoteTypeLookupId));
			}
			if (explainLogMap != null && !explainLogMap.containsKey(noteTypeLookup.getNoteTypeLookupId())) {
				PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
				expDetail.setExplainRetailNoteTypeLookupId(noteTypeLookup.getNoteTypeLookupId());
				expDetail.setNoteValues(additionalInfo);
				explainAdditionalDetail.add(expDetail);
				itemDTO.getExplainLog().setExplainAdditionalDetail(explainAdditionalDetail);
			}

		}

	}
	

	// Added by Karishma on 04/19 to set the current retail retained log for
	// individual LIG members
	public void writeExplainLogForLIGItem(PRItemDTO itemDTO, ExplainRetailNoteTypeLookup noteTypeLookup,
			List<String> additionalInfo) {
		Map<Integer, List<PRExplainAdditionalDetail>> explainLogMap = null;
		if (itemDTO.getExplainLog() != null) {
			List<PRExplainAdditionalDetail> explainAdditionalDetail = (itemDTO.getExplainLog()
					.getExplainAdditionalDetail() != null ? itemDTO.getExplainLog().getExplainAdditionalDetail()
							: new ArrayList<PRExplainAdditionalDetail>());
			if (explainAdditionalDetail != null) {
				explainLogMap = explainAdditionalDetail.stream()
						.collect(Collectors.groupingBy(PRExplainAdditionalDetail::getExplainRetailNoteTypeLookupId));
			}
			if (explainLogMap != null && !explainLogMap.containsKey(noteTypeLookup.getNoteTypeLookupId())) {
				PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
				expDetail.setExplainRetailNoteTypeLookupId(noteTypeLookup.getNoteTypeLookupId());
				expDetail.setNoteValues(additionalInfo);
				explainAdditionalDetail.add(expDetail);
				itemDTO.getExplainLog().setExplainAdditionalDetail(explainAdditionalDetail);
			}

		}

	}
	
	/**
	 * Added by Karishma for clearing the explian the unwanted explainlogs for items after final reccs are done 
	 * @param itemListWithLIG
	 */
	public void explainLogClearForLIGMembers(List<MWRItemDTO> itemListWithLIG) {
		for (MWRItemDTO itemDTO : itemListWithLIG) {
			if (itemDTO.getExplainLog() != null && itemDTO.getExplainLog().getExplainAdditionalDetail() != null) {
				List<PRExplainAdditionalDetail> explainAdditionalDetail = itemDTO.getExplainLog()
						.getExplainAdditionalDetail();
				if (hasCurrentRetailRetainedLog(explainAdditionalDetail)) {
					List<PRExplainAdditionalDetail> newExplainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
					for (PRExplainAdditionalDetail expAddDetail : explainAdditionalDetail) {
						if (isCurrentRetailRetainedId(expAddDetail.getExplainRetailNoteTypeLookupId())) {
							if (itemDTO.getRecommendedRegPrice() != null && itemDTO.getRegPrice() > 0
									&& itemDTO.getRecommendedRegPrice().getUnitPrice() == itemDTO.getRegPrice()) {
								newExplainAdditionalDetail.add(expAddDetail);
							}
							break;
						}
					}

					itemDTO.getExplainLog().setExplainAdditionalDetail(newExplainAdditionalDetail);
				}
				if (itemDTO.isCurrentPriceBelowMAP()) {
					if (itemDTO.getRecommendedRegPrice() != null && itemDTO.getRecommendedRegPrice()
							.getUnitPrice() != itemDTO.getRecommendedPricewithMap()) {
						List<PRExplainAdditionalDetail> newExplainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
						for (PRExplainAdditionalDetail expAddDetail : explainAdditionalDetail) {
							if (expAddDetail
									.getExplainRetailNoteTypeLookupId() != ExplainRetailNoteTypeLookup.CURR_RETAIL_OVERRIDEN_BY_MAP
											.getNoteTypeLookupId()) {
								newExplainAdditionalDetail.add(expAddDetail);
							}
						}
						itemDTO.getExplainLog().setExplainAdditionalDetail(newExplainAdditionalDetail);
					}
				}
			}
		}
	}
	
	/**
	 * Set the explain log for items 
	 * @param explainAdditionalDetail
	 * @return
	 */
	public List<PRExplainAdditionalDetail> setPendingRetailsLog(List<PRExplainAdditionalDetail> explainAdditionalDetail) {
		PRExplainAdditionalDetail expDetail = new PRExplainAdditionalDetail();
		expDetail.setExplainRetailNoteTypeLookupId(
				ExplainRetailNoteTypeLookup.PENDING_RETAIL_RECOMMENDED.getNoteTypeLookupId());
		expDetail.setNoteValues(new ArrayList<String>());
		explainAdditionalDetail.add(expDetail);

		return explainAdditionalDetail;
	}
	 
}
