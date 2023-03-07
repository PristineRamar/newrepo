package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
//import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
//import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
//import com.pristine.service.PredictionComponent;
//import com.pristine.service.offermgmt.constraint.LIGConstraint;
//import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MarginOpportunity {
	private static Logger logger = Logger.getLogger("MarginOpportunity");
	
	//TODO:: Setting price group item predicted number and margin opp are
	//combined in the same function, which has to separated properly
	//This class just need to find if there is margin oppor or not, i.e.
	//before this function, all the items prediction should be present
	
	/**
	 * Method populates items and its lig with margin opportunities
	 * @param itemListWithLIG	Item List
	 * @param retLirMap			HashMap<RET_LIR_ID, HashMap<RET_LIR_ITEM_CODE, ArrayList<ITEM_CODE>>
	 * @return
	 * @throws GeneralException 
	 */
	/*public List<PRItemDTO> findOpportunities(Connection conn, List<PRItemDTO> itemListWithLIG, 
			HashMap<Integer, List<PRItemDTO>> retLirMap, PRStrategyDTO recInputDTO, 
			List<PRProductGroupProperty> productProperties, List<ExecutionTimeLog> executionTimeLogs, 
			PRRecommendationRunHeader  recommendationRunHeader, boolean isOnline, List<Integer> priceZoneStores, RetailCalendarDTO curCalDTO) throws GeneralException {
		List<PRItemDTO> itemListWithOpp = null;
		boolean usePrediction = true;		 
		
		//11th Nov 2014 - as per discussion with anand
		//No need to find opportunity for categories using avg movement as prediction and stores as its also using avg mov
		//margin oppor with avg movement is meaningless
		usePrediction = PricingEngineHelper.isUsePrediction(productProperties, recInputDTO.getProductLevelId(), 
				recInputDTO.getProductId(), recInputDTO.getLocationLevelId(), recInputDTO.getLocationId());		
	 
		if (usePrediction) {
			PredictionComponent predictionComponent = new PredictionComponent();
			//26th Mar 2016, In order to improve strategy what-if performance, no of price point for prediction
			//is reduced by not finding the next and previous price points (only recommended price added,
			//so that when price group items predictions are done properly)
			// Populate next and previous price points of recommended price in Item object
			if(recommendationRunHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
				populateNextAndPreviousPricePoints(itemListWithLIG, true);
			} else {
				populateNextAndPreviousPricePoints(itemListWithLIG, false);
			}
			// Invoke prediction engine, this also finds prediction for price group 
			// as in previous iteration it was not found
			PredictionOutputDTO predictionOutput = predictionComponent.invokePredictionEngine(conn, itemListWithLIG, recInputDTO,
					"MARGIN_OPPURTUNITY", productProperties, executionTimeLogs, recommendationRunHeader, isOnline, retLirMap);
			itemListWithOpp = findMarginOppurtunities(itemListWithLIG, retLirMap, predictionOutput);
			
			// Mark margin opportunities
			findMarginOppurtunities(recommendationRunHeader, itemListWithOpp);
			
			//4th July 2016, price point marked as margin oppur must present in the history 
			//(i.e. it should have been sold with that price before in last X(2) years)
			resetOpporIfNotInHistory(conn, priceZoneStores, itemListWithOpp, curCalDTO);
			
			//For price group items the cur reg price prediction is not updated, so it is written as separate function to update it 
			//updateCurRegPricePredictedMov(itemListWithOpp, retLirMap);
			
			//NU:: 2nd Oct 2016, find lig level details again
			//as price group items prediction are found and setup during the margin finding process
			updateLigLevelPrediction(itemListWithOpp, retLirMap);
			
			//Update margin opportunity at lig level
			updateMarginOpportunity(itemListWithOpp, retLirMap);
		} else {
			itemListWithOpp = itemListWithLIG;
		}
		return itemListWithOpp;
	}*/
	
	// 30th Dec 2016, function re-worked, its only find margin opportunity
	public List<PRItemDTO> markOpportunities(Connection conn, List<PRItemDTO> itemListWithLIG, HashMap<Integer, List<PRItemDTO>> retLirMap,
			PRStrategyDTO recInputDTO, List<PRProductGroupProperty> productProperties, List<ExecutionTimeLog> executionTimeLogs,
			PRRecommendationRunHeader recommendationRunHeader, boolean isOnline, List<Integer> priceZoneStores, RetailCalendarDTO curCalDTO)
			throws GeneralException {

		// 11th Nov 2014 - as per discussion with anand
		// No need to find opportunity for categories using avg movement as prediction and stores as its also using avg mov
		// margin oppor with avg movement is meaningless
		boolean usePrediction = PricingEngineHelper.isUsePrediction(productProperties, recInputDTO.getProductLevelId(), recInputDTO.getProductId(),
				recInputDTO.getLocationLevelId(), recInputDTO.getLocationId());

		// no need to find margin oppor during strategy what-if
		if (usePrediction && !recommendationRunHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
			
			//Add price points
			populateNextAndPreviousPricePoints(itemListWithLIG, false);
			
			//Aggregate lig data
			
			// Mark margin opportunities
			markMarginOppurtunities(recommendationRunHeader, itemListWithLIG);

			// 4th July 2016, price point marked as margin oppur must present in the history
			// (i.e. it should have been sold with that price before in last X(2) years)
			resetOpporIfNotInHistory(conn, priceZoneStores, itemListWithLIG, curCalDTO);

			// Update margin opportunity at lig level
			updateMarginOpportunity(itemListWithLIG, retLirMap);
		}  
		
		return itemListWithLIG;
	}

	/**
	 * Populate next and previous price points of recommended price in Item object 
	 * @param itemListWithLIG	Item List
	 */
	private void populateNextAndPreviousPricePoints(List<PRItemDTO> itemListWithLIG, Boolean addOnlyRecPrice) {
		logger.debug("populateNextAndPreviousPricePoints() is Started");
		for(PRItemDTO item : itemListWithLIG){
			if(item.getRecommendedRegPrice() != null){
				if(item.getStrategyDTO() != null){
					logger.debug(
							"populateNextAndPreviousPricePoints:" + "itemCode:" + item.getItemCode() + "recPrice:" + item.getRecommendedRegPrice());
					item.getStrategyDTO().getConstriants().getRoundingConstraint().getNextAndPreviousPrice(item, addOnlyRecPrice);
				}
			}
		}
		logger.debug("populateNextAndPreviousPricePoints() is Completed");
	}
	
	/**
	 * Find Margin Opportunities and populate price with margin opportunity in Item Object
	 * @param itemListWithLIG		Item List
	 * @param retLirMap				HashMap<RET_LIR_ID, HashMap<RET_LIR_ITEM_CODE, ArrayList<ITEM_CODE>>
	 * @param predictionOutput		Output from Prediction Engine
	 * @return
	 */
//	private List<PRItemDTO> findMarginOppurtunities(List<PRItemDTO> itemListWithLIG,
//			HashMap<Integer, List<PRItemDTO>> retLirMap, PredictionOutputDTO predictionOutput) {
//		//HashMap<Integer, PRItemDTO> itemMap = new HashMap<Integer, PRItemDTO>();
//		HashMap<ItemKey, PRItemDTO> itemMap = new HashMap<ItemKey, PRItemDTO>();
//		PredictionComponent predictionComponent = new PredictionComponent();
//		//List<PRItemDTO> outputList = new ArrayList<PRItemDTO>();
//		
////		for(PRItemDTO item : itemListWithLIG){
////			itemMap.put(item.getItemCode(), item);
////		}
//		
//		for(PRItemDTO item : itemListWithLIG){
//			ItemKey itemKey = PRCommonUtil.getItemKey(item);
//			itemMap.put(itemKey, item);
//		}
//		
//		logger.debug("findMarginOppurtunities is Started");
//		List<PredictionItemDTO> predictedItemList = predictionOutput.predictionItems;
//		if(predictedItemList != null){
//			for(PredictionItemDTO predictedItem : predictedItemList){
//				// Populate predicted movement in Item Object
//				int itemCodeOrLirId = predictedItem.itemCodeOrLirId;
//				boolean isLir = predictedItem.lirInd;
//				if(isLir){
//					// TODO: Code block needs to be added here once prediction happens at LIG level
//				}else{
//					//Find margin for each price point of the item. 
//					//Also update prediction to each price point from the prediction output
//					ItemKey itemKey = new ItemKey(itemCodeOrLirId, PRConstants.NON_LIG_ITEM_INDICATOR);
//					//PRItemDTO item = itemMap.get(itemCodeOrLirId);
//					PRItemDTO item = itemMap.get(itemKey);
//					for(PricePointDTO input : item.getOppurtunities()){
//						for(PricePointDTO output : predictedItem.pricePoints){
//							if(input.getRegPrice().equals(output.getRegPrice()) && input.getRegQuantity() == output.getRegQuantity()){
//								input.setPredictedMovement(output.getPredictedMovement());
//								input.setPredictionStatus(output.getPredictionStatus());
//								// Changes to store margin for opportunities price point
//								if(item.getCost() != null){
//									double margin = (input.getRegPrice() * input.getPredictedMovement()) - (item.getCost() * input.getPredictedMovement());
//									item.addOpportunitiesMargin(input.getRegPrice(), margin);
//								}
//								// Changes to store margin for opportunities price point - Ends
//								break;
//							}
//						}
//					}
//					
//					if(item.getCurRegPricePredictedMovement() <= 0){			
//						predictionComponent.setCurRegPricePredictedMovement(item, predictedItem.pricePoints);
//					}
//				}
//			}
//		}else{
//			logger.warn("No output from prediction service");
//		}
//		
//		
//		//Find margin for each price point of the lig. 
//		//Also update prediction to each price point from the prediction output
//		logger.debug("Aggregate data at LIG level is Started");
//		// Aggregate data at LIG level
//		
//		//for(Map.Entry<Integer, HashMap<Integer, ArrayList<Integer>>> entry : retLirMap.entrySet()){
//			// Loop each lig 	
//			for(Map.Entry<Integer, List<PRItemDTO>> inEntry : retLirMap.entrySet()){
//				ItemKey ligKey = new ItemKey(inEntry.getKey(), PRConstants.LIG_ITEM_INDICATOR);
//				//if(itemMap.get(inEntry.getKey()) != null){
//				if(itemMap.get(ligKey) != null){
//					//PRItemDTO retLirItemDTO = itemMap.get(inEntry.getKey());
//					PRItemDTO retLirItemDTO = itemMap.get(ligKey);
//					//logger.debug("F/inding LIG Level Data for LIG: " + retLirItemDTO.getItemCode() + ",Predicted Movement:" +
//							//retLirItemDTO.getPredictedMovement() + ",Cur Price Predicted Mov: " + retLirItemDTO.getCurRegPricePredictedMovement());
//					//Loop each item in the lig
//					for(PRItemDTO itemDTO : inEntry.getValue()){
//						//int itemCode = itemDTO.getItemCode();
//						ItemKey ligMemberKey = PRCommonUtil.getItemKey(itemDTO);
//						//if(itemMap.get(itemCode) != null) {
//						if(itemMap.get(ligMemberKey) != null) {
//							//PRItemDTO ligMember = itemMap.get(itemCode);
//							PRItemDTO ligMember = itemMap.get(ligMemberKey);
//							if(retLirItemDTO.getOppurtunities() != null){
//								//logger.debug("Opp is available for LIG : " + retLirItemDTO.getItemCode());
//								//Loop each lig price point
//								for(PricePointDTO ligPricePoint : retLirItemDTO.getOppurtunities()){
//									boolean isValidPredictionFound = false;
//									PredictionStatus predictionStatus = null;
//									//logger.debug("LIG price point : " + ligPricePoint.getRegPrice());
//									if(ligMember.getOppurtunities() != null){
//										//logger.debug("Opp is available for LIG member : " + ligMember.getItemCode());
//										//Loop each lig member price point
//										for(PricePointDTO ligMemberPricePoint : ligMember.getOppurtunities()){
//											//logger.debug("Lig Member Price Point : " + ligMemberPricePoint.getRegQuantity() + "/" + ligMemberPricePoint.getRegPrice()
//													//+ "Lig Price Point: " + ligPricePoint.getRegQuantity() + "/" + ligPricePoint.getRegPrice());
//											if(ligPricePoint.getRegPrice().equals(ligMemberPricePoint.getRegPrice()) 
//													&& ligPricePoint.getRegQuantity() == ligMemberPricePoint.getRegQuantity()){
//												if(ligMemberPricePoint.getPredictionStatus() != null && 
//														ligMemberPricePoint.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()){
//													//logger.debug("LIG member predicted Movement : " + ligMemberPricePoint.getRegPrice() + "--" 
//														//+  ligMemberPricePoint.getPredictionStatus().getStatusCode() + "--"
//														//+ ligMemberPricePoint.getPredictedMovement());
//													ligPricePoint.setPredictedMovement(ligPricePoint.getPredictedMovement() + ligMemberPricePoint.getPredictedMovement());
//													ligPricePoint.setPredictionStatus(ligMemberPricePoint.getPredictionStatus());
//													// Changes to store margin for opportunities price point
//													if(retLirItemDTO.getOpportunitiesMarginMap().get(ligPricePoint.getRegPrice()) != null){
//														if(ligMember.getOpportunitiesMarginMap().get(ligPricePoint.getRegPrice()) != null){
//															double margin = retLirItemDTO.getOpportunitiesMarginMap().get(ligPricePoint.getRegPrice()) + ligMember.getOpportunitiesMarginMap().get(ligPricePoint.getRegPrice());
//															retLirItemDTO.addOpportunitiesMargin(ligPricePoint.getRegPrice(), margin);
//														}
//													}else{
//														if(ligMember.getOpportunitiesMarginMap().get(ligPricePoint.getRegPrice()) != null){
//															retLirItemDTO.addOpportunitiesMargin(ligPricePoint.getRegPrice(), ligMember.getOpportunitiesMarginMap().get(ligPricePoint.getRegPrice()));
//														}
//													}
//													// Changes to store margin for opportunities price point - Ends
//													isValidPredictionFound = true;
//												}else{
//													//If lig price point has already success status don't update,
//													if(ligPricePoint.getPredictionStatus() == null || 
//															ligPricePoint.getPredictionStatus().getStatusCode() != PredictionStatus.SUCCESS.getStatusCode()){
//														ligPricePoint.setPredictionStatus(ligMemberPricePoint.getPredictionStatus());
//													}
//												}
//												predictionStatus = ligMemberPricePoint.getPredictionStatus();
//												break;
//											}
//										}
//										
//										/*if(!isValidPredictionFound){
//											//If all the lig member prediction status is error
//											ligPricePoint.setPredictionStatus(predictionStatus);
//										}*/
//										
//									}
//									//logger.debug("Predicted Movement for LIG: " + retLirItemDTO.getItemCode() + "--" + ligPricePoint.getPredictedMovement());
//								}
//							}
//						}
//					}
//					//logger.debug("Completes LIG Level Data for LIG: " + retLirItemDTO.getItemCode() + ",Predicted Movement:" +
//							//retLirItemDTO.getPredictedMovement() + ",Cur Price Predicted Mov: " + retLirItemDTO.getCurRegPricePredictedMovement());
//				}
//			}
//		//}
//		logger.debug("Aggregate data at LIG level is Completed");
//		
////		findMarginOppurtunities(itemListWithLIG);
//		//For price group items the cur reg price prediction is not updated, so it is written as separate function to update it 
////		updateCurRegPricePredictedMov(itemListWithLIG, retLirMap);
////		updateMarginOpportunity(itemListWithLIG, retLirMap);
//		logger.debug("findMarginOppurtunities is Completed");
//		return itemListWithLIG;
//	}	
	
	/**
	 * Mark price point which is going to give highest margin
	 * @param itemListWithLIG
	 */
//	private void findMarginOppurtunities(PRRecommendationRunHeader recommendationRunHeader, List<PRItemDTO> itemListWithLIG) {
//		logger.debug("findMarginOppurtunities is Started");
//		int highestMar$UsingCurRetail = ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL.getObjectiveTypeId();
//		int highestMar$ = ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR.getObjectiveTypeId();
//		HashMap<Integer, Integer> ligWithHighestMarObj = new HashMap<Integer, Integer>();
//		
//		//get retlirid where even its one of the item's objective is either higher margin $ or highest margin $ using current retail
//		//These lig items won't be marked with margin oppor
//		for(PRItemDTO item : itemListWithLIG){
//			if (item.getObjectiveTypeId() == highestMar$UsingCurRetail
//					|| item.getObjectiveTypeId() == highestMar$ && item.getRetLirId() > 0){
//				ligWithHighestMarObj.put(item.getRetLirId(), 0);
//			}
//		}
//		
//		for(PRItemDTO item : itemListWithLIG){
//			//logger.debug("findMarginOppurtunities starts -- Predicted Movement for item : " + item.getItemCode() + "--" + item.getPredictedMovement());
//			//Double listCost = item.getListCost();
//			//Don't mark margin opportunity for substitute item as whose prediction is based on a group of items
//			if (item.getOppurtunities() != null) {
//				double margin = 0;
//				double price = 0;
//				int qty = 0;
//				boolean isFirst = true;
//				if(item.getOppurtunities() != null) {
//					for(PricePointDTO p : item.getOppurtunities()){				
//						if(p.getPredictionStatus() != null && p.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()){
//							//logger.debug("Opp Item Code: " + item.getItemCode() + ", Opp Price: " + p.getRegPrice());
//							Double pricePointMargin = item.getOpportunitiesMarginMap().get(p.getRegPrice());
//							if(pricePointMargin != null){
//								double tMargin = pricePointMargin.doubleValue();
//								//logger.debug("Predicted Movement: " + p.getPredictedMovement() + ", Margin: " + tMargin);
//								if(isFirst){
//									margin = tMargin;
//									price = p.getRegPrice();
//									qty = p.getRegQuantity();
//									isFirst = false;
//								}else{
//									if(margin < tMargin){
//										margin = tMargin;
//										price = p.getRegPrice();
//										qty = p.getRegQuantity();
//									}
//								}
//							}else{
//								logger.debug("Margin not set for price point");
//							}
//						}else{
//							//if(p.getPredictionStatus() != null)
//								//logger.debug("Opp Item Code: " + item.getItemCode() + ", Opp Price: " + p.getRegPrice() + ", Prediction Status: " + p.getPredictionStatus().getStatusCode() + " Ignored during find margin opportunities");
//							//else
//								//logger.debug("Opp Item Code: " + item.getItemCode() + ", Opp Price: " + p.getRegPrice() + ", Prediction Status: " + p.getPredictionStatus() + " Ignored during find margin opportunities");
//						}
//						
//						
//						/* Code block added to set predicted movement (This might set predicted movement 
//						 * for price group related items whose price points 
//						are not passed to prediction engine during the first call to prediction engine */
//						
//						
//						//logger.debug("Predicted Movement at Price Point for Item :" + item.getItemCode() + ", Rec Price --" + p.getRegPrice() + ", Pred Status--" 
//								//+ p.getPredictionStatus().getStatusCode() + ", Pred Mov--" + p.getPredictedMovement());
//						
//						//Any change in below code need through understanding, other wise price group
//						//item prediction will not be shown
//						//Recommended price's predicted movement
////						if(p.getRegPrice().equals(item.getRecommendedRegPrice()) && p.getRegQuantity() == item.getRecommendedRegMultiple()){
//						if(p.getRegPrice().equals(item.getRecommendedRegPrice().price) && p.getRegQuantity() == item.getRecommendedRegPrice().multiple){
//							// Set predicted movement only when prediction status is success
//							if(p.getPredictionStatus() != null){
//								if(p.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS.getStatusCode())
//									item.setPredictedMovement(p.getPredictedMovement());
//								item.setPredictionStatus(p.getPredictionStatus().getStatusCode());
//							}
//						}		
//					}
//				}
//				
//				//26th Mar 2016, don't mark margin opportunities if it is strategy what-if
//				if (!recommendationRunHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
//					if (!item.getIsPartOfSubstituteGroup() && item.getObjectiveTypeId() != highestMar$UsingCurRetail
//							&& item.getObjectiveTypeId() != highestMar$
//							&& ((item.isLir() && ligWithHighestMarObj.get(item.getItemCode()) == null) || !item.isLir())) {
////						if (item.getRecommendedRegPrice() != null && price > 0 && price != item.getRecommendedRegPrice()) {
//						if (item.getRecommendedRegPrice() != null && price > 0 && price != item.getRecommendedRegPrice().price) {
//							// logger.debug("Price set as Opp: " + price);
//							item.setIsOppurtunity(String.valueOf(Constants.YES));
//							item.setOppurtunityPrice(price);
//							item.setOppurtunityQty(qty);
//						}
//					}
//				}
//			}
//			//logger.debug("findMarginOppurtunities ends -- Predicted Movement for item : " + item.getItemCode() + "--" + item.getPredictedMovement());
//		}
//		logger.debug("findMarginOppurtunities is Completed");
//	}
	
	
	private void markMarginOppurtunities(PRRecommendationRunHeader recommendationRunHeader, List<PRItemDTO> itemListWithLIG) {
		logger.debug("findMarginOppurtunities is Started");
		int highestMar$UsingCurRetail = ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL.getObjectiveTypeId();
		int highestMar$ = ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR.getObjectiveTypeId();
		HashMap<Integer, Integer> ligWithHighestMarObj = new HashMap<Integer, Integer>();
		PricingEngineService pricingEngineService = new PricingEngineService();
		
		// get retlirid where even its one of the item's objective is either higher margin $ or highest margin $ using current
		// retail
		// These lig items won't be marked with margin oppor
		for (PRItemDTO item : itemListWithLIG) {
			if (item.getObjectiveTypeId() == highestMar$UsingCurRetail || item.getObjectiveTypeId() == highestMar$ && item.getRetLirId() > 0) {
				ligWithHighestMarObj.put(item.getRetLirId(), 0);
			}
		}

		for (PRItemDTO item : itemListWithLIG) {
			if (item.getOppurtunities() != null) {
				double margin = 0;
				double price = 0;
				double oppMovement =0;
				int qty = 0;
				boolean isFirst = true;
				if (item.getOppurtunities() != null) {
					for (PricePointDTO p : item.getOppurtunities()) {
						MultiplePrice multiplePrice = new MultiplePrice(p.getRegQuantity(), p.getRegPrice());
						PricePointDTO predictedPricePoint = null;
						
						//if the item is lig, get lig level margin
						if(item.isLir()) {
							PricePointDTO ligPricePoint = new PricePointDTO();
							ligPricePoint.setPredictedMovement(0d);
							
							List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(item.getItemCode(), itemListWithLIG);
							for (PRItemDTO ligMember : ligMembers) {
								PricePointDTO ligMemberPred = ligMember.getRegPricePredictionMap().get(multiplePrice);
								if (ligMemberPred != null && ligMemberPred.getPredictedMovement() != null
										&& ligMemberPred.getPredictedMovement() > 0) {
									ligPricePoint.setPredictedMovement(ligPricePoint.getPredictedMovement() + ligMemberPred.getPredictedMovement());
								}
							}
							predictedPricePoint = ligPricePoint;
//							logger.debug("LIG margin:" + item.getItemCode() + ",price point:" + "(" + p.getRegQuantity() + "/" + p.getRegPrice() + ")"
//									+ ",pred:" + ligPricePoint.getPredictedMovement());
						} else {
							predictedPricePoint = item.getRegPricePredictionMap().get(multiplePrice);
						}
						
						if (predictedPricePoint != null && predictedPricePoint.getPredictedMovement() != null
								&& predictedPricePoint.getPredictedMovement() > 0) {
							// logger.debug("Opp Item Code: " + item.getItemCode() + ", Opp Price: " + p.getRegPrice());
							//find margin
							Double pricePointMargin = null;
							if(item.getCost() != null){
								Double unitPrice = PRCommonUtil.getUnitPrice(multiplePrice, true);
								pricePointMargin = (unitPrice * predictedPricePoint.getPredictedMovement())
										- (item.getCost() * predictedPricePoint.getPredictedMovement());
							}
							if (pricePointMargin != null) {
								double tMargin = pricePointMargin.doubleValue();
								// logger.debug("Predicted Movement: " + p.getPredictedMovement() + ", Margin: " + tMargin);
								if (isFirst) {
									margin = tMargin;
									price = p.getRegPrice();
									qty = p.getRegQuantity();
									//Added to get opportunity movement if the opportunity price is available
									oppMovement = predictedPricePoint.getPredictedMovement();
									isFirst = false;
								} else {
									if (margin < tMargin) {
										margin = tMargin;
										price = p.getRegPrice();
										qty = p.getRegQuantity();
										oppMovement = predictedPricePoint.getPredictedMovement();
									}
								}
							} else {
								logger.debug("Margin not set for price point");
							}
						}
					}
				}

				if (!item.getIsPartOfSubstituteGroup() && item.getObjectiveTypeId() != highestMar$UsingCurRetail
						&& item.getObjectiveTypeId() != highestMar$
						&& ((item.isLir() && ligWithHighestMarObj.get(item.getItemCode()) == null) || !item.isLir())) {
					if (item.getRecommendedRegPrice() != null && price > 0 && price != item.getRecommendedRegPrice().price) {
						// logger.debug("Price set as Opp: " + price);
						item.setIsOppurtunity(String.valueOf(Constants.YES));
						item.setOppurtunityPrice(price);
						item.setOppurtunityQty(qty); 	
						//Included Opportunity Movement detail 
						item.setOppurtunityMovement(oppMovement);
					}
				}
			}
		}
		logger.debug("findMarginOppurtunities is Completed");
	}
	
	
	private void updateMarginOpportunity(List<PRItemDTO> itemListWithLIG, 
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		//8th Dec 2014 -- If an LIG is not marked as margin opportunity, then don't mark its members also
		//as margin oppor. for e.g. if two lig members are marked with margin opportunity,
		//but at lig level, if it is not marked, then update all the members as not margin oppor
		
		//HashMap<Integer, PRItemDTO> itemMap = new HashMap<Integer, PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemMap = new HashMap<ItemKey, PRItemDTO>();
		
//		for(PRItemDTO item : itemListWithLIG){
//			itemMap.put(item.getItemCode(), item);
//		}		 
		
		
		for(PRItemDTO item : itemListWithLIG){
			ItemKey itemKey = PRCommonUtil.getItemKey(item);
			itemMap.put(itemKey, item);
		}
		
		//for(Map.Entry<Integer, HashMap<Integer, ArrayList<Integer>>> entry : retLirMap.entrySet()){
			// Loop each lig 	
			for(Map.Entry<Integer, List<PRItemDTO>> inEntry : retLirMap.entrySet()){
				ItemKey retLirItemKey = PRCommonUtil.getItemKey(inEntry.getKey(), true);
				if (itemMap.get(retLirItemKey) != null) {
//				if(itemMap.get(inEntry.getKey()) != null){
//					PRItemDTO retLirItemDTO = itemMap.get(inEntry.getKey());		
					PRItemDTO retLirItemDTO = itemMap.get(retLirItemKey);
					//If lig is not marked as margin oppor
					if((retLirItemDTO.getIsOppurtunity() == null) || 
							retLirItemDTO.getIsOppurtunity().equals(String.valueOf(Constants.NO))){
						//Loop each item in the lig
						for(PRItemDTO itemDTO : inEntry.getValue()){
							ItemKey itemKey = PRCommonUtil.getItemKey(itemDTO);
							//int itemCode = itemDTO.getItemCode();
							//if(itemMap.get(itemCode) != null) {
							if (itemMap.get(itemKey) != null) {
								//PRItemDTO ligMember = itemMap.get(itemCode);
								PRItemDTO ligMember = itemMap.get(itemKey);
								//mark as no margin oppor (even if it already marked as margin oppor)
								ligMember.setIsOppurtunity(String.valueOf(Constants.NO));
								ligMember.setOppurtunityPrice(null);
								ligMember.setOppurtunityQty(null);
								ligMember.setOppurtunityMovement(null);
							}
						}
					}
				}
			}
		//}
	}
	
//	private void updateCurRegPricePredictedMov(List<PRItemDTO> itemListWithLIG,
//			HashMap<Integer, List<PRItemDTO>> retLirMap) {
//		//3rd Sep 2015, bug fix: when ret lir id and item code are same, it takes the ret lir id 
//		//predicted movement instead of that item's predicted movement
//		
//		///HashMap<Integer, PRItemDTO> itemMap = new HashMap<Integer, PRItemDTO>();
//		HashMap<ItemKey, PRItemDTO> itemMap = new HashMap<ItemKey, PRItemDTO>();
//		
//		// Convert to hashmap
////		for (PRItemDTO item : itemListWithLIG) {
////			itemMap.put(item.getItemCode(), item);
////		}
//
//		for(PRItemDTO item : itemListWithLIG){
//			ItemKey itemKey = PRCommonUtil.getItemKey(item);
//			itemMap.put(itemKey, item);
//		}
//		
//		//for (Map.Entry<Integer, HashMap<Integer, ArrayList<Integer>>> entry : retLirMap.entrySet()) {
//			// Loop each lig
//			for (Map.Entry<Integer, List<PRItemDTO>> inEntry : retLirMap.entrySet()) {
//				ItemKey retLirItemKey = PRCommonUtil.getItemKey(inEntry.getKey(), true);
//				if (itemMap.get(retLirItemKey) != null) {
//					//PRItemDTO retLirItemDTO = itemMap.get(inEntry.getKey());
//					PRItemDTO retLirItemDTO = itemMap.get(retLirItemKey);
//					retLirItemDTO.setCurRegPricePredictedMovement(0d);
//					// Loop each item in the lig
//					for (PRItemDTO itemDTO : inEntry.getValue()) {
//						ItemKey itemKey = PRCommonUtil.getItemKey(itemDTO);
//						//int itemCode = itemDTO.getItemCode();
//						//if (itemMap.get(itemCode) != null) {
//						if (itemMap.get(itemKey) != null) {
//							//PRItemDTO ligMember = itemMap.get(itemCode);
//							PRItemDTO ligMember = itemMap.get(itemKey);
//							//NU:: Bug Fix: 30th Sep 16, it considers even when the predicted movement is -1,
//							//so the total goes down
//							if (ligMember.getCurRegPricePredictedMovement() != null && ligMember.getCurRegPricePredictedMovement() > 0) {
//							//if (ligMember.getCurRegPricePredictedMovement() != null) {
//								retLirItemDTO.setCurRegPricePredictedMovement(retLirItemDTO
//										.getCurRegPricePredictedMovement()
//										+ ligMember.getCurRegPricePredictedMovement());
//							}
//						}
//					}
//				}
//				else {
//					logger.debug("Ret Lir Id is not present in itemMap: " + inEntry.getKey());
//			}
//			}
//		//}
//
//	}

//	private void updateLigLevelPrediction(List<PRItemDTO> itemListWithLIG, HashMap<Integer, List<PRItemDTO>> retLirMap) {
//		PricingEngineService pricingEngineService = new PricingEngineService();
//		LIGConstraint ligConstraint = new LIGConstraint();
//
//		for (Map.Entry<Integer, List<PRItemDTO>> inEntry : retLirMap.entrySet()) {
//			ItemKey retLirItemKey = PRCommonUtil.getItemKey(inEntry.getKey(), true);
//			List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(retLirItemKey.getItemCodeOrRetLirId(), itemListWithLIG);
//			PRItemDTO ligRepresentingItem = pricingEngineService.getLigRepresentingItem(retLirItemKey.getItemCodeOrRetLirId(), itemListWithLIG);
//			ligRepresentingItem = ligConstraint.sumCurRetailPredictionForLIG(ligRepresentingItem, ligMembers);
//			ligRepresentingItem = ligConstraint.sumPredictionForLIG(ligRepresentingItem, ligMembers);
//			
//			ligRepresentingItem = ligConstraint.sumCurRetailSalesDollarForLIG(ligRepresentingItem, ligMembers);
//			ligRepresentingItem = ligConstraint.sumRecRetailSalesDollarForLIG(ligRepresentingItem, ligMembers);
//			ligRepresentingItem = ligConstraint.sumCurRetailMarginDollarForLIG(ligRepresentingItem, ligMembers);
//			ligRepresentingItem = ligConstraint.sumRecRetailMarginDollarForLIG(ligRepresentingItem, ligMembers);
//		}
//
//	}
	
	public void resetOpporIfNotInHistory(Connection conn, List<Integer> priceZoneStores, List<PRItemDTO> itemListWithOpp,
			RetailCalendarDTO curCalDTO) throws GeneralException {
		// Get all items (exp lig rep) marked with margin oppur
		List<PRItemDTO> opporItems = new ArrayList<PRItemDTO>();
		for (PRItemDTO itemDTO : itemListWithOpp) {
			if (!itemDTO.isLir() && itemDTO.getIsOppurtunity() != null && itemDTO.getIsOppurtunity().equals(String.valueOf(Constants.YES))) {
				opporItems.add(itemDTO);
			}
		}

		//at least one item is marked as oppor
		if (opporItems.size() > 0) {
			Set<Integer> ligRepItem = new HashSet<Integer>();
			PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
			int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_EXISTING_PRICE_POINT_HISTORY"));
			HashMap<ProductKey, List<MultiplePrice>> itemsDistinctRegPrice = pricingEngineDAO.getExistingRegPricePointsForZone(conn, priceZoneStores,
					curCalDTO.getStartDate(), noOfWeeksBehind, opporItems);

			for (PRItemDTO itemDTO : opporItems) {
				boolean isOpporPriceInHistory = false;
				ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode());
				if (itemsDistinctRegPrice.get(productKey) != null) {
					MultiplePrice oppPrice = new MultiplePrice(itemDTO.getOppurtunityQty(), itemDTO.getOppurtunityPrice());
					// If the price not present in history
					List<MultiplePrice> existingPricePoints = itemsDistinctRegPrice.get(productKey);
					for (MultiplePrice existingPrice : existingPricePoints) {
						if (existingPrice.equals(oppPrice)) {
							isOpporPriceInHistory = true;
							break;
						}
					}
				}

				// margin oppor price is not sold before
				if (!isOpporPriceInHistory) {
					logger.debug("Item mar oppor is reset as there is no history, item-code:" + itemDTO.getItemCode() + ",oppor price:"
							+ itemDTO.getOppurtunityPrice() + ",lir-id:" + itemDTO.getRetLirId());
					itemDTO.setIsOppurtunity(String.valueOf(Constants.NO));
					itemDTO.setOppurtunityPrice(null);
					itemDTO.setOppurtunityQty(null);
					itemDTO.setOppurtunityMovement(null);
					if (itemDTO.getRetLirId() > 0) {
						ligRepItem.add(itemDTO.getRetLirId());
					}
				}
			}

			// update lig also
			for (Integer retLirId : ligRepItem) {
				for (PRItemDTO itemDTO : itemListWithOpp) {
					if (itemDTO.getItemCode() == retLirId && itemDTO.isLir()) {
						logger.debug("LIG mar oppor is reset as there is no history, lir-id:" + itemDTO.getItemCode() + ",oppor price:"
								+ itemDTO.getOppurtunityPrice());
						itemDTO.setIsOppurtunity(String.valueOf(Constants.NO));
						itemDTO.setOppurtunityPrice(null);
						itemDTO.setOppurtunityQty(null);
						itemDTO.setOppurtunityMovement(null);
						break;
					}
				}
			}
		}
	}
}
