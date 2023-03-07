package com.pristine.service.offermgmt.mwr.core.finalizeprice;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MultiWeekPriceFinalizer {
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	private static Logger logger = Logger.getLogger("MultiWeekPriceFinalizer");
	
	/**
	 * Updates recommended retail, prediction etc to all weeks
	 * @param itemDataMap
	 * @param weeklyItemDataMap
	 */
	public void applyRecommendationToAllWeeks(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {

		LocalDate recStartWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(), formatter);
		LocalDate quarterEndDate = recommendationInputDTO.getQuarterEndDate() != null
				? LocalDate.parse(recommendationInputDTO.getQuarterEndDate(), formatter)
				: LocalDate.parse(recommendationInputDTO.getEndWeek(), formatter);
		weeklyItemDataMap.forEach((recWeekKey, weeklyMap) -> {

			LocalDate weekStartDate = LocalDate.parse(recWeekKey.weekStartDate, formatter);
			LocalDate weekEndDate = weekStartDate.plus(6, ChronoUnit.DAYS);
			// update recommendation retail only for remaining weeks
			if (weekStartDate.isAfter(recStartWeek) || weekStartDate.isEqual(recStartWeek)) {
				
				// Map added for lig members.
				// For LIR row its will check this map if any member which is not representative
				// has recommendation
				// then mark LIG as recommended
				HashMap<Integer, List<PRItemDTO>> ligItemMap = new HashMap<>();
				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : weeklyMap.entrySet()) {

					if (!itemEntry.getValue().isLir() && itemEntry.getValue().getRetLirId() > 0) {
						PRItemDTO itemDTO = itemDataMap.get(itemEntry.getKey());
						List<PRItemDTO> temp = new ArrayList<>();
						if (ligItemMap.containsKey(itemDTO.getRetLirId())) {
							temp = ligItemMap.get(itemDTO.getRetLirId());
						}
						temp.add(itemDTO);
						ligItemMap.put(itemDTO.getRetLirId(), temp);
					}
				}
				
				
				weeklyMap.forEach((itemKey, mwrItemDTO) -> {

					PRItemDTO itemDTO = itemDataMap.get(itemKey);
					// condition added to not use prediction values when override is removed
					if (itemDTO.getOverrideRemoved() == 0) {
					try {
						MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(),
								itemDTO.getRegPrice(), itemDTO.getRegMPrice());

						mwrItemDTO.setxWeeksMov(itemDTO.getXweekMov());
						mwrItemDTO.setOverrideRemoved(itemDTO.getOverrideRemoved());

						// Check if the recommendation is post dated. Otherwise set recommendation
						if (itemDTO.getRecPriceEffectiveDate() != null
								&& !Constants.EMPTY.equals(itemDTO.getRecPriceEffectiveDate())) {

							LocalDate recEffDate = LocalDate.parse(itemDTO.getRecPriceEffectiveDate(), formatter);
							if (recEffDate.isEqual(weekEndDate) || recEffDate.isBefore(weekEndDate)) {
								// added on 09/05/22 to resolve issue with LIG , rep item was not marked as
								// recommeded if item which is not selected as rep had a new recommendation
								if (itemDTO.isLir()) {
									if (ligItemMap.get(itemDTO.getRetLirId()) != null) {
										List<PRItemDTO> ligMemebers = ligItemMap.get(itemDTO.getRetLirId());
										setRecommendationForLigRep(mwrItemDTO, itemDTO, ligMemebers);
									}
								}
								else
									setRecommendation(mwrItemDTO, itemDTO, curRegPrice,recommendationInputDTO.isGlobalZone());
							} else if (recEffDate.isAfter(quarterEndDate) ||recEffDate.isEqual(quarterEndDate) ) {
								mwrItemDTO.setRecommendedRegPrice(curRegPrice);
								mwrItemDTO.setPostdatedPrice(itemDTO.getRecommendedRegPrice());
								mwrItemDTO.setPostDatedPriceEffDate(itemDTO.getRecPriceEffectiveDate());
							} else {
								mwrItemDTO.setRecommendedRegPrice(curRegPrice);
							}

						} else {
							if (itemDTO.isLir()) {
								if (ligItemMap.get(itemDTO.getRetLirId()) != null) {
									List<PRItemDTO> ligMemebers = ligItemMap.get(itemDTO.getRetLirId());
									setRecommendationForLigRep(mwrItemDTO, itemDTO, ligMemebers);
								}
							} else
								setRecommendation(mwrItemDTO, itemDTO, curRegPrice,recommendationInputDTO.isGlobalZone());
						}

						mwrItemDTO.setRecError(itemDTO.getIsRecError());
						mwrItemDTO.setRecErrorCodes(itemDTO.getRecErrorCodes());
						mwrItemDTO.setExplainLog(itemDTO.getExplainLog());

						// Set predicted movement for recommended price point
						if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {
							MultiplePrice multipleSalePrice = new MultiplePrice(mwrItemDTO.getSaleMultiple(),
									mwrItemDTO.getSalePrice());
							PricePointDTO pricePointDTO = mwrItemDTO.getSalePricePredictionMap().get(multipleSalePrice);

							if (pricePointDTO != null) {
								// set the predicted movement only if there is no error from Prediction else set
								// it as 0
								// LIG aggregation uses -1 which results in incorrect summary
								if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
										.getStatusCode()) {
									mwrItemDTO.setFinalPricePredictedMovement(pricePointDTO.getPredictedMovement());

								} else {
									mwrItemDTO.setFinalPricePredictedMovement(0D);
								}
								mwrItemDTO.setFinalPricePredictionStatus(
										pricePointDTO.getPredictionStatus().getStatusCode());
								mwrItemDTO.setFinalPriceMargin(PRCommonUtil.getMarginDollar(multipleSalePrice,
										mwrItemDTO.getFinalCost(), pricePointDTO.getPredictedMovement()));
								mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(multipleSalePrice,
										pricePointDTO.getPredictedMovement()));
							
							}

						} else {
							PricePointDTO pricePointDTO = mwrItemDTO.getRegPricePredictionMap()
									.get(mwrItemDTO.getRecommendedRegPrice());

							if (pricePointDTO != null) {
								//set the predicted movement only if there is no error from Prediction else set it as 0
								//LIG aggregation  uses  -1 which results in incorrect summary
								if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
										.getStatusCode()) {
									mwrItemDTO.setFinalPricePredictedMovement(pricePointDTO.getPredictedMovement());

								} else {
									mwrItemDTO.setFinalPricePredictedMovement(0D);
								}
								mwrItemDTO.setFinalPricePredictionStatus(
										pricePointDTO.getPredictionStatus().getStatusCode());
								mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(
										mwrItemDTO.getRecommendedRegPrice(), pricePointDTO.getPredictedMovement()));
								mwrItemDTO.setFinalPriceMargin(
										PRCommonUtil.getMarginDollar(mwrItemDTO.getRecommendedRegPrice(),
												mwrItemDTO.getCost(), pricePointDTO.getPredictedMovement()));
							} else {
//								logger.debug("Week: " + recWeekKey.weekStartDate + ", ItemCode: "
//										+ mwrItemDTO.getItemCode());
							}

							if(mwrItemDTO.getOverrideRegPrice() != null) {
								PricePointDTO overPricePointDTO = mwrItemDTO.getRegPricePredictionMap()
										.get(mwrItemDTO.getOverrideRegPrice());
								if (overPricePointDTO != null) {
									//set the predicted movement only if there is no error from Prediction else set it as 0
									// LIG aggregation uses -1 which results in incorrect summary
									if (overPricePointDTO.getPredictionStatus()
											.getStatusCode() == PredictionStatus.SUCCESS.getStatusCode()) {
										mwrItemDTO.setFinalPricePredictedMovement(
												overPricePointDTO.getPredictedMovement());

									} else {
										mwrItemDTO.setFinalPricePredictedMovement(0D);
									}
									
									mwrItemDTO.setFinalPricePredictionStatus(
											overPricePointDTO.getPredictionStatus().getStatusCode());
									mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(
											mwrItemDTO.getOverrideRegPrice(), overPricePointDTO.getPredictedMovement()));
									mwrItemDTO.setFinalPriceMargin(
											PRCommonUtil.getMarginDollar(mwrItemDTO.getOverrideRegPrice(),
													mwrItemDTO.getCost(), overPricePointDTO.getPredictedMovement()));
								} else {
//									logger.debug("Week: " + recWeekKey.weekStartDate + ", ItemCode: "
//											+ mwrItemDTO.getItemCode() + ", No Prediction for over retail: "
//											+ mwrItemDTO.getOverrideRegPrice().toString());
								}
							}
							
							
							PricePointDTO currentPricePointDTO = mwrItemDTO.getRegPricePredictionMap().get(curRegPrice);

							if (currentPricePointDTO != null) {
								double predictedMovForCurrRetail = currentPricePointDTO.getPredictedMovement() == null
										? 0
										: currentPricePointDTO.getPredictedMovement();
								//Added by Karishma
								//Changed the condition to !=0 because Autozone can have -ve movement 
								if (predictedMovForCurrRetail != 0) {
									if (currentPricePointDTO.getPredictionStatus()
											.getStatusCode() == PredictionStatus.SUCCESS.getStatusCode())
										mwrItemDTO.setCurrentRegUnits(predictedMovForCurrRetail);
									else {
										mwrItemDTO.setCurrentRegUnits(0D);
										predictedMovForCurrRetail = 0D;
									}

									mwrItemDTO.setFinalPricePredictionStatus(
											currentPricePointDTO.getPredictionStatus().getStatusCode());
									mwrItemDTO.setCurrentRegRevenue(
											PRCommonUtil.getSalesDollar(curRegPrice, predictedMovForCurrRetail));
									if (mwrItemDTO.getCost() != null && mwrItemDTO.getCost() > 0) {
										mwrItemDTO.setCurrentRegMargin(PRCommonUtil.getMarginDollar(curRegPrice,
												mwrItemDTO.getCost(), predictedMovForCurrRetail));
									}
								} else {
//									logger.debug("Week: " + recWeekKey.weekStartDate + ", ItemCode: "
//											+ mwrItemDTO.getItemCode() + ", No Prediction for current retail");
								}
							}
							
							if(!recommendationInputDTO.isUsePrediction()) {
								mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(
										mwrItemDTO.getRecommendedRegPrice(), mwrItemDTO.getFinalPricePredictedMovement()));
								mwrItemDTO.setFinalPriceMargin(
										PRCommonUtil.getMarginDollar(mwrItemDTO.getRecommendedRegPrice(),
												mwrItemDTO.getCost(), mwrItemDTO.getFinalPricePredictedMovement()));
								mwrItemDTO.setCurrentRegUnits(mwrItemDTO.getFinalPricePredictedMovement());
								mwrItemDTO.setFinalPricePredictionStatus(PredictionStatus.SUCCESS.getStatusCode());
								mwrItemDTO.setCurrentRegRevenue(
										PRCommonUtil.getSalesDollar(curRegPrice, mwrItemDTO.getFinalPricePredictedMovement()));
								if (mwrItemDTO.getCost() != null && mwrItemDTO.getCost() > 0) {
									mwrItemDTO.setCurrentRegMargin(PRCommonUtil.getMarginDollar(curRegPrice,
											mwrItemDTO.getCost(), mwrItemDTO.getFinalPricePredictedMovement()));
								}
							} else if (recommendationInputDTO.getRecType().equals(PRConstants.MW_WEEK_RECOMMENDATION)) {
								mwrItemDTO.setFinalPricePredictedMovement(itemDTO.getPredictedMovement());
								mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(
										mwrItemDTO.getRecommendedRegPrice(), itemDTO.getPredictedMovement()));
								mwrItemDTO.setFinalPriceMargin(
										PRCommonUtil.getMarginDollar(mwrItemDTO.getRecommendedRegPrice(),
												mwrItemDTO.getCost(), itemDTO.getPredictedMovement()));
								mwrItemDTO.setCurrentRegUnits(itemDTO.getCurRegPricePredictedMovement());
								mwrItemDTO.setFinalPricePredictionStatus(itemDTO.getPredictionStatus());
								mwrItemDTO.setCurrentRegRevenue(
										PRCommonUtil.getSalesDollar(curRegPrice, itemDTO.getCurRegPricePredictedMovement()));
								if (mwrItemDTO.getCost() != null && mwrItemDTO.getCost() > 0) {
									mwrItemDTO.setCurrentRegMargin(PRCommonUtil.getMarginDollar(curRegPrice,
											mwrItemDTO.getCost(), itemDTO.getCurRegPricePredictedMovement()));
								}
							}
						}
						
						if (itemDTO.getPredictionStatus() != null
								&& itemDTO.getPredictionStatus() != PredictionStatus.SUCCESS.getStatusCode()) {
							mwrItemDTO.setFinalPricePredictedMovement(0D);
							mwrItemDTO.setFinalPricePredictionStatus(itemDTO.getPredictionStatus());
							mwrItemDTO.setFinalPriceRevenue(0D);
							mwrItemDTO.setFinalPriceMargin(0D);
						}
						
					} catch (Exception e) {
						logger.error("Exception in setting recommended price for week - " + recWeekKey.weekStartDate,
								e);
						return;
						}
					}
				});
			} else {
				weeklyMap.forEach((itemKey, mwrItemDTO) -> {
					PRItemDTO itemDTO = itemDataMap.get(itemKey);
					MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(),
							itemDTO.getRegPrice(), itemDTO.getRegMPrice());
					
					mwrItemDTO.setRecommendedRegPrice(curRegPrice);
					if (mwrItemDTO.getFinalPricePredictedMovement() != null
							&& mwrItemDTO.getFinalPricePredictedMovement() > 0) {
						mwrItemDTO.setFinalPricePredictionStatus(PredictionStatus.SUCCESS.getStatusCode());
						mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(mwrItemDTO.getRecommendedRegPrice(),
								mwrItemDTO.getFinalPricePredictedMovement()));
						
						if (mwrItemDTO.getCost() != null && mwrItemDTO.getCost() > 0) {
							mwrItemDTO.setFinalPriceMargin(
									PRCommonUtil.getMarginDollar(mwrItemDTO.getRecommendedRegPrice(),
											mwrItemDTO.getCost(), mwrItemDTO.getFinalPricePredictedMovement()));
						}
						
						mwrItemDTO.setCurrentRegUnits(mwrItemDTO.getFinalPricePredictedMovement());
						mwrItemDTO.setCurrentRegRevenue(mwrItemDTO.getFinalPriceRevenue() == null ? 0 : mwrItemDTO.getFinalPriceRevenue());
						mwrItemDTO.setCurrentRegMargin(mwrItemDTO.getFinalPriceMargin() == null ? 0 : mwrItemDTO.getFinalPriceMargin());
					} else {
						mwrItemDTO.setFinalPricePredictedMovement(0D);
						mwrItemDTO.setFinalPriceRevenue(0D);
						mwrItemDTO.setFinalPriceMargin(0D);
						
						mwrItemDTO.setCurrentRegUnits(0D);
						mwrItemDTO.setCurrentRegRevenue(0D);
						mwrItemDTO.setCurrentRegMargin(0D);
						/*PricePointDTO currentPricePointDTO = mwrItemDTO.getRegPricePredictionMap().get(curRegPrice);

						if (currentPricePointDTO != null) {
							double predictedMovForCurrRetail = currentPricePointDTO.getPredictedMovement() == null ? 0
									: currentPricePointDTO.getPredictedMovement();
							if (predictedMovForCurrRetail > 0) {
								mwrItemDTO.setFinalPricePredictedMovement(predictedMovForCurrRetail);
								mwrItemDTO.setFinalPricePredictionStatus(
										currentPricePointDTO.getPredictionStatus().getStatusCode());
								mwrItemDTO.setFinalPriceRevenue(
										PRCommonUtil.getSalesDollar(curRegPrice, predictedMovForCurrRetail));
								if (mwrItemDTO.getCost() != null && mwrItemDTO.getCost() > 0) {
									mwrItemDTO.setFinalPriceMargin(PRCommonUtil.getMarginDollar(curRegPrice,
											mwrItemDTO.getCost(), predictedMovForCurrRetail));
								}
								
								mwrItemDTO.setCurrentRegUnits(mwrItemDTO.getFinalPricePredictedMovement());
								mwrItemDTO.setCurrentRegRevenue(mwrItemDTO.getFinalPriceRevenue());
								mwrItemDTO.setCurrentRegMargin(mwrItemDTO.getFinalPriceMargin() == null ? 0 : mwrItemDTO.getFinalPriceMargin());
							} else {
								logger.debug(
										"Week: " + recWeekKey.weekStartDate + ", ItemCode: " + mwrItemDTO.getItemCode()
												+ ", No Prediction for current retail: " + curRegPrice.toString());
							}
						}*/
					}
					//for completed set this flag as 0
					if (mwrItemDTO.getPendingRetail() != null) {
						mwrItemDTO.setIsPendingRetailRecommended(0);
					}
					
				});
			}
		});

	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @param priceHistory
	 * @param inpBaseWeekStartDate
	 * @return recommend new price or not
	 * @throws Exception
	 * @throws GeneralException
	 */
/*	private boolean isNewPriceRequiredForWeek(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory, String inpBaseWeekStartDate)
					throws Exception, GeneralException {
		boolean isRecommendationRequired = false;
		RecommendationRulesFilter recommendationRulesFilter = new RecommendationRulesFilter();
		// Check if new price is required when there is a cost or comp change or brand or size violation
		boolean isNewPriceRequired = recommendationRulesFilter.isNewPriceToBeRecommended(itemDTO);

		if (isNewPriceRequired) {
			isRecommendationRequired = true;
		} else {

			// 1. retain current price if there is a price change in last X weeks
			boolean isCurrPriceRetainRequired = recommendationRulesFilter.isCurrPriceRetainRequired(itemDTO,
					recommendationRuleMap, priceHistory, inpBaseWeekStartDate);

			if (isCurrPriceRetainRequired) {
				
				logger.debug("Current Price Retain: " + isCurrPriceRetainRequired + " Item code: " + itemDTO.getItemCode());
				
				isRecommendationRequired = false;
			} else {
				isRecommendationRequired = true;
			}
		}

		return isRecommendationRequired;
	}*/

	/**
	 * 
	 * @param mwrItemDTO
	 * @param itemDTO
	 * @param curRegPrice
	 * @param isglobalZone 
	 */
	private void setRecommendation(MWRItemDTO mwrItemDTO, PRItemDTO itemDTO, MultiplePrice curRegPrice,
			boolean isglobalZone) {
		mwrItemDTO.setRecommendedRegPrice(itemDTO.getRecommendedRegPrice());
		if (curRegPrice != null && itemDTO.getRecommendedRegPrice() != null) {
			// Added condition for AZ to check if the pending retail is null as items having
			// pending retail should not be marked as recommended
			if (itemDTO.getPendingRetail() == null && !curRegPrice.equals(itemDTO.getRecommendedRegPrice())) {
				mwrItemDTO.setNewPriceRecommended(true);
			}
			// Added a new flag to mark the new impact calculated true for global zone items
			// if it has no pending retail and there is impact calculated
			// this will be used to mark is recommended flag in the weekly table

			if (isglobalZone && itemDTO.getPendingRetail() == null && itemDTO.getPriceChangeImpact() != 0) {
				mwrItemDTO.setIsnewImpactCalculated(true);
			}
		}
	}
	
	// mark LIG as recommended if any one member is recommended and is not having
	// pending retail
	private void setRecommendationForLigRep(MWRItemDTO mwrItemDTO, PRItemDTO itemDTO, List<PRItemDTO> ligMemebers) {
		boolean isPriceRecommended = false;
		for (PRItemDTO member : ligMemebers) {
			MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(member.getRegMPack(), member.getRegPrice(),
					member.getRegMPrice());
			if (curRegPrice != null && curRegPrice.getUnitPrice() > 0 && member.getRecommendedRegPrice() != null
					&& member.getRecommendedRegPrice().getUnitPrice() > 0 && member.getPendingRetail() == null
					&& curRegPrice.getUnitPrice() != member.getRecommendedRegPrice().getUnitPrice()) {
				isPriceRecommended = true;
				break;
			}
		}
		if (isPriceRecommended) {
			mwrItemDTO.setNewPriceRecommended(true);
		}
	}

}
