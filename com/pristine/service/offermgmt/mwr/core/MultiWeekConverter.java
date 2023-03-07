package com.pristine.service.offermgmt.mwr.core;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.Cost;
import com.pristine.dto.Price;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MultiWeekConverter {

	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	private static Logger logger = Logger.getLogger("MultiWeekConverter");
	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param allWeekDetails
	 * @param priceHistory
	 * @param recommendationRuleMap
	 * @param calendarIdToPrices TODO
	 * @return Multi week item data map
	 * @throws GeneralException
	 * @throws Exception
	 */
	public HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> convertToMultiWeekItemDataMap(
			RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<String, RetailCalendarDTO> allWeekDetails,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementDataMap,
			HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> parentRecInfo, 
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, 
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails, 
			HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails,
			HashMap<Integer, List<PRItemDTO>> retLirMap,
			List<RetailCalendarDTO> fullCalendar, 
			boolean usePrediction, 
			Map<Integer, List<Cost>> costByCalendarId, 
			Map<Integer, List<Price>> calendarIdToPrices) throws GeneralException, Exception {

		RecommendationRulesFilter recommendationRulesFilter = new RecommendationRulesFilter();

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap = new HashMap<>();

		long noOfWeeks = ChronoUnit.WEEKS.between(
				LocalDate.parse(recommendationInputDTO.getActualStartWeek(), formatter),
				LocalDate.parse(recommendationInputDTO.getActualEndWeek(), formatter));

		LocalDate recStartWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(), formatter);
		//LocalDate recStartWeekEnd = recStartWeek.plusDays(6);
		LocalDate recBaseWeek = LocalDate.parse(recommendationInputDTO.getBaseWeek(), formatter);

		List<String> weekStartDates = PricingEngineService.getFutureWeeks(recommendationInputDTO.getActualStartWeek(),
				(int) noOfWeeks);

		weekStartDates.add(recommendationInputDTO.getActualStartWeek());

		LocalDate wkStartDate, weekEndDate, futureCostEffDate;
		RetailCalendarDTO retailCalendarDTO;
		int periodCalId;
		for (String weekStartDate : weekStartDates) {
			if (allWeekDetails.containsKey(weekStartDate)) {

				wkStartDate = LocalDate.parse(weekStartDate, formatter);
				
				weekEndDate = wkStartDate.plus(6, ChronoUnit.DAYS);

				retailCalendarDTO = allWeekDetails.get(weekStartDate);

				RecWeekKey recWeekKey = new RecWeekKey(retailCalendarDTO.getCalendarId(), weekStartDate);

				periodCalId = getPeriodCalendarId(fullCalendar, weekStartDate);
				
				setRecWeekType(recWeekKey, wkStartDate, recStartWeek, recBaseWeek);
				
				HashMap<ItemKey, MWRItemDTO> itemDataMapNew = new HashMap<>();
				PRItemDTO prItemDTO;
				MultiplePrice currentPrice;
				
				for (Map.Entry<ItemKey, PRItemDTO> itemEntry : itemDataMap.entrySet()) {
					prItemDTO = itemEntry.getValue();
					MWRItemDTO mwrItemDTO = new MWRItemDTO();
					mwrItemDTO.copyFromWeeklyRecObj(prItemDTO);
					mwrItemDTO.setPeriodCalendarId(periodCalId);
					currentPrice = PRCommonUtil.getMultiplePrice(prItemDTO.getRegMPack(),
							prItemDTO.getRegPrice(), prItemDTO.getRegMPrice());
					
					mwrItemDTO.setCurrentPrice(currentPrice);
					
					if (prItemDTO.getFutureRecRetail() != null 
							&& prItemDTO.getRecPriceEffectiveDate() != null
							&& !Constants.EMPTY.equals(prItemDTO.getRecPriceEffectiveDate())) {
						LocalDate recPriceEffectiveDate = LocalDate.parse(prItemDTO.getRecPriceEffectiveDate(),
								formatter);
						if (recPriceEffectiveDate.isEqual(weekEndDate) || recPriceEffectiveDate.isBefore(weekEndDate)) {
							mwrItemDTO.setFuturePrice(prItemDTO.getFutureRecRetail());
							mwrItemDTO.setFuturePriceEffDate(prItemDTO.getRecPriceEffectiveDate());
						} else {
							mwrItemDTO.setFuturePrice(null);
							mwrItemDTO.setFuturePriceEffDate(null);
							//mwrItemDTO.setPriceChangeImpact(0.0);
						}
					}
					//Added existing property check to the function because future cost functionality/use is applicable for RA only and not FF
					//For FF future cost is for display purpose
					if (prItemDTO.getFutureListCost() != null && chkFutureCost) {
						futureCostEffDate = LocalDate.parse(prItemDTO.getFutureCostEffDate(), formatter);

						if (futureCostEffDate.isEqual(weekEndDate) || futureCostEffDate.isBefore(weekEndDate)) {

							mwrItemDTO.setPrevListCost(prItemDTO.getListCost());
							mwrItemDTO.setListCost(prItemDTO.getFutureListCost());
							mwrItemDTO.setCostEffDate(prItemDTO.getFutureCostEffDate());
							mwrItemDTO.setFutureCost(0.0);
							mwrItemDTO.setFutureCostEffDate(null);
							mwrItemDTO.setFutureCostEffDateForQtrLevel(prItemDTO.getFutureCostEffDate());
							mwrItemDTO.setFutureCostForQtrLevel(prItemDTO.getFutureListCost());
						}
					}
				
					if (wkStartDate.isAfter(recStartWeek) || wkStartDate.isEqual(recStartWeek)) {
						
						recommendationRulesFilter.applyRecommendationRules(prItemDTO, recommendationRuleMap,
								priceHistory, weekStartDate, mwrItemDTO);

						if(!usePrediction) {
							mwrItemDTO.setFinalPricePredictedMovement(prItemDTO.getAvgMovement());
							mwrItemDTO.setFinalPricePredictionStatus(PredictionStatus.SUCCESS.getStatusCode());
							mwrItemDTO.setCurrentRegUnits(prItemDTO.getAvgMovement());
						}
					} else {
						// If recommendation is for on-going quarter, populate actuals as recommended for completed weeks
						// Set recommended price as current retail
						mwrItemDTO.setRecommendedRegPrice(currentPrice);
						mwrItemDTO.setPriceChangeImpact(0.0);
						// Set predicted movement as actual movement
						if(prItemDTO.isLir()) {
							// Initialize 0 movement before aggregating
							mwrItemDTO.setFinalPricePredictedMovement(0D);
							List<PRItemDTO> members = retLirMap.get(prItemDTO.getRetLirId());
							members.forEach(member -> {
								if (movementDataMap.containsKey(member.getItemCode())) {
									HashMap<Integer, ProductMetricsDataDTO> movementData = movementDataMap
											.get(member.getItemCode());
									if (movementData.containsKey(recWeekKey.calendarId)) {
										ProductMetricsDataDTO productMetricsDataDTO = movementData.get(recWeekKey.calendarId);
										mwrItemDTO.setFinalPricePredictedMovement(
												mwrItemDTO.getFinalPricePredictedMovement()
														+ productMetricsDataDTO.getTotalMovement());
									}
								}
							});
						} else {
							if (movementDataMap.containsKey(prItemDTO.getItemCode())) {
								HashMap<Integer, ProductMetricsDataDTO> movementData = movementDataMap
										.get(prItemDTO.getItemCode());
								if (movementData.containsKey(recWeekKey.calendarId)) {
									ProductMetricsDataDTO productMetricsDataDTO = movementData.get(recWeekKey.calendarId);
									mwrItemDTO.setFinalPricePredictedMovement(productMetricsDataDTO.getTotalMovement());
								}//Changes done by Bhargavi on 3/12/2020
								//Recommendation Forecast 
								else if(!movementDataMap.containsKey(recWeekKey.calendarId)){
									mwrItemDTO.setFinalPricePredictedMovement(0D);
									if (parentRecInfo.containsKey(recWeekKey.calendarId)) {
										HashMap<ItemKey, MWRItemDTO> parentData = parentRecInfo.get(recWeekKey.calendarId);
										if (parentData.containsKey(itemEntry.getKey())) {
											MWRItemDTO lastPrediction = parentData.get(itemEntry.getKey());
											mwrItemDTO.setFinalPriceMargin(lastPrediction.getFinalPriceMargin());
											mwrItemDTO.setFinalPriceRevenue(lastPrediction.getFinalPriceRevenue());
											if(lastPrediction.getRegPricePredictionMap().containsKey(currentPrice)) {
												PricePointDTO curr = lastPrediction.getRegPricePredictionMap().get(currentPrice);
												mwrItemDTO.setFinalPricePredictedMovement(curr.getPredictedMovement());
											}
											else {
												PricePointDTO pred = lastPrediction.getRegPricePredictionMap().values().stream().findFirst().orElse(null);
												if(pred != null) {
													mwrItemDTO.setFinalPricePredictedMovement(pred.getPredictedMovement());
												}
											}
										}
									}	
								}
							}
						}
						 /*else if (parentRecInfo.containsKey(recWeekKey.calendarId)) {
							HashMap<ItemKey, MWRItemDTO> itemMap = parentRecInfo.get(recWeekKey.calendarId);
							if (itemMap.containsKey(itemEntry.getKey())) {
								MWRItemDTO lastPrediction = itemMap.get(itemEntry.getKey());
								mwrItemDTO.setFinalPricePredictedMovement(
										lastPrediction.getFinalPricePredictedMovement());
							}
						}*/
					}

					// Set promotions by checking the start date
					// setPromotions(prItemDTO.getCurSaleInfo(), mwrItemDTO, weekStartDate, formatter);
					setPromotions(prItemDTO.getRecWeekSaleInfo(), mwrItemDTO, weekStartDate, formatter);
					// setPromotions(prItemDTO.getFutWeekSaleInfo(), mwrItemDTO, weekStartDate, formatter);

					// Set Ad information
					setAdInfo(prItemDTO.getRecWeekAdInfo(), mwrItemDTO, weekStartDate, formatter);
					// setAdInfo(prItemDTO.getFutWeekAdInfo(), mwrItemDTO, weekStartDate, formatter);

					// Set Display information
					setDisplayInfo(prItemDTO.getRecWeekDisplayInfo(), mwrItemDTO, weekStartDate, formatter);
					// setDisplayInfo(prItemDTO.getFutWeekDisplayInfo(), mwrItemDTO, weekStartDate, formatter);

					if (!mwrItemDTO.isLir()) {

						if (saleDetails.get(mwrItemDTO.getItemCode()) != null) {
							List<PRItemSaleInfoDTO> saleEntries = saleDetails.get(mwrItemDTO.getItemCode());
							for (PRItemSaleInfoDTO prItemSaleInfoDTO : saleEntries) {
								if(prItemSaleInfoDTO != null && prItemSaleInfoDTO.getSalePrice() != null) {
									setPromotions(prItemSaleInfoDTO, mwrItemDTO, weekStartDate, formatter);									
								}
							}
						}

						if (adDetails.get(mwrItemDTO.getItemCode()) != null) {
							List<PRItemAdInfoDTO> adEntries = adDetails.get(mwrItemDTO.getItemCode());
							for (PRItemAdInfoDTO prItemAdInfoDTO : adEntries) {
								setAdInfo(prItemAdInfoDTO, mwrItemDTO, weekStartDate, formatter);
							}
						}

						if (displayDetails.get(mwrItemDTO.getItemCode()) != null) {
							List<PRItemDisplayInfoDTO> displayEntries = displayDetails.get(mwrItemDTO.getItemCode());
							for (PRItemDisplayInfoDTO prItemDisplayInfoDTO : displayEntries) {
								setDisplayInfo(prItemDisplayInfoDTO, mwrItemDTO, weekStartDate, formatter);
							}
						}
					}

					itemDataMapNew.put(itemEntry.getKey(), mwrItemDTO);
				}
				weeklyItemDataMap.put(recWeekKey, itemDataMapNew);
			}
		}

		updateLIGLevelPromo(weeklyItemDataMap, retLirMap);

		return weeklyItemDataMap;
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 */
	private void updateLIGLevelPromo(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weekEntry : weeklyItemDataMap.entrySet()) {
			HashMap<ItemKey, MWRItemDTO> itemMap = weekEntry.getValue();
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : itemMap.entrySet()) {
				MWRItemDTO ligItem = itemEntry.getValue();
				if (ligItem.isLir()) {
					List<MWRItemDTO> ligMembers = getLigMembers(ligItem, itemMap, retLirMap);
					MWRItemDTO saleItem = getSaleItemWithinMember(ligMembers);
					if(saleItem != null) {
						applySaleDetailToAllMembers(ligMembers, ligItem, saleItem);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param ligMembers
	 * @param ligItem
	 * @param saleItem
	 */
	private void applySaleDetailToAllMembers(List<MWRItemDTO> ligMembers, MWRItemDTO ligItem, MWRItemDTO saleItem) {
		for(MWRItemDTO mwrItemDTO: ligMembers) {
			copyPromoDetails(saleItem, mwrItemDTO);
		}
		copyPromoDetails(saleItem, ligItem);
	}
	
	/**
	 * 
	 * @param source
	 * @param dest
	 */
	private void copyPromoDetails(MWRItemDTO source, MWRItemDTO dest) {
		dest.setSalePrice(source.getSalePrice());
		dest.setSaleMultiple(source.getSaleMultiple());
		dest.setSaleStartDate(source.getSaleStartDate());
		dest.setPromoTypeId(source.getPromoTypeId());
		dest.setSaleEndDate(source.getSaleEndDate());
		dest.setAdPageNo(source.getAdPageNo());
		dest.setAdBlockNo(source.getAdBlockNo());
		dest.setDisplayTypeId(source.getDisplayTypeId());
	}
	
	/**
	 * 
	 * @param ligItem
	 * @param itemMap
	 * @param retLirMap
	 * @return ligMembers
	 */
	private List<MWRItemDTO> getLigMembers(MWRItemDTO ligItem, HashMap<ItemKey, MWRItemDTO> itemMap, HashMap<Integer, List<PRItemDTO>> retLirMap){
		List<MWRItemDTO> members = new ArrayList<>();
		if(retLirMap.get(ligItem.getRetLirId()) != null) {
			List<PRItemDTO> ligMembers = retLirMap.get(ligItem.getRetLirId());
			for(PRItemDTO itemDTO: ligMembers) {
				ItemKey itemKey = PRCommonUtil.getItemKey(itemDTO);
				if(itemMap.get(itemKey) != null) {
					MWRItemDTO mwrItemDTO = itemMap.get(itemKey); 
					members.add(mwrItemDTO);
				}
			}
		}
		return members;
	}
	
	/**
	 * 
	 * @param ligMembers
	 * @return sale item
	 */
	private MWRItemDTO getSaleItemWithinMember(List<MWRItemDTO> ligMembers) {
		MWRItemDTO saleItem = null;
		for(MWRItemDTO mwrItemDTO: ligMembers) {
			if(mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {
				saleItem = mwrItemDTO;
				break;
			}
		}
		return saleItem;
	}
	
	/**
	 * 
	 * @param recWeekKey
	 * @param wkStartDate
	 * @param recStartWeek
	 * @param recBaseWeek
	 */
	private void setRecWeekType(RecWeekKey recWeekKey, LocalDate wkStartDate, LocalDate recStartWeek,
			LocalDate recBaseWeek) {

		if (wkStartDate.isBefore(recStartWeek)
				&& (wkStartDate.isAfter(recBaseWeek) || wkStartDate.isEqual(recBaseWeek))) {
			
			recWeekKey.recWeekType = PRConstants.WEEK_TYPE_IN_BETWEEN;
			
		} else if (wkStartDate.isBefore(recStartWeek)) {

			recWeekKey.recWeekType = PRConstants.WEEK_TYPE_COMPLETED;
		} else {
			recWeekKey.recWeekType = PRConstants.WEEK_TYPE_FUTURE;
		}
	}

	/**
	 * 
	 * @param saleDTO
	 * @param mwrItemDTO
	 * @param weekStartDate
	 * @param formatter
	 */
	private void setPromotions(PRItemSaleInfoDTO saleDTO, MWRItemDTO mwrItemDTO, String weekStartDate,
			DateTimeFormatter formatter) throws GeneralException {
		if (saleDTO.getSaleWeekStartDate() != null && !Constants.EMPTY.equals(saleDTO.getSaleWeekStartDate())) {
			LocalDate startDate = LocalDate.parse(weekStartDate, formatter);
			LocalDate weekEndDate  = startDate.plus(6, ChronoUnit.DAYS);
			LocalDate saleStartDate = LocalDate.parse(saleDTO.getSaleStartDate(), formatter);
			LocalDate saleEndDate = LocalDate.parse(saleDTO.getSaleEndDate(), formatter);
			if ((saleStartDate.isBefore(weekEndDate) || saleStartDate.isEqual(weekEndDate)) 
					&& (saleEndDate.isAfter(startDate) || saleEndDate.isEqual(startDate))
					&& saleDTO.getSalePrice() != null) {
				mwrItemDTO.setSalePrice(saleDTO.getSalePrice().price);
				mwrItemDTO.setSaleMultiple(saleDTO.getSalePrice().multiple);
				//mwrItemDTO.setSaleStartDate(weekStartDate);
				mwrItemDTO.setSaleStartDate(saleDTO.getSaleStartDate());
				mwrItemDTO.setPromoTypeId(saleDTO.getPromoTypeId());
				//mwrItemDTO.setSaleEndDate(DateUtil.getWeekEndDate(DateUtil.toDate(weekStartDate)));
				mwrItemDTO.setSaleEndDate(saleDTO.getSaleEndDate());
			}
		}
	}


	/**
	 * 
	 * @param adDTO
	 * @param mwrItemDTO
	 * @param weekStartDate
	 * @param formatter
	 */
	private void setAdInfo(PRItemAdInfoDTO adDTO, MWRItemDTO mwrItemDTO, String weekStartDate,
			DateTimeFormatter formatter) {
		if(adDTO.getWeeklyAdStartDate() != null && !Constants.EMPTY.equals(adDTO.getWeeklyAdStartDate())){
			LocalDate startDate = LocalDate.parse(weekStartDate, formatter);
			LocalDate saleStartDate = LocalDate.parse(adDTO.getWeeklyAdStartDate(), formatter);
			if (startDate.isEqual(saleStartDate)) {
				mwrItemDTO.setAdPageNo(adDTO.getAdPageNo());
				mwrItemDTO.setAdBlockNo(adDTO.getAdBlockNo());
			}	
		}
	}

	/**
	 * 
	 * @param displayDTO
	 * @param mwrItemDTO
	 * @param weekStartDate
	 * @param formatter
	 */
	private void setDisplayInfo(PRItemDisplayInfoDTO displayDTO, MWRItemDTO mwrItemDTO, String weekStartDate,
			DateTimeFormatter formatter) {
		if (displayDTO.getDisplayWeekStartDate() != null
				&& !Constants.EMPTY.equals(displayDTO.getDisplayWeekStartDate())) {
			LocalDate startDate = LocalDate.parse(weekStartDate, formatter);
			LocalDate saleStartDate = LocalDate.parse(displayDTO.getDisplayWeekStartDate(), formatter);
			if (startDate.isEqual(saleStartDate)) {
				mwrItemDTO.setDisplayTypeId(displayDTO.getDisplayTypeLookup().getDisplayTypeId());
			}
		}
	}
	

	
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

		weeklyItemDataMap.forEach((recWeekKey, weeklyMap) -> {

			LocalDate weekStartDate = LocalDate.parse(recWeekKey.weekStartDate, formatter);
			// update recommendation retail only for remaining weeks
			if (weekStartDate.isAfter(recStartWeek) || weekStartDate.isEqual(recStartWeek)) {

				weeklyMap.forEach((itemKey, mwrItemDTO) -> {

					PRItemDTO itemDTO = itemDataMap.get(itemKey);
					try {

						boolean isNewPriceRequiredForWeek = isNewPriceRequiredForWeek(itemDTO, recommendationRuleMap,
								priceHistory, recWeekKey.weekStartDate);

						// Copy recommended price
						if (isNewPriceRequiredForWeek) {

							mwrItemDTO.setRecommendedRegPrice(itemDTO.getRecommendedRegPrice());

						} else {

							// Set recommended price as current retail
							MultiplePrice recommendedRegPrice = new MultiplePrice(itemDTO.getRegMPack(),
									itemDTO.getRegMPrice());
							mwrItemDTO.setRecommendedRegPrice(recommendedRegPrice);
						}

						// Set predicted movement for recommended price point
						if (mwrItemDTO.getSalePrice() > 0) {
							MultiplePrice multipleSalePrice = new MultiplePrice(mwrItemDTO.getSaleMultiple(),
									mwrItemDTO.getSalePrice());
							PricePointDTO pricePointDTO = mwrItemDTO.getSalePricePredictionMap().get(multipleSalePrice);

							mwrItemDTO.setFinalPricePredictedMovement(pricePointDTO.getPredictedMovement());
							mwrItemDTO.setFinalPriceMargin(PRCommonUtil.getMarginDollar(multipleSalePrice,
									mwrItemDTO.getFinalCost(), pricePointDTO.getPredictedMovement()));
							mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(multipleSalePrice,
									pricePointDTO.getPredictedMovement()));
						} else {
							PricePointDTO pricePointDTO = mwrItemDTO.getRegPricePredictionMap()
									.get(mwrItemDTO.getRecommendedRegPrice());

							mwrItemDTO.setFinalPricePredictedMovement(pricePointDTO.getPredictedMovement());
							mwrItemDTO.setFinalPriceMargin(
									PRCommonUtil.getMarginDollar(mwrItemDTO.getRecommendedRegPrice(),
											mwrItemDTO.getCost(), pricePointDTO.getPredictedMovement()));
							mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(
									mwrItemDTO.getRecommendedRegPrice(), pricePointDTO.getPredictedMovement()));

						}

					} catch (Exception | GeneralException e) {
						logger.error("Exception in setting recommended price for week - " + recWeekKey.weekStartDate,
								e);
						return;
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
	private boolean isNewPriceRequiredForWeek(PRItemDTO itemDTO,
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
				isRecommendationRequired = true;
			} else {
				isRecommendationRequired = false;
			}
		}

		return isRecommendationRequired;
	}
	
	/**
	 * 
	 * @param cal
	 * @param startDate
	 * @return period cal id
	 */
	private int getPeriodCalendarId(List<RetailCalendarDTO> cal, String startDate) {
		int calId = 0;
		
		LocalDate date = LocalDate.parse(startDate, PRCommonUtil.getDateFormatter());
		for(RetailCalendarDTO calendar: cal) {
			if(calendar.getRowType().equals(Constants.CALENDAR_PERIOD)) {
				if ((calendar.getStartDateAsDate().isEqual(date) || calendar.getStartDateAsDate().isBefore(date))
						&& (calendar.getEndDateAsDate().isEqual(date) || calendar.getEndDateAsDate().isAfter(date))) {
					calId = calendar.getCalendarId();
					break;
				}
			}
		}
		
		return calId;
	}
	
}
