package com.pristine.service.offermgmt.data;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dataload.service.DataInputForAPI;
import com.pristine.dto.Promotion;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RestAPIConnectionService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.PriceRollbackService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PromotionDataSetupCommonAPI {

	private static Logger logger = Logger.getLogger("PromotionDataSetupCommonAPI");

	private String fiscalOrAdCalendar = (PropertyManager.getProperty("CALENDAR_TYPE", "B"));
	boolean filterNonMovingItems = Boolean
			.parseBoolean(PropertyManager.getProperty("MARK_NON_MOVING_ITEMS", "FALSE"));
	
	int tprMinWeek = Integer.parseInt(PropertyManager.getProperty("LONG_TERM_PROMO_WEEKS"));

	RestAPIConnectionService restAPIService = new RestAPIConnectionService();
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

	public List<Promotion> setupData(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, BaseData baseData) throws Exception, GeneralException {

		int noOfsaleAdDisplayWeeks = MultiWeekRecConfigSettings.getMwrNoOfSaleAdDisplayWeeks();

		List<Promotion> promotionDataOfAllItems = restAPIService.getPromotionFromAPI(
				getPromotionDataInput(recommendationInputDTO, noOfsaleAdDisplayWeeks, itemDataMap));

		if (promotionDataOfAllItems != null) {
			logger.debug("promotionDataOfAllItems.size() = " + promotionDataOfAllItems.size());
			setPromotionForAllItems(promotionDataOfAllItems, itemDataMap, recommendationInputDTO.getBaseWeek(),
					recommendationInputDTO.getStartWeek(), noOfsaleAdDisplayWeeks,recommendationInputDTO.getQuarterEndDate());
			
			setBaseData(baseData, promotionDataOfAllItems);
		}
		return promotionDataOfAllItems;
	}
	
	private void setPromotionForAllItems(List<Promotion> promotionList, HashMap<ItemKey, PRItemDTO> itemDataMap,
			String curStartDate, String recWeekStartDate, int noOfsaleAdDisplayWeeks, String quarterEndDate) throws GeneralException {

		PricingEngineService pricingEngineService = new PricingEngineService();

		Set<Integer> promotionsNotPresentItemMap = new HashSet<>();

		// Add future weeks
		List<String> futureWeeks = PricingEngineService.getFutureWeeks(recWeekStartDate, noOfsaleAdDisplayWeeks);
		// Sort Future weeks in desc order
		List<String> futureWeekDates = futureWeeks.stream().map(date -> LocalDate.parse(date, formatter))
				.sorted(Comparator.naturalOrder()).map(date -> formatter.format(date)).collect(Collectors.toList());

		Map<Integer, List<Promotion>> promotionByItem = promotionList.stream()
				.collect(Collectors.groupingBy(Promotion::getItemCode));

		List<PRItemDTO> itemList = new ArrayList<>();
		itemDataMap.forEach((key, value) -> {
			itemList.add(value);
		});

		itemDataMap.forEach((item, itemDTO) -> {
			if (!itemDTO.isLir()) {
				
				if (promotionByItem.containsKey(itemDTO.getItemCode())) {

					List<Promotion> PromotionOfItem = promotionByItem.get(itemDTO.getItemCode());

					for (Promotion saleInfoDTO : PromotionOfItem) {

						if (saleInfoDTO.getStartDate() == null || saleInfoDTO.getStartDate().trim().isEmpty()
								|| !isDateParsable(saleInfoDTO.getStartDate())) {
							saleInfoDTO.setStartDate(recWeekStartDate);
						}

						// Set cur week sale details
						if (saleInfoDTO.getCalendarStartDate().equals(curStartDate)) {
							// pick first occurrence
							PRItemSaleInfoDTO saleDTO = new PRItemSaleInfoDTO();
							if (itemDTO.getCurSaleInfo().getSalePrice() == null) {
								CopytoPRItemSaleInfoDTO(saleDTO, saleInfoDTO);
								itemDTO.setCurSaleInfo(saleDTO);
								logger.debug("On going promotion: " + saleDTO.getSaleStartDate()  + ", Item Code: " + itemDTO.getItemCode());
							}
						}

						// Set rec week sale details
						if (saleInfoDTO.getCalendarStartDate().equals(recWeekStartDate)) {
							// pick first occurrence
							if (itemDTO.getRecWeekSaleInfo().getSalePrice() == null) {
								PRItemSaleInfoDTO saleDTO = new PRItemSaleInfoDTO();
								CopytoPRItemSaleInfoDTO(saleDTO, saleInfoDTO);
								itemDTO.setRecWeekSaleInfo(saleDTO);
								logger.debug("On going promotion (Rec Week): " + saleDTO.getSaleStartDate()
										+ ", Item Code: " + itemDTO.getItemCode());
								itemDTO.setOnGoingPromotion(true);
								pricingEngineService.setTPRAndSaleFlag(saleInfoDTO.getStartDate(),
										saleInfoDTO.getEndDate(), itemDTO, formatter, tprMinWeek);
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
					PRItemSaleInfoDTO futureSaleInfo = getfutureMostPromo(futureWeekDates, PromotionOfItem, itemDTO,
							formatter);
					if (futureSaleInfo != null && futureSaleInfo.getSalePrice() != null) {
						itemDTO.setFutWeekSaleInfo(futureSaleInfo);
						itemDTO.setFuturePromotion(true);
						logger.debug("Future promotion (Rec Week): " + futureSaleInfo.getSaleStartDate()
						+ ", Item Code: " + itemDTO.getItemCode());
						pricingEngineService.setTPRAndSaleFlag(futureSaleInfo.getSaleStartDate(),
								futureSaleInfo.getSaleEndDate(), itemDTO, formatter, tprMinWeek);

					}

					// Set Promo end within given X weeks flag
					PriceRollbackService priceRollbackService = new PriceRollbackService();
					try {
						if (!priceRollbackService.isItemPromoEndsWithinXWeeks(itemDTO, itemList, recWeekStartDate)) {
							itemDTO.setPromoEndsWithinXWeeks(false);
						}
						List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(itemDTO,
								itemList);

						// Mark flag for items whose promotions span for 180 days or more
						// For LIG indvidual member will be marked if its spanning 180 days
						priceRollbackService.isItemOnLongTermPromotion(ligMembersOrNonLig);
						// Mark flag for items whose promotions starts within x weeks from recc week and
						// is not a long term promo
						// For LIG indvidual member will be marked
						priceRollbackService.isItemPromoStartsWithinXWeeks(ligMembersOrNonLig, recWeekStartDate);
						
					} catch (ParseException e) {
						logger.error("Exception in priceRollbackservice " + e);

					}
				} else {
					promotionsNotPresentItemMap.add(itemDTO.getItemCode());
				}
			}
		});

		
		if (!filterNonMovingItems && promotionsNotPresentItemMap.size() > 0) {
			logger.info("setPromotionForAllItems()-#items for which Promotion is not found : "
					+ promotionsNotPresentItemMap.size());
			logger.debug("ItemCodes: " + promotionsNotPresentItemMap.toString());

		}
	}

	private PRItemSaleInfoDTO getfutureMostPromo(List<String> futureWeekDates, List<Promotion> promotionOfItem,
			PRItemDTO itemDTO, DateTimeFormatter formatter) {
		PRItemSaleInfoDTO saleInfo = null;
		// Loop weeks
		for (String weekStartDate : futureWeekDates) {
			for (Promotion saleInfoDTO : promotionOfItem) {
				if (saleInfoDTO.getCalendarStartDate().equals(weekStartDate)) {

					if (itemDTO.getRecWeekSaleInfo() != null && itemDTO.getRecWeekSaleInfo().getSaleEndDate() != null
							&& isDateParsable(itemDTO.getRecWeekSaleInfo().getSaleEndDate())
							&& ChronoUnit.DAYS.between(
									LocalDate.parse(itemDTO.getRecWeekSaleInfo().getSaleEndDate(), formatter),
									LocalDate.parse(saleInfoDTO.getStartDate(), formatter)) > 0) {
						saleInfo = new PRItemSaleInfoDTO();
						CopytoPRItemSaleInfoDTO(saleInfo, saleInfoDTO);

					} else if (itemDTO.getRecWeekSaleInfo() == null || (itemDTO.getRecWeekSaleInfo() != null
							&& itemDTO.getRecWeekSaleInfo().getSaleWeekStartDate() == null)) {
						saleInfo = new PRItemSaleInfoDTO();
						CopytoPRItemSaleInfoDTO(saleInfo, saleInfoDTO);
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

	/**
	 * 
	 * @param saleInfoDTO
	 * @param saleInfo
	 */
	private void CopytoPRItemSaleInfoDTO(PRItemSaleInfoDTO saleInfoDTO, Promotion saleInfo) {

		if (saleInfo.getMustBuyPriceQty() > 0 && saleInfo.getMustBuyPrice() > 0) {
			saleInfoDTO.setSalePrice(
					PRCommonUtil.getMultiplePrice(saleInfo.getMustBuyPriceQty(), saleInfo.getMustBuyPrice(), 0.0));

		} else {
			saleInfoDTO.setSalePrice(
					PRCommonUtil.getMultiplePrice(saleInfo.getOfferPriceQty(), saleInfo.getOfferPrice(), 0.0));
		}

		if (saleInfo.getCalendarStartDate() != null && !saleInfo.getCalendarStartDate().isEmpty()) {
			saleInfoDTO.setSaleWeekStartDate(saleInfo.getCalendarStartDate());
		}

		if (saleInfo.getStartDate() != null && !saleInfo.getStartDate().isEmpty()) {
			saleInfoDTO.setSaleStartDate(saleInfo.getStartDate());
		}

		if (saleInfo.getEndDate() != null && !saleInfo.getEndDate().isEmpty()) {
			saleInfoDTO.setSaleEndDate(saleInfo.getEndDate());
		}

		saleInfoDTO.setMinQtyReqd(saleInfo.getMinQtyReqd());
		saleInfoDTO.setOfferUnitType(saleInfo.getOfferUnitType());
		saleInfoDTO.setOfferValue(saleInfo.getOfferValue());
		saleInfoDTO.setConstraint(saleInfo.getConstraint());
		saleInfoDTO.setPromoTypeId(saleInfo.getPromotionTypeId());

	}

	private DataInputForAPI getPromotionDataInput(RecommendationInputDTO recommendationInputDto,
			int noOfsaleAdDisplayWeeks, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		DataInputForAPI promoDataInput = new DataInputForAPI();
		List<Integer>itemCodeList= new ArrayList<>();
		int noOfDays = (noOfsaleAdDisplayWeeks * 7) - 1;
		LocalDate endDate = null;
		//Condition added for strategy what if as its done at week level 
		if (recommendationInputDto.getRunType() == PRConstants.RUN_TYPE_TEMP) {
			endDate = DateUtil.stringToLocalDate(recommendationInputDto.getEndWeek(), Constants.APP_DATE_FORMAT);

		} else {
			endDate = DateUtil
					.stringToLocalDate(recommendationInputDto.getQuarterStartDate(), Constants.APP_DATE_FORMAT)
					.plusDays(noOfDays);
		}
		// Filtering the non-moving items and pass only the items which have moved in last 52 weeks 
		if (filterNonMovingItems) {
			itemDataMap.forEach((itemKey, itemDTO) -> {
				if (!itemDTO.isLir()) {
					if (itemDTO.getAvgMovement() !=0) {
						itemCodeList.add(itemDTO.getItemCode());
					}
				}
			});

			if (itemCodeList.size() > 0) {
				logger.info("getPromotionDataInput():- # of movingItems filtered to be passed to promotion API :"
						+ itemCodeList.size());
				promoDataInput.setItemCodes(itemCodeList);
				promoDataInput.setMovingItemsPresent(true);
			} else
				promoDataInput.setMovingItemsPresent(false);
		}
		else {
			promoDataInput.setMovingItemsPresent(true);
			promoDataInput.setProductLevelId(recommendationInputDto.getProductLevelId());
			promoDataInput.setProductId(recommendationInputDto.getProductId());
		}
		promoDataInput.setLocationLevelId(recommendationInputDto.getLocationLevelId());
		promoDataInput.setLocationId(recommendationInputDto.getLocationId());
		promoDataInput.setCalType(PRConstants.CALENDAR_WEEK);
		if (recommendationInputDto.getRunType() == PRConstants.RUN_TYPE_TEMP)
			promoDataInput.setStartDate(recommendationInputDto.getStartWeek());
		else
			promoDataInput.setStartDate(recommendationInputDto.getQuarterStartDate());
		promoDataInput.setEndDate(DateUtil.localDateToString(endDate, Constants.APP_DATE_FORMAT));
		promoDataInput.setFiscalOrAdCalendar(fiscalOrAdCalendar);
		promoDataInput.setFetchStorePrice(0);
		return promoDataInput;
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

	
	private void setBaseData(BaseData baseData, List<Promotion> promotionList) {
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<>();
		Map<Integer, List<Promotion>> promotionByItem = promotionList.stream()
				.collect(Collectors.groupingBy(Promotion::getItemCode));

		promotionByItem.forEach((itemCode, promoList) -> {
			List<PRItemSaleInfoDTO> saleList = new ArrayList<>();
			promoList.forEach(promo -> {
				PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();
				CopytoPRItemSaleInfoDTO(saleInfoDTO, promo);
				saleList.add(saleInfoDTO);
			});
			saleDetails.put(itemCode, saleList);
		});

		baseData.setSaleDetails(saleDetails);
	}
}
