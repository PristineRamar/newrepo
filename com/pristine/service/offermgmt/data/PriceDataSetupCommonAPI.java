package com.pristine.service.offermgmt.data;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dataload.service.DataInputForAPI;
import com.pristine.dto.Price;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RestAPIConnectionService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PriceDataSetupCommonAPI {
	private static Logger logger = Logger.getLogger("PriceDataSetupCommonAPI");

	private static final String fiscalOrAdCalendar = (PropertyManager.getProperty("CALENDAR_TYPE", "B"));
	private static final boolean checkFuturePrice = Boolean
			.parseBoolean(PropertyManager.getProperty("FETCH_FUTURE_PRICE", "FALSE"));
	private static final boolean checkForClearanceStores = Boolean
			.parseBoolean(PropertyManager.getProperty("FETCH_CLEARANCE_STORES", "FALSE"));

	RestAPIConnectionService restAPIService = new RestAPIConnectionService();
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

	/**
	 * @param conn
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param baseData               TODO
	 * @throws Exception
	 * @throws GeneralException
	 */
	public void setupData(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, BaseData baseData) throws Exception, GeneralException {

		List<Price> priceDataOfAllItems = restAPIService
				.getPriceFromAPI(getPriceDataInput(conn, recommendationInputDTO, baseData, false, false,null));
		// Set price for all the items
		List<Integer> priceFoundItemsList=setPriceForAllItems(priceDataOfAllItems, itemDataMap, recommendationInputDTO.getBaseWeek());

		if (checkFuturePrice) {
			List<Price> futurePrices = restAPIService
					.getPriceFromAPI(getPriceDataInput(conn, recommendationInputDTO, baseData, true, false,priceFoundItemsList));

			if (null == futurePrices || futurePrices.isEmpty()) {
				// logger.info("No future prices found!");
			} else {
				Map<Integer, List<Price>> priceBycalendarId = futurePrices.stream()
						.collect(Collectors.groupingBy(Price::getCalendarId));
				baseData.setFuturePrice(priceBycalendarId);

				Map<String, List<Price>> priceByStartDate = futurePrices.stream()
						.collect(Collectors.groupingBy(Price::getStartDate));
				String nextWeekstartDate = DateUtil.dateToString(
						DateUtil.incrementDate(DateUtil.toDate(recommendationInputDTO.getStartWeek()), 7),
						Constants.APP_DATE_FORMAT);
				checkandSetFuturePrice(itemDataMap,priceByStartDate, nextWeekstartDate);
			}
		}

		if (checkForClearanceStores) {
			List<Price> clearanceItems = restAPIService
					.getPriceFromAPI(getPriceDataInput(conn, recommendationInputDTO, baseData, false, true,priceFoundItemsList));
			if (null == clearanceItems || clearanceItems.isEmpty()) {
			} else {
				setClearanceItems(itemDataMap, clearanceItems, recommendationInputDTO.getStartWeek(),
						baseData.getStoreList());
			}
		}
	}

	private DataInputForAPI getPriceDataInput(Connection conn, RecommendationInputDTO recommendationInputDto,
			BaseData baseData, boolean isFuturePrice, boolean checkforclearancestores, List<Integer> priceFoundItemsList) throws GeneralException {

		RetailCalendarDTO calDTO = new RetailCalendarDAO().getCalendarId(conn, recommendationInputDto.getBaseWeek(),
				Constants.CALENDAR_WEEK);
		DataInputForAPI priceDataInput = new DataInputForAPI();
		if (priceFoundItemsList != null && priceFoundItemsList.size() > 0) {
			priceDataInput.setItemCodes(priceFoundItemsList);
		} else {
			priceDataInput.setProductLevelId(recommendationInputDto.getProductLevelId());
			priceDataInput.setProductId(recommendationInputDto.getProductId());
		}
		priceDataInput.setLocationLevelId(recommendationInputDto.getLocationLevelId());
		priceDataInput.setLocationId(recommendationInputDto.getLocationId());
		priceDataInput.setCalType(PRConstants.CALENDAR_WEEK);
		if (isFuturePrice) {
			String nextWeekstartDate = DateUtil.dateToString(
					DateUtil.incrementDate(DateUtil.toDate(recommendationInputDto.getStartWeek()), 7),
					Constants.APP_DATE_FORMAT);
			priceDataInput.setStartDate(nextWeekstartDate);
			priceDataInput.setPriceTypeId(Constants.REGULAR_PRICE_ID);
			priceDataInput.setFetchFuturePrice(true);
		} else if (checkforclearancestores) {
			priceDataInput.setStartDate(recommendationInputDto.getStartWeek());
			priceDataInput.setPriceTypeId(Constants.CLEARANCE_PRICE_ID);
			LocalDate recweekend = LocalDate.parse(recommendationInputDto.getStartWeek(),
					DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT)).plusDays(6);
			priceDataInput.setEndDate(DateUtil.localDateToString(recweekend, Constants.APP_DATE_FORMAT));
			priceDataInput.setFetchStorePrice(1);
		} else {
			priceDataInput.setStartDate(recommendationInputDto.getBaseWeek());
			priceDataInput.setEndDate(calDTO.getEndDate());
			priceDataInput.setPriceTypeId(Constants.REGULAR_PRICE_ID);
		}

		priceDataInput.setFiscalOrAdCalendar(fiscalOrAdCalendar);
		return priceDataInput;

	}

	private List<Integer> setPriceForAllItems(List<Price> priceList, HashMap<ItemKey, PRItemDTO> itemDataMap,
			String baseWeek) {
		Map<Integer, List<Price>> priceByItem = priceList.stream().collect(Collectors.groupingBy(Price::getItemCode));

		List<Integer> priceFoundItemsList= new ArrayList<Integer>();
		Set<Integer> priceNotFoundItems = new HashSet<>();

		itemDataMap.forEach((item, itemDTO) -> {
			if (!itemDTO.isLir()) {

				if (priceByItem.containsKey(itemDTO.getItemCode())) {
					List<Price> priceOfItem = priceByItem.get(itemDTO.getItemCode());
					// get the max calendar Id and set the price
					Price price = priceOfItem.stream().filter(p -> p.getCalendarStartDate().equals(baseWeek))
							.findFirst().orElse(null);
					if (price != null) {
						itemDTO.setRegPrice(price.getPrice());
						itemDTO.setRegMPack((price.getPriceQty() == 0 ? 1 : price.getPriceQty()));
						itemDTO.setCurRegPriceEffDate(price.getStartDate());
						itemDTO.setCoreRetail(price.getCoreRetail());
						itemDTO.setVdpRetail(price.getVdpRetail());
						priceFoundItemsList.add(itemDTO.getItemCode());

					} else {
						priceNotFoundItems.add(itemDTO.getItemCode());
					}

				} else {
					priceNotFoundItems.add(itemDTO.getItemCode());
					// logger.info("Price not found for item: " + itemDTO.getItemCode());
				}
			}
		});

		logger.info("setPriceForAllItems ()- Price set for # items" + priceFoundItemsList.size());
		
		if (priceNotFoundItems.size() > 0) {
			logger.info("# items for which price is not found: " + priceNotFoundItems.size());
			logger.info(" itemsCodes: " + priceNotFoundItems.toString());
		}
		priceNotFoundItems.clear();
		return priceFoundItemsList;
	}

	private void setClearanceItems(HashMap<ItemKey, PRItemDTO> itemDataMap, List<Price> clearanceItems, String reccWeek,
			List<Integer> storeList) {

		Map<Integer, List<Price>> priceByItem = clearanceItems.stream()
				.collect(Collectors.groupingBy(Price::getItemCode));
		itemDataMap.forEach((item, itemDTO) -> {
			if (!itemDTO.isLir()) {

				if (priceByItem.containsKey(itemDTO.getItemCode())) {
					List<Price> priceOfItem = priceByItem.get(itemDTO.getItemCode());
					Map<String, List<Price>> priceBycalendarDate = priceOfItem.stream()
							.collect(Collectors.groupingBy(Price::getCalendarStartDate));
					if (priceBycalendarDate.containsKey(reccWeek)) {
						Set<Integer> stores = priceBycalendarDate.get(reccWeek).stream()
								.collect(Collectors.groupingBy(Price::getLocationId)).keySet();
						if (stores.size() == storeList.size()) {
							Price price = getModePrice(priceBycalendarDate.get(reccWeek));
							if (price != null) {
								itemDTO.setClearanceItem(true);
								itemDTO.setClearanceRetailEffDate(price.getStartDate());
								itemDTO.setClearanceRetail(price.getPrice() / price.getPriceQty());
							}
						}
					}
				}
			}
		});

	}

	/**
	 * 
	 * @param priceListOfItems
	 * @return
	 */
	private Price getModePrice(List<Price> priceListOfItems) {
		Price modePrice = null;

		if (priceListOfItems != null) {
			Map<MultiplePrice, Long> elementCountMap = priceListOfItems.stream()
					.collect(Collectors.groupingBy(Price::getMultiPrice, Collectors.counting()));

			List<MultiplePrice> result = elementCountMap.values().stream().max(Long::compareTo)
					.map(maxValue -> elementCountMap.entrySet().stream()
							.filter(entry -> maxValue.equals(entry.getValue())).map(Map.Entry::getKey)
							.collect(Collectors.toList()))
					.orElse(Collections.emptyList());

			Double maxPrice = result.stream().map(MultiplePrice::getUnitPrice).max(Double::compareTo).orElse(null);
			if (maxPrice != null) {
				modePrice = priceListOfItems.stream().filter(c -> c.getMultiPrice().getUnitPrice() == maxPrice)
						.findFirst().orElse(null);
			}

		}

		return modePrice;
	}

	private void checkandSetFuturePrice(HashMap<ItemKey, PRItemDTO> itemDataMap,
			Map<String, List<Price>> priceByStartDate, String nextWeekstartDate) {
		for (Map.Entry<String, List<Price>> priceByDate : priceByStartDate.entrySet()) {

			if (LocalDate.parse(priceByDate.getKey(), formatter).isAfter(LocalDate.parse(nextWeekstartDate, formatter))
					|| LocalDate.parse(priceByDate.getKey(), formatter)
							.isEqual(LocalDate.parse(nextWeekstartDate, formatter))) {
				Map<Integer, List<Price>> priceByItem = priceByDate.getValue().stream()
						.collect(Collectors.groupingBy(Price::getItemCode));
				itemDataMap.forEach((item, itemDTO) -> {
					if (!itemDTO.isLir()) {
						if (priceByItem.containsKey(itemDTO.getItemCode())) {
							List<Price> priceOfItem = priceByItem.get(itemDTO.getItemCode());
							Price price = priceOfItem.stream().findFirst().orElse(null);
							if (price != null && price.getPriceQty() > 0 && itemDTO.getRegPrice() != null
									&& itemDTO.getRegPrice() > 0) {

								double unitPrice = price.getPrice() / price.getPriceQty();
								double unitRegprice = itemDTO.getRegPrice() / itemDTO.getRegMPack();
								if (unitPrice != unitRegprice) {
									itemDTO.setFuturePricePresent(true);
									itemDTO.setFuturePriceEffDate(price.getStartDate());
									itemDTO.setFutureUnitPrice(unitPrice);
								}
							}

						}
					}
				});

			}

		}
	}

}
