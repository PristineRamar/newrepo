package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.test.offermgmt.TestHelper;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class ItemPriceJUnitTest {
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	public static final Integer RET_LIR_ID = 110244;
	public static final Integer RET_LIR_ITEM_CODE = 950735;

	int CHAIN_ID = 50;
	int ZONE_ID_1 = 20;
	int ZONE_ID_2 = 21;
	int STORE_ID = 1258;

	private int itemPriceQty = 0;
	private int recLocationLevelId = 6, recLocationId = 6, recProductLevelId = 4, recProductId = 1423;
	private int divisionId = 22, calendarId1 = 1234, calendarId2 = 3456;
	private double itemPrice = 0, listCost = 2.1, dealCost = 2.1, compPrice = 3.49;
	private String recStartDate = "04/15/2018";

	@Before
	public void init() {
		PropertyManager.initialize("recommendation.properties");
	}

	/**
	 * Fetching chain level price, when there is only one chain level price
	 */
	@Test
	public void case1ChainLevelPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.99f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 2.99d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegPrice());

	}

	/**
	 * Fetching zone level price, when there is a chain level price and a zone level price
	 */
	@Test
	public void case2ZoneLevelPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.99f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.69f, 0f,
				1, "02/18/2017");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 2.69d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegPrice());

	}

	/**
	 * Fetching zone level price, when there is a chain level price and multiple zone level prices
	 */
	@Test
	public void case3ZoneLevelPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.99f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.69f, 0f,
				1, "02/18/2017");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, 2.89f, 0f,
				1, "02/18/2017");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 2.69d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegPrice());

	}

	/**
	 * Fetching latest chain level price, when we have multiple calendars
	 */
	@Test
	public void case4LatestChainLevelPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Price for Latest week
		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.89f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap1 = new HashMap<>();

		// Price for a week before
		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 2.99f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId2, priceMap1);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 2.89d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegPrice());

	}

	/**
	 * Fetching latest zone level price, when we have multiple calendars
	 */
	@Test
	public void case5LatestZoneLevelPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Price for Latest week
		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.89f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.69f, 0f,
				1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap1 = new HashMap<>();

		// Price for a week before
		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 2.99f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.99f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId2, priceMap1);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 2.69d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegPrice());

	}

	/**
	 * Fetching latest zone level price, when we have multiple calendars and multiple zones
	 */
	@Test
	public void case6LatestZoneLevelPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_2);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Price for Latest week
		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.89f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.79f, 0f,
				1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, 2.69f, 0f,
				1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap1 = new HashMap<>();

		// Price for a week before
		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 2.99f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.79f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, 2.89f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId2, priceMap1);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 2.69d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegPrice());

	}

	/**
	 * Fetching chain level multiple price, when there is only one chain level price.
	 */
	@Test
	public void case7ChainLevelMultiplePrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 0f, 10.99f, 2, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 10.99d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegMPrice());
		Integer qty = 2;
		assertEquals("Price Qty not matching!!!", qty, item.getRegMPack());

	}

	/**
	 * Fetching chain level multiple price, when there is only one chain level price.
	 */
	@Test
	public void case8ZoneLevelMultiplePrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 0f, 10.99f, 2, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 0f, 9.99f,
				2, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 9.99d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegMPrice());
		Integer qty = 2;
		assertEquals("Price Qty not matching!!!", qty, item.getRegMPack());

	}

	/**
	 * Fetching latest zone level multiple price, when we have multiple calendars and multiple zones
	 */
	@Test
	public void case9LatestZoneLevelMultiPrice() {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> weeklyPriceDataMap = new LinkedHashMap<>();
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap = new HashMap<>();

		ItemAttributeService itemAttributeService = new ItemAttributeService();
		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, CHAIN_ID, divisionId, 0, 0,
				recStartDate);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setPriceZoneId(ZONE_ID_2);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Price for Latest week
		RetailPriceDTO retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.CHAIN_LEVEL_TYPE_ID,
				CHAIN_ID, 2.89f, 0f, 1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.79f, 0f,
				1, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, 0f, 4.99f,
				2, "02/18/2018");

		priceMap = TestHelper.getPriceMap(priceMap, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId1, priceMap);

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap1 = new HashMap<>();

		// Price for a week before
		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 2.99f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, 2.79f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_1, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		retailPriceDTO = TestHelper.setRetailPriceDTO(calendarId2, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, 2.89f, 0f,
				1, "01/16/2017");

		priceMap1 = TestHelper.getPriceMap(priceMap1, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID_2, ITEM_CODE_TEST_1234,
				retailPriceDTO);

		weeklyPriceDataMap.put(calendarId2, priceMap1);

		itemAttributeService.setPriceData(recommendationInputDTO, itemDataMap, weeklyPriceDataMap);

		Double expectedPrice = 4.99d;
		assertEquals("Price not matching!!!", expectedPrice, item.getRegMPrice());
		Integer qty = 2;
		assertEquals("Price Qty not matching!!!", qty, item.getRegMPack());

	}
}
