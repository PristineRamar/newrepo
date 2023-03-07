package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.test.offermgmt.TestHelper;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class ItemStrategyJUnitTest {
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	public static final Integer RET_LIR_ID = 110244;
	public static final Integer RET_LIR_ITEM_CODE = 950735;

	int itemPriceQty = 2, locationId = 6, productId = 1423, strategyId = 1000;
	int recLocationLevelId = 6, recLocationId = 6, recProductLevelId = 7, recProductId = 1423;
	int chainId = 50, divisionId = 22;
	double itemPrice = 4.69, listCost = 2.1, dealCost = 2.1, compPrice = 3.49;
	String recStartDate = "04/15/2018";

	@Before
	public void init() {
		PropertyManager.initialize("recommendation.properties");
	}

	/**
	 * Strategy is at zone and category level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case1ZoneAndCategoryLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.RECOMMENDATIONUNIT;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.RECOMMENDATIONUNIT, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.RECOMMENDATIONUNIT);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setRecUnitProductId(productId);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at zone and department level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case2ZoneAndDeptLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.DEPARTMENTLEVELID;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at zone and All products level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case3ZoneAndAllProductsLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at chain and All products level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case4ChainAndAllProductsLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.CHAIN_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, chainId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, chainId, productLevelId, productId, "", "",
				false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at chain and Dept level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case5ChainAndDeptLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.CHAIN_LEVEL_ID;
		int productLevelId = Constants.DEPARTMENTLEVELID;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, chainId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, chainId, productLevelId, productId, "", "",
				false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at chain and Category level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case6ChainAndCategoryLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.CHAIN_LEVEL_ID;
		int productLevelId = Constants.CATEGORYLEVELID;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, chainId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, chainId, productLevelId, productId, "", "",
				false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at division and All products level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case7DivisionAndAllProductsLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.DIVISION_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, divisionId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, divisionId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at division and Department level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case8DivisionAndDeptLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.DIVISION_LEVEL_ID;
		int productLevelId = Constants.DEPARTMENTLEVELID;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, divisionId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, divisionId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at division and Category level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case9DivisionAndCategoryLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.DIVISION_LEVEL_ID;
		int productLevelId = Constants.CATEGORYLEVELID;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, divisionId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, divisionId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at zone and All products level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case10ZoneAndAllProductsLevelStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Category level strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at zone and All products level with a price check list
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case11ZoneAndAllProductsLevelStrategy_CheckList() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 5;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 0;
		int leadZoneDivisionId = 0;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		item.setPriceCheckListId(priceCheckListId);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Price check list
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		// Normal strategy
		strategies.add(TestHelper.getStrategy(strategyId + 1, locationLevelId, locationId, productLevelId, productId,
				"", "", false, 0, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at lead zone and All products level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case12ZoneAndAllProductsLevelStrategy_LeadZone() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 5;
		int leadZoneDivisionId = 10;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);
		StrategyKey strategyLeadZoneKey = new StrategyKey(locationLevelId, leadZoneId, productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		item.setPriceCheckListId(priceCheckListId);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Lead zone strategy
		strategies.add(TestHelper.getStrategy(strategyId, locationLevelId, leadZoneId, productLevelId, productId, "",
				"", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyLeadZoneKey, strategies);
		strategies = new ArrayList<>();

		// Dependant zone strategy
		strategies.add(TestHelper.getStrategy(strategyId + 1, locationLevelId, locationId, productLevelId, productId,
				"", "", false, 0, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

	/**
	 * Strategy is at division of lead zone and All products level
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case13ZoneAndAllProductsLevelStrategy_DivisionOfLeadZone() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.ZONE_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 0;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 5;
		int leadZoneDivisionId = 10;
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);
		StrategyKey strategyLeadZoneDivsionKey = new StrategyKey(Constants.DIVISION_LEVEL_ID, leadZoneDivisionId,
				productLevelId, productId);

		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		item.setPriceCheckListId(priceCheckListId);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Lead zone strategy
		strategies.add(TestHelper.getStrategy(strategyId, Constants.DIVISION_LEVEL_ID, leadZoneDivisionId,
				productLevelId, productId, "", "", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyLeadZoneDivsionKey, strategies);
		strategies = new ArrayList<>();

		// Dependant zone strategy
		strategies.add(TestHelper.getStrategy(strategyId + 1, locationLevelId, locationId, productLevelId, productId,
				"", "", false, 0, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}
	
	
	/**
	 * Strategy is at zone and all products level with price check list
	 * 
	 * @throws OfferManagementException
	 */
	@Test
	public void case14ZoneAndProductListStrategy() throws OfferManagementException {

		// inputs
		int locationLevelId = Constants.CHAIN_LEVEL_ID;
		int productLevelId = Constants.ALLPRODUCTS;
		int priceCheckListId = 21;
		int vendorId = 0;
		int stateId = 0;
		int leadZoneId = 5;
		int leadZoneDivisionId = 10;
	
		List<PRStrategyDTO> strategies = new ArrayList<PRStrategyDTO>();
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<>();
		HashMap<Integer, Integer> productParentChildRelationMap = new HashMap<>();

		int prodListId = 523;
		String productKey = Constants.CATEGORYLEVELID + "-" + productId;
		ArrayList<Integer> pList = new ArrayList<>();
		pList.add(prodListId);
		
		productListMap.put(productKey, pList);
		
		
		StrategyKey strategyKey = new StrategyKey(locationLevelId, 50, productLevelId, 0);
		StrategyKey strategyWithProdList = new StrategyKey(Constants.ZONE_LEVEL_ID, 5,
				PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID, prodListId);
		
		productParentChildRelationMap.put(Constants.SUBCATEGORYLEVELID, Constants.ITEMLEVELID);
		productParentChildRelationMap.put(Constants.CATEGORYLEVELID, Constants.SUBCATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.DEPARTMENTLEVELID, Constants.CATEGORYLEVELID);
		productParentChildRelationMap.put(Constants.ALLPRODUCTS, Constants.DEPARTMENTLEVELID);

		ItemAttributeService itemAttributeService = new ItemAttributeService();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setCategoryProductId(productId);
		item.setDeptProductId(productId);
		item.setPriceCheckListId(priceCheckListId);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, ITEM_CODE_TEST_1234);

		RecommendationInputDTO recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO(
				recLocationLevelId, recLocationId, recProductLevelId, recProductId, chainId, divisionId, leadZoneId,
				leadZoneDivisionId, recStartDate);

		// Product list level strategy
		strategies.add(TestHelper.getStrategy(strategyId, Constants.ZONE_LEVEL_ID, 5,
				PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID, prodListId, "", "", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyWithProdList, strategies);
		strategies = new ArrayList<>();

		// Global item list level
		strategies.add(TestHelper.getStrategy(strategyId + 1, locationLevelId, 50, productLevelId, 0,
				"", "", false, priceCheckListId, vendorId, stateId));

		strategyMap.put(strategyKey, strategies);

		itemAttributeService.setStrategies(recommendationInputDTO, itemDataMap, retLirMap, strategyMap,
				productParentChildRelationMap, productListMap, new ArrayList<>());

		assertEquals("Strategy Not Set!!!", strategyId, item.getStrategyDTO().getStrategyId());

	}

}
