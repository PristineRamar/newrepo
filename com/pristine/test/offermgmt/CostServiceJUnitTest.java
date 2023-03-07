package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostServiceJUnitTest {

	int CHAIN_ID = 50;
	int ZONE_ID = 20;
	int STORE_ID = 1258;
	int NO_OF_WEEK_COST_HISTORY = 8;
	String WEEK_START_DATE = "03/05/2017";
	int ITEM_CODE_1 = 54846;
	double COST_THRESHOLD = 0.05d;
	
	RetailCostServiceOptimized retailCostServiceOptimized = null;
	RetailCalendarDTO startWeekCalDTO = null;
	HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = null;
	Set<Integer> itemCodeSet = null;
	HashMap<ItemKey, PRItemDTO> itemDataMap = null;
	HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory = null;
	HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemStoreDataMap = null;
	
	@Before
	public void init() throws GeneralException {
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");
		setCurrenCalDTO();
		setAllWeekCalendarDetails();
		setItemCodeSet();
	}
	
	
	/**
	 * Check latest cost for zone
	 * Cost is there for all three levels
	 * Cost is different for different weeks,  last week cost history is missing
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase1() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add store level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.STORE_LEVEL_TYPE_ID, STORE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add store level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.STORE_LEVEL_TYPE_ID, STORE_ID, 1.59f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.49f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.39f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		
		retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails);
		
		
		Double expectedCost = 1.79d;
		assertEquals("JSON Not Matching", expectedCost, itemDTO.getListCost());
		
	}
	
	/**
	 * Check latest cost for zone
	 * Cost is there at zone and chain
	 * Cost is different for different weeks,  last week cost history is missing
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase2() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.49f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.39f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		
		retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails);
		
		
		Double expectedCost = 1.99d;
		assertEquals("JSON Not Matching", expectedCost, itemDTO.getListCost());
		
	}
	
	/**
	 * Check latest cost for zone
	 * Cost is there at chain
	 * Cost is different for different weeks,  last week cost history is missing
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase3() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.39f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		
		retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails);
		
		
		Double expectedCost = 1.89d;
		assertEquals("JSON Not Matching", expectedCost, itemDTO.getListCost());
		
	}
	
	/**
	 * Check latest cost for store
	 * Cost is there for all three levels
	 * Cost is different for different weeks,  last week cost history is missing
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase4() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add store level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.STORE_LEVEL_TYPE_ID, STORE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add store level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.STORE_LEVEL_TYPE_ID, STORE_ID, 1.59f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.49f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.39f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		itemStoreDataMap = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		itemStoreDataMap.put(STORE_ID, itemDataMap);
		
		retailCostServiceOptimized.getLatestCostOfStoreItems(itemCostHistory, itemCodeSet, itemStoreDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO,
				NO_OF_WEEK_COST_HISTORY, allWeekCalendarDetails);
		
		Double expectedCost = 1.69d;
		assertEquals("JSON Not Matching", expectedCost, itemDTO.getListCost());
	}
	
	/**
	 * Check latest cost for store
	 * Cost is there at zone and chain
	 * Cost is different for different weeks,  last week cost history is missing
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase5() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.49f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.39f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		itemStoreDataMap = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		itemStoreDataMap.put(STORE_ID, itemDataMap);
		
		retailCostServiceOptimized.getLatestCostOfStoreItems(itemCostHistory, itemCodeSet, itemStoreDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO,
				NO_OF_WEEK_COST_HISTORY, allWeekCalendarDetails);
		
		Double expectedCost = 1.79d;
		assertEquals("JSON Not Matching", expectedCost, itemDTO.getListCost());
	}
	
	/**
	 * Check latest cost for store
	 * Cost is there at chain
	 * Cost is different for different weeks,  last week cost history is missing
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase6() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.39f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		itemStoreDataMap = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		itemStoreDataMap.put(STORE_ID, itemDataMap);
		
		retailCostServiceOptimized.getLatestCostOfStoreItems(itemCostHistory, itemCodeSet, itemStoreDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO,
				NO_OF_WEEK_COST_HISTORY, allWeekCalendarDetails);
		
		Double expectedCost = 1.89d;
		assertEquals("JSON Not Matching", expectedCost, itemDTO.getListCost());
	}
	
	/**
	 * Check previous cost for zone
	 * Cost is there at chain and zone level
	 * No cost change 
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase7() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		
		// Set current list cost
		itemDTO.setListCost(1.79d);
		
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.79d;
		Double expectedPreCost = 1.79d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
		
	}
	
	/**
	 * Check previous cost for zone
	 * Cost is there at chain and zone level
	 * Cost is changed, but there is no price change
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase8() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDTO.setCurRegPriceEffDate("01/08/2017");
		itemDataMap.put(itemKey, itemDTO);
		
		// Set current list cost
		itemDTO.setListCost(1.79d);
		
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.79d;
		Double expectedPreCost = 1.69d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
		
	}
	
	/**
	 * Check previous cost for zone
	 * Cost is there at chain and zone level
	 * Cost is changed, but there is no price change and there is no reg effective date and no cost effective date
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase9() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		
		// Set current list cost
		itemDTO.setListCost(1.79d);
		
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.79d;
		Double expectedPreCost = 1.69d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
		
	}
	
	
	/**
	 * Check previous cost for zone
	 * Cost is there at chain and zone level
	 * Cost is changed, there is a price change happened on the cost effective date
	 * This is considered as no cost change
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase10() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDTO.setListCostEffDate("02/19/2017");
		itemDTO.setCurRegPriceEffDate("02/19/2017");
		itemDataMap.put(itemKey, itemDTO);
		
		// Set current list cost
		itemDTO.setListCost(1.79d);
		
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.79d;
		Double expectedPreCost = 1.79d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
		
	}
	
	/**
	 * Check previous cost for zone
	 * Cost is there at chain and zone level
	 * Cost is changed, there is a price change happened after the cost effective date
	 * This is considered as no cost change
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase11() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDTO.setListCostEffDate("02/19/2017");
		itemDTO.setCurRegPriceEffDate("02/24/2017");
		itemDataMap.put(itemKey, itemDTO);
		
		// Set current list cost
		itemDTO.setListCost(1.79d);
		
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.79d;
		Double expectedPreCost = 1.79d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
	}
	
	
	/**
	 * Check previous cost for zone
	 * Cost is there at chain and zone level
	 * Cost is changed, there is no price change happened on or after the cost effective date
	 * This is considered as no cost change
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase12() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDTO.setListCostEffDate("02/19/2017");
		itemDTO.setCurRegPriceEffDate("02/01/2017");
		itemDataMap.put(itemKey, itemDTO);
		
		// Set current list cost
		itemDTO.setListCost(1.79d);
		
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.79d;
		Double expectedPreCost = 1.69d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
	}
	
	/**
	 * Check previous cost for store
	 * Cost is there at chain and zone and store level
	 * No cost change 
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase13() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add store level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.STORE_LEVEL_TYPE_ID, STORE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		itemStoreDataMap = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDataMap.put(itemKey, itemDTO);
		itemStoreDataMap.put(STORE_ID, itemDataMap);
		
		// Set current list cost
		itemDTO.setListCost(1.69d);
		
		retailCostServiceOptimized.getPreviousCostOfStoreItems(itemCostHistory, itemCodeSet, itemStoreDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.69d;
		Double expectedPreCost = 1.69d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
		
	}
	
	/**
	 * Check previous cost for store
	 * Cost is there at chain and zone level
	 * Cost is changed, there is no price change happened on or after the cost effective date
	 * This is considered as no cost change
	 * @throws Exception
	 * @throws GeneralException 
	 */
	@Test
	public void testCase15() throws Exception, GeneralException {
		retailCostServiceOptimized = new RetailCostServiceOptimized(null);
		itemCostHistory = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		List<RetailCostDTO> allLocationCostList = new ArrayList<RetailCostDTO>();
		RetailCostDTO retailCostDTO = null;
		
		//Set cost history
		HashMap<String, List<RetailCostDTO>> weekCost = new HashMap<String, List<RetailCostDTO>>();
		
		// Add store level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.STORE_LEVEL_TYPE_ID, STORE_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.79f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.89f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		
		weekCost.put("02/26/2017", allLocationCostList);
		
		
		allLocationCostList = new ArrayList<RetailCostDTO>();
		// Add zone level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.ZONE_LEVEL_TYPE_ID, ZONE_ID, 1.69f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);
		// Add chain level cost
		retailCostDTO = TestHelper.setRetailCostDTO(0, Constants.CHAIN_LEVEL_TYPE_ID, CHAIN_ID, 1.99f, null, null, null, null, null, null);
		allLocationCostList.add(retailCostDTO);

		weekCost.put("02/19/2017", allLocationCostList);
		
		itemCostHistory.put(ITEM_CODE_1, weekCost);
		
		// Set item data map
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		itemStoreDataMap = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		ItemKey itemKey = new ItemKey(ITEM_CODE_1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = TestHelper.getTestItem(ITEM_CODE_1, 0, 0d, 0d, 0, null);
		itemDTO.setPriceZoneId(ZONE_ID);
		itemDTO.setListCostEffDate("02/19/2017");
		itemDTO.setCurRegPriceEffDate("02/01/2017");
		itemDataMap.put(itemKey, itemDTO);
		itemStoreDataMap.put(STORE_ID, itemDataMap);
		
		// Set current list cost
		itemDTO.setListCost(1.89d);
		
		retailCostServiceOptimized.getPreviousCostOfStoreItems(itemCostHistory, itemCodeSet, itemStoreDataMap, CHAIN_ID, ZONE_ID, startWeekCalDTO, NO_OF_WEEK_COST_HISTORY,
				allWeekCalendarDetails, COST_THRESHOLD);
		
		Double expectedListCost = 1.89d;
		Double expectedPreCost = 1.69d;
		assertEquals("JSON Not Matching", expectedListCost, itemDTO.getListCost());
		assertEquals("JSON Not Matching", expectedPreCost, itemDTO.getPreListCost());
	}
	
	private void setCurrenCalDTO() {
		startWeekCalDTO = TestHelper.getCalendarDetails(WEEK_START_DATE, "");
	}
	
	//Add 12 weeks calendar details
	private void setAllWeekCalendarDetails() throws GeneralException {
		RetailCalendarDTO retailCalendarDTO = null;
		allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		Date latestWeekWithPriceStartDate = DateUtil.toDate("03/19/2017");

		for (int i = 0; i < 12; i++) {
			String tempWeekStartDate = DateUtil.dateToString(DateUtil.incrementDate(latestWeekWithPriceStartDate, -(7 * (i))),
					Constants.APP_DATE_FORMAT);
			retailCalendarDTO = TestHelper.getCalendarDetails(tempWeekStartDate, "");
			allWeekCalendarDetails.put(tempWeekStartDate, retailCalendarDTO);
		}
	}
	
	private void setItemCodeSet() {
		itemCodeSet = new HashSet<Integer>();
		itemCodeSet.add(ITEM_CODE_1);
	}
	
}
