package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStorePrice;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PriceAndCostTest {

	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	ObjectMapper mapper = new ObjectMapper();
	private int chainId = 50;
	
	/**
	 * @param args
	 * @throws GeneralException 
	 * @throws OfferManagementException 
	 * @throws JsonProcessingException 
	 */
	public static void main(String[] args) throws GeneralException, OfferManagementException, JsonProcessingException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		PriceAndCostTest priceAndCostTest = new PriceAndCostTest();
		priceAndCostTest.intialSetup();
		//priceAndCostTest.getPriceForStore();
		//priceAndCostTest.getCostForStore();
//		priceAndCostTest.getPriceOptimized();
		//priceAndCostTest.getPriceOfStores();
		priceAndCostTest.getPriceHistory();
		//priceAndCostTest.testCostSetup();
	}
	
	private void testCostSetup() throws GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		List<String> priceAndStrategyZoneNos = new ArrayList<String>();
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		
		double costChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PR_DASHBOARD.COST_CHANGE_THRESHOLD", "0.05"));
		
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);
		int zoneId = 15;
		int costHistory = 8;
		
		priceZoneStores.add(4818);
		priceZoneStores.add(5730);
		priceZoneStores.add(5736);
		priceZoneStores.add(5849);
		priceZoneStores.add(6122);
		priceZoneStores.add(5726);
		
		priceAndStrategyZoneNos.add("0019");
		curCalDTO.setStartDate("11/20/2016");

		PRItemDTO itemDTO = new PRItemDTO();
		itemDTO.setItemCode(750028);
		itemDataMap.put(PRCommonUtil.getItemKey(750028, false), itemDTO);
		
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(750028);
		storeItems.put(PRCommonUtil.getItemKey(750028, false), itemDTO);
		itemDataMapStore.put(4818, storeItems);
		
		Set<Integer> nonCachedItemCodeSet = new HashSet<Integer>();
		nonCachedItemCodeSet.add(750028);
		
		RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(conn);

		// Get non-cached item's zone and store cost history
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory = retailCostServiceOptimized.getCostHistory(chainId, curCalDTO,
				costHistory, allWeekCalendarDetails, nonCachedItemCodeSet, priceAndStrategyZoneNos, priceZoneStores);

		// Find latest cost of the item for zone and store
		retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, nonCachedItemCodeSet, itemDataMap, chainId, zoneId, curCalDTO,
				costHistory, allWeekCalendarDetails);

		retailCostServiceOptimized.getLatestCostOfStoreItems(itemCostHistory, nonCachedItemCodeSet, itemDataMapStore, chainId, zoneId, curCalDTO,
				costHistory, allWeekCalendarDetails);

		// Find if there is cost change for zone
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, nonCachedItemCodeSet, itemDataMap, chainId, zoneId, curCalDTO,
				costHistory, allWeekCalendarDetails, costChangeThreshold);

		// Find if there is cost change for store
		retailCostServiceOptimized.getPreviousCostOfStoreItems(itemCostHistory, nonCachedItemCodeSet, itemDataMapStore, chainId, zoneId, curCalDTO,
				costHistory, allWeekCalendarDetails, costChangeThreshold);
	}

	private void getPriceHistory() throws GeneralException {
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);
		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		RetailCalendarDTO resetCalDTO = new RetailCalendarDTO();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		boolean fetchStorePrice = true;
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		List<String> priceAndStrategyZoneNos = new ArrayList<String>();
		PricingEngineService pricignEngineService = new PricingEngineService();
        int itemCode1 = 939658;
        
		priceZoneStores.add(4817);
		priceZoneStores.add(5642);
		priceZoneStores.add(5643);
		priceZoneStores.add(5644);
		priceZoneStores.add(5645);
		priceZoneStores.add(5646);
		
		
		priceAndStrategyZoneNos.add("0018");
		calDTO.setStartDate("04/02/2017");
		resetCalDTO.setStartDate("04/02/2017");

		PRItemDTO itemDTO = new PRItemDTO();
		itemDTO.setItemCode(itemCode1);
		itemDataMap.put(PRCommonUtil.getItemKey(itemCode1, false), itemDTO);

		HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = pricingEngineDAO.getPriceHistory(conn, chainId,
				allWeekCalendarDetails, calDTO, resetCalDTO, itemDataMap, priceAndStrategyZoneNos, priceZoneStores, fetchStorePrice);
		
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = pricignEngineService.getItemZonePriceHistory(chainId, 5,
				itemPriceHistory);
		
		ObjectiveService objectiveService = new ObjectiveService();
		//boolean result = objectiveService.isRegPriceIncreasedInLastXDays(itemZonePriceHistory.get(itemCode1), calDTO.getStartDate(), 90);
		
//		boolean result = objectiveService.isRegPriceUnchangedInLastXWeeks(itemZonePriceHistory.get(itemCode1), calDTO.getStartDate(), 52);
		
		//MultiplePrice maxPrice = objectiveService.getMaxPriceInLastXWeeks(itemZonePriceHistory.get(itemCode1), calDTO.getStartDate(), 52, "REG");
		
		//MultiplePrice maxPrice1 = objectiveService.getMaxPriceInLastXWeeks(itemZonePriceHistory.get(itemCode1), calDTO.getStartDate(), 52, "SALE");
		
		//boolean isItemInSale = objectiveService.isItemInSaleXWeeks(itemZonePriceHistory.get(itemCode1), calDTO.getStartDate(), 52);
		
		for (Entry<Integer, HashMap<String, RetailPriceDTO>> itemWeeklyPrice : itemZonePriceHistory.entrySet()) {
			logger.debug("Item Code:" + itemWeeklyPrice.getKey());
			
			for (Entry<String, RetailPriceDTO> tempPrices : itemWeeklyPrice.getValue().entrySet()) {
				logger.debug("week:" + tempPrices.getKey() + ",zone prie:" + tempPrices.getValue().getRegPrice());
			}
		}
		
		logger.debug("Completed");
	}
	
	private void getPriceOfStores() throws OfferManagementException, JsonProcessingException{
		PricingEngineService pricingEngineService = new PricingEngineService();
		List<Integer> items = new ArrayList<Integer>();
		items.add(209325);
		//items.add(15719);
		
		List<PRStorePrice> storePrices  = pricingEngineService.getStorePrices(conn, 16444, items);
		
		String output = mapper.writeValueAsString(storePrices);
		logger.debug(output);
	}
	
	private void getPriceOptimized() throws GeneralException{
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		RetailCalendarDTO resetCalDTO = null;
		boolean fetchZoneData = true;
		List<String> strategyZoneNos = new ArrayList<String>();
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreNotInCache = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemservice = new ItemService(executionTimeLogs);
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();		
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();		
		HashMap<ItemKey, PRItemDTO> storeItemsNotInCache = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey;
		//Input DTO
		inputDTO.setProductLevelId(4);
		inputDTO.setProductId(264);
		inputDTO.setLocationLevelId(6);
		inputDTO.setLocationId(66);
		
		//Calendar DTO
		curCalDTO.setCalendarId(5038);
		
		//Strategy Zones
		strategyZoneNos.add("41"); //641

		//Price Store Zones
//		priceZoneStores = itemservice.getPriceZoneStores(conn, inputDTO.getProductLevelId(), inputDTO.getProductId(), 
//				inputDTO.getLocationLevelId(), inputDTO.getLocationId());
		
		priceZoneStores.add(6124);
		
		//Item Data Map
		PRItemDTO itemDTO = new PRItemDTO();
		itemDTO.setItemCode(296);		
		itemDTO.setPriceZoneId(10);
		itemKey = PRCommonUtil.getItemKey(itemDTO);
		itemDataMap.put(itemKey, itemDTO);
		
		storeItems = new HashMap<ItemKey, PRItemDTO>();
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(296);		
		itemDTO.setPriceZoneId(10);
		itemKey = PRCommonUtil.getItemKey(itemDTO);
		storeItems.put(itemKey, itemDTO);
		itemDataMapStore.put(6124, storeItems);
		storeItemsNotInCache.put(itemKey, itemDTO);
		itemDataMapStoreNotInCache.put(6124, storeItemsNotInCache);
		
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(4064);	
//		itemDTO.setPriceZoneId(289);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		itemDataMap.put(itemKey, itemDTO);
//		
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(4065);	
//		itemDTO.setPriceZoneId(289);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		itemDataMap.put(itemKey, itemDTO);
//		
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(4180);		
//		itemDTO.setPriceZoneId(66);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		itemDataMap.put(itemKey, itemDTO);
//		
//		//Item Data Map Store
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(296);		
//		itemDTO.setPriceZoneId(289);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		storeItems.put(itemKey, itemDTO);
//		itemDataMapStore.put(1718, storeItems);
		
//		storeItems = new HashMap<ItemKey, PRItemDTO>();
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(4064);	
//		itemDTO.setPriceZoneId(289);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		storeItems.put(itemKey, itemDTO);
//		itemDataMapStore.put(1650, storeItems);
//		
//		storeItems = new HashMap<ItemKey, PRItemDTO>();
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(4065);	
//		itemDTO.setPriceZoneId(289);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		storeItems.put(itemKey, itemDTO);
//		itemDataMapStore.put(1564, storeItems);
//		
//		storeItems = new HashMap<ItemKey, PRItemDTO>();
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(4180);		
//		itemDTO.setPriceZoneId(66);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		storeItems.put(itemKey, itemDTO);
//		itemDataMapStore.put(1620, storeItems);
		
//		pricingEngineDAO.getCostDataOptimized(conn, 50, inputDTO, curCalDTO, resetCalDTO, 0, itemDataMap,
//				itemDataMapStore, itemDataMapStoreNotInCache, fetchZoneData, strategyZoneNos, priceZoneStores);
//		
	}
	
	private void getPriceForStore(){
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		RetailCalendarDTO resetCalDTO = null; //new RetailCalendarDTO();
		
		 
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();		
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreNotInCache = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> storeItemsNotInCache = new HashMap<ItemKey, PRItemDTO>();
		ItemKey itemKey;
		
		
		curCalDTO.setCalendarId(5038);
		//resetCalDTO.setCalendarId(835);
		
		PRItemDTO prItemDTO = new PRItemDTO();		
		prItemDTO.setItemCode(296);
		itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		storeItemsNotInCache.put(itemKey, prItemDTO);
		itemDataMapStoreNotInCache.put(6124, storeItemsNotInCache);
		itemDataMapStoreNotInCache.put(6124, storeItemsNotInCache);
		
		
		storeItems.put(itemKey, prItemDTO);
		itemDataMapStore.put(6124, storeItems);
		itemDataMapStore.put(6124, storeItems);
	}
	
	private RetailCalendarDTO getCurCalDTO(RetailCalendarDAO retailCalendarDAO) throws GeneralException {
		RetailCalendarDTO curCalDTO;
		curCalDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);

		return curCalDTO;
	}
	
	private void getCostForStore() throws GeneralException{
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		RetailCalendarDTO resetCalDTO = new RetailCalendarDTO();
		int costHistory = 8;
		int locationId = 1054;
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		
		HashMap<ItemKey, PRItemDTO> itemDataMap  = new HashMap<ItemKey, PRItemDTO>();
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();		
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreNotInCache = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> storeItemsNotInCache = new HashMap<ItemKey, PRItemDTO>();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		ItemKey itemKey;
		
		inputDTO.setLocationLevelId(6);
		inputDTO.setLocationId(261);
		
		inputDTO.setProductLevelId(4);
		inputDTO.setProductId(249);
		
		//curCalDTO = getCurCalDTO(retailCalendarDAO);
		curCalDTO.setCalendarId(855);
		resetCalDTO.setCalendarId(825);
		
		PRItemDTO prItemDTO = new PRItemDTO();		
		prItemDTO.setItemCode(145637);
		itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		storeItemsNotInCache.put(itemKey, prItemDTO);
		itemDataMapStoreNotInCache.put(locationId, storeItemsNotInCache);
		
		itemDataMapStore.put(locationId, storeItems);
		
		itemDataMap.put(itemKey, prItemDTO);
	}
	
	public void intialSetup() {
		initialize();
	}
	
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

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
