package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.ProductService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class GetStrategy {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	private String chainId;
	private int divisionId = 0;
	
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
	
	
	public static void main(String[] args) throws GeneralException, Exception, OfferManagementException {
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("recommendation.properties");
		GetStrategy getStrategy = new GetStrategy();
		getStrategy.intialSetup();
		//getStrategy.GetStrategyDefinition();		
		//getStrategy.setStrategyForEachItem();
		//getStrategy.testDSDRecommendZonePrice();
		//getStrategy.getDSDItem();
		getStrategy.getStrategyDetail();

	}
	
	 
	public void getStrategyDetail() throws GeneralException, OfferManagementException{
		long tStartTime = System.currentTimeMillis();
		long tEndTime = System.currentTimeMillis();	
		PRStrategyDTO actualStrategy, expStrategy;
		
		StrategyDAO strategyDAO = new StrategyDAO();
		tStartTime = System.currentTimeMillis();
		actualStrategy = strategyDAO.getStrategyDefinition(conn, 24);
		tEndTime = System.currentTimeMillis();	
		logger.debug("^^^ Time -- New Strategy Function" + ((tEndTime - tStartTime)/1000) + " s ^^^");
//		
//		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
//		tStartTime = System.currentTimeMillis();
//		expStrategy = pricingEngineDAO.getStrategyDefinition(conn, 9);
//		tEndTime = System.currentTimeMillis();
//		logger.debug("^^^ Time -- Old Strategy Function" + ((tEndTime - tStartTime) / 1000) + " s ^^^");
		
		 
		
	}
	
	private void setStrategyForEachItem() throws GeneralException, Exception{
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		PricingEngineWS pricingEngineWS = new PricingEngineWS();
		HashMap<Integer, PRItemDTO> itemDataMap = new HashMap<Integer, PRItemDTO>();
		HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> retLirMap = new HashMap<Integer, HashMap<Integer, ArrayList<Integer>>>();
		//HashMap<String, HashMap<String, PRStrategyDTO>> strategyMap;
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap;
		HashMap<Integer, HashMap<Integer, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<Integer, PRItemDTO>>();
		
		inputDTO = prepareInputDTO();
		 
		HashMap<Integer,String> itemsForTesting = new HashMap<Integer,String>();
		setTestItems(itemsForTesting);
		
		//ArrayList<PRItemDTO> itemList = pricingEngineDAO.getAllItems(conn, inputDTO);
		ArrayList<PRItemDTO> itemList= new ArrayList<PRItemDTO>();
		ArrayList<PRItemDTO> tItemList = new ArrayList<PRItemDTO>();
		for(PRItemDTO itemDTO : itemList){	
			if(itemsForTesting.get(itemDTO.getItemCode()) != null){
				tItemList.add(itemDTO);
			}
		}
		
		for(PRItemDTO itemDTO : tItemList){		 
			if(itemsForTesting.get(itemDTO.getItemCode()) != null){
				itemDTO.setChildLocationLevelId(inputDTO.getLocationLevelId());
				itemDTO.setChildLocationId(inputDTO.getLocationId());
				itemDataMap.put(itemDTO.getItemCode(), itemDTO);
			}
		}
		//retLirMap = pricingEngineDAO.populateRetLirDetailsInMap(tItemList);
		//strategyMap = pricingEngineDAO.getAllStrategies(conn, inputDTO);	
//		HashMap<String, HashMap<String, PRStrategyDTO>> strategyMapStore = 
//				pricingEngineDAO.getAllStrategiesForStoresUnderZone(conn, inputDTO);
//		pricingEngineWS.getItemsAtStoreLevel(conn, inputDTO, strategyMapStore, itemDataMapStore, retLirMap, 
//				itemDataMap, pricingEngineDAO);
//		HashMap<String, ArrayList<Integer>> productListMap = new ProductGroupDAO().getProductListForProducts(conn, 
//				inputDTO.getProductLevelId(), inputDTO.getProductId());
//		
//		pricingEngineWS.getStrategiesForEachItem(conn, inputDTO, strategyMap, itemDataMap, pricingEngineDAO, 
//				retLirMap, productListMap);	
//		applySameStrategyForLigMember(retLirMap, itemDataMap);		
//		
//		pricingEngineWS.getSrategiesAtStoreForEachItem(conn, strategyMapStore, itemDataMapStore, itemDataMap, 0, pricingEngineDAO,
//				strategyMap, retLirMap, inputDTO, productListMap);
//		for(Map.Entry<Integer, HashMap<Integer, PRItemDTO>> outEntry : itemDataMapStore.entrySet()){
//			applySameStrategyForLigMember(retLirMap, outEntry.getValue());
//		}
	}
	
	private void applySameStrategyForLigMember(HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> retLirMap,
			HashMap<Integer, PRItemDTO> itemDataMap) {
		// Loop through each lig
		for (Map.Entry<Integer, HashMap<Integer, ArrayList<Integer>>> lirIdMap : retLirMap.entrySet()) {
			PRStrategyDTO strategyDTO = null;
			// Lig Map
			HashMap<Integer, ArrayList<Integer>> ligMap = lirIdMap.getValue();

			// Check if item level strategy exists
			strategyDTO = getItemOrKVILevelStrategy(ligMap, itemDataMap, "ITEM");

			// If there is no item level strategy, then look for KVI level
			// strategy
			if (strategyDTO == null) {
				strategyDTO = getItemOrKVILevelStrategy(ligMap, itemDataMap, "KVI");
			}

			// If there is item level or kvi level present
			if (strategyDTO != null) {
				// Update all members & Lig representing item with item level
				// strategy
				for (Map.Entry<Integer, ArrayList<Integer>> representingItem : ligMap.entrySet()) {
					for (Integer ligMember : representingItem.getValue()) {
						if (itemDataMap.get(ligMember) != null) {
							PRItemDTO itemDTO = itemDataMap.get(ligMember);
							itemDTO.setStrategyDTO(strategyDTO);
							logger.debug("Item Code: " + itemDTO.getItemCode() + ",Strategy Id: " + strategyDTO.getStrategyId());
						}
					}
					if (itemDataMap.get(representingItem.getKey()) != null) {
						PRItemDTO itemDTO = itemDataMap.get(representingItem.getKey());
						itemDTO.setStrategyDTO(strategyDTO);
						logger.debug("Item Code: " + itemDTO.getItemCode() + ",Strategy Id: " + strategyDTO.getStrategyId());
					}
				}
			}
		}
	}
	
	private PRStrategyDTO getItemOrKVILevelStrategy(HashMap<Integer, ArrayList<Integer>> ligMap, HashMap<Integer, PRItemDTO> itemDataMap,
			String key) {
		PRStrategyDTO strategyDTO = null;

		for (Map.Entry<Integer, ArrayList<Integer>> representingItem : ligMap.entrySet()) {
			for (Integer ligMember : representingItem.getValue()) {
				if (itemDataMap.get(ligMember) != null) {
					PRItemDTO itemDTO = itemDataMap.get(ligMember);
					if (key.equals("ITEM")) {
						if (itemDTO.isItemLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					} else {
						if (itemDTO.isCheckListLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					}

				}
			}

			// Check representing item also
			if (strategyDTO == null) {
				if (itemDataMap.get(representingItem.getKey()) != null) {
					PRItemDTO itemDTO = itemDataMap.get(representingItem.getKey());
					if (key.equals("ITEM")) {
						if (itemDTO.isItemLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					} else {
						if (itemDTO.isCheckListLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					}
				}
			}
		}

		return strategyDTO;
	}
	
	private void setTestItems(HashMap<Integer,String> itemsForTesting){
		 
		//Pre price
		itemsForTesting.put(170952, "");
		itemsForTesting.put(171046, "");
		itemsForTesting.put(941889, "");
		itemsForTesting.put(160105, "");
		
		//KVI
		itemsForTesting.put(15749, "");
		itemsForTesting.put(950735, "");
		itemsForTesting.put(41076, "");
		itemsForTesting.put(977211, "");
		itemsForTesting.put(996229, "");
		itemsForTesting.put(158118, "");
		itemsForTesting.put(71768, "");
		itemsForTesting.put(15749, "");
		itemsForTesting.put(131247, "");
		itemsForTesting.put(160876, "");
	}
		 	
	public void GetStrategyDefinition(){
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		
		try {
			HashMap<Integer,String> test = null;
			
			test.get(1);
			
		} catch (Exception e) {
			//logger.error("Error in getCompDataForMultiComp() -- " + e.toString(), e);
			logger.error("", e);
		}
		
		
		try {
			PRStrategyDTO strategyDTO = null;
			//strategyDTO = pricingEngineDAO.getStrategyDefinition(conn, 1494);
			//strategyDTO = pricingEngineDAO.getStrategyDetails(conn, 9);
			
			PRStrategyDTO inputDTO = prepareInputDTO();	
			inputDTO.setLocationLevelId(1);
			inputDTO.setLocationId(1715);
			inputDTO.setProductLevelId(1);
			inputDTO.setProductId(84725);
//			pricingEngineDAO.getAllStrategiesForStoresUnderZone(conn,inputDTO);
			int a = 10;
			a= a++;
			
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private PRStrategyDTO prepareInputDTO() throws GeneralException{
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		
		RetailCalendarDTO runForWeek = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
		inputDTO.setStartCalendarId(runForWeek.getCalendarId());
		inputDTO.setEndCalendarId(runForWeek.getCalendarId());
		inputDTO.setStartDate(runForWeek.getStartDate());
		inputDTO.setEndDate(runForWeek.getEndDate());
		inputDTO.setLocationLevelId(6);
		inputDTO.setLocationId(66); // Zone - 693
		inputDTO.setProductLevelId(4); // Category
		inputDTO.setProductId(149); // Trash bag(149), peanut butter(264)	
		return inputDTO;
	}
	private void testDSDRecommendZonePrice() throws GeneralException{
		PricingEngineWS pricingEngineWS = new PricingEngineWS();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		//HashMap<String, HashMap<String, PRStrategyDTO>> strategyMap;
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap;
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		PRItemDTO itemDTO = new PRItemDTO();
		ProductService service = new ProductService();
		HashMap<Integer, PRItemDTO> itemDataMap = new HashMap<Integer, PRItemDTO>();
		
		// Trash bag(149), peanut butter(264)
		itemDTO.setCategoryProductId(149);
		itemDTO.setPortfolioProductId(658);
		itemDTO.setDeptProductId(4);
		
		inputDTO = prepareInputDTO();		
		//strategyMap = pricingEngineDAO.getAllStrategies(conn, inputDTO);
//		pricingEngineDAO.getAllItems(conn, inputDTO);
		//HashMap<Integer, Integer> parentChildRelationMap = service.getProductLevelRelationMap(conn, Constants.CATEGORYLEVELID);
//		pricingEngineWS.setChainId("74");
//		pricingEngineWS.setDivisionId(71);
		
		itemDataMap.put(1234, itemDTO);
//		pricingEngineWS.isRecommendZonePriceForDSD(conn, inputDTO, itemDataMap, strategyMap, pricingEngineDAO);
	}
	
	private void getDSDItem() throws GeneralException, Exception{
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		List<PRItemDTO> dsdItems;
		long tStartTime = System.currentTimeMillis();
		long tEndTime = System.currentTimeMillis();
		
		//HashMap<Integer, Integer> stateOfStores = pricingEngineDAO.getStateOfStores(conn, 66);
		
		inputDTO.setLocationLevelId(6);
		inputDTO.setLocationId(66);
		inputDTO.setProductLevelId(4);
		inputDTO.setProductId(149);
		
//		tStartTime = System.currentTimeMillis();
//		dsdItems = pricingEngineDAO.getDSDItems(conn, inputDTO);
//		tEndTime = System.currentTimeMillis();
		logger.info("^^^ Time -- (getDSDItem) --> " + ((tEndTime - tStartTime)/1000) + " s ^^^");
	}
}
