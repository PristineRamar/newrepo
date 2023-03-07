package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
//import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRZoneStoreReccommendationFlag;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
//import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.CheckListService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.StrategyService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
//import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class FindStrategyOfItemJUnitTest  {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	private static final String CHAIN_ID = "74";
	private static final int DIVISION_ID = 7;
	private static final int PRICE_ZONE_ID_66 = 66;
	private PricingEngineDAO pricingEngineDAOMock;
	private ProductGroupDAO productGroupDAOMock;
	private HashMap<Integer, Integer> productParentChildRelationMap; 
	private PRZoneStoreReccommendationFlag zoneStoreRecFlag;
	ObjectMapper mapper = new ObjectMapper();
	HashMap<String, ArrayList<Integer>> productListMap = new HashMap<String, ArrayList<Integer>>();
	
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
 
	
	@Before
	public void setup() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
//		intialSetup();
		
		productParentChildRelationMap = new HashMap<Integer, Integer>();
		productParentChildRelationMap.put(1, 0);
		productParentChildRelationMap.put(3, 1);
		productParentChildRelationMap.put(4, 3);
		productParentChildRelationMap.put(5, 4);
		
		pricingEngineDAOMock = EasyMock.createMock(PricingEngineDAO.class);
		
//		pricingEngineDAOMock = EasyMock
//		         .createMockBuilder(PricingEngineDAO.class) //create builder first
//		         .addMockedMethod("getPriceCheckListInfo")       // tell EasyMock to mock which method
//		         .createMock();
		
		productGroupDAOMock = EasyMock
		         .createMockBuilder(ProductGroupDAO.class) //create builder first
		         .addMockedMethod("getProductListForProducts")
		         .createMock();
	}

	/**
	 * Item - from Milk of Zone(693/66) 
	 * Strategy(1) -> Product - Milk(Category), Location - 693/66(Zone) 
	 * Expected: Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO.setPriceCheckListId(10);
		
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 172, "11/23/2014", null, false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 1, Actual Strategy Id: " + prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}

	/**
	 * Item - from Soda Pop Tonic of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(2) -> Product - Milk(Category), Location - 693/66(Zone) 
	 * Expected: Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();

		PRItemDTO prItemDTO = TestHelper.setItemDTO(56002, 1073, 1074, 202, 957, 4, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO.setPriceCheckListId(10);
		
		
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 99, 0, "12/28/2014", "01/03/2014", true, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 1, Actual Strategy Id: " + prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals(true, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}

	/**
	 * Item - from Soda Pop Tonic of Zone(690/65) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain) 
	 * Strategy(2) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(3) -> Product - Milk(Category), Location - 693/66(Zone) 
	 * Expected: Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO = TestHelper.setItemDTO(56002, 1073, 1074, 202, 957, 4, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);

		inputDTO = TestHelper.getStrategy(0, 6, 65, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO.setPriceCheckListId(10);
				
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 99, 0, "12/28/2014", "01/03/2014", true, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 1, Actual Strategy Id: " + prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Item - from Soda Pop Tonic of Zone(690/65) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - Soda Pop Tonic(Category), Location - 7(Division)  
	 * Strategy(4) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Milk(Category), Location - 693/66(Zone) 
	 * Expected: Strategy(3)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO = TestHelper.setItemDTO(56002, 1073, 1074, 202, 957, 4, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);

		inputDTO = TestHelper.getStrategy(0, 6, 65, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO.setPriceCheckListId(10);
		
		strategyDTO = TestHelper.getStrategy(1, Constants.CHAIN_LEVEL_ID, Integer.valueOf(CHAIN_ID), 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, Constants.DIVISION_LEVEL_ID, DIVISION_ID, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, Constants.DIVISION_LEVEL_ID, DIVISION_ID, 4, 202, "12/28/2014", "01/03/2014", true, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, Constants.ZONE_LEVEL_ID, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, Constants.ZONE_LEVEL_ID, 66, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 3, Actual Strategy Id: " + prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 3, prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals(true, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(2) -> Product - Trash Bag(Category), Location - 693/66(Zone), Item List (10)
	 * Expected: Item 1 - Strategy(2), Item 2 - Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO1.setPriceCheckListId(10);
		
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, 10, -1, -1);		 
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Item List Testing - Item List is defined at higher level
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74 (Chain) , Item List (10) 
	 * Strategy(2) -> Product - Trash Bag(Category), Location - 693/66(Zone)
	 * Expected: Item 1 - Strategy(1), Item 2 - Strategy(2)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase25() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO1.setPriceCheckListId(10);
		
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 00, "12/28/2014", "01/03/2014", false, 10, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);		 
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		assertEquals("Strategy For Item is incorrect", 1, prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Item List Testing - Item List is defined at higher level and at category level
	 * Category level strategy to be picked
	 * Item - Trash Bag of Zone(693/66)
	 * Strategy(3) -> Product - All Products, Location - 74 (Chain)
	 * Strategy(1) -> Product - All Products, Location - 74 (Chain) , Item List (10) 
	 * Strategy(2) -> Product - Trash Bag(Category), Location - 693/66(Zone), Item List (10) 
	 * Expected: Item 1 - Strategy(2), Item 2 - Strategy(3)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase26() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO1.setPriceCheckListId(10);
		
		strategyDTO = TestHelper.getStrategy(3, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, 10, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, 10, -1, -1);		 
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 3, prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Item List Testing - Item List is defined at higher level and at category level
	 * Category level strategy to be picked
	 * Item - Trash Bag of Zone(693/66)
	 * Strategy(3) -> Product - All Products, Location - 74 (Chain)
	 * Strategy(1) -> Product - All Products, Location - 74 (Chain) , Item List (11) 
	 * Strategy(2) -> Product - Trash Bag(Category), Location - 693/66(Zone), Item List (10) 
	 * Expected: Item 1 - Strategy(2), Item 2 - Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase27() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO1.setPriceCheckListId(10);
		prItemDTO2.setPriceCheckListId(11);
		
		strategyDTO = TestHelper.getStrategy(3, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, 11, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, 10, -1, -1);		 
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Item - from Trash Bag of Zone(693/66), Item Level
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone)
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - 56002(Item), Location - 693/66(Zone) 
	 * Expected: Item 1 - Strategy(5)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase6() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		
		prItemDTO1.setPriceCheckListId(10);
		
		//Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 1, 56002, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 5, Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	/**
	 * Test - Store Level Strategy
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone)
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 10521(Store) 
	 * Expected: Item 1 of Store 10521 - Strategy(5)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase7() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 32, 2);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 10521, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		
		prItemDTO1.setPriceCheckListId(10);
		
		//Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 5, 10521, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 5, Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone)
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 10521(Store) 
	 * Expected: Item 1 of Store 1723 - Strategy(4)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase8() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 11, 3);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		
		prItemDTO1.setPriceCheckListId(10);
		
		//Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 5, 10521, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 4, Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 4, prItemDTO1.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy, Item List
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone)
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 10521(Store)
	 * Strategy(6) -> Product - Trash Bag(Category), Location - 10521(Store), Item List  
	 * Expected: Item 1 of Store 1723 - Strategy(6), Item 2 of Store 1723 - Strategy(5)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase9() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 2);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(6, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, 10, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 6, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 6, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO2.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy, Vendor
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone), Vendor 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Expected: Item 1 of Store 1723 - Strategy(3), Item 2 of Store 1723 - Strategy(0)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase10() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 3, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 3, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 0, " +
				"Actual Strategy Id: " + (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
		assertEquals("Strategy For Item is incorrect", 0, (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
	}
	
	/**
	 * Test - Store Level Strategy, Vendor
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division), Vendor 
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone)
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Expected: Item 1 of Store 1723 - Strategy(2)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase28() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 2, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		
	}
	
	/**
	 * Test - Store Level Strategy, Item Level
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - 56002(Item), Location - 1723(Store)
	 * Expected: Item 1 of Store 1723 - Strategy(3), Item 2 of Store 1723 - Strategy(0)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase11() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 11);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 11);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 5, 1723, 1, 56002, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 0, " +
				"Actual Strategy Id: " + (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
		assertEquals("Strategy For Item is incorrect", 0, (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
	}
	
	/**
	 * Test - Store Level Strategy, State
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), State 
	 * Expected: Item 1 of Store 1723 - Strategy(5), Item 2 of Store 1723 - Strategy(5)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase12() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 10);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 14, 10);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, 10);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO2.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy, State
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain), State 
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone)
	 * Expected: Item 1 of Store 1723 - Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase29() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 10);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, 10);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 1, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO1.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy, Vendor & State
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), State
	 * Strategy(6) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor, State  
	 * Strategy(7) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor
	 * Expected: Item 1 of Store 1723 - Strategy(5), Item 2 of Store 1723 - Strategy(6)
	 * , Item 3 of Store 1723 - Strategy(7)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase13() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);
		
		PRItemDTO prItemDTO3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO3);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, 9);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(6, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, 12, 9);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(7, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 6, " +
				"Actual Strategy Id: " + prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 6, prItemDTO2.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO3.getItemCode() + " Strategy Id: 7, " +
				"Actual Strategy Id: " + prItemDTO3.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 7, prItemDTO3.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy, Vendor & State (Preference to Vendor)
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), State
	 * Strategy(7) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor
	 * Expected: Item 1 of Store 1723 - Strategy(7), Item 2 of Store 1723 - Strategy(7)
	 * , Item 3 of Store 1723 - Strategy(5)
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase14() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);
		
		PRItemDTO prItemDTO3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 16, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO3);
		
		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, 9);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(7, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 7, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 7, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 7, " +
				"Actual Strategy Id: " + prItemDTO2.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 7, prItemDTO2.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO3.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO3.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO3.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Test - Store Level Strategy, Vendor & Item List
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor, Item List
	 * Expected: Item 1 of Store 1723 - Strategy(5), Item 2 of Store 1723 - Strategy(0)
	 * , Item 3 of Store 1723 - Strategy(0)
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase15() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);
		
		PRItemDTO prItemDTO3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO3);
		
		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, 10, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(6, 6, 66, 4, 149, "11/28/2014", null, false, 10, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);
		
		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 0, " +
				"Actual Strategy Id: " + (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
		assertEquals("Strategy For Item is incorrect", 0, (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
		
		logger.debug("Expected Item Code : " + prItemDTO3.getItemCode() + " Strategy Id: 0, " +
				"Actual Strategy Id: " + (prItemDTO3.getStrategyDTO() == null ? 0 : prItemDTO3.getStrategyDTO().getStrategyId()));
		assertEquals("Strategy For Item is incorrect", 0, (prItemDTO3.getStrategyDTO() == null ? 0 : prItemDTO3.getStrategyDTO().getStrategyId()));
	}
	
	/**
	 * Test - Store Level Strategy, State & Item List
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), State, Item List
	 * Expected: Item 1 of Store 1723 - Strategy(5), Item 2 of Store 1723 - Strategy(0)
	 * , Item 3 of Store 1723 - Strategy(0)
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase16() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 12);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);
		
		PRItemDTO prItemDTO3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 12);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO3);
		
		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, 10, -1, 12);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 5, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 5, prItemDTO1.getStrategyDTO().getStrategyId());
		
		logger.debug("Expected Item Code : " + prItemDTO2.getItemCode() + " Strategy Id: 0, " +
				"Actual Strategy Id: " + (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
		assertEquals("Strategy For Item is incorrect", 0, (prItemDTO2.getStrategyDTO() == null ? 0 : prItemDTO2.getStrategyDTO().getStrategyId()));
		
		logger.debug("Expected Item Code : " + prItemDTO3.getItemCode() + " Strategy Id: 0, " +
				"Actual Strategy Id: " + (prItemDTO3.getStrategyDTO() == null ? 0 : prItemDTO3.getStrategyDTO().getStrategyId()));
		assertEquals("Strategy For Item is incorrect", 0, (prItemDTO3.getStrategyDTO() == null ? 0 : prItemDTO3.getStrategyDTO().getStrategyId())); 
	}
	
	/***
	 * Product List Test Cases
	 * There is a strategy defined at product list (with 2 categories) for a zone
	 * Also there is strategy at All Products for that zone
	 * The 2 categories must get the strategies defined at product list rather than one defined
	 * at zone level
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase30() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> productList = new ArrayList<Integer>();
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 131, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 0, 0, 131, 0, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		// Strategies
		//Strategy at All products - Zone
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		//Strategy at Product List - Zone
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 21, 8396, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		productList.add(8396);
		productListMap.put("4-131", productList);
//		//Mock Product List
		EasyMock.expect(
				productGroupDAOMock.getProductListForProducts(conn, inputDTO.getProductLevelId(),
						inputDTO.getProductId())).andReturn(productListMap);
		EasyMock.replay(productGroupDAOMock);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 2, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		
	}
	
	/***
	 * Product List Test Cases
	 * There is a strategy defined at product list (with 2 categories) for a zone
	 * Also there is strategy for one of the Category for that zone
	 * Expected to get Strategy defined at zone level
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase31() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> productList = new ArrayList<Integer>();
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 131, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 0, 0, 131, 0, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		// Strategies
		//Strategy at Product List - Zone
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 21, 8396, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 4, 131, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		productList.add(8396);
		productListMap.put("4-131", productList);
//		//Mock Product List
		EasyMock.expect(
				productGroupDAOMock.getProductListForProducts(conn, inputDTO.getProductLevelId(),
						inputDTO.getProductId())).andReturn(productListMap);
		EasyMock.replay(productGroupDAOMock);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 2, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		
	}
	
	/***
	 * Product List Test Cases
	 * There is a strategy defined at product list (with 2 categories) for a zone
	 * Expected to get Strategy defined at product list
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase32() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> productList = new ArrayList<Integer>();
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 131, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 0, 0, 131, 0, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		// Strategies
		//Strategy at Product List - Zone
		strategyDTO = TestHelper.getStrategy(10, 6, 66, 21, 8396, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		productList.add(8396);
		productListMap.put("4-131", productList);
//		//Mock Product List
		EasyMock.expect(
				productGroupDAOMock.getProductListForProducts(conn, inputDTO.getProductLevelId(),
						inputDTO.getProductId())).andReturn(productListMap);
		EasyMock.replay(productGroupDAOMock);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 10, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 10, prItemDTO1.getStrategyDTO().getStrategyId());
		
	}
	
	/**
	 * Test - Check if Store Level Flag, Vendor Level Flag, State Level Flag is set properly
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), State
	 * Strategy(6) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor  
	 * Expected: State and Vendor Flag - True
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase17() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		
		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 10);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 10, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, 10);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(6, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, 10, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		zoneStoreRecFlag.isCheckIfStoreLevelStrategyPresent = true;
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		 
		assertEquals("Incorrect Store Level Flag", false, zoneStoreRecFlag.isStoreLevelStrategyPresent);
		assertEquals("Incorrect Vendor Level Flag", true, zoneStoreRecFlag.isVendorLevelStrategyPresent);
		assertEquals("Incorrect State Level Flag", true, zoneStoreRecFlag.isStateLevelStrategyPresent);
	}
	
	/**
	 * Test - Check if Store Level Flag, Vendor Level Flag, State Level Flag is set properly
	 * Item - from Trash Bag of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 74/1(Chain)
	 * Strategy(2) -> Product - All Products, Location - 7(Division)
	 * Strategy(3) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(4) -> Product - Trash Bag(Category), Location - 693/66(Zone) 
	 * Strategy(5) -> Product - Trash Bag(Category), Location - 693/66(Zone), State
	 * Strategy(6) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor, State  
	 * Strategy(7) -> Product - Trash Bag(Category), Location - 693/66(Zone), Vendor
	 * Expected: Item 1 of Store 1723 - Strategy(5), Item 2 of Store 1723 - Strategy(6)
	 * , Item 3 of Store 1723 - Strategy(7)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase18() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = false;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		PRItemDTO prItemDTO2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO2);
		
		PRItemDTO prItemDTO3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO3);

		prItemDTO1.setPriceCheckListId(10);
		
		// Input Strategy
		inputDTO = TestHelper.getStrategy(0, 5, 1723, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		// Strategies
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 2, 7, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(4, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, 9);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(6, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, 12, 9);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(7, 6, 66, 4, 149, "12/28/2014", "01/03/2014", false, -1, 12, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		zoneStoreRecFlag.isCheckIfStoreLevelStrategyPresent = true;
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		assertEquals("Incorrect Store Level Flag", false, zoneStoreRecFlag.isStoreLevelStrategyPresent);
		assertEquals("Incorrect Vendor Level Flag", true, zoneStoreRecFlag.isVendorLevelStrategyPresent);
		assertEquals("Incorrect State Level Flag", true, zoneStoreRecFlag.isStateLevelStrategyPresent);
	}
	
	/**
	 * Test - If zone loc-price is copied to store
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase21() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		PRStrategyDTO strategyDTO;
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		
		// Zone item map
		PRItemDTO zoneItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategyDTO, 1, 2.19);
		zoneItem1.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem1);

		PRItemDTO zoneItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(2, 6, 64, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategyDTO, 1, 2.29);
		zoneItem2.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem2);

		PRItemDTO zoneItem3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem3);
		
		// Store item map
		PRItemDTO storeItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		storeItem1.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem1);

		PRItemDTO storeItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(6, 6, 65, 4, 179, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		//storeItem2.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem2);
		
		itemDataMapStore.put(1054, storeItems);
		
		strategyService.copyPreLocMinMaxFromZoneToStore(itemDataMap, itemDataMapStore);
		
		try {
			String item1StrategyAct = mapper.writeValueAsString(storeItem1.getStrategyDTO());
			String item1StrategyExp = mapper.writeValueAsString(zoneItem1.getStrategyDTO());
			assertEquals("Store Item 1 Strategy Not Matching", item1StrategyExp, item1StrategyAct);
			
			String item2StrategyAct = mapper.writeValueAsString(storeItem2.getStrategyDTO());
			String item3StrategyExp = mapper.writeValueAsString(zoneItem2.getStrategyDTO());
			assertEquals("Store Item 2 Strategy Not Matching", item3StrategyExp, item2StrategyAct);
			
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test - If loc-price of store is retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase22() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		PRStrategyDTO strategyDTO;
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		
		// Zone item map
		PRItemDTO zoneItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategyDTO, 1, 2.19);
		zoneItem1.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem1);

		PRItemDTO zoneItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(2, 6, 64, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategyDTO, 1, 2.29);
		zoneItem2.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem2);

		// Store item map
		PRItemDTO storeItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategyDTO, 1, 2.49);
		storeItem1.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem1);

		PRItemDTO storeItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(6, 6, 65, 4, 179, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategyDTO, 1, 2.39);
		storeItem2.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem2);
		
		itemDataMapStore.put(1054, storeItems);
		
		strategyService.copyPreLocMinMaxFromZoneToStore(itemDataMap, itemDataMapStore);
		
		try {
			String item1StrategyAct = mapper.writeValueAsString(storeItem1.getStrategyDTO());
			String item1StrategyExp = mapper.writeValueAsString(storeItem1.getStrategyDTO());
			assertEquals("Store Item 1 Strategy Not Matching", item1StrategyExp, item1StrategyAct);
			
			String item2StrategyAct = mapper.writeValueAsString(storeItem2.getStrategyDTO());
			String item2StrategyExp = mapper.writeValueAsString(storeItem2.getStrategyDTO());
			assertEquals("Store Item 2 Strategy Not Matching", item2StrategyExp, item2StrategyAct);
			
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test - If zone min/max constraint alone is copied to store
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase23() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		PRStrategyDTO strategyDTO;
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();

		// Zone item map
		PRItemDTO zoneItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setMinMaxConstraint(strategyDTO, 2.10, 2.19, 1);
		zoneItem1.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem1);

		PRItemDTO zoneItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(2, 6, 64, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setMinMaxConstraint(strategyDTO, 1.89, 1.99, 1);
		zoneItem2.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem2);

		PRItemDTO zoneItem3 = TestHelper.setItemDTO(35513, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem3);

		// Store item map
		PRItemDTO storeItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		storeItem1.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem1);

		PRItemDTO storeItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(6, 6, 65, 4, 179, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		storeItem2.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem2);

		itemDataMapStore.put(1054, storeItems);

		strategyService.copyPreLocMinMaxFromZoneToStore(itemDataMap, itemDataMapStore);

		try {
			PRStrategyDTO item1Exp = storeItem1.getStrategyDTO();
			item1Exp.getConstriants().setMinMaxConstraint(
					zoneItem1.getStrategyDTO().getConstriants().getMinMaxConstraint());
			
			String item1StrategyAct = mapper.writeValueAsString(storeItem1.getStrategyDTO());
			String item1StrategyExp = mapper.writeValueAsString(item1Exp);
			assertEquals("Store Item 1 Strategy Not Matching", item1StrategyExp, item1StrategyAct);

			PRStrategyDTO item2Exp = storeItem2.getStrategyDTO();
			item2Exp.getConstriants().setMinMaxConstraint(
					zoneItem2.getStrategyDTO().getConstriants().getMinMaxConstraint());
			
			String item2StrategyAct = mapper.writeValueAsString(storeItem2.getStrategyDTO());
			String item3StrategyExp = mapper.writeValueAsString(item2Exp);
			assertEquals("Store Item 2 Strategy Not Matching", item3StrategyExp, item2StrategyAct);

		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test - If min/max of store is retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase24() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		PRStrategyDTO strategyDTO;
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> storeItems = new HashMap<ItemKey, PRItemDTO>();
		
		// Zone item map
		PRItemDTO zoneItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(1, 1, 74, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setMinMaxConstraint(strategyDTO, 2.10, 2.19, 1);
		zoneItem1.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem1);

		PRItemDTO zoneItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(2, 6, 64, 4, 149, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setMinMaxConstraint(strategyDTO, 1.89, 1.99, 1);
		zoneItem2.setStrategyDTO(strategyDTO);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, zoneItem2);

		// Store item map
		PRItemDTO storeItem1 = TestHelper.setItemDTO(56002, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, -1, 9);
		strategyDTO = TestHelper.getStrategy(5, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setMinMaxConstraint(strategyDTO, 3.10, 3.19, 1);
		storeItem1.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem1);

		PRItemDTO storeItem2 = TestHelper.setItemDTO(142117, 1035, 1036, 149, 658, 5, 0, 0, 10, "", false, 12, 9);
		strategyDTO = TestHelper.getStrategy(6, 6, 65, 4, 179, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		TestHelper.setMinMaxConstraint(strategyDTO, 2.89, 2.99, 1);
		storeItem2.setStrategyDTO(strategyDTO);
		storeItems = TestHelper.setItemDataMap(storeItems, storeItem2);
		
		itemDataMapStore.put(1054, storeItems);
		
		strategyService.copyPreLocMinMaxFromZoneToStore(itemDataMap, itemDataMapStore);
		
		try {
			String item1StrategyAct = mapper.writeValueAsString(storeItem1.getStrategyDTO());
			String item1StrategyExp = mapper.writeValueAsString(storeItem1.getStrategyDTO());
			assertEquals("Store Item 1 Strategy Not Matching", item1StrategyExp, item1StrategyAct);
			
			String item2StrategyAct = mapper.writeValueAsString(storeItem2.getStrategyDTO());
			String item2StrategyExp = mapper.writeValueAsString(storeItem2.getStrategyDTO());
			assertEquals("Store Item 2 Strategy Not Matching", item2StrategyExp, item2StrategyAct);
			
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * If Lead Zone and Dependent Zone both has WIC items Strategy
	 * Dependent Zone Strategy needs to be considered. 
	 * Strategy defined at Item level
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase33() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId = 8, leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		ItemKey itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		//Price check list id for both Lead and Dependent Zone
		TestHelper.getCheckList(allCheckList,1,5,6,6,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList,2,5,6,5,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		//Strategy for both Lead and Dependent Zone
		strategyDTO = TestHelper.getStrategy(1, 6, 6, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, 5);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Dependent Zone Recommendation
	 * Check List - WIC in both Dependent and Lead Zone
	 * Strategy - Strategy defined at Lead zone level
	 * Result - Lead zone strategy needs to be considered
	 */
	@Test
	public void testCase34() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId = 8, leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		ItemKey itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		//Price check list id for both Lead and Dependent Zone
		TestHelper.getCheckList(allCheckList,2,5,6,5,209821,PRConstants.NON_LIG_ITEM_INDICATOR);

		strategyDTO = TestHelper.getStrategy(2, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Dependent Zone Recommendation
	 * Check List - Same item in 2 different Zones
	 * Strategy - Strategies defined at zone level
	 * Result - Strategy needs to be considered based on check list with respect to processing zone
	 */
	@Test
	public void testCase35() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId = 8, leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		ItemKey itemKey = new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		inputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		//Price check list id for both Lead and Dependent Zone
		TestHelper.getCheckList(allCheckList,1,5,6,6,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList,2,5,6,5,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		//Strategy for both Lead and Dependent Zone
		strategyDTO = TestHelper.getStrategy(1, 6, 6, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,0,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - dependent zone
	 * Strategy- WIC item list of Lead and dependent zone, category & dependent division level
	 * Item in checklist- 1 WIC
	 * Expected output 	- Strategy defined for WIC item list of dependent zone 
	 */
	@Test
	public void testCase36() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		//Price check list id for dependent Zone
		TestHelper.getCheckList(allCheckList,2,5,6,6,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		//Strategy for WIC item list of Lead and dependent zone, category & dependent division level
		strategyDTO = TestHelper.getStrategy(1, 2, 7, 4, 172, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 6, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(3, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 3, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - dependent zone
	 * Strategy- category & dependent division level, category & lead division level, 1 WIC at lead zone level
	 * Item in checklist- 2 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase37() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// 2 WIC item Price check list id at dependent Zone level
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 6, 209822, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 6, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);

		// Strategy at dependent division level for WIC item check list
		strategyDTO = TestHelper.getStrategy(1, 2, 7, 4, 1723, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Strategy at lead division level for WIC item check list
		strategyDTO = TestHelper.getStrategy(2, 2, 8, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Strategy at lead zone level for WIC item check list
		strategyDTO = TestHelper.getStrategy(4, 6, 5, 1, 209821, "12/28/2015", "01/03/2015", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 4, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - dependent zone
	 * Strategy- WIC item list of Lead zone, chain level
	 * Item in checklist- 1 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone 
	 */
	@Test
	public void testCase38() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		//2 WIC item Price check list id at dependent Zone level 
		TestHelper.getCheckList(allCheckList,2,5,6,6,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList,3,5,6,6,209821,PRConstants.NON_LIG_ITEM_INDICATOR);
		
		//Strategy at dependent division level for WIC item check list
		strategyDTO = TestHelper.getStrategy(1, 1, 7, 4, 172, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		//Strategy at chain level for WIC item check list
		strategyDTO = TestHelper.getStrategy(2, 0, 50, 4, 172, "12/28/2014", "01/03/2014", false, -1,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		//Strategy at lead zone level for WIC item check list
		strategyDTO = TestHelper.getStrategy(4, 6, 5, 1, 209821, "12/28/2015", "01/03/2015", false, 2,-1,-1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 4, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - dependent zone
	 * Strategy- WIC item list of Lead and dependent zone
	 * Item in checklist- 1 WIC, KVI
	 * Expected output 	- Strategy defined for WIC item list of dependent zone
	 */
	
	@Test
	public void testCase39() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// Price check list id for normal item
		TestHelper.getCheckList(allCheckList, 1, 1, 6, 6, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		// Price check list id for WIC item
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 6, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 6, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - dependent zone
	 * Strategy- Defined at category level of dependent zone and lead zone
	 * Item in checklist- No
	 * Expected output 	-Strategy defined at lead zone
	 */
	
	@Test
	public void testCase40() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// Strategy for both Lead and Dependent Zone at category level
		strategyDTO = TestHelper.getStrategy(1, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - lead zone
	 * Strategy- WIC item list of Lead 
	 * Item in checklist- 3 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase41() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// Price check list id for 3 WIC item
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 5, 209822, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 5, 209824, PRConstants.NON_LIG_ITEM_INDICATOR);

		// Strategy for Lead Zone for WIC item
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - lead zone
	 * Strategy- WIC item list of Lead and dependent zone
	 * Item in checklist- 1 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase42() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// Price check list id for 3 WIC item
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);

		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 6, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - lead zone
	 * Strategy- WIC item list of Lead 
	 * Item in checklist- 1 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase43() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// Price check list id for 1 WIC item
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);

		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - lead zone
	 * Strategy- No strategy at WIC level
	 * Item in checklist- 1 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase44() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 6, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();
		// Price check list id for 3 WIC item
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);

		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(2, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - Dependent zone
	 * Strategy- strategy at lead WIC level and All product level
	 * Item in checklist- 2 WIC
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase45() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 1, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 6, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 4, 1, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 3, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - Dependent zone
	 * Strategy- strategy at lead WIC level and All product level at dependent zone
	 * Item in checklist- 3 WIC list and one other check list
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase46() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 6, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 4, 1, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 3, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - Lead zone
	 * Strategy- strategy at lead WIC level and All product level at lead zone
	 * Item in checklist- 3 WIC list and one other check list
	 * Expected output 	- Strategy defined for WIC item list of lead zone
	 */
	@Test
	public void testCase47() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=0;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 1, 5, 6, 6, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 4, 1, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 3, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 5, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Recommendation - Dependent zones
	 * Strategy- strategy at lead WIC level and strategy at dependent WIC level
	 * Item in checklist- 2 WIC list(Lead check list and dependent check list)
	 * Expected output 	- Strategy defined for WIC item list of dependent zone
	 */
	@Test
	public void testCase48() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 2, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 1, 209821, "12/28/2014", "01/03/2014", false, 3, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Item falls in multiple WIC 
	 * Recommendation for dependent zone 
	 * there is strategy for dependent zone and lead zone at WIC level and global level
	 * strategy of dependent zone
	 */
	@Test
	public void testCase49() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO strategyDTO2;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 2, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 1, 209821, "12/28/2014", "01/03/2014", false, 3, -1, -1);
		strategyDTO2 = TestHelper.getStrategy(3, 6, 4, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO2);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Item falls in multiple WIC 
	 * Recommendation for dependent zone 
	 * there is strategy for lead zone at WIC level
	 * strategy of lead zone
	 */
	@Test
	public void testCase50() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO strategyDTO2;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 2, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		strategyDTO2 = TestHelper.getStrategy(2, 6, 4, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
//		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO2);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Item falls in multiple WIC 
	 * Recommendation for dependent zone 
	 * there is strategy for lead zone at WIC level and other strategy at dependent zone level and Global strategy at lead zone
	 * strategy of lead zone
	 */
	@Test
	public void testCase51() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO strategyDTO2;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		
		// Strategy for both Lead and Dependent Zone for WIC item
		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 2, -1, -1);
		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		strategyDTO2 = TestHelper.getStrategy(3, 6, 5, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO2);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Item falls in multiple WIC 
	 * Recommendation for dependent zone 
	 * there is strategy for lead zone Global strategy
	 * Global strategy of lead zone
	 */
	@Test
	public void testCase52() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		CheckListService checkListService = new CheckListService();
		PRStrategyDTO strategyDTO;
		PRStrategyDTO strategyDTO1;
		PRStrategyDTO strategyDTO2;
		PRStrategyDTO inputDTO;
		PRStrategyDTO leadInputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		int leadZoneDivisionId = 8, dependentZoneDivisionId =7 , leadZoneId=5;
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
		
		inputDTO = TestHelper.getStrategy(0, 6, 4, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		leadInputDTO = TestHelper.getStrategy(0, 6, 5, 4, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		prItemDTO.setPriceCheckListId(10);
		HashMap<ItemKey, List<PriceCheckListDTO>>  allCheckList = new HashMap<ItemKey, List<PriceCheckListDTO>> ();

		// Different Check list in same zone for same item. 1 check list is WIC checklist
		TestHelper.getCheckList(allCheckList, 3, 5, 6, 4, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		TestHelper.getCheckList(allCheckList, 2, 5, 6, 5, 209821, PRConstants.NON_LIG_ITEM_INDICATOR);
		
		// Strategy for both Lead and Dependent Zone for WIC item
//		strategyDTO = TestHelper.getStrategy(1, 6, 5, 1, 209821, "12/28/2014", "01/03/2014", false, 2, -1, -1);
//		strategyDTO1 = TestHelper.getStrategy(2, 6, 4, 1, 209821, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		strategyDTO2 = TestHelper.getStrategy(3, 6, 5, 99, 0, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
//		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO2);
		// Category Id for the given Item code
		ArrayList<Integer> productList = new ArrayList<Integer>();
		productList.add(209821);
		productListMap.put("4-172", productList);
		EasyMock.expect(
				pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(
				pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt()))
				.andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		checkListService.populatePriceCheckListDetails(conn,Integer.valueOf(CHAIN_ID), dependentZoneDivisionId, inputDTO.getLocationId(), inputDTO.getLocationLevelId(),inputDTO.getLocationId(), inputDTO.getProductLevelId(),
				inputDTO.getProductId(),itemDataMap,allCheckList,leadZoneId,inpStrategyMap, retLirMap, productListMap, 
				zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAOMock, strategyService, leadInputDTO, inputDTO,leadZoneDivisionId, null, null, 0);
		 
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap,
				itemDataMap, productParentChildRelationMap, retLirMap, productListMap,
				CHAIN_ID, dependentZoneDivisionId, true, zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
		
		assertEquals("Strategy For Item is incorrect", 3, prItemDTO.getStrategyDTO().getStrategyId());
	}
	
	/**
	 * Item - from Milk of Zone(693/66) 
	 * Strategy(1) -> Product List, Location - 693/66(Zone) 
	 * Expected: Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase53() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		PRItemDTO prItemDTO = TestHelper.setItemDTO(209821, 2209, 2210, 172, 1148, 5, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 00, 172, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO.setPriceCheckListId(10);
		
		productListMap = TestHelper.setProductListMap(productListMap, 4, 172, 6483);
		
		strategyDTO = TestHelper.getStrategy(10, 6, 66, 21, 6483, "12/28/2014", "01/03/2014", false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 21, 6483, "11/23/2014", null, false, -1, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);
		
		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 1, Actual Strategy Id: " + prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 10, prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	
	/**
	 * Item - from Soda Pop Tonic of Zone(693/66) 
	 * Strategy(1) -> Product - All Products, Location - 693/66(Zone) 
	 * Strategy(2) -> Product - Milk(Category), Location - 693/66(Zone) 
	 * Expected: Strategy(1)
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase54() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();

		PRItemDTO prItemDTO = TestHelper.setItemDTO(56002, 1073, 1074, 202, 957, 4, 0, 0, 10, "", false, -1, -1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);

		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);

		prItemDTO.setPriceCheckListId(21);
		
		
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, 21, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 21, 172, "12/28/2014", "01/03/2014", false, 21, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);

		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Strategy Id: 1, Actual Strategy Id: " + prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 1, prItemDTO.getStrategyDTO().getStrategyId());
		assertEquals(false, zoneStoreRecFlag.isRecommendAtStoreLevel);
	}
	
	
	/***
	 * Product List Test Cases
	 * There is a strategy defined at product list (with 2 categories) for a zone with item list
	 * Also there is strategy at All Products for that zone
	 * The 2 categories must get the strategies defined at product list rather than one defined
	 * at zone level
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase55() throws GeneralException, Exception, OfferManagementException {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO strategyDTO;
		PRStrategyDTO inputDTO;
		HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap = null;
		Boolean isZoneItem = true;
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> productList = new ArrayList<Integer>();
		zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
		
		//Input Strategy
		inputDTO = TestHelper.getStrategy(0, 6, 66, 4, 131, "12/28/2014", "01/03/2014", false, 28, -1, -1);

		// Items
		PRItemDTO prItemDTO1 = TestHelper.setItemDTO(56002, 0, 0, 131, 0, 5, 0, 0, 10, "", false, -1, -1);
		prItemDTO1.setPriceCheckListId(28);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO1);
		
		// Strategies
		//Strategy at All products - Zone
		strategyDTO = TestHelper.getStrategy(1, 6, 66, 99, 0, "12/28/2014", "01/03/2014", false, 28, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		//Strategy at Product List - Zone
		strategyDTO = TestHelper.getStrategy(2, 6, 66, 21, 8396, "12/28/2014", "01/03/2014", false, 28, -1, -1);
		inpStrategyMap = TestHelper.setStrategyMap(inpStrategyMap, strategyDTO);
		
		productList.add(8396);
		productListMap.put("4-131", productList);
//		//Mock Product List
		EasyMock.expect(
				productGroupDAOMock.getProductListForProducts(conn, inputDTO.getProductLevelId(),
						inputDTO.getProductId())).andReturn(productListMap);
		EasyMock.replay(productGroupDAOMock);
		
		EasyMock.expect(pricingEngineDAOMock.getLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getStoreLocationListId(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
				.andReturn(new ArrayList<Integer>()).anyTimes();
		EasyMock.expect(pricingEngineDAOMock.getPriceZoneIdForStore(EasyMock.anyObject(), EasyMock.anyInt())).andReturn(PRICE_ZONE_ID_66).anyTimes();
		EasyMock.replay(pricingEngineDAOMock);

		strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAOMock, inpStrategyMap, itemDataMap, 
				productParentChildRelationMap, retLirMap, productListMap, CHAIN_ID, DIVISION_ID, isZoneItem,
				zoneStoreRecFlag,inputDTO,0,0);

		logger.debug("Expected Item Code : " + prItemDTO1.getItemCode() + " Strategy Id: 2, " +
				"Actual Strategy Id: " + prItemDTO1.getStrategyDTO().getStrategyId());
		assertEquals("Strategy For Item is incorrect", 2, prItemDTO1.getStrategyDTO().getStrategyId());
		
	}
}

 