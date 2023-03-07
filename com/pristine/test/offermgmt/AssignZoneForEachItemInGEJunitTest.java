package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.*;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class AssignZoneForEachItemInGEJunitTest {
	Connection conn = null;
	boolean isRecommendAtStoreLevel = false;
	private static Logger logger = Logger.getLogger("AssignZoneForEachItem");
	boolean isOnline = false;
	PRStrategyDTO inputDTO;
	HashMap<ItemKey, PRItemDTO> itemDataMap;
	ItemService itemService;
	String recWeekStartDate = "03/01/2018", recWeekEndDate = "03/07/2018", curWeekStartDate = "02/22/2018", curWeekEndDate = "02/28/2018";
	int zone1Id = 1, zone2Id = 2, zone3Id = 3;
	int zone1DSD1Id =10, zone1DSD2Id=11, zone2DSD1Id=20, zone2DSD2Id=21, zone3DSD1Id=30, zone3DSD2Id=31;
	int locationLevelId = 6, locationId =zone1Id, productLevelId = 4, productId = 5768, strategyId = 1;
	String zone1Num = "GE-106-1", zone2Num = "GE-24-1", zone3Num = "GE-26-1";
	String zone1DSD1 = "GE-106-1-1", zone1DSD2 = "GE-106-1-2", zone2DSD1 = "GE-24-1-1", zone2DSD2 = "GE-24-1-2", zone3DSD1 = "GE-26-1-1",
			zone3DSD2 = "GE-26-1-2";
	int zone1Str1 = 1001, zone1Str2 = 1002, zone1Str3 = 1003, zone1Str4= 1004, zone1Str5 =1005, zone1Str6=1006, zone1Str7=1007, zone1Str8=1008;
	int zone2Str1 = 2001, zone2Str2 = 2002, zone2Str3 = 2003, zone2Str4= 2004, zone2Str5 =2005, zone2Str6=2006, zone2Str7=2007, zone2Str8=2008;
	int zone3Str1 = 3001, zone3Str2 = 3002, zone3Str3 = 3003, zone3Str4= 3004, zone3Str5 =3005, zone3Str6=3006, zone3Str7=3007, zone3Str8=3008;
	int item1 = 101, item2=102, item3=103, item4=104, item5=105, item6=106, item7=107, item8=108, item9=109, item10=110, item11= 111;
	long runId = 1234;
	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/SpecificGE.properties");

		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, recWeekStartDate, recWeekEndDate,
				isRecommendAtStoreLevel, -1, -1, -1);
		itemService = new ItemService(null);
	}
	
	/**
	 * Same item with multiple Stores within a same Zone (Primary zone)
	 * 
	 * Expected: one item with Primary Zone Id and zone number
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase1() throws OfferManagementException{
		
		logger.info("Test case 1...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str3);
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str4);
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str5);
		
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", 1,itemDataMap.size());
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone1Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone1Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Same item with multiple Stores with different Zones (Primary zone(GE-106-1) and matching zones (GE-24-1 & GE-26-1))
	 * 
	 * Expected: one item with Primary Zone Id and zone number
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase2() throws OfferManagementException{
		
		logger.info("Test case 2...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str1);
		
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", 1,itemDataMap.size());
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone1Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone1Num, finalItem.getPriceZoneNo());
	}

	/**
	 * DSD Item with multiple Stores with DSD Zones and one normal item with primary zone
	 * 
	 * Expected: DSD item should have Primary Zone Id and zone number(GE-106-1)
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase3() throws OfferManagementException{
		
		logger.info("Test case 3...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items
		TestHelper.getAuthItems(allStoreItems, item1, zone1DSD1Id, zone1DSD1, true, zone1Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone1DSD2Id, zone1DSD2, true, zone1Str2);
		TestHelper.getAuthItems(allStoreItems, item2, zone1Id, zone1Num, false, zone1Str1);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", 2,itemDataMap.size());
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone1Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone1Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Item authorized in only one matching zone (GE-24-1)
	 * 
	 * Expected: Item should have Matching Zone Id and zone number(GE-24-1)
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase4() throws OfferManagementException{
		
		logger.info("Test case 4...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", 1,itemDataMap.size());
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone2Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone2Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Item 1 is authorized in one matching zone(GE-24-1) and DSD Zone
	 * 
	 * Expected: Item 1 should have Primary Zone Id and zone number(GE-106-1)
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase5() throws OfferManagementException{
		
		logger.info("Test case 5...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone2DSD1Id, zone2DSD1, true, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item2, zone1Id, zone1Num, false, zone1Str1);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", 2,itemDataMap.size());
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone1Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone1Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Item 1 is authorized in both matching zone(GE-24-1 & GE-26-1). 
	 * In GE-24-1 zone has 5 stores and GE-26-1 zone has 3 stores as authorized
	 * 
	 * Expected: Item 1 should have zone number(GE-24-1) and its respective zone id
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase6() throws OfferManagementException{
		
		logger.info("Test case 6...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items for Zone GE-24-1 (5 Stores)
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str3);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str4);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str5);
		
		//Set items for Zone GE-26-1 (3 Stores)
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str3);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone2Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone2Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Item 1 is authorized in both matching zone(GE-24-1 & GE-26-1) and Primary Zone(GE-106-1). 
	 * In GE-106-1 zone has 1 store, GE-24-1 zone has 5 stores and GE-26-1 zone has 3 stores as authorized
	 * 
	 * Expected: Item 1 should have zone number(GE-106-1) and its respective zone id
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase7() throws OfferManagementException{
		
		logger.info("Test case 7...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items for Zone GE-24-1 (5 Stores)
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str3);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str4);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str5);
		
		//Set items for Zone GE-26-1 (3 Stores)
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str3);
		
		//Set items for Zone GE-106-1 (1 store)
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str1);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone1Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone1Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Item 1 is authorized in both matching zone(GE-24-1 & GE-26-1) and DSD Zone(GE-106-1-1). 
	 * In GE-106-1-1 zone has 1 store, GE-24-1 zone has 5 stores and GE-26-1 zone has 3 stores as authorized
	 * 
	 * Expected: Item 1 should have zone number(GE-106-1) and its respective zone id
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase8() throws OfferManagementException{
		
		logger.info("Test case 8...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items for Zone GE-24-1 (5 Stores)
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str3);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str4);
		TestHelper.getAuthItems(allStoreItems, item1, zone2Id, zone2Num, false, zone2Str5);
		
		//Set items for Zone GE-26-1 (3 Stores)
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str1);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str2);
		TestHelper.getAuthItems(allStoreItems, item1, zone3Id, zone3Num, false, zone3Str3);
		
		//Set items for Zone GE-106-1 (1 store)
		TestHelper.getAuthItems(allStoreItems, item2, zone1Id, zone1Num, false, zone1Str1);
		
		//Set items for Zone GE-106-1-1 (1 store)
		TestHelper.getAuthItems(allStoreItems, item1, zone1DSD1Id, zone1DSD1, true, zone1Str1);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		ItemKey itemKey = new ItemKey(item1, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO finalItem = itemDataMap.get(itemKey);
		
		assertEquals("Mismatch", item1,finalItem.getItemCode());
		assertEquals("Mismatch", zone1Id, finalItem.getPriceZoneId());
		assertEquals("Mismatch", zone1Num, finalItem.getPriceZoneNo());
	}
	
	/**
	 * Item 1 & Item 2 is authorized in Primary Zone(GE-106-1). 
	 * 
	 * Expected: Item 1 & 2 should have zone number(GE-106-1) and its respective zone id
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase9() throws OfferManagementException{
		
		logger.info("Test case 9...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		
		//Set items for Zone GE-106-1 (1 store)
		TestHelper.getAuthItems(allStoreItems, item1, zone1Id, zone1Num, false, zone1Str1);
		TestHelper.getAuthItems(allStoreItems, item2, zone1Id, zone1Num, false, zone1Str1);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		itemDataMap.forEach((key, value)->{
			assertEquals("Mismatch", zone1Id, value.getPriceZoneId());
			assertEquals("Mismatch", zone1Num, value.getPriceZoneNo());
		});
	}
	
	/**
	 * Item 1 & Item 2 is authorized in DSD Zone(GE-106-1-1) and Item 3 is authorized in Matching zones.
	 * Item 3 is in 4 stores in Zone GE-24-1 and 5 Stores in Zone GE-26-1 
	 * 
	 * Expected: Item 1 & 2 should have zone number(GE-106-1) and its respective zone id
	 * and Item 3 should be in Zone GE-26-1 and respective zone id
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase10() throws OfferManagementException{
		
		logger.info("Test case 10...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		TestHelper.getAuthItems(allStoreItems, item4, zone1Id, zone1Num, false, zone1Str1);
		//Set items for Zone GE-106-1 (1 store)
		TestHelper.getAuthItems(allStoreItems, item1, zone1DSD1Id, zone1DSD1, true, zone1Str1);
		TestHelper.getAuthItems(allStoreItems, item2, zone2DSD1Id, zone2DSD1, true, zone1Str1);
		
		//Set Item 3 in GE-24-1 with 4 diff Stores
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str2);
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str3);
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str4);
		
		//Set Item 3 in GE-26-1 with 4 diff Stores
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str1);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str2);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str3);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str4);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str5);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		itemDataMap.forEach((key, value)->{
			if(value.getItemCode() == item1 || value.getItemCode() == item2 || value.getItemCode() == item4){
				assertEquals("Mismatch", zone1Id, value.getPriceZoneId());
				assertEquals("Mismatch", zone1Num, value.getPriceZoneNo());
			}else{
				assertEquals("Mismatch", zone3Id, value.getPriceZoneId());
				assertEquals("Mismatch", zone3Num, value.getPriceZoneNo());
			}
		});
	}
	
	/**
	 * Item 1 & Item 2 is authorized in DSD Zone(GE-106-1-1) and Item 3 is authorized in Matching zones.
	 * Item 3 is in 4 stores in Zone GE-24-1 and 5 Stores in Zone GE-26-1 
	 * Item 5 is in 4 stores in Zone GE-24-1 and 4 Stores in Zone GE-26-1 
	 * 
	 * Expected: Item 1 & 2 should have zone number(GE-106-1) and its respective zone id
	 * Item 3 & Item 5 should be in Zone GE-26-1 and respective zone id
	 * @throws OfferManagementException 
	 */
	@Test
	public void testCase11() throws OfferManagementException{
		
		logger.info("Test case 11...");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<>();
		TestHelper.getAuthItems(allStoreItems, item4, zone1Id, zone1Num, false, zone1Str1);
		//Set items for Zone GE-106-1 (1 store)
		TestHelper.getAuthItems(allStoreItems, item1, zone1DSD1Id, zone1DSD1, true, zone1Str1);
		TestHelper.getAuthItems(allStoreItems, item2, zone2DSD1Id, zone2DSD1, true, zone1Str1);
		
		//Set Item 3 in GE-24-1 with 4 diff Stores
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str2);
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str3);
		TestHelper.getAuthItems(allStoreItems, item3, zone2Id, zone2Num, false, zone2Str4);
		
		//Set Item 3 in GE-26-1 with 4 diff Stores
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str1);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str2);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str3);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str4);
		TestHelper.getAuthItems(allStoreItems, item3, zone3Id, zone3Num, false, zone3Str5);
		
		// Set Item 5 in GE-24-1 with 4 diff Stores
		TestHelper.getAuthItems(allStoreItems, item5, zone2Id, zone2Num, false, zone2Str1);
		TestHelper.getAuthItems(allStoreItems, item5, zone2Id, zone2Num, false, zone2Str2);
		TestHelper.getAuthItems(allStoreItems, item5, zone2Id, zone2Num, false, zone2Str3);
		TestHelper.getAuthItems(allStoreItems, item5, zone2Id, zone2Num, false, zone2Str4);

		// Set Item 5 in GE-26-1 with 4 diff Stores
		TestHelper.getAuthItems(allStoreItems, item5, zone3Id, zone3Num, false, zone3Str1);
		TestHelper.getAuthItems(allStoreItems, item5, zone3Id, zone3Num, false, zone3Str2);
		TestHelper.getAuthItems(allStoreItems, item5, zone3Id, zone3Num, false, zone3Str3);
		TestHelper.getAuthItems(allStoreItems, item5, zone3Id, zone3Num, false, zone3Str4);
		
		itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
		
		itemDataMap.forEach((key, value)->{
			if(value.getItemCode() == item1 || value.getItemCode() == item2 || value.getItemCode() == item4){
				assertEquals("Mismatch", zone1Id, value.getPriceZoneId());
				assertEquals("Mismatch", zone1Num, value.getPriceZoneNo());
			}else {
				assertEquals("Mismatch", zone3Id, value.getPriceZoneId());
				assertEquals("Mismatch", zone3Num, value.getPriceZoneNo());
			}
		});
	}
}
