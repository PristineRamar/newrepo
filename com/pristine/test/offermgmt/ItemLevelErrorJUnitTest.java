package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemRecErrorService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class ItemLevelErrorJUnitTest {
//	private static Logger logger = Logger.getLogger("ItemLevelErrorJUnitTest");
	ObjectMapper mapper = new ObjectMapper();
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	String curWeekStartDate = DateUtil.getWeekStartDate(0);
	
	@Before
	public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("analysis.properties");
	}
	
	//Test: There is no Strategy for an item
	//Exp: Item marked as error, error code as 2, no explain log, recommended price is null
	@Test
	public void testCase1() throws Exception {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 2, null, 4.69, 2.1, 2.1, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, null, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		//Update Error Status
		ItemRecErrorService res = new ItemRecErrorService();
		res.setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		
		List<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		//compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);
		
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		
		List<Integer> expErroCodes = new ArrayList<Integer>();
		expErroCodes.add(2);
		
		assertEquals("Explain Log Mismatch", item.getExplainLog(), new PRExplainLog());
		assertEquals("Rec Error Status Mismatch", item.getIsRecError(), true);
		assertEquals("Rec Error Codes Mismatch", item.getRecErrorCodes(), expErroCodes);
		assertEquals("Error But Recommend Mismatch", item.getErrorButRecommend(), false);
		assertEquals("Rec Price Mismatch", item.getRecommendedRegPrice(), null);
	}
	
	//Test: There is no Current Retail for an Item
	//Exp: Item marked as error, error code as 3, no explain log, recommended price is null
	@Test
	public void testCase2() throws Exception {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 0, null, null, 2.1, 2.1, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Update Error Status
		ItemRecErrorService res = new ItemRecErrorService();
		res.setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		List<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		//compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		List<Integer> expErroCodes = new ArrayList<Integer>();
		expErroCodes.add(3);

		assertEquals("Explain Log Mismatch", item.getExplainLog(), new PRExplainLog());
		assertEquals("Rec Error Status Mismatch", item.getIsRecError(), true);
		assertEquals("Rec Error Codes Mismatch", item.getRecErrorCodes(), expErroCodes);
		assertEquals("Error But Recommend Mismatch", item.getErrorButRecommend(), false);
		assertEquals("Rec Price Mismatch", item.getRecommendedRegPrice(), null);
	}
	
	//Test: There is no Current Cost for an Item
	//Exp: Item marked as error, error code as 4, no explain log, recommended price is null
	@Test
	public void testCase3() throws Exception {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 4.19, null, null, null, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Update Error Status
		ItemRecErrorService res = new ItemRecErrorService();
		res.setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		List<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		//compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		List<Integer> expErroCodes = new ArrayList<Integer>();
		expErroCodes.add(4);

		assertEquals("Explain Log Mismatch", item.getExplainLog(), new PRExplainLog());
		assertEquals("Rec Error Status Mismatch", item.getIsRecError(), true);
		assertEquals("Rec Error Codes Mismatch", item.getRecErrorCodes(), expErroCodes);
		assertEquals("Error But Recommend Mismatch", item.getErrorButRecommend(), false);
		assertEquals("Rec Price Mismatch", item.getRecommendedRegPrice(), null);
	}
	
	//Test: There is no Current Cost & Current Retail for an Item
	//Exp: Item marked as error, error code as 3 & 4, no explain log, recommended price is null
	@Test
	public void testCase4() throws Exception {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 0, null, null, null, null, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// Update Error Status
		ItemRecErrorService res = new ItemRecErrorService();
		res.setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		List<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		//compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		List<Integer> expErroCodes = new ArrayList<Integer>();
		expErroCodes.add(3);
		expErroCodes.add(4);

		assertEquals("Explain Log Mismatch", item.getExplainLog(), new PRExplainLog());
		assertEquals("Rec Error Status Mismatch", item.getIsRecError(), true);
		assertEquals("Rec Error Codes Mismatch", item.getRecErrorCodes(), expErroCodes);
		assertEquals("Error But Recommend Mismatch", item.getErrorButRecommend(), false);
		assertEquals("Rec Price Mismatch", item.getRecommendedRegPrice(), null);
	}
}
