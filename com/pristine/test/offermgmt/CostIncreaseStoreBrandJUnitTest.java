package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
//import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ApplyStrategy;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostIncreaseStoreBrandJUnitTest {
	private static Logger logger = Logger.getLogger("TestCases");
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer REALTED_ITEM_CODE_BRAND_TEST_1245 = 12345;
	public static final Integer REALTED_ITEM_CODE_SIZE_TEST_1246 = 12346;
	public static final Integer MULTI_COMP_1 = 952;
	public static final Integer MULTI_COMP_2 = 978;
	public static final Integer MULTI_COMP_3 = 959;
	public static final Integer MULTI_COMP_4 = 982;
	public static final Integer MULTI_COMP_5 = 921;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	String curWeekStartDate = DateUtil.getWeekStartDate(0);
	ObjectMapper mapper = new ObjectMapper();
	PriceGroupService priceGroupService = new PriceGroupService();

	// private PricingEngineWS pricingEngineWSMock;

	@Before
	public void init() {
		// PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/SpecificAhold.properties");

		// pricingEngineWSMock = EasyMock
		// .createMockBuilder(PricingEngineWS.class) //create builder first
		// .addMockedMethod("getCostChangeLogic") // tell EasyMock to mock which
		// method
		// .createMock();
	}

	// Multiple Brand relation (Brand, Index, Margin)
	// Brand precedence is set based on current retail price (with below operator)
	@Test
	public void testSBMultiRelation1() throws Exception, OfferManagementException {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.77, 1.37, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		TestHelper.updateItemDTO(relatedItem, new MultiplePrice(1, 3.79));

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		TestHelper.updateItemDTO(relatedItem, new MultiplePrice(1, 3.49));
		
		HashMap<Integer, List<PRItemDTO>> retLirMap = getRetLirIdMap(itemDataMap);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
		priceGroupService.setDefaultBrandPrecedence(itemDataMap,retLirMap);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.14, 3.14, 3.14, 3.14, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.41, 3.41, 3.14, 3.14, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14, 3.14,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.60, 3.60, 3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.51, 3.07, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14,
				3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.77, Constants.DEFAULT_NA, 3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.99));

		// EasyMock.expect(pricingEngineWSMock.getCostChangeLogic()).andReturn("");
		// EasyMock.replay(pricingEngineWSMock);

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}
	
	// Multiple Brand relation (Brand, Index, Margin)
	// Brand precedence is set based on current retail price (with below operator)
	@Test
	public void testCase17() throws Exception, OfferManagementException {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.77, 1.37, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		TestHelper.updateItemDTO(relatedItem, new MultiplePrice(1, 3.79));

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_ABOVE, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		TestHelper.updateItemDTO(relatedItem, new MultiplePrice(1, 3.49));
		
		HashMap<Integer, List<PRItemDTO>> retLirMap = getRetLirIdMap(itemDataMap);


		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_ABOVE, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
		priceGroupService.setDefaultBrandPrecedence(itemDataMap,retLirMap);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 4.17, 4.17, 4.17, 4.17, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.84, 3.84, 4.17, 4.17, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.17, 4.17,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.60, 3.60, 4.17, 4.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.51, 3.07, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.17,
				4.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.77, Constants.DEFAULT_NA, 4.17, 4.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.19);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.19));

		// EasyMock.expect(pricingEngineWSMock.getCostChangeLogic()).andReturn("");
		// EasyMock.replay(pricingEngineWSMock);

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}
	
	// Multiple Brand relation (Brand, Index, Margin)
	// Default Brand precedence is not applied as precedence defined in the feed itself
	@Test
	public void testCase18() throws Exception, OfferManagementException {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.77, 1.37, COST_INCREASE, 0, 0d,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		TestHelper.updateItemDTO(relatedItem, new MultiplePrice(1, 3.79));

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 1, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		
		HashMap<Integer, List<PRItemDTO>> retLirMap = getRetLirIdMap(itemDataMap);

		TestHelper.updateItemDTO(relatedItem, new MultiplePrice(1, 3.49));
		
		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 2, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);
		
		priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
		priceGroupService.setDefaultBrandPrecedence(itemDataMap,retLirMap);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.41, 3.41, 3.41, 3.41,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 3.14, 3.14, 3.41, 3.41,
				REALTED_ITEM_CODE_SIZE_TEST_1246, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.41, 3.41, "No Competition Price", null);	

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.60, 3.60, 3.41, 3.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.51, 3.07, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.41, 3.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.77, Constants.DEFAULT_NA,
				3.41, 3.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.39));

		// EasyMock.expect(pricingEngineWSMock.getCostChangeLogic()).andReturn("");
		// EasyMock.replay(pricingEngineWSMock);

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}
	

	// Multiple Brand relation (Margin, Index, Brand, , )
	@Test
	public void testSBMultiRelation4() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.77, 1.37, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.60, 3.60, 3.60, 3.60, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.60, 3.60,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.41, 3.41, 3.41, 3.41, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 3.14, 3.14, 3.41, 3.41, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.51, 3.07, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.41,
				3.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.77, Constants.DEFAULT_NA, 3.41, 3.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.39));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Multiple Brand relation (Brand, Index, Margin) Brand 2 within Brand 1
	@Test
	public void testSBMultiRelation3() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.77, 1.37, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, 20,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 5, 10,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.03, 3.41, 3.03, 3.41, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.14, 3.32, 3.14, 3.32, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14, 3.32,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.60, 3.60, 3.32, 3.32, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.51, 3.07, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.32,
				3.32, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.77, Constants.DEFAULT_NA, 3.32, 3.32, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.29);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.29);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.29));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// SB vs NB(0.30 below), Margin, PI, Brand
	@Test
	public void testSBvsNBCostIncrease1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 95, Constants.DEFAULT_NA);
		TestHelper.setBrandGuideline(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.09, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.99);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION, 0.30, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, Constants.DEFAULT_NA, 3.25, 3.91, 3.91, "",
				new MultiplePrice(1, 3.09));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.69, 3.69, 3.69, 3.69, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69,
				3.69, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.69, 3.69, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.69);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.69));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Store brand cost increase (threshold is conflict
	@Test
	public void sbCostIncrease2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.69, null, 1.70, 1.34, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.99);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.59, 3.59, 3.59, 3.59, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.59, 3.59,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.41, 3.41, 3.59, 3.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.42, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.59,
				3.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.70, Constants.DEFAULT_NA, 3.59, 3.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.59);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.59));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Store brand cost increase With multiple relation
	@Test
	public void testSBMultiRelation2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.77, 1.37, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set another Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.14, 3.14, 3.14, 3.14, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.41, 3.41, 3.14, 3.14, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14, 3.14,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.60, 3.60, 3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.51, 3.07, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14,
				3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.77, Constants.DEFAULT_NA, 3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(2.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.99));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Store brand cost increase (within threshold)
	@Test
	public void testSBCostIncWithinThresh() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 5.49, null, 4.23, 4.12, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set another Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.49);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.24, 5.62, 5.24, 5.62, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 5.24, 5.62,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 5.64, 5.64, 5.62, 5.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 4.94, 6.04, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 5.62,
				5.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 4.23, Constants.DEFAULT_NA, 5.62, 5.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(5.59);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(5.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 5.59));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Store brand cost increase (within threshold) round down
	@Test
	public void testSBCostIncWithThresholdRoundDown() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 4.29, null, 3.29, 3.21, COST_INCREASE, COMP_STR_ID_TEST_967, 5.99, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set another Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.19);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 35, 40,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 4.31, 4.67, 4.31, 4.67, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 6.05, 6.31, 4.31, 4.67, "", new MultiplePrice(1, 5.99));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 4.40, 4.40, 4.40, 4.67, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 3.86, 4.72, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.40,
				4.67, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.29, Constants.DEFAULT_NA, 4.40, 4.67, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.49);
		roundingDigits.add(4.59);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(4.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.59));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Store brand cost increase (within threshold) round up
	@Test
	public void testSBCostIncWithinThreshold() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.49, null, 6.64, 6.46, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set another Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 12.49);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 8.74, 9.37, 8.74, 9.37, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.74, 9.37,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.73, 8.73, 8.74, 8.74, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.64, 9.34, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.74,
				8.74, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.64, Constants.DEFAULT_NA, 8.74, 8.74, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.79));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// SIZE, BRAND, PI, MARGIN
	// Production Issue: In brand relation, guidelines not marked as conflict
	// (store brand)
	@Test
	public void testConflict2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'S', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.54, 11.44, 5.54, 11.44, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false, 38);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 8.32, 8.91, 8.32, 8.91, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, 8.32, 8.91, "", new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.85, 8.85, 8.85, 8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);
		// item.setRecommendedRegPrice(8.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.79));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// MARGIN, SIZE, BRAND, PI
	// Production Issue: In brand relation, guidelines not marked as conflict
	// (store brand)
	@Test
	public void testCase10() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.85, 8.85, 8.85, 8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.54, 11.44, 8.85, 8.85, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false, 38);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 8.32, 8.91, 8.85, 8.91, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, 8.85, 8.91, "", new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.79));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// SIZE, BRAND, PI, MARGIN
	// < relation and use margin
	@Test
	public void testCase11() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 15, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.54, 11.44, 5.54, 11.44, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false, 38);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 10.10, Constants.DEFAULT_NA, 10.10,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, Constants.DEFAULT_NA, 10.10, "",
				new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.85, 8.85, 8.85, 10.10, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				9.45, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 9.45, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.99);
		roundingDigits.add(9.19);
		roundingDigits.add(9.29);
		roundingDigits.add(9.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(9.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 9.39));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// SIZE, BRAND, PI, MARGIN
	// > relation and use margin
	@Test
	public void testCase12() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 6.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 5, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.54, 11.44, 5.54, 11.44, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false, 38);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 6.95, Constants.DEFAULT_NA, 6.95, Constants.DEFAULT_NA,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, 6.95, Constants.DEFAULT_NA, "",
				new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.85, 8.85, 8.85, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				9.45, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 9.45, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.99);
		roundingDigits.add(9.19);
		roundingDigits.add(9.29);
		roundingDigits.add(9.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(9.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 9.39));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	@Test
	public void testSBvsNBCostIncrease2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 95, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 7.99, null, 6.00, 5.85, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 12.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 8.60, 9.22, 8.60, 9.22, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.19, 8.19, 8.60, 8.60, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.60, 8.60,
				"No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.19, 8.79, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.60,
				8.60, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.00, Constants.DEFAULT_NA, 8.60, 8.60, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.69);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.69));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Only Size Present (Brand is excluded), Price group has both Brand and
	// Size
	@Test
	public void testExcludeOnlySize() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		// TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.54, 11.44, 5.54, 11.44, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false, 38);

		// guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		// TestHelper.setBrandLog(explainLog, guidelineAndConstraintLog, true,
		// false, 8.32, 8.91, 8.32, 8.91,
		// REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 5.55, 5.78, 5.55, 5.78, "", new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 8.85, 8.85, 8.85, 8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.99));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// Only Brand Present (Size is excluded), Price group has both Brand and
	// Size
	@Test
	public void testExcludeOnlyBrand() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		// TestHelper.setSizeGuideline(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		/*
		 * guidelineAndConstraintLog = new PRGuidelineAndConstraintLog(); TestHelper.setSizeLog(explainLog,
		 * guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 11.44, Constants.DEFAULT_NA, 11.44,
		 * REALTED_ITEM_CODE_SIZE_TEST_1246, false);
		 */

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 8.32, 8.91, 8.32, 8.91, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, 8.32, 8.91, "", new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.85, 8.85, 8.85, 8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.79));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// Both Brand and Size is excluded, Price group has both Brand and Size
	@Test
	public void testExcludeBrandAndSize() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		// TestHelper.setSizeGuideline(strategy);
		// TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		/*
		 * guidelineAndConstraintLog = new PRGuidelineAndConstraintLog(); TestHelper.setSizeLog(explainLog,
		 * guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 11.44, Constants.DEFAULT_NA, 11.44,
		 * REALTED_ITEM_CODE_SIZE_TEST_1246, false);
		 * 
		 * guidelineAndConstraintLog = new PRGuidelineAndConstraintLog(); TestHelper.setBrandLog(explainLog,
		 * guidelineAndConstraintLog, true, false, 8.32, 8.91, 8.32, 8.91, REALTED_ITEM_CODE_BRAND_TEST_1245, false);
		 */

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 5.55, 5.78, 5.55, 5.78, "", new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 8.85, 8.85, 8.85, 8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.85, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.99));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// SIZE, BRAND, PI, MARGIN
	// > relation and use margin
	@Test
	public void testCase14() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Related Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 6.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 5, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.54, 11.44, 5.54, 11.44, REALTED_ITEM_CODE_SIZE_TEST_1246,
				false, 38);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 6.95, Constants.DEFAULT_NA, 6.95, Constants.DEFAULT_NA,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, 6.95, Constants.DEFAULT_NA, "",
				new MultiplePrice(1, 5.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 8.85, 8.85, 8.85, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 8.85,
				9.45, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 9.45, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 8.59, 8.59, 8.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(8.59);

		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(8.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 8.59));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// BRAND, MARGIN
	@Test
	public void testCase15() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 15);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 5.49, null, 4.23, 4.12, COST_INCREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.99);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
				PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.59, 5.99, 5.59, 5.99, REALTED_ITEM_CODE_BRAND_TEST_1245,
				false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 5.64, 5.64, 5.64, 5.99, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 4.67, 6.04, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 5.64,
				5.99, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 4.23, Constants.DEFAULT_NA, 5.64, 5.99, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(5.69);
		roundingDigits.add(5.79);
		roundingDigits.add(5.99);

		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(5.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 5.99));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// SIZE, BRAND, PI, MARGIN
	// > relation and use margin (higher to lower flag true)
	@Test
	public void testCase16() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 5.49, null, 4.45, 4.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.29, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 38, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Size
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 8.59);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'Y', 80, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 4.12, 8.58, 4.12, 8.58, REALTED_ITEM_CODE_SIZE_TEST_1246, false,
				80);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 5.34, 5.57, 5.34, 5.57, "", new MultiplePrice(1, 5.29));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 5.73, 5.73, 5.73, 5.73, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 4.94, 6.04, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 5.73,
				5.73, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 4.45, Constants.DEFAULT_NA, 5.73, 5.73, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 5.49, 5.49, 5.49, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(5.49);

		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(5.49);
		item.setRecommendedRegPrice(new MultiplePrice(1, 5.49));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// Brand, Size - Strategy, Brand precedence in Price Group
	@Test
	public void BrandSizePrecedence1() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy); // 63
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setSizeGuideline(strategy, 1); // 64

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'B', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[63],\"2\":[62],\"3\":[61],\"4\":[64]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Brand, Size - Strategy, Size precedence in Price Group
	@Test
	public void BrandSizePrecedence2() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy); // 63
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setSizeGuideline(strategy, 1); // 64

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'S', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[64],\"2\":[62],\"3\":[61],\"4\":[63]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Brand, Size - Strategy, No Precedence in Price Group
	@Test
	public void BrandSizePrecedence3() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy); // 63
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setSizeGuideline(strategy, 1); // 64

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[63],\"2\":[62],\"3\":[61],\"4\":[64]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Size, Brand - Strategy, Size precedence in Price Group
	@Test
	public void BrandSizePrecedence4() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1); // 64
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setBrandGuideline(strategy); // 63

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'S', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[64],\"2\":[62],\"3\":[61],\"4\":[63]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Size, Brand - Strategy, Brand precedence in Price Group
	@Test
	public void BrandSizePrecedence5() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1); // 64
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setBrandGuideline(strategy); // 63

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'B', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[63],\"2\":[62],\"3\":[61],\"4\":[64]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Size, Brand - Strategy, No Precedence in Price Group
	@Test
	public void BrandSizePrecedence6() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1); // 64
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setBrandGuideline(strategy); // 63

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[64],\"2\":[62],\"3\":[61],\"4\":[63]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Brand, Size same precedence in Strategy, size Precedence in Price Group
	@Test
	public void BrandSizePrecedence7() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1); // 64
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setBrandGuideline(strategy); // 63

		TreeMap<Integer, ArrayList<Integer>> execOrderMap = new TreeMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> guidelineMap;
		guidelineMap = new ArrayList<Integer>();
		guidelineMap.add(61);
		execOrderMap.put(1, guidelineMap);
		guidelineMap = new ArrayList<Integer>();
		guidelineMap.add(62);
		execOrderMap.put(2, guidelineMap);
		guidelineMap = new ArrayList<Integer>();
		guidelineMap.add(63);
		guidelineMap.add(64);
		execOrderMap.put(3, guidelineMap);

		strategy.getGuidelines().setExecOrderMap(execOrderMap);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'S', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[61],\"2\":[62],\"3\":[64,63]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	// Brand, Size same precedence in Strategy, brand Precedence in Price Group
	@Test
	public void BrandSizePrecedence8() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1); // 64
		TestHelper.setPIGuideline(strategy, 95, 99); // 62
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy); // 61
		TestHelper.setBrandGuideline(strategy); // 63

		TreeMap<Integer, ArrayList<Integer>> execOrderMap = new TreeMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> guidelineMap;
		guidelineMap = new ArrayList<Integer>();
		guidelineMap.add(61);
		execOrderMap.put(1, guidelineMap);
		guidelineMap = new ArrayList<Integer>();
		guidelineMap.add(62);
		execOrderMap.put(2, guidelineMap);
		guidelineMap = new ArrayList<Integer>();
		guidelineMap.add(63);
		guidelineMap.add(64);
		execOrderMap.put(3, guidelineMap);

		strategy.getGuidelines().setExecOrderMap(execOrderMap);

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy,
				LAST_X_WEEKS_MOV_1);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'B', ' ', ' ');

		try {
			String actualExecutionOrder = mapper.writeValueAsString(new ApplyStrategy()
					.reOrderSizeAndBrand(strategy.getGuidelines().getExecOrderMap(), item, strategy.getGuidelines().getGuidelineIdMap()));
			// Size then Brand
			String expExecutionOrder = "{\"1\":[61],\"2\":[62],\"3\":[63,64]}";

			logger.debug("Actual:" + actualExecutionOrder + ", Expected: " + expExecutionOrder);
			assertEquals("JSON Not Matching", expExecutionOrder, actualExecutionOrder);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	private HashMap<Integer, List<PRItemDTO>> getRetLirIdMap(HashMap<ItemKey, PRItemDTO> itemDataMap)
			throws OfferManagementException {
		ItemService itemService = new ItemService(null);
		HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);

		return retLirMap;
	}
}
