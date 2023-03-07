package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostNoChangeStoreBrandJUnitTest {
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer REALTED_ITEM_CODE_BRAND_TEST_1245 = 12345;
	public static final Integer REALTED_ITEM_CODE_SIZE_TEST_1246 = 12346;
	public static final Integer REALTED_ITEM_CODE_SIZE_TEST_1247 = 12347;
	public static final Integer MULTI_COMP_1 = 952;
	public static final Integer MULTI_COMP_2 = 978;
	public static final Integer MULTI_COMP_3 = 959;
	public static final Integer MULTI_COMP_4 = 982;
	public static final Integer MULTI_COMP_5 = 921;
	public static final Integer MULTI_COMP_OBS_DAY_30 = 30;
	public static final Integer MULTI_COMP_OBS_DAY_60 = 60;
	public static final Integer MULTI_COMP_OBS_DAY_90 = 90;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	String curWeekStartDate = DateUtil.getWeekStartDate(0);
	ObjectMapper mapper = new ObjectMapper();

	@Before
	public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");
	}

	// SB vs NB(10-15% below), Margin, Brand, PI
	@Test
	public void testSBvsNBNoCostChange1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 100, 105);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.69, null, 2.62, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.99);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				10, 15, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.39, 3.59, 3.39, 3.59,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 3.32, 3.49, 3.39, 3.49, "", new MultiplePrice(1, 3.49));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 3.32, 4.06,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.39, 3.49, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA,
				3.39, 3.49, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.39);
		roundingDigits.add(3.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.39);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// SB vs NB(0.30 below), Margin, Brand, PI
	@Test
	public void testSBvsNBNoCostChange2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 100);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.69, null, 2.62, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.79, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.99);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				0.30, Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_$);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.69, 3.69, 3.69, 3.69,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 3.79, 3.99, 3.69, 3.69, "", new MultiplePrice(1, 3.79));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 3.32, 4.06,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69, 3.69, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA,
				3.69, 3.69, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.69);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Store brand (outside threshold) multiple rounding digit
	@Test
	public void testSBNoCost() throws Exception {

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
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.99, null, 2.76, 2.76, COST_NO_CHANGE, 0, 0d,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set another Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.49);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 30,
				35, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 4.87, 5.24, 4.87, 5.24,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 4.87, 5.24, "No Competition Price", null);	

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 4.87, 5.24, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 3.59, 4.39,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.39, 4.39, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.76, Constants.DEFAULT_NA,
				4.39, 4.39, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(4.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.39));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Range after threshold is higher than threshold SB vs NB(35-40% below),
	// Margin, Brand, PI
	@Test
	public void testSBvsNBNoCostChange3() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 4.29, null, 3.29, 3.29, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 5.99, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				35, 40, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 4.37, 4.74, 4.37, 4.74,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 6.05, 6.31, 4.74, 4.74, "", new MultiplePrice(1, 5.99));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 3.86, 4.72,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.72, 4.72, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.29, Constants.DEFAULT_NA,
				4.72, 4.72, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(4.69);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.69));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// SIZE, No Relation, Lower Retail but round down
	@Test
	public void testLowerRetail2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 11.29, null, 6.76, 6.76, COST_NO_CHANGE, 0, 0d,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 10.16, 12.42,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 10.16, 12.42, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.76, Constants.DEFAULT_NA,
				10.16, 12.42, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false,
				Constants.DEFAULT_NA, 11.29, 10.16, 11.29, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(10.29);
		roundingDigits.add(10.49);
		roundingDigits.add(10.69);
		roundingDigits.add(10.79);
		roundingDigits.add(10.99);
		roundingDigits.add(11.29);

		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(11.29);
		item.setRecommendedRegPrice(new MultiplePrice(1, 11.29));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// New rounding logic
	@Test
	public void testRoundDownSB() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 15);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.39, null, 1.83, 1.83, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 2.39, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 2.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM,
				PRConstants.RETAIL_TYPE_SHELF, ' ');

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.28,
				Constants.DEFAULT_NA, 2.28, REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 2.41, 2.52, 2.28, 2.28, "", new MultiplePrice(1, 2.39));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.28, 2.28, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.03, 2.63,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.28, 2.28, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.83, Constants.DEFAULT_NA,
				2.28, 2.28, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.19);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.19);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.19));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// No Cost Available (with price index)
	@Test
	public void noCostAvailbleWithAllRules1() throws Exception {

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
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, null, 1.37, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 2.79d, strategy, LAST_X_WEEKS_MOV_1);
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

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 2.79, 2.79, 2.79, 2.79,
				"Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.41, 3.41, 2.79, 2.79,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.14, 3.14, 2.79, 2.79,
				REALTED_ITEM_CODE_SIZE_TEST_1246, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 2.82, 2.94, 2.79, 2.79, "", new MultiplePrice(1, 2.79));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.79, 2.79, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.51, 3.07,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.79, 2.79, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.79, 2.79, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.79));
		
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

	// No Cost Available (with multi comp)
	@Test
	public void noCostAvailbleWithAllRules3() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, MULTI_COMP_OBS_DAY_60);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, null, 1.37, COST_NO_CHANGE, 0, 0d,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 4.77f, 0, 0,
				DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 6.77f, 0, 0,
				DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 5.98f, 0, 0,
				DateUtil.getWeekStartDate(0));

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.79);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 2.79, 2.79, 2.79, 2.79,
				"Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.41, 3.41, 2.79, 2.79,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.14, 3.14, 2.79, 2.79,
				REALTED_ITEM_CODE_SIZE_TEST_1246, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "",
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "",
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "",
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false,
				Constants.DEFAULT_NA, 4.77, 2.79, 2.79, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.79, 2.79, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.51, 3.07,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.79, 2.79, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.79, 2.79, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.79));
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

	// No Cost and No Price Available (with price index)
	@Test
	public void noCostAvailbleWithAllRules2() throws Exception {

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
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 0, null, null, null, 1.37, COST_NO_CHANGE, 0, 0d,
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

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 10,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

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
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.41, 3.41, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.41, 3.41,
				"Current Retail not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.41, 3.41, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.39);
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

	/***
	 * Tops reported issue, recommended price is questioned
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSBvsNBNoCostChange4() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 80, 100);
		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 32, 61);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.03, 1.03, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 2.29, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.29);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				10, 20, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 2.63, 2.96, 2.63, 2.96,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 2.29, 2.86, 2.63, 2.86, "", new MultiplePrice(1, 2.29));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 1.51, 2.64, 2.63, 2.64, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.37, 3.21,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.63, 2.64, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.03, Constants.DEFAULT_NA,
				2.63, 2.64, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.69);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.69));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	/***
	 * Tops reported issue, (but modified to see what happens if price < cost)
	 * Cost can't be broken
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSBvsNBNoCostChange5() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 80, 100);
		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 32, 61);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 3.10, 3.10, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.79, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.49);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				10, 20, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 2.79, 3.14, 2.79, 3.14,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 3.79, 4.74, 3.14, 3.14, "", new MultiplePrice(1, 3.79));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 4.56, 7.95, 3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.37, 3.21,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.1, Constants.DEFAULT_NA,
				3.14, 3.14, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.19);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.19);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.19));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	/***
	 * Tops reported issue, (but modified to see what happens if threshold is
	 * broken) threshold can't be broken
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSBvsNBNoCostChange6() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, 80, 100);
		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 32, 61);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 2.29, 2.29, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.79, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 1.79);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION,
				10, 40, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 1.07, 1.61, 1.07, 1.61,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 3.79, 4.74, 1.61, 1.61, "", new MultiplePrice(1, 3.79));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.37, 5.87, 1.61, 1.61, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.37, 3.21,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.37, 2.37, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.29, Constants.DEFAULT_NA,
				2.37, 2.37, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.39);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.39);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.39));
		
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// Tops reported issue. Multiple Brand relation ( 3 brands)
	@Test
	public void testSBvsNBNoCostChange7() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 29, 75);
		TestHelper.setPIGuideline(strategy, 80, 100);
		
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 18, 18);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 1.79, null, 0.96, 0.96, COST_NO_CHANGE, COMP_STR_ID_TEST_967, 1.39,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item for Brand
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 2.49);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 1.99);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1247, 2.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1247, 0, 'X', 0, PRConstants.BRAND_RELATION, 20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 1.99,
				Constants.DEFAULT_NA, 1.99, REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 1.59,
				Constants.DEFAULT_NA, 1.59, REALTED_ITEM_CODE_SIZE_TEST_1246, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 1.83,
				Constants.DEFAULT_NA, 1.59, REALTED_ITEM_CODE_SIZE_TEST_1247, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 1.35, 3.84, 1.35, 1.59, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.39, 1.74, 1.39, 1.59, "", new MultiplePrice(1, 1.39));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.47, 2.11, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 1.47, 1.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 0.96, Constants.DEFAULT_NA,
				1.47, 1.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(1.49);
		roundingDigits.add(1.59);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(1.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 1.59));
		
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
	
	//> test
	@Test
	public void testSBvsNBNoCostChange8() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 29, 75);
		TestHelper.setPIGuideline(strategy, 80, 100);
		
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 18, 18);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 1.79, null, 0.96, 0.96, COST_NO_CHANGE, COMP_STR_ID_TEST_967, 1.39,
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
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 2.49);
		// itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 1.99);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'X', 0, PRConstants.BRAND_RELATION, 20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set another Item for Brand
		relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1247, 2.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set another Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1247, 0, 'X', 0, PRConstants.BRAND_RELATION, 20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, PRConstants.RETAIL_TYPE_SHELF,
				PRConstants.VALUE_TYPE_PCT);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 2.99, Constants.DEFAULT_NA,
				2.99, Constants.DEFAULT_NA, REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 2.39, Constants.DEFAULT_NA,
				2.99, Constants.DEFAULT_NA, REALTED_ITEM_CODE_SIZE_TEST_1246, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 2.75, Constants.DEFAULT_NA,
				2.99, Constants.DEFAULT_NA, REALTED_ITEM_CODE_SIZE_TEST_1247, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 1.35, 3.84, 2.99, 3.84, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 1.39, 1.74, 2.99, 2.99, "", new MultiplePrice(1, 1.39));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.47, 2.11, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.11, 2.11, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 0.96, Constants.DEFAULT_NA,
				2.11, 2.11, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(1.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(1.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 1.99));
		
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
}
