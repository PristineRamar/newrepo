package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

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
import com.pristine.dto.offermgmt.PRConstraintRounding;
import com.pristine.dto.offermgmt.PRConstraintThreshold;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRRoundingTableDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostNoChangeNoRelationJUnitTest {

	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
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

	// Price Index guideline not in conflict with Threshold
	// MARGIN, PI
	@Test
	public void testCase1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 100, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.10,
				Constants.DEFAULT_NA, 3.10, "", new MultiplePrice(1, 3.10));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.87, 3.10, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				2.87, 3.10, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.99);
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

	// Price Index guideline in conflict with Threshold (ROUND UP)
	// MARGIN, PI
	@Test
	public void testCase2() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 100, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 2.50, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
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
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, Constants.DEFAULT_NA, 2.50,
				Constants.DEFAULT_NA, 2.50, "", new MultiplePrice(1, 2.50));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.87, 2.87, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				2.87, 2.87, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.99);
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

	// Threshold (Round Down)
	// MARGIN, PI
	@Test
	public void testCase3() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.51, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 3.51, Constants.DEFAULT_NA,
				3.51, Constants.DEFAULT_NA, "", new MultiplePrice(1, 3.51));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.51, 3.51, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				3.51, 3.51, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.49);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.49));
		
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

	// Threshold (ROUND DOWN)
	// MARGIN, PI
	@Test
	public void testCase4() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.56, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 3.56, Constants.DEFAULT_NA,
				3.56, Constants.DEFAULT_NA, "", new MultiplePrice(1, 3.56));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.51, 3.51, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				3.51, 3.51, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.49);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.49));
		
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

	// No comp price
	// MARGIN, PI
	@Test
	public void testCase5() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 4.49, null, 2.58, 2.58, COST_NO_CHANGE, 0, 0d,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
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
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 4.04, 4.94,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.04, 4.94, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.58, Constants.DEFAULT_NA,
				4.04, 4.94, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.19);
		roundingDigits.add(4.29);
		roundingDigits.add(4.39);
		roundingDigits.add(4.49);
		roundingDigits.add(4.59);
		roundingDigits.add(4.69);
		roundingDigits.add(4.79);
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
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// Cost, Competitor Price not available
	// MARGIN, PI
	@Test
	public void testCase6() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 95, 99);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 4.49, null, 0d, 0d, COST_NO_CHANGE, 0, 0d,
				strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 4.49, 4.49, 4.49, 4.49,
				"Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 4.49, 4.49, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 4.49, 4.49, "No Competition Price", null);	

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 4.04, 4.94,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.49, 4.49, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 4.49, 4.49, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(4.49);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.49));
		
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
	public void test() {
		PRConstraintRounding rConstraintDTO = new PRConstraintRounding();
		PRRoundingTableDTO roundingTableDTO = new PRRoundingTableDTO();
		TreeMap<String, PRRoundingTableDTO> roundingTableContent = new TreeMap<String, PRRoundingTableDTO>();

		roundingTableDTO
				.setAllowedEndDigits("0.03,0.05,0.07,0.09,0.13,0.15,0.17,0.19,0.23,0.25,0.27,0.29,0.35,0.39,0.45,0.49,0.55,0.59,0.65,0.69,0.75,0.79,0.85,0.89,0.95,0.99");
		roundingTableContent.put("0-0.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.09,0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("1-1.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("2-2.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.19,0.29,0.39,0.49,0.59,0.69,0.79,0.99");
		roundingTableContent.put("3-9.99", roundingTableDTO);

		roundingTableDTO = new PRRoundingTableDTO();
		roundingTableDTO.setAllowedEndDigits("0.29,0.49,0.69,0.79,0.99");
		roundingTableContent.put("10-0", roundingTableDTO);

		// int[] roundingDigits = new int[2];
		// roundingDigits[0] = 0;
		// roundingDigits[1] = 9;
		// rConstraintDTO.setRoundingDigits(roundingDigits);

		rConstraintDTO.setRoundingTableContent(roundingTableContent);
		// rConstraintDTO.roundupPriceToPreviousRoundingDigit(0);
		rConstraintDTO.getClosestPriceRange(0, 0);

		// rConstraintDTO.getRange(0, 0, PRConstants.ROUND_CLOSEST);
		rConstraintDTO.getRange(0, 0);
		PRConstraintThreshold constraintThreshold = new PRConstraintThreshold();
		constraintThreshold.setMinValue(0);
		constraintThreshold.setMaxValue2(15);
		constraintThreshold.setMaxValue(10);
		constraintThreshold.setValueType(PRConstants.VALUE_TYPE_PCT);

		PRRange inputRange;
		PRExplainLog explainlog = new PRExplainLog();
		PRItemDTO itemInfo = new PRItemDTO();
		Double[] actOutput;
		String actual = "", exp = "";

		// Threshold 4.5 -- 5.82
		// Threshold in negative range, only end range present
		inputRange = new PRRange();
		inputRange.setEndVal(4.77);
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",4.59,4.69";
		assertEquals("Mismatch", exp, actual);

		// Threshold in positive range, only end range present
		inputRange = new PRRange();
		inputRange.setEndVal(5.39);
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",4.59,4.69,4.79,4.99,5.19,5.29,5.39";
		assertEquals("Mismatch", exp, actual);

		// Threshold in negative range, only start range present
		inputRange = new PRRange();
		inputRange.setStartVal(4.77);
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",4.79,4.99,5.19,5.29,5.39,5.49,5.59,5.69,5.79";
		assertEquals("Mismatch", exp, actual);

		// Threshold in positive range, only start range present
		inputRange = new PRRange();
		inputRange.setStartVal(5.39);
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",5.39,5.49,5.59,5.69,5.79";
		assertEquals("Mismatch", exp, actual);

		// No star and end range present
		inputRange = new PRRange();
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",4.59,4.69,4.79,4.99,5.19,5.29,5.39,5.49,5.59,5.69,5.79";
		assertEquals("Mismatch", exp, actual);

		// Both range present (in negative range)
		inputRange = new PRRange();
		inputRange.setStartVal(4.5);
		inputRange.setEndVal(4.77);
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",4.59,4.69";
		assertEquals("Mismatch", exp, actual);

		// Both range present (span from negative to positive)
		inputRange = new PRRange();
		inputRange.setStartVal(4.5);
		inputRange.setEndVal(5.39);
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",4.59,4.69,4.79,4.99,5.19,5.29,5.39";
		assertEquals("Mismatch", exp, actual);

		// Both range present (in positive range)
		inputRange = new PRRange();
		inputRange.setStartVal(5.29);
		inputRange.setEndVal(5.69);
		actual = "";
		exp = "";
		actOutput = constraintThreshold.getPriceRangeWithThreshold(5.29, inputRange, rConstraintDTO, itemInfo,
				explainlog);
		Arrays.sort(actOutput);
		for (Double tPrice : actOutput) {
			actual = actual + "," + tPrice;
		}
		exp = ",5.29,5.39,5.49,5.59,5.69";
		assertEquals("Mismatch", exp, actual);
	}

	// Product issue, rounding after threshold is not good
	@Test
	public void testCase7() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, MULTI_COMP_OBS_DAY_60);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 15);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 5.29, null, 4.02, 4.02, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.19, strategy, LAST_X_WEEKS_MOV_1);
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

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

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
				Constants.DEFAULT_NA, 4.77, Constants.DEFAULT_NA, 4.77, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.77, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 4.5, 5.82, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 4.5, 4.77, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 4.02, Constants.DEFAULT_NA,
				4.50, 4.77, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.59);
		roundingDigits.add(4.69);
		// roundingDigits.add(4.79);
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
			e.printStackTrace();
		}
		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	@Test
	public void testMinMaxAlone() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMinMaxConstraint(strategy, 0, 3.3, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.30,
				Constants.DEFAULT_NA, 3.30, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.87, 3.30, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				2.87, 3.30, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.99);
		roundingDigits.add(3.19);
		roundingDigits.add(3.29);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.99);
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

	// (MIN/MAX BETWEEN)
	@Test
	public void testMinMaxAlone1() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMinMaxConstraint(strategy, 1.50, 3.3, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.03, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, false, 1.50, 3.30, 1.50, 3.30, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.83, 2.23,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 1.83, 2.23, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				2.14, 2.23, "");

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
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// (MIN/MAX LOWER SIDE)
	@Test
	public void testMinMaxAlone2() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMinMaxConstraint(strategy, 2.50, 3.3, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.03, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, false, 2.50, 3.30, 2.50, 3.30, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 1.83, 2.23, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.50, 2.50, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				2.50, 2.50, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.59);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.59));
		
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

	// (MIN/MAX HIGHER SIDE)
	@Test
	public void testMinMaxAlone3() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMinMaxConstraint(strategy, 1.10, 1.80, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.03, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, false, 1.10, 1.80, 1.10, 1.80, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 1.83, 2.23, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 1.80, 1.80, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, true, 2.14, Constants.DEFAULT_NA,
				1.80, 1.80, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(1.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(1.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 1.79));
		
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

	// (MIN/MAX NO ROUNDING - USE CURRENT PRICE)
	@Test
	public void testMinMaxAlone5() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMinMaxConstraint(strategy, 2.21, 2.23, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.03, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, true, 2.21, 2.23, 2.21, 2.23, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.83, 2.23,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.21, 2.23, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, true, 2.14, Constants.DEFAULT_NA,
				2.21, 2.23, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.03);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.03);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.03));
		
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

	// (MIN/MAX NO ROUNDING - NO CURRENT RETAIL - ROUND UP)
	@Test
	public void testMinMaxAlone6() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMinMaxConstraint(strategy, 2.21, 2.23, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 0d, null, 2.14, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, true, 2.21, 2.23, 2.21, 2.23, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.21, 2.23,
				"Current Retail not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.14, Constants.DEFAULT_NA,
				2.21, 2.23, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.29);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.29);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.29));
		
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

	// MARGIN, PI
	@Test
	public void testNoCostNoPrice() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_HIGHER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 0d, null, 0d, 0d, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.10, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, "Current Retail not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, false, false,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"Current Retail not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(0.03);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(0.03);
		item.setRecommendedRegPrice(new MultiplePrice(1, 0.03));
		
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

	// No Cost Available (with price index)
	@Test
	public void noCostAvailbleWithAllRules() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);

		TestHelper.setMinMaxConstraint(strategy, 2.21, 2.23, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_HIGHER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, null, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.56, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 3.19, 3.19, 3.19, 3.19,
				"Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.19, 3.19, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 3.56, Constants.DEFAULT_NA,
				3.19, 3.19, "", new MultiplePrice(1, 3.56));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, true, 2.21, 2.23, 3.19, 3.19, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.19, 3.19, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.19, 3.19, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.19,
				Constants.DEFAULT_NA, 3.19, 3.19, "");

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
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// No Cost Available (with multi comp)
	@Test
	public void noCostAvailbleWithAllRules1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, MULTI_COMP_OBS_DAY_60);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				"", false);

		TestHelper.setMinMaxConstraint(strategy, 2.21, 2.23, 1);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_HIGHER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, null, 2.14, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.56, strategy, LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
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

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 3.19, 3.19, 3.19, 3.19,
				"Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.19, 3.19, "Cost not available");

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
				Constants.DEFAULT_NA, 4.77, 3.19, 3.19, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMinMaxLog(explainLog, guidelineAndConstraintLog, true, true, 2.21, 2.23, 3.19, 3.19, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.19, 3.19, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.19, 3.19, "Cost not available");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.19,
				Constants.DEFAULT_NA, 3.19, 3.19, "");

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
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}
}
