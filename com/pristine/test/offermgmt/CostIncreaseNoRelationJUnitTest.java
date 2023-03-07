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
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostIncreaseNoRelationJUnitTest {

	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer MULTI_COMP_1 = 952;
	public static final Integer MULTI_COMP_2 = 978;
	public static final Integer MULTI_COMP_3 = 959;
	public static final Integer MULTI_COMP_4 = 982;
	public static final Integer MULTI_COMP_5 = 921;
	public static final Integer MULTI_COMP_6 = 918;
	public static final Integer MULTI_COMP_7 = 915;
	public static final Integer MULTI_COMP_8 = 987;
	public static final Integer MULTI_COMP_9 = 980;
	public static final Integer MULTI_COMP_10 = 960;
	public static final Integer MULTI_COMP_OBS_DAY_30 = 30;
	public static final Integer MULTI_COMP_OBS_DAY_60 = 60;
	public static final Integer MULTI_COMP_OBS_DAY_90 = 90;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	ObjectMapper mapper = new ObjectMapper();
	String curWeekStartDate = DateUtil.getWeekStartDate(0);

	@Before
	public void init() {
		// PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/SpecificAhold.properties");
	}

	// PI, MARGIN
	@Test
	public void testCase1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234,item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();// dummy variable, output items are stored here.
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY,COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, Constants.DEFAULT_NA, 3.26, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));

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

	// MARGIN
	@Test
	public void testCase2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 11.99, null, 8.07, 6.65, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 14.55, 14.55, 14.55, 14.55, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 10.79, 13.19, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				14.55, 14.55, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 8.07, Constants.DEFAULT_NA, 14.55, 14.55, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(14.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(14.69);
		item.setRecommendedRegPrice(new MultiplePrice(1, 14.69));

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

	// Production Issue: Don't mark margin and constraint as conflict if its
	// doesn't have any brand/size relation.
	// PI, MARGIN
	@Test
	public void testCaseProduction1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setPIGuideline(strategy, 95, 99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 6.79, null, 5.94, 5.39, COST_INCREASE, COMP_STR_ID_TEST_967, 6.79, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 6.86, 7.15, 6.86, 7.15, "", new MultiplePrice(1, 6.79));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 7.48, 7.48, 7.48, 7.48, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 6.11, 7.47, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 7.48,
				7.48, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 5.94, Constants.DEFAULT_NA, 7.48, 7.48, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(7.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(7.49);
		item.setRecommendedRegPrice(new MultiplePrice(1, 7.49));
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

	// MARGIN, PI
	@Test
	public void testCase3() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234,item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();// dummy variable, output items are stored here.
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY,COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, 3.91, 3.91, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// MULTI COMP (< Min), PI, MARGIN (PI Must be Ignored)
	@Test
	public void testCase4() throws GeneralException, Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);

		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.19f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 3.10f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.98f, 0, 0, DateUtil.getWeekStartDate(0));

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "", Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "", Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "", Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.97,
				Constants.DEFAULT_NA, 2.97, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// MARGIN, MULTI COMP,
	@Test
	public void testCase5() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_AVG, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.19f, 0, 0, DateUtil.getWeekStartDate(0));
		// TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 3.10f, 0, 0,
		// DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.98f, 0, 0, "01/01/2014");

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "", Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "", Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, "", Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.18,
				3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// No Margin guideline, but there is cost change
	@Test
	public void testCostChgNoMarGuideline() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);
		// TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234,item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();// dummy variable, output items are stored here.
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY,COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, Constants.DEFAULT_NA, 3.26, "",
				new MultiplePrice(1, 3.19));

		// guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		// TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.87,
				3.26, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 2.87, 3.26, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.99);
		roundingDigits.add(3.19);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// MARGIN, PI (Don't recommend below cost)
	@Test
	public void testCase7() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.09, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234,item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();// dummy variable, output items are stored here.
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY,COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 2.56, 2.56, 2.56, 2.56, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, 2.56, 2.56, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 1.88, 2.3, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.56,
				2.56, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 2.62, 2.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, true, Constants.DEFAULT_NA, 2.09, 2.62, 2.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(2.69);
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

	// MARGIN, PI (LOWER RETAIL)
	@Test
	public void testCase8() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.16, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.87, 3.87, 3.87, 3.87, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, 3.87, 3.87, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.84, 3.48, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.87,
				3.87, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.87, 3.87, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.16, 3.16, 3.16, "");

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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);

	}

	// MARGIN, PI (LOWER RETAIL, Cost is carried)
	@Test
	public void testCase10() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.59, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.17, 3.17, 3.17, 3.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, 3.17, 3.17, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.33, 2.85, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.17,
				3.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.17, 3.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, true, Constants.DEFAULT_NA, 2.59, 2.62, 2.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(2.69);
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

	// MARGIN, PI (HIGHER RETAIL)
	@Test
	public void testCase9() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_HIGHER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, 3.91, 3.91, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.19, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// MARGIN, PI (HIGHER RETAIL, Follow Cost)
	@Test
	public void testCase11() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_HIGHER);

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.49, null, 2.62, 2.60, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 2.51, 2.51, 2.51, 2.51, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.26, 2.51, 2.51, "",
				new MultiplePrice(1, 3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.24, 2.74, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.51,
				2.51, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 2.62, 2.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.49, Constants.DEFAULT_NA, 2.62, 2.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(2.69);
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

	// MULTI COMP (Use Rule)
	// 10% lower, > 10%, within 10%, not > 10%
	@Test
	public void testCase12() throws GeneralException, Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_RULE, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		// 10% lower
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, PRConstants.VALUE_TYPE_PCT, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LOWER, false);
		// > 10%
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, PRConstants.VALUE_TYPE_PCT, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_ABOVE, false);
		// within 10%
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, PRConstants.VALUE_TYPE_PCT, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_WITHIN, false);
		// not > 10%
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_4, PRConstants.VALUE_TYPE_PCT, 10, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, false);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 4.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_4, ITEM_CODE_TEST_1234, 5.00f, 0, 0, DateUtil.getWeekStartDate(0));

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_LOWER,
				Constants.DEFAULT_NA, 2.7f, Constants.DEFAULT_NA, 2.7f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_ABOVE,
				4.4, 4.4, 2.7, 2.7);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_WITHIN, 1.8, 2.2, 2.7, 2.7);
		guidelineCompDetail.setCompStrId(MULTI_COMP_4);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, Constants.DEFAULT_NA, 5.5, 2.7, 2.7);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, 2.7, 2.7, 2.7, 2.7, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// MULTI COMP (Use Rule)
	// 10-20% lower, > 10-20%, within 10-20%, not > 10-20%,
	@Test
	public void testCase13() throws GeneralException, Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_RULE, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		// 10-20% lower
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, PRConstants.VALUE_TYPE_PCT, 10, 20, PRConstants.PRICE_GROUP_EXPR_LOWER, false);
		// > 10-20%
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, PRConstants.VALUE_TYPE_PCT, 10, 20, PRConstants.PRICE_GROUP_EXPR_GREATER_SYM,
				false);
		// within 10-20%
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, PRConstants.VALUE_TYPE_PCT, 10, 20, PRConstants.PRICE_GROUP_EXPR_WITHIN,
				false);
		// not > 10-20%
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_4, PRConstants.VALUE_TYPE_PCT, 10, 20,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, false);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 4.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_4, ITEM_CODE_TEST_1234, 5.00f, 0, 0, DateUtil.getWeekStartDate(0));

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_LOWER,
				2.4, 2.7, 2.4, 2.7);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, 4.4, 4.8, 2.7, 2.7);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_WITHIN, 1.8, 2.2, 2.7, 2.7);
		guidelineCompDetail.setCompStrId(MULTI_COMP_4);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, 5.5, 6, 2.7, 2.7);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, 2.7, 2.7, 2.7, 2.7, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// $1 lower, > $1, within $1, not > $1
	@Test
	public void testCase14() throws GeneralException, Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_RULE, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		// $1 lower
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, PRConstants.VALUE_TYPE_$, 1, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LOWER, false);
		// > $1
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, PRConstants.VALUE_TYPE_$, 1, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_ABOVE, false);
		// within $1
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, PRConstants.VALUE_TYPE_$, 1, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_WITHIN, false);
		// not > $1
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_4, PRConstants.VALUE_TYPE_$, 1, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, false);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 4.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_4, ITEM_CODE_TEST_1234, 5.00f, 0, 0, DateUtil.getWeekStartDate(0));

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_LOWER,
				Constants.DEFAULT_NA, 2, Constants.DEFAULT_NA, 2);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_ABOVE,
				5, 5, 2, 2);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_WITHIN, 1, 3, 2, 2);
		guidelineCompDetail.setCompStrId(MULTI_COMP_4);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, Constants.DEFAULT_NA, 6, 2, 2);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, 2, 2, 2, 2, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// $1-2 lower, > $1-2, within $1-2, not > $1-2,
	@Test
	public void testCase15() throws GeneralException, Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_RULE, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		// $1-2 lower
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, PRConstants.VALUE_TYPE_$, 1, 2, PRConstants.PRICE_GROUP_EXPR_LOWER, false);
		// > $1-2
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, PRConstants.VALUE_TYPE_$, 1, 2, PRConstants.PRICE_GROUP_EXPR_ABOVE, false);
		// within $1-2
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, PRConstants.VALUE_TYPE_$, 1, 2, PRConstants.PRICE_GROUP_EXPR_WITHIN, false);
		// not > $1-2
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_4, PRConstants.VALUE_TYPE_$, 1, 2, PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN,
				false);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 4.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_4, ITEM_CODE_TEST_1234, 5.00f, 0, 0, DateUtil.getWeekStartDate(0));

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_LOWER,
				1, 2, 1, 2);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_ABOVE,
				5, 6, 2, 2);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_WITHIN, 1, 3, 2, 2);
		guidelineCompDetail.setCompStrId(MULTI_COMP_4);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, 6, 7, 2, 2);
		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, 2, 2, 2, 2, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// >, >=, =, equal, above, below, <, lower, <=, not >
	@Test
	public void testCase16() throws GeneralException, Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_RULE, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, MULTI_COMP_OBS_DAY_60);
		// >
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, false);
		// >=
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM, false);
		// =
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM, false);
		// equal
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_4, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_EQUAL, false);

		// above
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_5, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_ABOVE, false);

		// below
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_6, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_BELOW, false);

		// <
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_7, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, false);

		// lower
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_8, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LOWER, false);

		// <=
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_9, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, false);

		// not >
		TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_10, ' ', Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, false);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Multi Comp Data
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 3.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 4.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 2.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_4, ITEM_CODE_TEST_1234, 5.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_5, ITEM_CODE_TEST_1234, 6.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_6, ITEM_CODE_TEST_1234, 9.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_7, ITEM_CODE_TEST_1234, 12.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_8, ITEM_CODE_TEST_1234, 1.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_9, ITEM_CODE_TEST_1234, 3.00f, 0, 0, DateUtil.getWeekStartDate(0));
		TestHelper.addMutiCompData(multiCompData, MULTI_COMP_10, ITEM_CODE_TEST_1234, 2.00f, 0, 0, DateUtil.getWeekStartDate(0));

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

		PRGuidelineCompDetail guidelineCompDetail = new PRGuidelineCompDetail();
		guidelineCompDetail.setCompStrId(MULTI_COMP_1);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_GREATER_SYM, 3.01f, Constants.DEFAULT_NA, 3.01f, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_2);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM, 4f, Constants.DEFAULT_NA, 4f, Constants.DEFAULT_NA);
		guidelineCompDetail.setCompStrId(MULTI_COMP_3);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM, 2f, 2f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_4);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_EQUAL,
				5f, 5f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_5);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_ABOVE,
				6.01f, 6.01f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_6);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_BELOW,
				8.99f, 8.99f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_7);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, Constants.DEFAULT_NA, 11.99f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_8);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail, PRConstants.PRICE_GROUP_EXPR_LOWER,
				Constants.DEFAULT_NA, 0.99f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_9);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, Constants.DEFAULT_NA, 3f, 4f, 4f);
		guidelineCompDetail.setCompStrId(MULTI_COMP_10);
		TestHelper.setCompDetailLog(item, strategy, multiCompData, guidelineAndConstraintLog, guidelineCompDetail,
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN, Constants.DEFAULT_NA, 1.99f, 4f, 4f);

		TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, false, 4, 4, 4, 4, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.91,
				3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

		// item.setRecommendedRegPrice(3.99);
		item.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
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

	// NU:: 4th July 2017, when there is cost increase, threshold can be broken
	// item level maintain penny profit on cost change
	 
	public void testCase17() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMGItemMaintainPennyProfitOnCostChange(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 1, 3);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.19, strategy,
				LAST_X_WEEKS_MOV_1);
		// itemDataMap.put(ITEM_CODE_TEST_1234,item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();// dummy variable, output items are stored here.
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
		// compIdMap.put(PRConstants.COMP_TYPE_PRIMARY,COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.67, 4.77, 3.67, 4.77, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 3.09, 3.22, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.22,
				3.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.22, 3.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.19);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

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
