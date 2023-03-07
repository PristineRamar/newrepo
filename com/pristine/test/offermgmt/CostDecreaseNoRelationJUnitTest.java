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
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostDecreaseNoRelationJUnitTest {

	public static final Integer COST_DECREASE = -1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
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

	@Before
	public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/SpecificAhold.properties");
	}

	// PI, MARGIN
	@Test
	public void testCase1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setPIGuideline(strategy, 98, 102);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 1.60, 2.14, COST_DECREASE,
				COMP_STR_ID_TEST_967, 3.19, strategy, LAST_X_WEEKS_MOV_1);
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
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 3.13, 3.26, 3.13, 3.26, "", new MultiplePrice(1,3.19));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 2.65, 2.65, 2.65, 2.65, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.65, 2.65, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.60, Constants.DEFAULT_NA,
				2.65, 2.65, "");

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

	// Incomplete test case
	@Test
	public void marginInNegative1() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, 98, Constants.DEFAULT_NA);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 6.25, null, 4.24, 15.69, COST_DECREASE, 0, 0d,
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
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 0, 0, 0, 0, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 0, 0, "No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 5.62, 6.88, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 0, 0, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 4.24, Constants.DEFAULT_NA,
				4.24, 4.24, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.29);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(4.29);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.29));
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

	@Test
	public void testNegativeRange() {
		FilterPriceRange filterRange = new FilterPriceRange();
		PRRange range;

		range = new PRRange();
		range.setStartVal(-5.2);
		range.setEndVal(-5.2);
		filterRange.handleNegativeRange(range);

		range = new PRRange();
		range.setStartVal(-5.2);
		range.setEndVal(3.19);
		filterRange.handleNegativeRange(range);

		range = new PRRange();
		range.setStartVal(3.19);
		range.setEndVal(-3.19);
		filterRange.handleNegativeRange(range);

		range = new PRRange();
		range.setStartVal(Constants.DEFAULT_NA);
		range.setEndVal(Constants.DEFAULT_NA);
		filterRange.handleNegativeRange(range);

		range = new PRRange();
		range.setStartVal(Constants.DEFAULT_NA);
		range.setEndVal(-5.2);
		filterRange.handleNegativeRange(range);

		range = new PRRange();
		range.setStartVal(-5.2);
		range.setEndVal(Constants.DEFAULT_NA);
		filterRange.handleNegativeRange(range);
	}
}
