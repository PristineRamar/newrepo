package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
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
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class LockedPriceJUnitTest {
	private static Logger logger = Logger.getLogger("LockedPriceJunitTest");
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
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

	// With multiple current price
	@Test
	public void testLocPrice2() throws Exception {
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 2, 4.99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 2, null, 4.69, 2.1, 2.1, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		List<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 4.99, 4.99, 4.99, 4.99, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.99, 4.99, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.1, Constants.DEFAULT_NA,	4.99, 4.99, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.99);
		
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);
		
//		item.setRecommendedRegMultiple(2);
//		item.setRecommendedRegPrice(4.99);
		item.setRecommendedRegPrice(new MultiplePrice(2, 4.99));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}
	
	// With multiple current price, cost is changed
	@Test
	public void testLocPrice9() throws Exception {
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 2, 4.99);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 2, null, 4.99, 2.2, 2.1, COST_INCREASE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		List<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, true, 4.99, 4.99, 4.99, 4.99, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, 2.62, 2.62, 2.62, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.2, Constants.DEFAULT_NA,	2.62, 2.62, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.69);
		
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);
		
//		item.setRecommendedRegMultiple(1);
//		item.setRecommendedRegPrice(2.69);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.69));

		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}
	
	// With multiple current price, breaking cost and rounding (compare cost against unit price
	@Test
	public void testLocPrice8() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 3, 5.00);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 3, null, 5.0, 2.6, 2.6, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 5.00, 5.00, 5.00, 5.00, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 5.00, 5.00, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		//Unit cost
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, true, 2.6, Constants.DEFAULT_NA, 5.00, 5.00, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		//roundingDigits.add(5.19);
		roundingDigits.add(4.99);

		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, roundingDigits);

//		item.setRecommendedRegMultiple(3);
//		item.setRecommendedRegPrice(5.00);
		item.setRecommendedRegPrice(new MultiplePrice(3, 5.00));
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// With single current price
	@Test
	public void testLocPrice1() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 1, 3.69);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.29, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, true, 3.69, 3.69, 3.69, 3.69, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 4.03, 4.03, 4.03, 4.03, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 4.03, 4.03, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(4.19);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(4.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 4.59));
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// With no current price, but cost available
	@Test
	public void testLocPrice3() throws Exception {
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 1, 3.69);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, null, null, 2.62, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 3.69, 3.69, 3.69, 3.69,"");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69, 3.69, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA,	3.69, 3.69, "");

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
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// With no current price, no cost available
	@Test
	public void testLocPrice4() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 1, 3.69);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, null, null, null, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 3.69, 3.69, 3.69, 3.69,"");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69, 3.69, "Cost not available");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69, 3.69, "Cost not available");

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
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// With no cost available
	@Test
	public void testLocPrice5() throws Exception {
		PRStrategyDTO strategy = TestHelper
				.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 1, 3.69);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.69, 3.59, null, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 3.69, 3.69, 3.69, 3.69, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69, 3.69, "Cost not available");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.69, 3.69, "Cost not available");

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
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// With single current price, current price is less than cost (cost and rounding in conflict)
	@Test
	public void testLocPrice6() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 1, 2.59);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.49, null, 2.62, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 2.59, 2.59, 2.59, 2.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.59, 2.59, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, true, 2.62, Constants.DEFAULT_NA, 2.59, 2.59, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.69);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, roundingDigits);

//		item.setRecommendedRegPrice(2.59);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.59));
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

	// With single current price, current price is not following rounding
	@Test
	public void testLocPrice7() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setLocPriceConstraint(strategy, 1, 2.75);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		TestHelper.setCostConstraint(strategy, false);
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.75, null, 2.62, 2.62, COST_NO_CHANGE,
				COMP_STR_ID_TEST_967, 3.49, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setLockedPriceLog(explainLog, guidelineAndConstraintLog, true, false, 2.75, 2.75, 2.75, 2.75, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,	2.75, 2.75, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA,	2.75, 2.75, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, roundingDigits);

//		item.setRecommendedRegPrice(2.75);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.75));
		// call applyStrategies
		new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
				new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
				recommendationRunHeader);
		try {
			String actualExplainLog = mapper.writeValueAsString(item.getExplainLog());
			logger.debug(actualExplainLog);
			String expExplainLog = mapper.writeValueAsString(explainLog);
			logger.debug(expExplainLog);
			assertEquals("JSON Not Matching", expExplainLog, actualExplainLog);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals("Mismatch", item.getExplainLog(), explainLog);
	}

}
