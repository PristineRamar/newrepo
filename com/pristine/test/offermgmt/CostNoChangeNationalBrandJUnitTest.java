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

public class CostNoChangeNationalBrandJUnitTest {

	public static final Integer COST_NO_CHANGE = 0;	 
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer REALTED_ITEM_CODE_BRAND_TEST_1245 = 12345;
	public static final Integer REALTED_ITEM_CODE_SIZE_TEST_1246 = 12346;
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
	
	
	// NB vs NB(0.20 below), Margin, Brand, PI
	@Test
	public void testNBvsNB1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
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
		PRItemDTO item = TestHelper
				.getTestItem(ITEM_CODE_TEST_1234, 1, 6.59, null, 5.25, 5.25, COST_NO_CHANGE, COMP_STR_ID_TEST_967, 6.69, strategy, LAST_X_WEEKS_MOV_1);
		//itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 6.99);
		//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		
		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 0.20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);
		
		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 6.79, 6.79, 6.79, 6.79,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 6.76, 7.04, 6.79, 6.79, "", new MultiplePrice(1, 6.69));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 5.93, 7.25, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 6.79, 6.79,"");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 5.25, Constants.DEFAULT_NA, 6.79, 6.79, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(6.79);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(6.79);
		item.setRecommendedRegPrice(new MultiplePrice(1, 6.79));
		
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
	
	// Production Issue(PROM-444), threshold is not applied (No previous cost, no comp price)
	// properly(SIZE,PI,MARGIN) 
	@Test
	public void testThreshold1() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 12.29, null, 9.75, 0d, COST_NO_CHANGE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		//itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 68, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 7.49);
		//itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 25, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;


		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 7.56, 20.17, 7.56, 20.17,
				REALTED_ITEM_CODE_SIZE_TEST_1246, false, 25);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				7.56, 20.17, "No Competition Price", null);	
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				7.56, 20.17, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 11.06, 13.52, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 11.06, 13.52,"");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 9.75, Constants.DEFAULT_NA, 11.06, 13.52, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		 
		roundingDigits.add(11.29);	 
		roundingDigits.add(11.49);		 
		roundingDigits.add(11.69);
		roundingDigits.add(11.79);
		roundingDigits.add(11.99);		 
		roundingDigits.add(12.29);		 
		roundingDigits.add(12.49);		 
		roundingDigits.add(12.69);
		roundingDigits.add(12.79);
		roundingDigits.add(12.99);	 
		roundingDigits.add(13.29);		 
		roundingDigits.add(13.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);


//		item.setRecommendedRegPrice(12.29);
		item.setRecommendedRegPrice(new MultiplePrice(1, 12.29));
		
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
	
	// NB vs NB(0.20 below), Margin, Brand, PI
	@Test
	public void testNBvsNB2() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setBrandGuideline(strategy);
		//TestHelper.setPIGuideline(strategy, 95, 99);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper
				.getTestItem(ITEM_CODE_TEST_1234, 1, 6.59, null, 5.25, 5.25, COST_NO_CHANGE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		//itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 0, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.49);
		//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		
		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 0.20,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 7.29, 7.29, 7.29, 7.29,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 5.93, 7.25, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 7.25, 7.25,"");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 5.25, Constants.DEFAULT_NA, 7.25, 7.25, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(7.19);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(7.19);
		item.setRecommendedRegPrice(new MultiplePrice(1, 7.19));
		
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
		public void noCostAvailbleWithAllRules1() throws Exception {

			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
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
			PRItemDTO item = TestHelper
					.getTestItem(ITEM_CODE_TEST_1234, 1, 6.59, null, null, 5.25, COST_NO_CHANGE, COMP_STR_ID_TEST_967, 5.2d, strategy, LAST_X_WEEKS_MOV_1);
			//itemDataMap.put(ITEM_CODE_TEST_1234, item);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
			
			// dummy variable, output items are stored here.
			ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//			compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

			// Price Group Information
			TestHelper.setPriceGroup(item, 0, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

			// Set Related Item
			PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.49);
			//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
			
			// Set Brand Relation
			TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 0.20,
					Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

			// Set Expected Results
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 6.59, 6.59, 6.59, 6.59, "Cost not available");
			
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					6.59, 6.59, "Cost not available");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 7.29, 7.29, 6.59, 6.59,
					REALTED_ITEM_CODE_BRAND_TEST_1245, false);
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.25, 5.47, 6.59,
					6.59, "", new MultiplePrice(1, 5.20));

			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 5.93, 7.25, Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, 6.59, 6.59,"");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 6.59, 6.59, "Cost not available");
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			List<Double> roundingDigits = new ArrayList<Double>();
			roundingDigits.add(6.59);
			TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//			item.setRecommendedRegPrice(6.59);
			item.setRecommendedRegPrice(new MultiplePrice(1, 6.59));
			
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
		
		// No Cost, No Price Available (with price index)
				@Test
				public void noCostAvailbleWithAllRules2() throws Exception {

					// initialize
					PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
					TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

					TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
					TestHelper.setBrandGuideline(strategy);
					//TestHelper.setPIGuideline(strategy, 95, 99);

					TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
					TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
					TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

					PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
					// curr_price, curr_cost, prev_cost, ..., comp_price
					HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
					PRItemDTO item = TestHelper
							.getTestItem(ITEM_CODE_TEST_1234, 0, null, null, null, 5.25, COST_NO_CHANGE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
					//itemDataMap.put(ITEM_CODE_TEST_1234, item);
					itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
					
					// dummy variable, output items are stored here.
					ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
					HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
					TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//					compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

					// Price Group Information
					TestHelper.setPriceGroup(item, 0, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

					// Set Related Item
					PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.49);
					//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
					itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
					
					// Set Brand Relation
					TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 0.20,
							Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

					// Set Expected Results
					PRExplainLog explainLog = new PRExplainLog();
					PRGuidelineAndConstraintLog guidelineAndConstraintLog;

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, Constants.DEFAULT_NA, "Cost not available");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 7.29, 7.29, 7.29, 7.29,
							REALTED_ITEM_CODE_BRAND_TEST_1245, false);
					
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 7.29, 7.29,"Current Retail not available");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
							7.29, 7.29, "Cost not available");
					
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					List<Double> roundingDigits = new ArrayList<Double>();
					roundingDigits.add(7.29);
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//					item.setRecommendedRegPrice(7.29);
					item.setRecommendedRegPrice(new MultiplePrice(1, 7.29));
					
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
					PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
					TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

					TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
					TestHelper.setBrandGuideline(strategy);
					TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM,
							MULTI_COMP_OBS_DAY_60);
					TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_1, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);
					TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_2, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);
					TestHelper.setMultiCompDetailGuideline(strategy, MULTI_COMP_3, 'O', Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", false);

					TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
					TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
					TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

					PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
					// curr_price, curr_cost, prev_cost, ..., comp_price
					HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
					PRItemDTO item = TestHelper
							.getTestItem(ITEM_CODE_TEST_1234, 1, 6.59, null,  null, 5.25, COST_NO_CHANGE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
					//itemDataMap.put(ITEM_CODE_TEST_1234, item);
					itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
					
					// dummy variable, output items are stored here.
					ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
					HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
					TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//					compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

					// Set Multi Comp Data
					TestHelper.addMutiCompData(multiCompData, MULTI_COMP_1, ITEM_CODE_TEST_1234, 4.77f, 0, 0, DateUtil.getWeekStartDate(0));
					TestHelper.addMutiCompData(multiCompData, MULTI_COMP_2, ITEM_CODE_TEST_1234, 6.77f, 0, 0, DateUtil.getWeekStartDate(0));
					TestHelper.addMutiCompData(multiCompData, MULTI_COMP_3, ITEM_CODE_TEST_1234, 5.98f, 0, 0, DateUtil.getWeekStartDate(0));
					
					// Price Group Information
					TestHelper.setPriceGroup(item, 0, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

					// Set Related Item
					PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 7.49);
					//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
					itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
					
					// Set Brand Relation
					TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 0, PRConstants.BRAND_RELATION, 0.20,
							Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

					// Set Expected Results
					PRExplainLog explainLog = new PRExplainLog();
					PRGuidelineAndConstraintLog guidelineAndConstraintLog;

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostNotAvailableLog(explainLog, guidelineAndConstraintLog, true, false, 6.59, 6.59, 6.59, 6.59, "Cost not available");
					
					
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
							6.59, 6.59, "Cost not available");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 7.29, 7.29, 6.59, 6.59,
							REALTED_ITEM_CODE_BRAND_TEST_1245, false);
					
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
					TestHelper.setMultiCompLog(item, strategy, multiCompData, explainLog, guidelineAndConstraintLog, true, true, Constants.DEFAULT_NA,
							4.77, 6.59, 6.59, "");
					
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 5.93, 7.25, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 6.59, 6.59,"");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 6.59, 6.59, "Cost not available");
					
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					List<Double> roundingDigits = new ArrayList<Double>();
					roundingDigits.add(6.59);
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//					item.setRecommendedRegPrice(6.59);
					item.setRecommendedRegPrice(new MultiplePrice(1, 6.59));
					
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
