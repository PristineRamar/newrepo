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
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CostIncreaseNationalBrandJUnitTest {

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
	
	@Before
    public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/SpecificAhold.properties");
    }
	
	//NB vs NB(0.30 below)
	//MARGIN, BRAND, PI
	@Test
	public void testNBvsNB1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.09, strategy,LAST_X_WEEKS_MOV_1);
		//itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 16.3, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.99);
		//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION, 0.30,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

		
		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 3.69, 3.69, 3.91, 3.91,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);
		 
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 3.09, Constants.DEFAULT_NA, 3.91,
				3.91, "", new MultiplePrice(1, 3.09));		

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.87, 3.51, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.91, 3.91,"");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(3.99);
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
	
	// Size between NB vs NB, Size
	@Test
	public void testSizeCostIncrease1() throws Exception {

		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setSizeGuideline(strategy, 2);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 2.62, 2.14, COST_INCREASE, COMP_STR_ID_TEST_967, 3.09, strategy, LAST_X_WEEKS_MOV_1);
		//itemDataMap.put(ITEM_CODE_TEST_1234, item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 20, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 1.79);
		//itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 10, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 3.91, 3.91, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, true, 1.83, 3.54, 3.91, 3.91,
				REALTED_ITEM_CODE_SIZE_TEST_1246, false, 10);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 3.09, Constants.DEFAULT_NA, 3.91, 3.91, "", new MultiplePrice(1, 3.09));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.87, 3.51, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 3.91, 3.91,"");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 2.62, Constants.DEFAULT_NA, 3.91, 3.91, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(3.99);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);
		
//		item.setRecommendedRegPrice(3.99);
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

	
	//Only Size Present (Brand is excluded), Price group has both Brand and Size
			@Test
			public void testExcludeOnlySize() throws Exception {

				// initialize
				PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
				TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

				TestHelper.setSizeGuideline(strategy, 0);
				//TestHelper.setBrandGuideline(strategy);
				TestHelper.setPIGuideline(strategy, 95, 99);
				TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

				TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
				TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
				TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

				PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
				// curr_price, curr_cost, prev_cost, ..., comp_price
				HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
				PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy, LAST_X_WEEKS_MOV_1);
				//itemDataMap.put(ITEM_CODE_TEST_1234, item);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
				
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
				TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//				compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

				// Price Group Information
				TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

				// Set Related Item for Size
				PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
				//itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
				
				// Set Size Relation
				TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

				// Set Related Item for Brand
				relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
				//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
				
				// Set Brand Relation
				TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
						PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

				// call applyStrategies
				new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
						recommendationRunHeader);
				
				// Set Expected Results
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 5.5, 11.44, 5.5, 11.44,
						REALTED_ITEM_CODE_SIZE_TEST_1246, false, 38);

				//guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				//TestHelper.setBrandLog(explainLog, guidelineAndConstraintLog, true, false, 8.32, 8.91, 8.32, 8.91,
						//REALTED_ITEM_CODE_BRAND_TEST_1245, false);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 5.55, 5.78, 5.55, 5.78, "", new MultiplePrice(1, 5.49));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 8.85, 8.85, 8.85, 8.85, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 8.85, 8.85,"");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.85, "");
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				List<Double> roundingDigits = new ArrayList<Double>();
				roundingDigits.add(8.99);
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

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
			
			
		// Only Brand Present (Size is excluded), Price group has both Brand and Size
		@Test
		public void testExcludeOnlyBrand() throws Exception {

			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			//TestHelper.setSizeGuideline(strategy);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

			PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
			// curr_price, curr_cost, prev_cost, ..., comp_price
			HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
			PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy, LAST_X_WEEKS_MOV_1);
			//itemDataMap.put(ITEM_CODE_TEST_1234, item);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
			
			ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//			compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

			// Price Group Information
			TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

			// Set Related Item for Size
			PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
			//itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
			
			// Set Size Relation
			TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

			// Set Related Item for Brand
			relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
			//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
			
			// Set Brand Relation
			TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

		

			// Set Expected Results
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;

			/*guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setSizeLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 11.44, Constants.DEFAULT_NA, 11.44,
					REALTED_ITEM_CODE_SIZE_TEST_1246, false);*/

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 8.32, 8.91, 8.32, 8.91,			
					REALTED_ITEM_CODE_BRAND_TEST_1245, false);

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 5.55, 5.78, 8.32, 8.32, "", new MultiplePrice(1, 5.49));

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 8.85, 8.85, 8.85, 8.85, "");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, 8.85, 8.85,"");
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.85, "");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			List<Double> roundingDigits = new ArrayList<Double>();
			roundingDigits.add(8.99);
			TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//			item.setRecommendedRegPrice(8.99);
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
				// TODO Auto-generated catch block
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

				//TestHelper.setSizeGuideline(strategy);
				//TestHelper.setBrandGuideline(strategy);
				TestHelper.setPIGuideline(strategy, 95, 99);
				TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

				TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
				TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
				TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

				PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
				// curr_price, curr_cost, prev_cost, ..., comp_price
				HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
				PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 8.59, null, 6.45, 6.26, COST_INCREASE, COMP_STR_ID_TEST_967, 5.49, strategy, LAST_X_WEEKS_MOV_1);
				//itemDataMap.put(ITEM_CODE_TEST_1234, item);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
				
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
				TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//				compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

				// Price Group Information
				TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

				// Set Related Item for Size
				PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 5.49);
				//itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
				
				// Set Size Relation
				TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 38, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

				// Set Related Item for Brand
				relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 11.29);
				//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
				
				// Set Brand Relation
				TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 76, PRConstants.BRAND_RELATION, 25, 30,
						PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_UNIT, PRConstants.VALUE_TYPE_PCT);

				// call applyStrategies
				new PricingEngineWS().applyStrategies(item, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
						recommendationRunHeader);

				// Set Expected Results
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				/*guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setSizeLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 11.44, Constants.DEFAULT_NA, 11.44,
						REALTED_ITEM_CODE_SIZE_TEST_1246, false);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(explainLog, guidelineAndConstraintLog, true, false, 8.32, 8.91, 8.32, 8.91,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);*/

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 5.55, 5.78, 5.55, 5.78, "", new MultiplePrice(1, 5.49));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 8.85, 8.85, 8.85, 8.85, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 7.73, 9.45, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 8.85, 8.85,"");
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 6.45, Constants.DEFAULT_NA, 8.85, 8.85, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				List<Double> roundingDigits = new ArrayList<Double>();
				roundingDigits.add(8.99);
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

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

			
			//Lower Constraint rounding down can't break cost constraint
			//MARGIN, BRAND, PI
			@Test
			public void testCase11() throws Exception {

				// initialize
				PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
				TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

				TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
				TestHelper.setBrandGuideline(strategy);			 
				TestHelper.setLowerHigherConstraint(strategy, PRConstants.CONSTRAINT_LOWER);

				TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
				TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
				TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

				PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
				// curr_price, curr_cost, prev_cost, ..., comp_price
				HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
				PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 3.19, null, 3.06, 2.85, COST_INCREASE, COMP_STR_ID_TEST_967, 3.09, strategy, LAST_X_WEEKS_MOV_1);
				//itemDataMap.put(ITEM_CODE_TEST_1234, item);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
				
				// dummy variable, output items are stored here.
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
				TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//				compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

				// Price Group Information
				TestHelper.setPriceGroup(item, 16.3, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

				// Set Related Item
				PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 3.39);
				//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
				
				// Set Brand Relation
				TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION, 0.30,
						Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);

				
				// Set Expected Results
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, true, 3.43, 3.43, 3.43, 3.43, "");
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 3.09, 3.09, 3.43, 3.43,
						REALTED_ITEM_CODE_BRAND_TEST_1245, false);
				 
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.87, 3.51, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 3.43, 3.43,"");
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.06, Constants.DEFAULT_NA, 3.43, 3.43, "");
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLowerHigherConstraintLog(explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 3.19, 3.19, 3.19, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				List<Double> roundingDigits = new ArrayList<Double>();
				roundingDigits.add(3.19);
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//				item.setRecommendedRegPrice(3.19);
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
}
