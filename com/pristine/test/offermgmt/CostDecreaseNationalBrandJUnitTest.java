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

public class CostDecreaseNationalBrandJUnitTest {
	
	public static final Integer COST_DECREASE = -1;	 
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
		//PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/SpecificAhold.properties");
    }
	
	
	
	// BRAND, MARGIN, INDEX
	@Test
	public void testNBvsNB2() throws Exception {
		// initialize
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setBrandGuideline(strategy);
		TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);
		TestHelper.setPIGuideline(strategy, Constants.DEFAULT_NA, 100);

		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		// curr_price, curr_cost, prev_cost, ..., comp_price
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 2.79, null, 1.63, 2.01, COST_DECREASE, 0, 0d, strategy, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// dummy variable, output items are stored here.
		ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//		compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

		// Price Group Information
		TestHelper.setPriceGroup(item, 16.3, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 2.79);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);

		// Set Brand Relation
		TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');

		// Set Expected Results
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, Constants.DEFAULT_NA, 2.79,
				REALTED_ITEM_CODE_BRAND_TEST_1245, false);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 2.41, 2.41, 2.41, 2.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 2.41,
				2.41, "No Competition Price", null);

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.51, 3.07, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, 2.41, 2.41,"");
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.63, Constants.DEFAULT_NA, 2.41, 2.41, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		List<Double> roundingDigits = new ArrayList<Double>();
		roundingDigits.add(2.49);
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//		item.setRecommendedRegPrice(2.49);
		item.setRecommendedRegPrice(new MultiplePrice(1, 2.49));
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
	
	//SIZE, BRAND, INDEX, MARGIN
		@Test
		public void testNBvsNB3() throws Exception {

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
			PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 11.49, null, 8.84, 9.26, COST_DECREASE, COMP_STR_ID_TEST_967, 12.49, strategy, LAST_X_WEEKS_MOV_1);
			//itemDataMap.put(ITEM_CODE_TEST_1234, item);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

			// dummy variable, output items are stored here.
			ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
//			compIdMap.put(PRConstants.COMP_TYPE_PRIMARY, COMP_STR_ID_TEST_967);

			// Price Group Information
			TestHelper.setPriceGroup(item, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

			// Set Related Item
			PRItemDTO relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_SIZE_TEST_1246, 7.19);
			//itemDataMap.put(REALTED_ITEM_CODE_SIZE_TEST_1246, relatedItem);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
			
			// Set Size Relation
			TestHelper.setSizeRelation(item, REALTED_ITEM_CODE_SIZE_TEST_1246, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			
			// Set Related Item
			relatedItem = TestHelper.getRelatedItem(REALTED_ITEM_CODE_BRAND_TEST_1245, 12.49);
			//itemDataMap.put(REALTED_ITEM_CODE_BRAND_TEST_1245, relatedItem);
			itemDataMap = TestHelper.setItemDataMap(itemDataMap, relatedItem);
			
			// Set Brand Relation
			TestHelper.setBrandRelation(item, REALTED_ITEM_CODE_BRAND_TEST_1245, 0, 'X', 16, PRConstants.BRAND_RELATION, 0.20,
					Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_$);
			
			// Set Expected Results
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 7.26, 12.02, 7.26, 12.02,
					REALTED_ITEM_CODE_SIZE_TEST_1246, false, 45);
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 12.29, 12.29, 12.02, 12.02,
					REALTED_ITEM_CODE_BRAND_TEST_1245, false);
			 
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 12.62, 13.15, 12.02, 12.02, "", new MultiplePrice(1,12.49));		
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 11.07, 11.07, 11.07, 11.07, "");
			

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 10.34, 12.64, Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, 11.07, 11.07,"");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 8.84, Constants.DEFAULT_NA, 11.07, 11.07, "");
			
			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			List<Double> roundingDigits = new ArrayList<Double>();
			roundingDigits.add(11.29);
			TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, roundingDigits);

//			item.setRecommendedRegPrice(11.29);
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
}
