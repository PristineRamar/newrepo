package com.pristine.test.offermgmt;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.service.offermgmt.PriceRollbackService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.RerecommendationService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class ReRecommendationJUnitTest {
	List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompData = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	ItemService itemService = new ItemService(executionTimeLogs);
	String curWeekStartDate = DateUtil.getWeekStartDate(0);
	private String chainId = "50";
	@Before
	public void setup() {
//		PropertyManager.initialize("recommendation.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");
	}
	/**
	 * User overridden for Dependent item and Parent item needs to be changed 
	 */
	@Test
	public void testCase1(){
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();
		
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
//		String item_27010_log="{\"logs\":[{\"g-t-id\":2,\"c-t-id\":0,\"conflict\":true,\"applied\":true,\"pr1\":{\"s-v\":2.01,\"e-v\":2.16},\"pr2\":"
//				+ "{\"s-v\":-9999.0,\"e-v\":-9999.0},\"opr\":{\"s-v\":2.01,\"e-v\":2.16},\"c-d\":[],\"r1\":{\"s-v\":\"2.01\",\"e-v\":\"2.16\"},\"r2\":"
//				+ "{\"s-v\":\"-\",\"e-v\":\"-\"},\"o-r\":{\"s-v\":\"2.01\",\"e-v\":\"2.16\"},\"msg\":\"\",\"r-m\":null,\"r-p\":null,\"lir\":false,\""
//				+ "r-i-c\":0,\"r-i-s\":\"\",\"r-u-n\":\"\",\"r-t\":\"\",\"r-pe\":null,\"r-d\":[],\"l-c\":true,\"i-c-p\":{\"m\":1,\"p\":1.99}},"
//				+ "{\"g-t-id\":0,\"c-t-id\":5,\"conflict\":false,\"applied\":true,\"pr1\":{\"s-v\":2.29,\"e-v\":3.09},\"pr2\":{\"s-v\":-9999.0,\"e-v\":"
//				+ "-9999.0},\"opr\":{\"s-v\":2.29,\"e-v\":2.29},\"c-d\":[],\"r1\":{\"s-v\":\"2.29\",\"e-v\":\"3.09\"},\"r2\":{\"s-v\":\"-\",\"e-"
//				+ "v\":\"-\"},\"o-r\":{\"s-v\":\"2.29\",\"e-v\":\"2.29\"},\"msg\":\"\",\"r-m\":null,\"r-p\":null,\"lir\":false,\"r-i-c\":0,\"r-i-"
//				+ "s\":\"\",\"r-u-n\":\"\",\"r-t\":\"\",\"r-pe\":null,\"r-d\":[],\"l-c\":true,\"i-c-p\":null},{\"g-t-id\":0,\"c-t-id\":8,\"conflict"
//				+ "\":false,\"applied\":true,\"pr1\":{\"s-v\":1.91,\"e-v\":-9999.0},\"pr2\":{\"s-v\":-9999.0,\"e-v\":-9999.0},\"opr\":{\"s-v\":2.29"
//				+ ",\"e-v\":2.29},\"c-d\":[],\"r1\":{\"s-v\":\"1.91\",\"e-v\":\"-\"},\"r2\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"o-r\":{\"s-v\":\"2.29\","
//				+ "\"e-v\":\"2.29\"},\"msg\":\"\",\"r-m\":null,\"r-p\":null,\"lir\":false,\"r-i-c\":0,\"r-i-s\":\"\",\"r-u-n\":\"\",\"r-t\":\"\","
//				+ "\"r-pe\":null,\"r-d\":[],\"l-c\":true,\"i-c-p\":null},{\"g-t-id\":0,\"c-t-id\":4,\"conflict\":true,\"applied\":false,\"pr1\":"
//				+ "{\"s-v\":-9999.0,\"e-v\":-9999.0},\"pr2\":{\"s-v\":-9999.0,\"e-v\":-9999.0},\"opr\":{\"s-v\":-9999.0,\"e-v\":-9999.0},\"c-d\":"
//				+ "[],\"r1\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"r2\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"o-r\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"msg\":\"\","
//				+ "\"r-m\":null,\"r-p\":null,\"lir\":false,\"r-i-c\":0,\"r-i-s\":\"\",\"r-u-n\":\"\",\"r-t\":\"\",\"r-pe\":null,\"r-d\":[2.39],\"l-c"
//				+ "\":true,\"i-c-p\":null}],\"a-d\":[{\"n-t-i\":3,\"n-v\":[\"2.39\"]}]}";
//		String item_224074_log = "{\"logs\":[{\"g-t-id\":0,\"c-t-id\":8,\"conflict\":false,\"applied\":true,\"pr1\":{\"s-v\":7.28,\"e-v\":-9999.0},"
//				+ "\"pr2\":{\"s-v\":-9999.0,\"e-v\":-9999.0},\"opr\":{\"s-v\":7.99,\"e-v\":7.99},\"c-d\":[],\"r1\":{\"s-v\":\"7.28\",\"e-v\":\"-\"},"
//				+ "\"r2\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"o-r\":{\"s-v\":\"7.99\",\"e-v\":\"7.99\"},\"msg\":\"\",\"r-m\":null,\"r-p\":null,\"lir\":"
//				+ "false,\"r-i-c\":0,\"r-i-s\":\"\",\"r-u-n\":\"\",\"r-t\":\"\",\"r-pe\":null,\"r-d\":[],\"l-c\":true,\"i-c-p\":null},{\"g-t-id\":0,"
//				+ "\"c-t-id\":4,\"conflict\":false,\"applied\":false,\"pr1\":{\"s-v\":-9999.0,\"e-v\":-9999.0},\"pr2\":{\"s-v\":-9999.0,\"e-v\":-9999.0}"
//				+ ",\"opr\":{\"s-v\":-9999.0,\"e-v\":-9999.0},\"c-d\":[],\"r1\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"r2\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"o-"
//				+ "r\":{\"s-v\":\"-\",\"e-v\":\"-\"},\"msg\":\"\",\"r-m\":null,\"r-p\":null,\"lir\":false,\"r-i-c\":0,\"r-i-s\":\"\",\"r-u-n\":\"\",\""
//				+ "r-t\":\"\",\"r-pe\":null,\"r-d\":[7.99],\"l-c\":true,\"i-c-p\":null}],\"a-d\":null}";
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long)105423,(long)38574165,"N",27010,(long)3122,0,1.91,0.0,0,"N",21,1, 2.69,3.69,0,0.0, 1,1.99,1,"", 
					0, 98,0.0,0,"N", 109, 0, 0,1, 2.69, "","",2, 1, 99,0,21,-1,-1,1,"001500004830",0,0.0,"","",0,0,0,0,1,strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);
			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
//			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long)105423,(long)38574030,"N",224074,(long)5131,0,7.28,0.0,29098,"N",0,1, 7.99,0,0,0.0, 0,0,0,item_224074_log, 
//					0, 12.06,0.0,0,"N", 0, 0, 0,1, 7.99, "3","",2, 1, 99,0,-1,-1,-1,1,"001500093758",0,0.0,"","",0,0,0,0,0,strategy);
//			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long)105423,(long)38574031,"N",224076,(long)5131,0,7.28,0.0,29098,"N",0,1, 7.99,0,0,0.0, 0,0,0,item_224074_log, 
//					0, 12.06,0.0,0,"N", 0, 0, 0,1, 7.99, "3","",2, 1, 99,0,-1,-1,-1,1,"001500093758",0,0.0,"","",0,0,0,0,0,strategy);
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long)105423,(long)38574032,"N",29098,(long)5131,0,7.28,0.0,0,"N",0,1, 7.99,0,0,0.0, 0,0,0,"", 
					0, 12.06,0.0,0,"N", 0, 0, 0,1, 7.99, "3","",2, 1, 99,0,-1,-1,-1,1,"001500093758",0,0.0,"","",0,0,0,0,0,strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long)105423,6,5,4,1277,6842,"09/22/2017","09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
//				rerecommendationService.getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId, leadZoneDivisionId, divisionId, pricingEngineDAO,
//						pricingEngineService);
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap,retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false,
						recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>(); 
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO,
						recommendationRuleMap);
				
				assertEquals("System Override not done",true,itemDTO1.isSystemOverrideFlag());
				assertEquals(7.29, itemDTO1.getRecommendedRegPrice().price,0);
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}
			
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * User overridden for parent item and dependent item needs to be changed 
	 */
	@Test
	public void testCase2() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				// rerecommendationService.getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId,
				// leadZoneDivisionId, divisionId, pricingEngineDAO,
				// pricingEngineService);
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done", true, itemDTO.isSystemOverrideFlag());
				assertEquals(2.69, itemDTO.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * User overridden for parent item and dependent item needs to be changed based on Brand relation
	 */
	@Test
	public void testCase3() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
//			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				// rerecommendationService.getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId,
				// leadZoneDivisionId, divisionId, pricingEngineDAO,
				// pricingEngineService);
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done", true, itemDTO.isSystemOverrideFlag());
				assertEquals(2.69, itemDTO.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * User overridden for parent item and multiple dependent items needs to be changed
	 * Relation: Size and Brand relations
	 */
	@Test
	public void testCase4() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
//			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO1, 29099, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				// rerecommendationService.getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId,
				// leadZoneDivisionId, divisionId, pricingEngineDAO,
				// pricingEngineService);
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done", true, itemDTO.isSystemOverrideFlag());
				assertEquals("System Override not done", true, itemDTO1.isSystemOverrideFlag());
				assertEquals(2.69, itemDTO.getRecommendedRegPrice().price, 0);
				assertEquals(7.29, itemDTO1.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * User overridden for middle item(Medium Size) and system override needs to be done for Small and Large items
	 * Relation: Size relation
	 */
	@Test
	public void testCase5() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 60, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO1, 29099, 0, 'N', 30, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done", true, itemDTO.isSystemOverrideFlag());
				assertEquals("System Override not done", true, itemDTO2.isSystemOverrideFlag());
				assertEquals(2.69, itemDTO.getRecommendedRegPrice().price, 0);
				assertEquals(7.99, itemDTO2.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * User overridden for Large item and system override needs to be done for Small and medium items
	 * Relation: Size relation
	 */
	@Test
	public void testCase6() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 4.99, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 60, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO1, 29099, 0, 'N', 30, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done", true, itemDTO1.isSystemOverrideFlag());
				assertEquals("System Override not done", true, itemDTO2.isSystemOverrideFlag());
				assertEquals(7.29, itemDTO1.getRecommendedRegPrice().price, 0);
				assertEquals(7.99, itemDTO2.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * User overridden for Large item(Which has Size and Brand relation of other 2 items), system override needs to be done for
	 * Small and medium items 
	 * Relation: Size and Brand relation
	 */
	@Test
	public void testCase7() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 4.99, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 60, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.setBrandRelation(itemDTO, 29099, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
//			TestHelper.setSizeRelation(itemDTO1, 29099, 0, 'N', 30, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done", true, itemDTO1.isSystemOverrideFlag());
				assertEquals("System Override not done", true, itemDTO2.isSystemOverrideFlag());
				assertEquals(7.29, itemDTO1.getRecommendedRegPrice().price, 0);
				assertEquals(7.99, itemDTO2.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * User overridden for Small item and system override needs to be done for Large and medium items 
	 * Relation: Size relation
	 */
	@Test
	public void testCase8() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 60, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
//			TestHelper.setBrandRelation(itemDTO, 29099, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					4.99, 0, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setSizeRelation(itemDTO1, 29099, 0, 'N', 30, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				assertEquals("System Override not done Large item", true, itemDTO.isSystemOverrideFlag());
				assertEquals("System Override not done medium item", true, itemDTO1.isSystemOverrideFlag());
				assertEquals(2.69, itemDTO.getRecommendedRegPrice().price, 0);
				assertEquals(7.99, itemDTO1.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * OVERRIDDEN ITEM IS LIG AND RELATED ITEM IS AT LIG LEVEL
	 * Related Item and Overridden item related at LIG level
	 * Output: Relation should be applied at LIG level.
	 */
	@Test
	public void testCase9() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// LIG Member 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			itemDTO.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
//			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(LIGItemDTO1, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			// Set IsLir flag
			LIGItemDTO1.getPgData().getRelationList().forEach((key,value)->{
				value.forEach(item-> item.setIsLig(true));
			});
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
//				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
//				for(Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry: itemDTO1.getPgData().getRelationList().entrySet()){
//					for(PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO: entry.getValue()){
//						
//					}
//				}
				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly for LIG item", 1001, item.getRelatedItemCode());
					});
				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly for LIG Member", 1001, item.getRelatedItemCode());
					});
				});
				
 			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * OVERRIDDEN ITEM IS LIG MEMBER AND RELATED ITEM IS AT LIG LEVEL
	 * Overridden LIG Member is related at LIG level
	 * Output: Relation should be applied at LIG Member level.
	 */
	@Test
	public void testCase10() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// LIG Member 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			// Set IsLir flag
			itemDTO.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				
				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly for LIG item", 27010, item.getRelatedItemCode());
					});
				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly for LIG Member", 27010, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * OVERRIDDEN ITEM IS AT LIG LEVEL AND RELATED ITEM IS AT LIG MEMBER.
	 * Output: Relation should be applied at LIG level.
	 */
	@Test
	public void testCase11() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// LIG Member 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			// Set IsLir flag
//			itemDTO.getPgData().getRelationList().forEach((key, value) -> {
//				value.forEach(item -> item.setIsLig(true));
//			});
//			TestHelper.setSizeRelation(itemDTO, 29098, 0, 'N', 45, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(LIGItemDTO1, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				
				assertEquals("Price Group relation should be Null", null, LIGItemDTO2.getPgData());
				
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at LIG level", 1001, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Related Item is LIG Member and Overridden item is also LIG Member
	 * Output: Relation should be applied at LIG member level.
	 */
	@Test
	public void testCase12() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// LIG Member 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);

				assertEquals("Price Group relation should be Null", null, LIGItemDTO2.getPgData());
				
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly for LIG Member", 27010, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Related Item is LIG and Overridden item is at Non LIG
	 * Output: Relation should be applied at Non LIG level.
	 */
	@Test
	public void testCase13() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// Non LIG 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			itemDTO.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);

				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
					});
				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Related Item is Non LIG and Overridden item is LIG
	 * Output: Relation should be applied at LIG level.
	 */
	@Test
	public void testCase14() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// Non LIG 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(LIGItemDTO1, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
//			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
//					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
//					0.0, "", "", 0, 0, 0, 0, 0, strategy);
//			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
//			LIGItemDTO2.setLastXWeeksMov(45387);
//			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);

//				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
//					value.forEach(item->{
//						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
//					});
//				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at LIG level", 1001, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Related Item is Non LIG and Overridden item is Non LIG-item 
	 * Output: Relation should be applied at not LIG item level
	 */
	@Test
	public void testCase15() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// Non LIG 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 0, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
//			
//			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
//					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
//					0.0, "", "", 0, 0, 0, 0, 1, strategy);
//			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
//			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
//			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
//			TestHelper.setBrandRelation(itemDTO, 1001, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 0, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
//			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
//					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
//					0.0, "", "", 0, 0, 0, 0, 0, strategy);
//			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
//			LIGItemDTO2.setLastXWeeksMov(45387);
//			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);

//				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
//					value.forEach(item->{
//						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
//					});
//				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Related Item is Non LIG and Overridden item is LIG Member
	 * Output: Relation should be applied at LIG level.
	 */
	@Test
	public void testCase16() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// Non LIG 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
//			
//			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
//					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
//					0.0, "", "", 0, 0, 0, 0, 1, strategy);
//			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
//			LIGItemDTO1.setLastXWeeksMov(45387);
//
//			// Price Group Information
//			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
//			TestHelper.setBrandRelation(LIGItemDTO1, 29098, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
//					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);

//				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
//					value.forEach(item->{
//						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
//					});
//				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 27010, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Same overridden item has multiple relation at LIG Level
	 * Output: Relation should be applied at LIG level for Multiple LIG items.
	 */
	@Test
	public void testCase17() {
		RerecommendationService rerecommendationService = new RerecommendationService();

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1,Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			
			// Non LIG 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 1003, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			TestHelper.setBrandRelation(itemDTO, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			itemDTO.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(LIGItemDTO1, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			TestHelper.setBrandRelation(LIGItemDTO1, 1003, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			LIGItemDTO1.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			// 2 Lig 
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 1003, "N", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1003, (long) 5131, 0, 7.28, 0.0, 1003, "Y", 0, 1,
					7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758", 0,
					0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO3 = itemDataMap.get(new ItemKey(1003, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO3.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO3, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());

				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);

				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 1001, item.getRelatedItemCode());
					});
				});
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 1001, item.getRelatedItemCode());
					});
				});
				
				LIGItemDTO3.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 1001, item.getRelatedItemCode());
					});
				});
				itemDTO2.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Related item is not assigned Properly at Non LIG level", 1001, item.getRelatedItemCode());
					});
				});
				
			} catch (OfferManagementException | Exception e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * User overridden for parent item and dependent item needs to be changed 
	 */
	@Test
	public void testCase18() {
		RerecommendationService rerecommendationService = new RerecommendationService();
		PricingEngineService pricingEngineService = new PricingEngineService();

		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		try {
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getRecommendationRule();
			RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
			curCalDTO.setStartDate(curWeekStartDate);
			HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
			TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, COMP_STR_ID_TEST_967);
			// initialize
			PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 5, 4, 1277, "09/22/2017", "10/04/2017", false, -1, -1, -1);
			TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

			TestHelper.setSizeGuideline(strategy, 1);
			TestHelper.setBrandGuideline(strategy);
			TestHelper.setPIGuideline(strategy, 95, 99);
			TestHelper.setMarginGuidelineMaintainCurrentMargin(strategy);

			TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 10, 10);
			TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
			TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 27010, (long) 3122, 0, 1.91, 0.0, 1001, "N", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO = itemDataMap.get(new ItemKey(27010, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(itemDTO, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			itemDTO.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574165, "N", 1001, (long) 3122, 0, 1.91, 0.0, 1001, "Y", 21,
					1, 2.69, 0, 0, 0.0, 1, 1.99, 1, "", 0, 98, 0.0, 0, "N", 109, 0, 0, 1, 2.69, "", "", 2, 1, 99, 0, 21, -1, -1, 1, "001500004830", 0,
					0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO1 = itemDataMap.get(new ItemKey(1001, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO1.setLastXWeeksMov(45387);

			// Price Group Information
			TestHelper.setPriceGroup(LIGItemDTO1, 76, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(LIGItemDTO1, 1002, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			LIGItemDTO1.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29098, (long) 5131, 0, 7.28, 0.0, 1002, "N", 0,
					1, 7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758",
					0, 0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO itemDTO1 = itemDataMap.get(new ItemKey(29098, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO1.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO1, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(itemDTO1, 1003, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			itemDTO1.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1002, (long) 5131, 0, 7.28, 0.0, 1002, "Y", 0,
					1, 7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758",
					0, 0.0, "", "", 0, 0, 0, 0, 0, strategy);
			PRItemDTO LIGItemDTO2 = itemDataMap.get(new ItemKey(1002, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
			TestHelper.setBrandRelation(LIGItemDTO2, 1003, 0, 'X', 0, PRConstants.BRAND_RELATION, 10, Constants.DEFAULT_NA,
					PRConstants.PRICE_GROUP_EXPR_BELOW, PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
			// Set IsLir flag
			LIGItemDTO2.getPgData().getRelationList().forEach((key, value) -> {
				value.forEach(item -> item.setIsLig(true));
			});
			// 2 Lig
			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 29099, (long) 5131, 0, 7.28, 0.0, 1003, "N", 0,
					1, 7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758",
					0, 0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO itemDTO2 = itemDataMap.get(new ItemKey(29099, PRConstants.NON_LIG_ITEM_INDICATOR));
			itemDTO2.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(itemDTO2, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

			TestHelper.getRecItemDetailsBasedOnRunId(itemDataMap, (long) 105423, (long) 38574032, "N", 1003, (long) 5131, 0, 7.28, 0.0, 1003, "Y", 0,
					1, 7.99, 4.99, 0, 0.0, 0, 0, 0, "", 0, 12.06, 0.0, 0, "N", 0, 0, 0, 1, 7.99, "3", "", 2, 1, 99, 0, -1, -1, -1, 1, "001500093758",
					0, 0.0, "", "", 0, 0, 0, 0, 1, strategy);
			PRItemDTO LIGItemDTO3 = itemDataMap.get(new ItemKey(1003, PRConstants.LIG_ITEM_INDICATOR));
			LIGItemDTO3.setLastXWeeksMov(45387);
			TestHelper.setPriceGroup(LIGItemDTO3, 56, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

			PRRecommendationRunHeader recRunHeader = TestHelper.getRecommendationRunHeader((long) 105423, 6, 5, 4, 1277, 6842, "09/22/2017",
					"09/24/2017");
			try {
				ArrayList<PRItemDTO> prOutputItemList = new ArrayList<PRItemDTO>();
				List<PRProductGroupProperty> productGroupProperties = null;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				inputDTO.setLocationId(recRunHeader.getLocationId());
				inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
				inputDTO.setProductId(recRunHeader.getProductId());
				inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
				inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
				inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
				inputDTO.setStartDate(recRunHeader.getStartDate());
				inputDTO.setEndDate(recRunHeader.getEndDate());
				// rerecommendationService.getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId,
				// leadZoneDivisionId, divisionId, pricingEngineDAO,
				// pricingEngineService);
				rerecommendationService.updateIsprocessedForOverriddenItem(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
				rerecommendationService.adjustSizeAndBrandRelation(itemDataMap, retLirMap);
				rerecommendationService.changeBrandOpertorForRelationOverridden(itemDataMap);
				rerecommendationService.removeExitingRelations(itemDataMap);
				rerecommendationService.updateSystemOverrideFlag(itemDataMap, pricingEngineService, retLirMap);
				rerecommendationService.updateSystemOverrideForLIG(itemDataMap);
				rerecommendationService.resetIsProcessedFlagForSysOverriddenItems(itemDataMap);
				HashMap<Integer, List<PRItemDTO>> processedLIGMap = rerecommendationService.getProcessedLIGAndMembersMap(itemDataMap);

				new LIGConstraint().applyLIGConstraint(processedLIGMap, itemDataMap, retLirConstraintMap);
				new PricingEngineWS().applyStrategies(itemDTO1, prOutputItemList, itemDataMap, compIdMap, 123,
						new HashMap<Integer, HashMap<Integer, Character>>(), multiCompData, curWeekStartDate, leadZoneDetails, false, recRunHeader);
				List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
				itemDataMap.forEach((key, itemDTOs) -> {
					if (itemDTOs.getUserOverrideFlag() == 0) {
						itemsToApplyActualObj.add(itemDTOs);
					}
				});
				HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
				new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
				
				itemDTO1.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Operator text is not changed properly", "above", item.getPriceRelation().getOperatorText());
						assertEquals("Related item is not assigned Properly at LIG level", 1001, item.getRelatedItemCode());
					});
				});
				
				LIGItemDTO2.getPgData().getRelationList().forEach((key,value)->{
					value.forEach(item->{
						assertEquals("Operator text is not changed properly", "above", item.getPriceRelation().getOperatorText());
						assertEquals("Related item is not assigned Properly at LIG level", 1001, item.getRelatedItemCode());
					});
				});
				assertEquals("System Override not done", true, itemDTO1.isSystemOverrideFlag());
				assertEquals(7.29, itemDTO1.getRecommendedRegPrice().price, 0);
				
			} catch (OfferManagementException | Exception | GeneralException e) {
				e.printStackTrace();
			}

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
}
