package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.easymock.EasyMock;
//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
//import org.junit.Test;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemRecErrorService;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class ApplyLIGConstraintJUnitTest {
	public static final Integer LOCATION_ID = 66;
	//SB_TRASH_IN_DR_OUT_DR_DRWSTRNG_28_TO_45_CO
	public static final Integer RET_LIR_ID = 110244;
	public static final Integer RET_LIR_ITEM_CODE = 950735;
	public static final char LIG_CONSTRAINT_DIFF = 'D';
	public static final Integer COMP_STR_ID = 978;
	
	Connection conn = null;
	boolean isRecommendAtStoreLevel = false;
	boolean isOnline = false;
	PredictionComponent predictionComponent = new PredictionComponent();
	HashMap<Integer, List<PRItemDTO>> finalLigMap = new HashMap<Integer, List<PRItemDTO>>();

	// Lead zone is not covered here
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	// Not needed, as it is used only in prediction and prediction is not called and mocked up here
	HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
	// Not needed, as it is used only in prediction and prediction is not called and mocked up here
	HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = new HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>>();
	// Sale information not needed, this class doesn't cover cases with sale price
	HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
	// Ad information not needed, this class doesn't cover cases with ad
	HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
	// Display information
	HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = new HashMap<>();
	// Not needed as multiple comp guideline is not covered here
	HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();

	PRStrategyDTO inputDTO;
	PRRecommendationRunHeader recommendationRunHeader;
	List<PRProductGroupProperty> productGroupProperties;
	RetailCalendarDTO curCalDTO;
	List<Integer> priceZoneStores;
	PredictionService predictionServiceMock;
	ObjectiveService objectiveService = null;

	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
	// price history of item
	HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
	
	HashMap<ItemKey, PRItemDTO> itemDataMap;
	HashMap<Integer, List<PRItemDTO>> retLirMap;
	HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap;
	int locationLevelId = 6, locationId = 6, productLevelId = 4, productId = 5768, strategyId = 1;
	boolean usePrediction = true;
	String recWeekStartDate = "03/01/2018", recWeekEndDate = "03/07/2018", curWeekStartDate = "02/22/2018", curWeekEndDate = "02/28/2018";
	String storesInZone = "5712,5713,5716,5717,5718,5719,5720";
	Integer compStrId = 967;
	int lig1Member1 = 1000, lig1Member2 = 1001, lig1Member3 = 1002, lig1Member4 = 1004, lig1Member5 = 1005, lig1Member6 = 1006, lig1Member7 = 1007;
	int nonLig1 = 1100, nonLig2 = 2200;
	int lig1 = 3000, lig2 = 3100;
	int lig2Member1 = 3101, lig2Member2 = 3102, lig2Member3 = 3103;
	String lig1Member1UPC = "010101010", lig1Member2UPC = "010101011", lig1Member3UPC = "010101012", lig1Member4UPC = "010101013",
			lig1Member5UPC = "010101014", lig1Member6UPC = "010101016", lig1Member7UPC = "010101017";
	String lig2Member1UPC = "020101010", lig2Member2UPC = "020101011", lig2Member3UPC = "020101012", lig2Member4UPC = "020101013";
	String nonLig1UPC = "020202020";
	char LIG_CONSTRAINT_SAME = 'S';
	Integer COST_NO_CHANGE = 0;
	Integer COST_INCREASED = 1;
	Long LAST_X_WEEKS_MOV_1 = 4587l;
	int startCalendarId = 3456, endCalendarId = 3459;
	ProductKey lig1Key = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lig1);
	ProductKey lig2Key = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lig2);
	ProductKey ligMember1Key = new ProductKey(Constants.ITEMLEVELID, lig1Member1);
	ProductKey ligMember2Key = new ProductKey(Constants.ITEMLEVELID, lig1Member2);
	ProductKey nonLig1Key = new ProductKey(Constants.ITEMLEVELID, nonLig1);
	ProductKey nonLig2Key = new ProductKey(Constants.ITEMLEVELID, nonLig2);
	List<PredictionItemDTO> predictionItems;

	String week1StartDate = "01/11/2018";
	String week2StartDate = "11/25/2018";
	String week3StartDate = "11/13/2016";
	String week4StartDate = "11/06/2016";
	String week5StartDate = "10/30/2016";
	String week6StartDate = "10/23/2016";
	String week7StartDate = "10/16/2016";
	String week8StartDate = "10/09/2016";
	String week9StartDate = "10/02/2016";
	String week15StartDate = "08/21/2016";

	String curPriceEffDate1 = "11/04/2018";

	private static final Integer COST_UNCHANGED = 0;

	PRStrategyDTO strategy1 = new PRStrategyDTO();
	PRStrategyDTO strategy2 = new PRStrategyDTO();
	PRStrategyDTO strategy3 = new PRStrategyDTO();
	double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
	private static Logger logger = Logger.getLogger("ApplyLIGConstraintJUnitTest");
	int noOfsaleAdDisplayWeeks = 0;
	ObjectMapper mapper = new ObjectMapper();
	
	public void clearVariables(){
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		displayDetails = new HashMap<>();
	}
	
//	@Before
//    public void init() {
////		PropertyConfigurator.configure("log4j-pricing-engine.properties");
//		PropertyManager.initialize("recommendation.properties");
//    }
	
	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");
		noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, recWeekStartDate, recWeekEndDate,
				isRecommendAtStoreLevel, -1, -1, -1);
		recommendationRunHeader = TestHelper.getRecommendationRunHeader(recWeekStartDate);
		productGroupProperties = TestHelper.addProductGroupProperty(productGroupProperties, productLevelId, productId, usePrediction);
		curCalDTO = TestHelper.getCalendarDetails(curWeekStartDate, curWeekEndDate);
		priceZoneStores = Stream.of(storesInZone.split(",")).map(Integer::parseInt).collect(Collectors.toList());
		recommendationRuleMap = TestHelper.getRecommendationRuleGE2();
		// recommendationRuleMap = TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);

		// Set strategies
		setStrategy1();

		// Set LIG Data
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member1, lig1Member1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member2, lig1Member2);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member3, lig1Member3);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member4, lig1Member4);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member5, lig1Member5);

		retLirMap = TestHelper.setRetLirMap(retLirMap, lig2, lig2Member1, lig2Member1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig2, lig2Member2, lig2Member2);

		// Set LIG Constraint
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig1, LIG_CONSTRAINT_SAME);
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig2, LIG_CONSTRAINT_SAME);

		// Mockup
		predictionServiceMock = EasyMock.createMock(PredictionService.class);
	}
	
//	@Test
//	public void testCase1(){
//		HashMap<Integer, List<PRItemDTO>> itemPriceRangeMap = null;
//		HashMap<ItemKey, PRItemDTO> itemDataMap = null;
//		HashMap<Integer, List<PRItemDTO>> retLirMap = null;
//		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = null;
//		PRItemDTO prItemDTO = null;
//	    @SuppressWarnings("unused")
//		List<PRItemDTO> withLigConstraint = new ArrayList<PRItemDTO>();
//		 
//		prItemDTO = TestHelper.setItemDTO(LOCATION_ID, 160876, 110244, 40, "CO", "", 6.49, null, 3.23, null, 3.23, null, COMP_STR_ID, "", 0d, null, 2, 6.89, 33, 44);	
//		itemPriceRangeMap = TestHelper.setItemPriceRangeMap(itemPriceRangeMap, LOCATION_ID, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(LOCATION_ID, 131247, 110244, 38, "CO",  "", 6.49, null, 3.23, null, 3.23, null, COMP_STR_ID, "", 0d, null, 2, 6.89, 33, 44);
//		itemPriceRangeMap = TestHelper.setItemPriceRangeMap(itemPriceRangeMap, LOCATION_ID, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(LOCATION_ID, 15749, 110244, 45, "CO",  "", 6.49, null, 3.33, null, 3.23, null, COMP_STR_ID, "", 0d, null, 2, 6.89, 33, 44);		
//		itemPriceRangeMap = TestHelper.setItemPriceRangeMap(itemPriceRangeMap, LOCATION_ID, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(LOCATION_ID, 71768, 110244, 40, "CO",  "", 6.49, null, 3.43, null, 3.23, null, COMP_STR_ID, "", 0d, null, 4, 6.89, 33, 44);		
//		itemPriceRangeMap = TestHelper.setItemPriceRangeMap(itemPriceRangeMap, LOCATION_ID, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(LOCATION_ID, 158118, 110244, 44, "CO",  "", 6.49, null, 3.33, null, 3.23, null, COMP_STR_ID, "", null, null, 4, 6.89, 43, 44);	
//		itemPriceRangeMap = TestHelper.setItemPriceRangeMap(itemPriceRangeMap, LOCATION_ID, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(LOCATION_ID, 41076, 110244, 40, "CO",  "", 6.49, null, 3.43, null, 3.23, null, COMP_STR_ID, "", null, null, 4, 6.89, 33, 44);		
//		itemPriceRangeMap = TestHelper.setItemPriceRangeMap(itemPriceRangeMap, LOCATION_ID, prItemDTO);
//		
//		prItemDTO = TestHelper.setItemDTO(950735, -1, -1, -1, -1, -1, 110244, 950735, 38, "L068826700279", true, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(160876, -1, -1, -1, -1, -1, 110244, 950735, 40, "068826714069", false, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(131247, -1, -1, -1, -1, -1, 110244, 950735, 38, "068826700279", false, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(15749, -1, -1, -1, -1, -1, 110244, 950735, 45, "068826700759", false, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(71768, -1, -1, -1, -1, -1, 110244, 950735, 40, "068826703453", false, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(158118, -1, -1, -1, -1, -1, 110244, 950735, 44, "068826714008", false, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		prItemDTO = TestHelper.setItemDTO(41076, -1, -1, -1, -1, -1, 110244, 950735, 40, "068826700800", false, -1, -1);		
//		itemDataMap = TestHelper.setItemDataMap(itemDataMap, prItemDTO);
//		
//		//110244={950735=[160876, 131247, 15749, 71768, 158118, 41076]}
//		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, 160876);
//		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, 131247);
//		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, 15749);
//		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, 71768);
//		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, 158118);
//		retLirMap = TestHelper.setRetLirMap(retLirMap, RET_LIR_ID, RET_LIR_ITEM_CODE, 41076);
//		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, LOCATION_ID, RET_LIR_ID, LIG_CONSTRAINT_SAME);
//		
//		withLigConstraint = new LIGConstraint().applyLIGConstraint(itemPriceRangeMap, itemDataMap, retLirConstraintMap);
//		
//	}
	
	
	
	/**
	 * There are 3 items in a LIG. Item 1, 2 and 3 as 30, 25, 20 as 13 weeks average movement respectively. 
	 * There is no item level relationship. All members are non-shipper and have cost and current retail. 
	 * All members have similar recommended price of 2.99. 
	 * 
	 * Expectations: 
	 * 	Recommended price of all members: 2.99
	 * 	Representing item code: Item1
	 * 	Recommended price of LIG: 3.99 
	 * 	Log of LIG: same as representing item code log
	 * @throws GeneralException 
	 * @throws OfferManagementException 
	 * @throws Exception 
	 */
	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					2.39, 3.17, "");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			Double[] roundingDigits = { 2.39,2.49,2.59,2.69,2.79,2.99};
			TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(5) + "%");
			additionalDetails.add(String.valueOf(52));
			TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
			assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
			assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member1, item.getLigRepItemCode());
			}
		}
	}
	
	/**
	 * There are 3 items in a LIG. Item 1, 2 and 3 as 30, 25, 20 as 13 weeks average movement respectively. 
	 * There is no item level relationship. All members are non-shipper and have cost and current retail. 
	 * Item1 price: 2.99, Item2 price: 2.89, Item3 price: 2.89. 
	 * 
	 * Expectations:
	 * 	Recommended price of all members: 2.89; 
	 * 	Representing item code: Item2; 
	 * 	Recommended price of LIG: 2.89; 
	 * 	Log of LIG: same as representing item code log; 
	 * 	Log of item1 must say LIG same
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			if(item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3 || item.getItemCode() == lig1){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.23, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.29,2.39,2.49,2.59,2.69,2.79};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member1 ){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.99);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member2, item.getLigRepItemCode());
			}
		}
	}
	
	/**
	 * There are 5 items in a LIG. Item 1, 2, 3, 4 & 5 as 30, 25, 20, 80 & 100 as 13 weeks average movement respectively. 
	 * There is no item level relationship. All members are non-shipper and have cost and current retail. 
	 * Item1 price: 2.99, Item2 price: 2.79, Item3 price: 2.79, item4 price: 2.99, item5 price: 2.39.
	 * 
	 * Expectations:
	 *  Recommended price of all members: 2.99; 
	 *  Representing item code: Item4; 
	 *  Recommended price of LIG: 2.99; 
	 *  Log of LIG: same as representing item code log; 
	 *  Log of item2,3,5 must say LIG same
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(lig1Member4, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 80, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(80);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);
		PRItemDTO lig1Member5Item = TestHelper.getTestItem2(lig1Member5, 1, 2.39, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 100, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member5Item.setShipperItem(false);
		lig1Member5Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member5Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member4, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member4, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member4, lig1Member4UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member5, lig1Member5UPC, new MultiplePrice(1, 2.39), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			
			if(item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.23, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.29,2.39,2.49,2.59,2.69,2.79};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.79);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			}
			
			else if(item.getItemCode() == lig1Member5){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 1.91, 2.53, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						1.91, 2.53, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 1.91, 2.53, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {1.99,2.19,2.29,2.39,2.49};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.39);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			}
			
			else if(item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member4 || item.getItemCode() == lig1){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member4, item.getLigRepItemCode());
			}
		}
	}
	
	/**
	 * There are 5 items in a LIG. Item 1, 2, 3, 4 & 5 as 30, 25, 20, 80 & 100 as 13 weeks average movement respectively. 
	 * There is no item level relationship. All members are non-shipper and have cost and current retail. 
	 * Item1 price: 2.99, Item2 price: 2.79, Item3 price: 2.79, item4 price: 2.99, item5 price: 2.39.
	 * 
	 * Expectations:
	 *  Recommended price of all members: 2.99; 
	 *  Representing item code: Item4; 
	 *  Recommended price of LIG: 2.99; 
	 *  Log of LIG: same as representing item code log; 
	 *  Log of item2,3,5 must say LIG same
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(lig1Member4, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 80, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(80);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);
		PRItemDTO lig1Member5Item = TestHelper.getTestItem2(lig1Member5, 1, 2.39, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 100, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member5Item.setShipperItem(false);
		lig1Member5Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member5Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member4, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member4, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		// Set price group
		TestHelper.setPriceGroup(lig1Member5Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		lig1Member5Item.setItemLevelRelation(true);
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 3.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member5Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member4, lig1Member4UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member5, lig1Member5UPC, new MultiplePrice(1, 2.39), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			
			if(item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member4 || 
					item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.99);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.49), item.getRecommendedRegPrice());
			}
			
			else if(item.getItemCode() == lig1Member5 || item.getItemCode() == lig1){
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 3.03, 3.32, 3.03, 3.32, lig2, true);
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 3.03, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.91, 2.53, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.53, 2.53, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.53, 2.53, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.49};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.49), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member5, item.getLigRepItemCode());
			}
		}
	}
	
	/**
	 * There are 5 items in a LIG. Item 1, 2, 3, 4 & 5 as 30, 25, 20, 80 & 75 as 13 weeks average movement respectively. 
	 * Item5 and Item4 is in relationship. All members are non-shipper and have cost and current retail. 
	 * Item1 price: 3.99, Item2 price: 3.89, Item3 price: 3.89, item4 price: 3.99, item5 price: 3.69.
	 * 
	 * Expectations:
	 *  Recommended price of all members: 2.99; 
	 *  Representing item code: Item4; 
	 *  Recommended price of LIG: 2.99; 
	 *  Log of LIG: same as representing item code log; 
	 *  Log of item2,3,5 must say LIG same
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase6() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.89, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(lig1Member4, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 80, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(80);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);
		PRItemDTO lig1Member5Item = TestHelper.getTestItem2(lig1Member5, 1, 2.39, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 100, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member5Item.setShipperItem(false);
		lig1Member5Item.setAvgMovement(75);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member5Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member4, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member4, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		
		// Set price group
		TestHelper.setPriceGroup(lig1Member4Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member5Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		lig1Member4Item.setItemLevelRelation(true);
		lig1Member5Item.setItemLevelRelation(true);
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 3.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member4Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member5Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member4, lig1Member4UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member5, lig1Member5UPC, new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.89), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			
			if(item.getItemCode() == lig1Member1){
				logger.info("Processing Item code:"+item.getItemCode());
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.31, 3.06, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.31, 3.06, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.31, 3.06, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.89);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}
			else if(item.getItemCode() == lig1Member5 || 
					item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
				logger.info("Processing Item code:"+item.getItemCode());
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.99);
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}
			
			else if(item.getItemCode() == lig1Member4 || item.getItemCode() == lig1){
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 3.03, 3.32, 3.03, 3.32, lig2, true);
				
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 3.03, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.91, 2.53, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.53, 2.53, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.53, 2.53, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.49};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
				
				if(item.isLir()){
					assertEquals("Mimatch", lig1Member4, item.getLigRepItemCode());
				}
			}
		}
	}
	
	/**
	 * There are 3 items in a LIG. Item 1, 2 and 3 as 30, 25, 20 as 13 weeks average movement respectively. 
	 * There is no item level relationship. All members are shipper items. All members have similar recommended price of 2.99.
	 * 
	 * Expectations:
	 *  Recommended price of all members: 2.99
	 *  Representing item code: Item1; 
	 *  Recommended price of LIG: 2.99; 
	 *  Log of LIG: same as representing item code log; 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase7() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(true);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(true);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(true);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
					2.39, 3.17, "");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

			guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
			Double[] roundingDigits = { 2.39,2.49,2.59,2.69,2.79,2.99};
			TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(5) + "%");
			additionalDetails.add(String.valueOf(52));
			TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
			assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
			assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member1, item.getLigRepItemCode());
			}
		}
	}
	
	/**
	 * There are 3 items in a LIG. Item 1, 2 and 3 as 30, 25, 20 as 13 weeks average movement respectively. 
	 * There is no item level relationship. Item2 & 3 are shipper items. Item1 price: 2.99, Item2 price: 2.79, Item3 price: 2.79. 
	 * 
	 * Expectations:
	 *  Recommended price of all members: 2.99
	 *  Representing item code: Item1; 
	 *  Recommended price of LIG: 2.99; 
	 *  Log of LIG: same as representing item code log; 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase8() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(true);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(true);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			if( item.getItemCode() == lig1Member1 || item.getItemCode() == lig1){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, true, 2.23, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.29,2.39,2.49,2.59,2.69,2.79};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.79);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.99), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member1, item.getLigRepItemCode());
			}
		}
	}
	
	/**
	 * There are 5 items in a LIG. Item 1, 2, 3, 4 & 5 as 30, 25, 20, 80 & 100 as 13 weeks average movement respectively. 
	 * There is no item level relationship. Item1&2 are shipper and item4 is not having cost and item5 is not having current retail. 
	 * Item1 price: 3.99, Item2 price: 3.99, Item3 price: 3.89, item4 price: No Rec, item5 price: No Rec. 
	 * 
	 * Expectations:Recommended price of all members: 3.89; Representing item code: Item3; 
	 * Recommended price of LIG: 3.89; Log of LIG: same as representing item code log; Log of item1,2,4,5 must say LIG same 
	 * 
	 *  Log of LIG: same as representing item code log; 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase9() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(true);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(true);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(lig1Member4, 1, 2.79, null, null, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member4UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(80);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);
		PRItemDTO lig1Member5Item = TestHelper.getTestItem2(lig1Member5, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member5UPC);
		lig1Member5Item.setShipperItem(false);
		lig1Member5Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member5Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member4, lig1Member4UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			if( item.getItemCode() == lig1Member3 || item.getItemCode() == lig1){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.23, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.29,2.39,2.49,2.59,2.69,2.79};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.99);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member5){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 0);
				//List<String> additionalDetails = new ArrayList<String>();
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member3, item.getLigRepItemCode());
			}
		}
	}
	
	
	/**
	 * There are 5 items in a LIG. Item 1, 2, 3, 4 & 5 as 30, 25, 20, 80 & 100 as 13 weeks average movement respectively. 
	 * There is no item level relationship. Item1&2 are shipper and item4 is not having cost and item5 is not having current retail. 
	 * Item1 price: 3.99, Item2 price: 3.99, Item3 price: 3.89, item4 price: No Rec, item5 price: No Rec. 
	 * 
	 * Expectations:Recommended price of all members: 3.89; Representing item code: Item3; 
	 * Recommended price of LIG: 3.89; Log of LIG: same as representing item code log; Log of item1,2,4,5 must say LIG same 
	 * 
	 *  Log of LIG: same as representing item code log; 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase10() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(true);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(true);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(lig1Member4, 1, 2.79, null, null, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member4UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(80);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);
		PRItemDTO lig1Member5Item = TestHelper.getTestItem2(lig1Member5, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member5UPC);
		lig1Member5Item.setShipperItem(false);
		lig1Member5Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member5Item);
		PRItemDTO lig1Member6Item = TestHelper.getTestItem2(lig1Member6, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member6UPC);
		lig1Member6Item.setShipperItem(false);
		lig1Member6Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member6Item);
		PRItemDTO lig1Member7Item = TestHelper.getTestItem2(lig1Member7, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member7UPC);
		lig1Member7Item.setShipperItem(false);
		lig1Member7Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member7Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member4, lig1Member4UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			if( item.getItemCode() == lig1Member3 || item.getItemCode() == lig1){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.23, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.29,2.39,2.49,2.59,2.69,2.79};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.99);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member5){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 0);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member3, item.getLigRepItemCode());
			}
		}
	}
	
	
	/**
	 * There are 5 items in a LIG. Item 1, 2, 3, 4 & 5 as 30, 25, 20, 80 & 100 as 13 weeks average movement respectively. 
	 * There is no item level relationship. Item1&3 are shipper and item4 is not having cost and item5 is not having current retail. 
	 * Item1 price: 3.99, Item2 price: 3.99, Item3 price: 3.89, item4 price: No Rec, item5 price: No Rec. 
	 * 
	 * Expectations:Recommended price of all members: 3.89; Representing item code: Item3; 
	 * Recommended price of LIG: 3.89; Log of LIG: same as representing item code log; Log of item1,2,4,5 must say LIG same 
	 * 
	 *  Log of LIG: same as representing item code log; 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase11() throws GeneralException, Exception, OfferManagementException {
		
		clearVariables();
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 30, 0, 0d,
				0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setShipperItem(true);
		lig1Member1Item.setAvgMovement(30);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 2.99, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 25, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(25);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 2.79, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member3UPC);
		lig1Member3Item.setShipperItem(true);
		lig1Member3Item.setAvgMovement(20);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(lig1Member4, 1, 2.79, null, null, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member4UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(80);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);
		PRItemDTO lig1Member5Item = TestHelper.getTestItem2(lig1Member5, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member5UPC);
		lig1Member5Item.setShipperItem(false);
		lig1Member5Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member5Item);
		PRItemDTO lig1Member6Item = TestHelper.getTestItem2(lig1Member6, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member6UPC);
		lig1Member6Item.setShipperItem(false);
		lig1Member6Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member6Item);
		PRItemDTO lig1Member7Item = TestHelper.getTestItem2(lig1Member7, 1, null, null, 1.25, 1.25, COST_UNCHANGED, compStrId, 2.56, strategy1, 20, 0,
				0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member7UPC);
		lig1Member7Item.setShipperItem(false);
		lig1Member7Item.setAvgMovement(100);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member7Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week1StartDate, 1, 1.25, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member5, week4StartDate, 1, 1.25, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 2.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member4, lig1Member4UPC, new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			if( item.getItemCode() == lig1Member3 || item.getItemCode() == lig1){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.23, 2.96, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.23, 2.96, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.29,2.39,2.49,2.59,2.69,2.79};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 1.42, 3.2, 1.42, 3.2, "", new MultiplePrice(1, 2.56));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.39, 3.17, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
						2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.25, Constants.DEFAULT_NA, 2.39, 3.17, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = {2.39,2.49,2.59,2.69,2.79,2.99};
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 2.99);
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5) + "%");
				additionalDetails.add(String.valueOf(52));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL, additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}else if(item.getItemCode() == lig1Member5){
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 0);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 2.79), item.getRecommendedRegPrice());
			}
			
			if(item.isLir()){
				assertEquals("Mimatch", lig1Member3, item.getLigRepItemCode());
			}
		}
	}
	
	
	private void setStrategy1() {
		strategy1 = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate, recWeekEndDate, false, -1, -1,
				-1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy1);

		TestHelper.setBrandGuideline(strategy1);
		TestHelper.setSizeGuideline(strategy1, 1);
		TestHelper.setPIGuideline(strategy1, 80, 180);
		TestHelper.setThreasholdConstraint(strategy1, PRConstants.VALUE_TYPE_PCT, 6, 20);
		TestHelper.setLigConstraint(strategy1, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setCostConstraint(strategy1, false);
		TestHelper.setRoundingConstraint(strategy1, TestHelper.getRoundingTableTableGE1());
		
	}
}
