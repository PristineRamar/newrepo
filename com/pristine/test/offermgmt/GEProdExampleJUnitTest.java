package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Before;
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
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class GEProdExampleJUnitTest {

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
	String recWeekStartDate = "12/23/2018", recWeekEndDate = "12/29/2018", curWeekStartDate = "12/16/2018", curWeekEndDate = "12/22/2018";
	String storesInZone = "5712,5713,5716,5717,5718,5719,5720";
	Integer compStrId = 967;
	int lig1Member1 = 1000, lig1Member2 = 1001, lig1Member3 = 1002;
	int nonLig1 = 1100, nonLig2 = 2200;
	int lig1 = 3000, lig2 = 3100;
	int lig2Member1 = 3101, lig2Member2 = 3102, lig2Member3 = 3103;
	String lig1Member1UPC = "010101010", lig1Member2UPC = "010101011", lig1Member3UPC = "010101012", lig1Member4UPC = "010101013";
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

	String week1StartDate = "12/09/2018";
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
	String recommendationWeekStartDate = "01/08/2017";
	String recRunningWeekStartDate = "01/01/2017";

	PRStrategyDTO strategy1 = new PRStrategyDTO();
	PRStrategyDTO strategy2 = new PRStrategyDTO();
	PRStrategyDTO strategy3 = new PRStrategyDTO();
	double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
	private static Logger logger = Logger.getLogger("GEProdExampleJUnitTest");
	ObjectMapper mapper = new ObjectMapper();
	HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = new HashMap<>();
	int noOfsaleAdDisplayWeeks = 0;
	
	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClientsPIReverse.properties");
		noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, recWeekStartDate, recWeekEndDate,
				isRecommendAtStoreLevel, -1, -1, -1);
		recommendationRunHeader = TestHelper.getRecommendationRunHeader(recWeekStartDate);
		productGroupProperties = TestHelper.addProductGroupProperty(productGroupProperties, productLevelId, productId, usePrediction);
		curCalDTO = TestHelper.getCalendarDetails(curWeekStartDate, curWeekEndDate);
		priceZoneStores = Stream.of(storesInZone.split(",")).map(Integer::parseInt).collect(Collectors.toList());
		recommendationRuleMap = TestHelper.getRecommendationRuleGE1();
		// recommendationRuleMap = TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);

		// Set strategies
		setStrategy3();

		// Set LIG Data
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member1, lig1Member1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member2, lig1Member2);

		retLirMap = TestHelper.setRetLirMap(retLirMap, lig2, lig2Member1, lig2Member1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig2, lig2Member2, lig2Member2);

		// Set LIG Constraint
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig1, LIG_CONSTRAINT_SAME);
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig2, LIG_CONSTRAINT_SAME);

		// Mockup
		predictionServiceMock = EasyMock.createMock(PredictionService.class);
	}

	/**
	 * GE production issue, Additional log is not shown when retail is retained due to TPR
	 * 
	 * LIG vs LIG relation
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase1.... ");
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 1.99, null, 1.29, 1.29, COST_UNCHANGED, compStrId, 1.48, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setIsTPR(1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 1.99, null, 1.29, 1.29, COST_UNCHANGED, compStrId, 1.48, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setIsTPR(1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy3);
		lig1Item.setIsTPR(1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, lig1Item);

		PRItemDTO lig2Member1Item = TestHelper.getTestItem2(lig2Member1, 1, 1.39, null, 0.61, 0.61, COST_UNCHANGED, compStrId, 0.84, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig2, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig2Member2Item = TestHelper.getTestItem2(lig2Member2, 1, 1.39, null, 0.61, 0.61, COST_UNCHANGED, compStrId, 0.84, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig2, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig2Item = TestHelper.setLIGItemDTO(lig2, LAST_X_WEEKS_MOV_1, locationId, strategy3);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, lig1Item);

		// Lig2 dependent on lig1
		// Set price group
		TestHelper.setPriceGroup(lig2Member1Item, 15, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig2Member2Item, 15, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(lig2Item, 15, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		TestHelper.setSaleDetails(saleDetails, lig2Member1, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		TestHelper.setSaleDetails(saleDetails, lig2Member2, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		TestHelper.setSaleDetails(saleDetails, lig2Member3, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);

		TestHelper.setBrandRelation(lig2Member1Item, lig1, 0, 'X', 15, PRConstants.BRAND_RELATION, 15, 35, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelation(lig2Member2Item, lig1, 0, 'X', 15, PRConstants.BRAND_RELATION, 15, 35, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig2Item, lig1, 0, 'X', 15, PRConstants.BRAND_RELATION, 15, 35, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 150d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 150d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 1.39), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 1.39), PredictionStatus.SUCCESS, 150d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 1.39), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 1.39), PredictionStatus.SUCCESS, 150d, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 0.0, 1.78, 0.0, 1.78, "", new MultiplePrice(1, 1.48));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.39, 2.09, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 1.39,
				1.78, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.29, Constants.DEFAULT_NA, 1.39, 1.78, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 1.39, 1.49, 1.59, 1.69 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));

		// Set additional Log
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "02/09/2019");

		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(lig1Item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 1.99));
	}

	/**
	 * Check Additional log when retail is retained due to TPR
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase1.... ");
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 1.99, null, 1.29, 1.29, COST_UNCHANGED, compStrId, 1.48, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		lig1Member1Item.setIsTPR(1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 1.99, null, 1.29, 1.29, COST_UNCHANGED, compStrId, 1.48, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		lig1Member2Item.setIsTPR(1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy3);
		lig1Item.setIsTPR(1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, lig1Item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 1.29), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 1.29), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 1.29), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 1.29), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 1.29), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 1.29), PredictionStatus.SUCCESS, 110d));
		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 0.0, 1.78, 0.0, 1.78, "", new MultiplePrice(1, 1.48));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.39, 2.09, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 1.39,
				1.78, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.29, Constants.DEFAULT_NA, 1.39, 1.78, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 1.39, 1.49, 1.59, 1.69 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));

		// Set additional Log
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "02/09/2019");

		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(lig1Item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 1.99));
	}

	/**
	 * Check Additional log when retail is retained due to TPR (Non lig item)
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase1.... ");
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.29, 1.29, COST_UNCHANGED, compStrId, 1.48, strategy3,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		nonLig1Item.setIsTPR(1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		
		TestHelper.setSaleDetails(saleDetails, nonLig1, 0.99, 1, "12/23/2018", "04/25/2019", recWeekStartDate);
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 0.0, 1.78, 0.0, 1.78, "", new MultiplePrice(1, 1.48));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.39, 2.09, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 1.39,
				1.78, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 1.29, Constants.DEFAULT_NA, 1.39, 1.78, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 1.39, 1.49, 1.59, 1.69 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));

		// Set additional Log
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "02/09/2019");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(nonLig1Item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 1.99));
	}

	private void setStrategy3() {
		strategy3 = TestHelper.getStrategy(3, locationLevelId, locationId, productLevelId, productId, curWeekStartDate, recWeekEndDate, false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy3);

		TestHelper.setSizeGuideline(strategy3, 0);
		TestHelper.setBrandGuideline(strategy3);
		TestHelper.setPIGuideline(strategy3, 0, 120);
		TestHelper.setThreasholdConstraint(strategy3, PRConstants.VALUE_TYPE_PCT, 5, 30);
		TestHelper.setLigConstraint(strategy3, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setCostConstraint(strategy3, false);
		TestHelper.setRoundingConstraint(strategy3, TestHelper.getRoundingTableTableGE());
	}

}
