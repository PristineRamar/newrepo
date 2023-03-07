package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.*;
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

public class TPRAndSaleHandlingJUnitTest {

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

	PRStrategyDTO strategy1 = new PRStrategyDTO();
	PRStrategyDTO strategy2 = new PRStrategyDTO();
	PRStrategyDTO strategy3 = new PRStrategyDTO();
	double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
	private static Logger logger = Logger.getLogger("TopsProdExampleJUnitTest");
	ObjectMapper mapper = new ObjectMapper();
	
	public void clearVariables(){
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		displayDetails = new HashMap<>();
	}

	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");

		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, recWeekStartDate, recWeekEndDate,
				isRecommendAtStoreLevel, -1, -1, -1);
		recommendationRunHeader = TestHelper.getRecommendationRunHeader(recWeekStartDate);
		productGroupProperties = TestHelper.addProductGroupProperty(productGroupProperties, productLevelId, productId, usePrediction);
		curCalDTO = TestHelper.getCalendarDetails(curWeekStartDate, curWeekEndDate);
		priceZoneStores = Stream.of(storesInZone.split(",")).map(Integer::parseInt).collect(Collectors.toList());
		recommendationRuleMap = TestHelper.getRecommendationRuleTops1();
		// recommendationRuleMap = TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);

		// Set strategies
		setStrategy1();
		setStrategy2();
		setStrategy3();

		// Set LIG Data
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member1, lig1Member1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member2, lig1Member2);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, lig1Member3, lig1Member3);

		retLirMap = TestHelper.setRetLirMap(retLirMap, lig2, lig2Member1, lig2Member1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig2, lig2Member2, lig2Member2);

		// Set LIG Constraint
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig1, LIG_CONSTRAINT_SAME);
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig2, LIG_CONSTRAINT_SAME);

		// Mockup
		predictionServiceMock = EasyMock.createMock(PredictionService.class);
	}

	/**
	 * Non LIG Item is in on-going promotion from 02/01/2018 to 03/21/2018(Short term promo, ends within 6 weeks), 
	 * there is recommendation, effective date is set as 03/22/2018. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase1...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "02/01/2018", "03/21/2018", "03/01/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 20.49));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getFutureRecRetail(), new MultiplePrice(1, 20.49));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFutureRetailRecommended(), true);
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), "03/22/2018");
		assertEquals("Mismatching", itemMap.get(nonLig1Key).getIsTPR(), 1);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFuturePromotion(), false);
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 02/01/2018 to 04/25/2018 (long term promo, ends after 6 weeks),
	 * there is recommendation, no effective date is set. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase3...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 20, 22.22, 20, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
		// Set additional Log
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/18/2018");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 19.99));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), null);
		assertEquals("Mismatch", null, itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isOnGoingPromotion(), true);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFuturePromotion(), false);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getIsTPR(), 1);
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 02/01/2018 to 04/25/2018 (for 12 weeks), 
	 * there is recommendation due to brand/size relation, no effective date is set. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase4...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 9.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, true, 7.95, 8.72, 7.95, 8.72, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 20, 22.22, 8.72, 8.72, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 16.99, 16.99, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 16.99, 16.99, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 16.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 16.99));
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), null);
		assertEquals("Mismatch", null, itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isOnGoingPromotion(), true);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFuturePromotion(), false);
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getIsTPR(), 1);
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 02/01/2018 to 04/25/2018 (for 12 weeks), there is recommendation, 
	 * but current retail doesn’t violate brand/size relation, current retail is retained, and no effective date is set. 
	 * Marked as TPR, additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase5...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
		// Set additional Log
//		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/25/2018");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.29), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), null);
		assertEquals("Mismatch", null, itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isOnGoingPromotion(), true);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFuturePromotion(), false);
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getIsTPR(), 1);
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 03/01/2018 to 03/07/2018 (for 1 week), 
	 * there is recommendation, effective date is set as 03/08/2018. Marked as Sale
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase6() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase6...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/01/2018", "03/07/2018", recWeekStartDate);
		TestHelper.setAdDetails(adDetails, nonLig1, "03/01/2018", 1, 1);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), "03/08/2018");
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isOnGoingPromotion(), true);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFuturePromotion(), false);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getIsTPR(), 0);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getIsOnAd(), 1);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecWeekAdInfo().getWeeklyAdStartDate(), recWeekStartDate);
	}
	
	/**
	 * Non LIG Item is in future promotion from 03/08/2018 to 04/11/2018(for 5 weeks), 
	 * there is recommendation, effective date is set as 04/12/2018. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase7() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase7...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/08/2018", "04/11/2018", "03/08/2018");
		TestHelper.setAdDetails(adDetails, nonLig1, "03/08/2018", 1, 1);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49,20.99,21.49,21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", "04/12/2018", itemMap.get(nonLig1Key).getRecPriceEffectiveDate());
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isOnGoingPromotion(), false);
		assertEquals("Mismatch", itemMap.get(nonLig1Key).isFuturePromotion(), true);
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", null,itemMap.get(nonLig1Key).getRecWeekAdInfo().getWeeklyAdStartDate());
	}
	
	/**
	 * Non LIG Item is in future promotion from 03/08/2018 to 05/09/2018(for 8 weeks), 
	 * there is recommendation, no effective date is set. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase8() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase8...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/08/2018", "05/09/2018", "03/08/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49,20.99,21.49,21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", null, itemMap.get(nonLig1Key).getRecPriceEffectiveDate());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isOnGoingPromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFuturePromotion());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", null,itemMap.get(nonLig1Key).getRecWeekAdInfo().getWeeklyAdStartDate());
	}
	
	/**
	 * Non LIG Item is in future promotion from 03/15/2018 to 05/09/2018 (for 8 weeks), there is recommendation, 
	 * but current retail doesn’t violate brand/size relation, current retail is retained, and no effective date is set. 
	 * Marked as TPR, additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase9() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase9...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
		// Set additional Log
//		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "05/09/2018");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.29), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", null, itemMap.get(nonLig1Key).getRecPriceEffectiveDate());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isOnGoingPromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFuturePromotion());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", 0, itemMap.get(nonLig1Key).getIsOnAd());
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 03/15/2018 to 03/21/2018 (for 1 week), 
	 * there is recommendation, effective date is set as 03/22/2018. Marked as Sale
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase10() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase10...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/15/2018", "03/21/2018", "03/15/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
		
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", "03/22/2018", itemMap.get(nonLig1Key).getRecPriceEffectiveDate());
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isOnGoingPromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFuturePromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 0, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsOnAd());
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 03/15/2018 to 03/21/2018 (for 1 week) and 04/12/2018 to 04/18/2018,
	 * there is recommendation, effective date is set as 04/19/2018. Marked as Sale
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase11() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase11...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/15/2018", "03/21/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "04/05/2018", "04/11/2018", "04/05/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
				pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", "04/12/2018", itemMap.get(nonLig1Key).getRecPriceEffectiveDate());
		assertEquals("Mismatch", new MultiplePrice(1, 20.49), itemMap.get(nonLig1Key).getFutureRecRetail());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isOnGoingPromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFuturePromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 0, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsOnAd());
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 02/01/2018 to 04/25/2018 (for 12 weeks) and 04/12/2018 to 04/18/2018,
	 * there is recommendation, no effective date is set. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase12() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase12...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "04/05/2018", "04/11/2018", "04/05/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 20, 22.22, 20, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
		// Set additional Log
		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/18/2018");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 19.99));
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), null);
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isOnGoingPromotion());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isFuturePromotion());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", 0, itemMap.get(nonLig1Key).getIsOnAd());
	}
	
	/**
	 * Non LIG Item is in future promotion from 03/15/2018 to 03/21/2018 (for 8 weeks), 03/29/2018 to 04/04/2018 
	 * and 04/12/2018 to 05/16/2018,
	 * there is recommendation, but current retail doesn’t violate brand/size relation, current retail is retained,  
	 * and no effective date is set. Marked as TPR, additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase13() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase13...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/15/2018", "03/21/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "03/29/2018", "04/04/2018", "03/29/2018");
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
		// Set additional Log
//		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "05/16/2018");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.29), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", null, itemMap.get(nonLig1Key).getRecPriceEffectiveDate());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isFutureRetailRecommended());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isOnGoingPromotion());
		assertEquals("Mismatch", true, itemMap.get(nonLig1Key).isFuturePromotion());
		assertEquals("Mismatch", false, itemMap.get(nonLig1Key).isPromoEndsWithinXWeeks());
		assertEquals("Mismatch", 1, itemMap.get(nonLig1Key).getIsTPR());
		assertEquals("Mismatch", 0, itemMap.get(nonLig1Key).getIsOnAd());
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going and future promotion from 02/01/2018 to 03/07/2018 
	 * and 04/12/2018 to 05/16/2018, there is recommendation, but current retail doesn’t violate brand/size relation, 
	 * current retail is retained, and no effective date is set. Marked as TPR (pick the far promotion within 6 weeks and decide accordingly),
	 * additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase14() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase14...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);
		
		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "04/12/2018", "05/16/2018", "04/12/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "02/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "04/12/2018", "05/16/2018", "04/12/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "02/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "04/12/2018", "05/16/2018", "04/12/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
//				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "05/16/2018");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price
				
				assertEquals("Mismatch", new MultiplePrice(1, 20.29), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isFutureRetailRecommended());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "03/07/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "04/12/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "05/16/2018", item.getFutWeekSaleInfo().getSaleEndDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going promotion from 02/01/2018 to 03/21/2018(for 7 weeks), 
	 * there is recommendation, effective date is set as 03/22/2018. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase15() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase15...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "03/21/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "02/01/2018", "03/21/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "02/01/2018", "03/21/2018", "03/01/2018");
		
		// Set Ad Details
		TestHelper.setAdDetails(adDetails, lig1Member1, "03/01/2018", 1, 1);
		TestHelper.setAdDetails(adDetails, lig1Member2, "03/01/2018", 1, 1);
		TestHelper.setAdDetails(adDetails, lig1Member3, "03/01/2018", 1, 1);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", "03/22/2018", item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getFutureRecRetail());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", false, item.isFuturePromotion());
				assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				if(item.getItemCode() != lig1){
					assertEquals("Mismatch", true, item.isFutureRetailRecommended());
				}
				assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "03/21/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "03/01/2018", item.getRecWeekAdInfo().getWeeklyAdStartDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going promotion from 02/01/2018 to 04/25/2018 (for 12 weeks),
	 * there is recommendation due to brand/size relation, no effective date is set. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase16() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase16...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", false, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "04/25/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", null, item.getFutureRecRetail());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going from 02/01/2018 to 04/25/2018,
	 * there is recommendation, but current retail doesn’t violate brand/size relation, 
	 * current retail is retained, and no effective date is set. Marked as TPR (pick the far promotion within 6 weeks and decide accordingly),
	 * additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase17() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase17...");
		
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);
		
		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
//				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/25/2018");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price
				
				assertEquals("Mismatch", new MultiplePrice(1, 20.29), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", false, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "04/25/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going promotion from 03/01/2018 to 03/07/2018 (for 1 week), 
	 * there is recommendation, effective date is set as 03/08/2018. Marked as Sale
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase18() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase16...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/01/2018", "03/07/2018", "03/01/2018");

		// Set Ad Details
		TestHelper.setAdDetails(adDetails, lig1Member1, "03/01/2018", 1, 1);
		TestHelper.setAdDetails(adDetails, lig1Member2, "03/01/2018", 1, 1);
		TestHelper.setAdDetails(adDetails, lig1Member3, "03/01/2018", 1, 1);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", "03/08/2018", item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", false, item.isFuturePromotion());
				assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 0, item.getIsTPR());
				assertEquals("Mismatch", 1, item.getIsOnAd());
				
				if(item.getItemCode() != lig1){
					assertEquals("Mismatch", true, item.isFutureRetailRecommended());
				}
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getFutureRecRetail());
				assertEquals("Mismatch", "03/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "03/07/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "03/01/2018", item.getRecWeekAdInfo().getWeeklyAdStartDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in future promotion from 03/15/2018 to 04/11/2018(for 5 weeks), 
	 * there is recommendation, effective date is set as 04/12/2018. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase19() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase16...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/15/2018", "04/11/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/15/2018", "04/11/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/15/2018", "04/11/2018", "03/15/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", "04/12/2018", item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				if(item.getItemCode() != lig1){
					assertEquals("Mismatch", true, item.isFutureRetailRecommended());
				}
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getFutureRecRetail());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "03/15/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "04/11/2018", item.getFutWeekSaleInfo().getSaleEndDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in future promotion from 03/15/2018 to 05/09/2018 (for 8 weeks), 
	 * there is recommendation due to brand/size relation, no effective date is set. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase20() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase16...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", null, item.getFutureRecRetail());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "03/15/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "05/09/2018", item.getFutWeekSaleInfo().getSaleEndDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in future promotion from 03/15/2018 to 05/09/2018 (for 8 weeks), 
	 * there is recommendation, but current retail doesn’t violate brand/size relation, current retail is retained, 
	 * and no effective date is set. Marked as TPR, additional log must not be there since recommended and Current reg price are same
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase21() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase17...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);
		
		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
//				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "05/09/2018");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price
				
				assertEquals("Mismatch", new MultiplePrice(1, 20.29), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", null, item.getFutureRecRetail());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "03/15/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "05/09/2018", item.getFutWeekSaleInfo().getSaleEndDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going promotion from 03/15/2018 to 03/21/2018 (for 1 week), 
	 * there is recommendation, effective date is set as 03/22/2018. Marked as Sale
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase22() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase22...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/15/2018", "03/21/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/15/2018", "03/21/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/15/2018", "03/21/2018", "03/15/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", "03/22/2018", item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 0, item.getIsTPR());
				assertEquals("Mismatch", 1, item.getIsOnAd());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in future promotion from 03/15/2018 to 03/21/2018, 03/29/2018 to 04/04/2018 
	 * and 04/05/2018 to 05/16/2018 there is recommendation, but current retail doesn’t violate brand/size relation, current retail is retained, 
	 * and no effective date is set. Marked as TPR, additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase23() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase23...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		
		// Set Ad Details
		TestHelper.setAdDetails(adDetails, lig1Member1, "03/15/2018", 1, 1);
		TestHelper.setAdDetails(adDetails, lig1Member2, "03/15/2018", 1, 1);
		TestHelper.setAdDetails(adDetails, lig1Member3, "03/15/2018", 1, 1);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);
		
		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/15/2018", "05/09/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/29/2018", "04/04/2018", "03/29/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/29/2018", "04/04/2018", "03/29/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/29/2018", "04/04/2018", "03/29/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
//				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "05/16/2018");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price
				
				assertEquals("Mismatch", new MultiplePrice(1, 20.29), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "04/05/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "05/16/2018", item.getFutWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", null, item.getRecWeekAdInfo().getWeeklyAdStartDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going and future promotion from 02/01/2018 to 03/07/2018 and 
	 * 04/12/2018 to 05/16/2018, there is recommendation, but current retail doesn’t violate brand/size relation, 
	 * current retail is retained, and no effective date is set. Marked as TPR (pick the far promotion within 6 weeks 
	 * and decide accordingly), additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase24() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase24...");
		
		clearVariables();
		
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);
		
		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "02/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "02/01/2018", "03/07/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "04/05/2018", "05/16/2018", "04/05/2018");
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
//				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "05/16/2018");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price
				
				assertEquals("Mismatch", new MultiplePrice(1, 20.29), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "03/07/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "04/05/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "05/16/2018", item.getFutWeekSaleInfo().getSaleEndDate());
			}
		}
	}
	
	/**
	 * LIG has 3 members and only one of the LIG member is in on-going promotion from 02/01/2018 to 03/21/2018(for 7 weeks), 
	 * there is recommendation only for TPR item, effective date is set as 03/22/2018 only for the TPR item. 
	 * Marked as TPR for that item and LIG
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase25() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase25...");
		
		clearVariables();
		
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.49, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 20.49, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "03/21/2018", "03/01/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				
				if (item.getItemCode() == lig1) {
					assertEquals("Recommend Price Effective date is Mismatching", "03/22/2018", item.getRecPriceEffectiveDate());

					assertEquals("Mismatch", true, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 1, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
				}
				if (item.getItemCode() == lig1Member1) {
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

					// Compare explain log
					assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
					// Check recommended price
					assertEquals("Recommend Price Effective date is Mismatching", "03/22/2018", item.getRecPriceEffectiveDate());
					
					assertEquals("Mismatch", true, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 1, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
					assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", "03/21/2018", item.getRecWeekSaleInfo().getSaleEndDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleEndDate());
				}
				
				else if(item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.42, 23.56, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
					TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
					// Compare explain log
					assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
					// Check recommended price
					assertEquals("Recommend Price Effective date is Mismatching", "03/22/2018", item.getRecPriceEffectiveDate());
					assertEquals("Mismatch", false, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 0, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
					assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleEndDate());
				}
			}
		}
	}
	
	/**
	 * LIG has 3 members and only one of the LIG member is in on-going promotion from 02/01/2018 to 04/25/2018 (for 12 weeks), 
	 * there is recommendation for all 3 items, but current retail doesn’t violate brand/size relation, current retail is retained, 
	 * and no effective date is set. Marked as TPR for that item and LIG
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase26() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase26...");
		
		clearVariables();
		
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.49, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				
				if (item.getItemCode() == lig1Member1) {
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.42, 23.56, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
					
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setLigConstraintLog(explainLog, guidelineAndConstraintLog, false, false, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 
							Constants.DEFAULT_NA, Constants.DEFAULT_NA, "", 20.29);
					
					// Set additional Log
					TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/18/2018");
					// Compare explain log
					assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
					// Check recommended price
					assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
					
					assertEquals("Mismatch", true, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 1, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
					assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", "04/25/2018", item.getRecWeekSaleInfo().getSaleEndDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleEndDate());
				}
				
				else if(item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
					TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
					// Compare explain log
					assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
					// Check recommended price
					assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
					assertEquals("Mismatch", false, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 0, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
					assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getFutWeekSaleInfo().getSaleEndDate());
				}
			}
		}
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in on-going promotion from 02/01/2018 to 04/25/2018, 
	 * there is no recommendation, effective date is set as null. Marked as TPR, additional log must say price retained, 
	 * set this item as parent item (i.e. just have brand relation with another item) 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase27() throws	 GeneralException, Exception, OfferManagementException {
		logger.debug("testCase27...");
		
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		PRItemDTO lig2Member1Item = TestHelper.getTestItem2(lig2Member1, 1, 24.69, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig2, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig2Member1Item);
		PRItemDTO lig2Member2Item = TestHelper.getTestItem2(lig2Member2, 1, 24.69, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig2, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig2Member2Item);
		PRItemDTO lig2Member3Item = TestHelper.getTestItem2(lig2Member3, 1, 24.69, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig2, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig2Member3Item);
		PRItemDTO lig2Item = TestHelper.setLIGItemDTO(lig2, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, lig2Item);
		

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig2Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig2Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig2Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig2Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig2Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig2Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig2Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig2Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig2Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		
		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
//		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
//		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
//		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig2Member1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig2Member2, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		TestHelper.setSaleDetails(saleDetails, lig2Member3, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		
		predictionItems.add(TestHelper.setPredictionItemDTO(lig2Member1, lig2Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig2Member2, lig2Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig2Member3, lig2Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			// Set expected log

			if (item.getItemCode() == lig1 || item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2
					|| item.getItemCode() == lig1Member3) {
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, false, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
//				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/25/2018");
				// Compare explain log
//				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", false, item.isFuturePromotion());
				assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 0, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
			} else {
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 20, 22.22, 20, 22.22, "", new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 20.99, 28.39, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.99, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.99, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				// Set additional Log
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/18/2018");
				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 24.69), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", null, item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", true, item.isOnGoingPromotion());
				assertEquals("Mismatch", false, item.isFuturePromotion());
				assertEquals("Mismatch", false, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "04/25/2018", item.getRecWeekSaleInfo().getSaleEndDate());
			}
			
		}
	}
	
	/**
	 * LIG has 3 members and only one of the LIG member is in on-going promotion from 02/01/2018 to 04/11/2018, 
	 * there is recommendation only for other 2 items, TPR item retail is determined based on other 2 items due to LIG constraint, 
	 * effective date is set as 04/12/2018. Marked as TPR for that item and LIG, additional log must not say price retained for the TPR item
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase28() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase28...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "02/01/2018", "04/11/2018", "03/01/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getFutureRecRetail());
				if (item.getItemCode() == lig1Member1) {
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
					TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE, "21.49,21.99");
					// Compare explain log
					assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
					// Check recommended price
					assertEquals("Recommend Price Effective date is Mismatching", "04/12/2018", item.getRecPriceEffectiveDate());
					assertEquals("Mismatch", true, item.isFutureRetailRecommended());
					assertEquals("Mismatch", true, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 1, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
					assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", "04/11/2018", item.getRecWeekSaleInfo().getSaleEndDate());
				}
				
				else if(item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3){
					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
							Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

					guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
					Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
					TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

					// Compare explain log
					assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
					// Check recommended price
					assertEquals("Recommend Price Effective date is Mismatching", "04/12/2018", item.getRecPriceEffectiveDate());
					assertEquals("Mismatch", false, item.isOnGoingPromotion());
					assertEquals("Mismatch", false, item.isFuturePromotion());
					assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
					assertEquals("Mismatch", 0, item.getIsTPR());
					assertEquals("Mismatch", 0, item.getIsOnAd());
					assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
					assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
				}
			}
		}
	}
	
	/**
	 * Non LIG Item is in on-going promotion from 02/01/2018 to 04/25/2018 (for 12 weeks) and AD, Display promotions 
	 * were provided. there is recommendation, 
	 * but current retail doesn’t violate brand/size relation, current retail is retained, and no effective date is set. 
	 * Marked as TPR, additional log must say price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase29() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase29...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO item = TestHelper.getTestItem(nonLig1, 1, 20.29, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1, LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 19.99, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week4StartDate, 1, 20.99, 0d, week4StartDate, 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		// Set Related Item
		PRItemDTO brandParentNonLig1 = TestHelper.getRelatedItem(nonLig2, 24.69);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentNonLig1);
		// Set Brand Relation
		TestHelper.setBrandRelation(item, nonLig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
						PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		
		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, nonLig1, 15.49, 1, "02/01/2018", "04/25/2018", "03/01/2018");
		
		TestHelper.setAdDetails(adDetails, nonLig1, "03/22/2018", 1, 1);
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		// Set expected log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog;

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, nonLig2, false);
		
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "", new MultiplePrice(1, 20.00));

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 17.25, 23.33, Constants.DEFAULT_NA, Constants.DEFAULT_NA, 20.25,
				22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
		TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
		// Set additional Log
//		TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, "04/25/2018");
		// Compare explain log
		assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));

		// Check recommended price
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));
		assertEquals("Mismatch", new MultiplePrice(1, 20.29), itemMap.get(nonLig1Key).getRecommendedRegPrice());
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getRecPriceEffectiveDate(), null);
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).isOnGoingPromotion(), true);
		assertEquals("Recommend Price Effective date is Mismatching", itemMap.get(nonLig1Key).getIsTPR(), 1);
		assertEquals("Mismatch", "02/01/2018", item.getRecWeekSaleInfo().getSaleStartDate());
		assertEquals("Mismatch", "04/25/2018", item.getRecWeekSaleInfo().getSaleEndDate());
		assertEquals("Mismatch", null, item.getRecWeekAdInfo().getWeeklyAdStartDate());
	}
	
	/**
	 * LIG has 3 members and all the LIG members is in future promotion from 03/15/2018 to 04/11/2018(for 5 weeks), 
	 * there is recommendation, effective date is set as 04/12/2018. Marked as TPR
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase30() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase30...");
		clearVariables();
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(lig1Member1, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(lig1Member2, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, lig1Member2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);
		PRItemDTO lig1Member3Item = TestHelper.getTestItem(lig1Member3, 1, 19.99, null, 3.15, 3.15, COST_UNCHANGED, compStrId, 20d, strategy1,
				LAST_X_WEEKS_MOV_1);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, curWeekStartDate, 1, 19.99, 0d, curWeekStartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member1, "01/04/2018", 1, 20.99, 0d, "01/04/2018", 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, curWeekStartDate, 1, 19.99, 0d, curWeekStartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member2, "01/04/2018", 1, 20.99, 0d, "01/04/2018", 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, curWeekStartDate, 1, 19.99, 0d, curWeekStartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, lig1Member3, "01/04/2018", 1, 20.99, 0d, "01/04/2018", 0, 0, 0, "");

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
		// Set price group
		TestHelper.setPriceGroup(lig1Member1Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member2Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(lig1Member3Item, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(ligItem, 12, BrandClassLookup.STORE.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO brandParentLig2 = TestHelper.getRelatedItemLig(lig2, 24.69);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, brandParentLig2);

		// Set Brand Relation
		TestHelper.setBrandRelationLig(lig1Member1Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member2Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(lig1Member3Item, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);
		TestHelper.setBrandRelationLig(ligItem, lig2, 0, 'X', 8, PRConstants.BRAND_RELATION, 10, 18, PRConstants.PRICE_GROUP_EXPR_BELOW,
				PRConstants.RETAIL_TYPE_SHELF, PRConstants.VALUE_TYPE_PCT);

		// Set Sale details
		TestHelper.setSaleDetails(saleDetails, lig1Member1, 15.49, 1, "03/15/2018", "04/11/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member2, 15.49, 1, "03/15/2018", "04/11/2018", "03/15/2018");
		TestHelper.setSaleDetails(saleDetails, lig1Member3, 15.49, 1, "03/15/2018", "04/11/2018", "03/15/2018");

		PricingEngineService pricingEngineService = new PricingEngineService();
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, curWeekStartDate, recWeekStartDate,
				noOfsaleAdDisplayWeeks);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member1, lig1Member1UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member2, lig1Member2UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(lig1Member3, lig1Member3UPC, new MultiplePrice(1, 19.99), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 20.49), PredictionStatus.SUCCESS, 150d, new MultiplePrice(1, 21.49), PredictionStatus.SUCCESS, 120d,
				new MultiplePrice(1, 21.99), PredictionStatus.SUCCESS, 110d));

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
		pricingEngineService.setEffectiveDate(allItems, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
		
		for (PRItemDTO item : allItems) {
			if (item.getItemCode() == lig1Member1 || item.getItemCode() == lig1Member2 || item.getItemCode() == lig1Member3
					|| item.getItemCode() == lig1) {
				// Set expected log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog;

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 20.25, 22.22, 20.25, 22.22, lig2, true);

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, false, 20, 22.22, 20.25, 22.22, "",
						new MultiplePrice(1, 20.00));

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 16.99, 22.99, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 3.15, Constants.DEFAULT_NA, 20.25, 22.22, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 20.49, 20.99, 21.49, 21.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));

				// Compare explain log
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog), mapper.writeValueAsString(item.getExplainLog()));
				// Check recommended price

				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getRecommendedRegPrice());
				assertEquals("Recommend Price Effective date is Mismatching", "04/12/2018", item.getRecPriceEffectiveDate());
				assertEquals("Mismatch", false, item.isOnGoingPromotion());
				assertEquals("Mismatch", true, item.isFuturePromotion());
				assertEquals("Mismatch", true, item.isPromoEndsWithinXWeeks());
				assertEquals("Mismatch", 1, item.getIsTPR());
				assertEquals("Mismatch", 0, item.getIsOnAd());
				if(item.getItemCode() != lig1){
					assertEquals("Mismatch", true, item.isFutureRetailRecommended());
				}
				assertEquals("Mismatch", new MultiplePrice(1, 20.49), item.getFutureRecRetail());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", null, item.getRecWeekSaleInfo().getSaleEndDate());
				assertEquals("Mismatch", "03/15/2018", item.getFutWeekSaleInfo().getSaleStartDate());
				assertEquals("Mismatch", "04/11/2018", item.getFutWeekSaleInfo().getSaleEndDate());
			}
		}
	}
	
	private void setStrategy1() {
		strategy1 = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate, recWeekEndDate, false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy1);

		TestHelper.setBrandGuideline(strategy1);
		TestHelper.setSizeGuideline(strategy1, 1);
		TestHelper.setPIGuideline(strategy1, 90, 100);
		TestHelper.setThreasholdConstraint(strategy1, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(strategy1, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setCostConstraint(strategy1, false);
		TestHelper.setRoundingConstraint(strategy1, TestHelper.getRoundingTableTableTops1());
	}

	private void setStrategy2() {
		strategy2 = TestHelper.getStrategy(2, locationLevelId, locationId, productLevelId, productId, curWeekStartDate, recWeekEndDate, false, -1, -1,
				-1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningUnits(strategy2);

		TestHelper.setSizeGuideline(strategy2, 0);
		TestHelper.setBrandGuideline(strategy2);
		TestHelper.setPIGuideline(strategy2, 88, 91);
		TestHelper.setThreasholdConstraint(strategy2, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(strategy2, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setCostConstraint(strategy2, false);
		TestHelper.setRoundingConstraint(strategy2, TestHelper.getRoundingTableTableTops1());
	}
	
	private void setStrategy3() {
		strategy3 = TestHelper.getStrategy(3, locationLevelId, locationId, productLevelId, productId, curWeekStartDate, recWeekEndDate, false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy3);

		TestHelper.setSizeGuideline(strategy3, 0);
		TestHelper.setBrandGuideline(strategy3);
		TestHelper.setPIGuideline(strategy3, 80, 100);
		TestHelper.setThreasholdConstraint(strategy3, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(strategy3, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setCostConstraint(strategy3, false);
		TestHelper.setRoundingConstraint(strategy3, TestHelper.getRoundingTableTableTops1());
	}
}
