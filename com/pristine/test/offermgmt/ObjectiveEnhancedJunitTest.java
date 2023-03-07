package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

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
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
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

public class ObjectiveEnhancedJunitTest {
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
	int ligMember1 = 1000, ligMember2 = 1001, ligMember3 = 1002, ligMember4 = 1003;
	int nonLig1 = 1100,nonLig2 = 2200;
	int lig1 = 3000;
	String ligMember1UPC = "010101010", ligMember2UPC = "010101011", ligMember3UPC = "010101012", ligMember4UPC = "010101013";
	String nonLig1UPC = "020202020";
	char LIG_CONSTRAINT_SAME = 'S';
	Integer COST_NO_CHANGE = 0;
	Integer COST_INCREASED = 1;
	Long LAST_X_WEEKS_MOV_1 = 4587l;
	int startCalendarId = 3456, endCalendarId = 3459;
	ProductKey lig1Key = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lig1);
	ProductKey ligMember1Key = new ProductKey(Constants.ITEMLEVELID, ligMember1);
	ProductKey ligMember2Key = new ProductKey(Constants.ITEMLEVELID, ligMember2);
	ProductKey nonLig1Key = new ProductKey(Constants.ITEMLEVELID, nonLig1);
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

	private int itemCode1 = 100001;
	private int itemCode2 = 100002;
	private static final Integer COST_UNCHANGED = 0;
	String recommendationWeekStartDate = "01/08/2017";
	String recRunningWeekStartDate = "01/01/2017";
	
	PRStrategyDTO maxMarMaintainSalesStrategy = null, maxMarMaintainSalesStrategy1 = null;
	PRStrategyDTO maxMarMaintainUnitsStrategy = null, maxMarMaintainUnitsStrategy1 = null;
	PRStrategyDTO highestRevStrategy = null;
	PRStrategyDTO highestMarginDollarStrategy = null;
	double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
	private static Logger logger = Logger.getLogger("ObjectiveEnhancedJunitTest");
	
	/*************************************/
	/** Have one lig with 4 items and one non lig */
	/*************************************/
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
		recommendationRuleMap = TestHelper.getRecommendationRuleAllEnabled();
		// recommendationRuleMap = TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);

		// Set Strategy
		maxMarMaintainSalesStrategy = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate,
				recWeekEndDate, false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(maxMarMaintainSalesStrategy);

		TestHelper.setPIGuideline(maxMarMaintainSalesStrategy, 80, 87);
		TestHelper.setThreasholdConstraint(maxMarMaintainSalesStrategy, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(maxMarMaintainSalesStrategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(maxMarMaintainSalesStrategy, TestHelper.getRoundingTableTable1());

		maxMarMaintainSalesStrategy1 = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate,
				recWeekEndDate, false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(maxMarMaintainSalesStrategy1);

		TestHelper.setPIGuideline(maxMarMaintainSalesStrategy1, 80, 90);
		TestHelper.setThreasholdConstraint(maxMarMaintainSalesStrategy1, PRConstants.VALUE_TYPE_PCT, 20, 20);
		TestHelper.setLigConstraint(maxMarMaintainSalesStrategy1, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(maxMarMaintainSalesStrategy1, TestHelper.getRoundingTableTable1());

		maxMarMaintainUnitsStrategy = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate,
				recWeekEndDate, false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningUnits(maxMarMaintainUnitsStrategy);

		TestHelper.setPIGuideline(maxMarMaintainUnitsStrategy, 80, 87);
		TestHelper.setThreasholdConstraint(maxMarMaintainUnitsStrategy, PRConstants.VALUE_TYPE_PCT, 15, 15);
		TestHelper.setLigConstraint(maxMarMaintainUnitsStrategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(maxMarMaintainUnitsStrategy, TestHelper.getRoundingTableTable1());

		maxMarMaintainUnitsStrategy1 = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate,
				recWeekEndDate, false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningUnits(maxMarMaintainUnitsStrategy1);

		TestHelper.setPIGuideline(maxMarMaintainUnitsStrategy1, 80, 90);
		TestHelper.setThreasholdConstraint(maxMarMaintainUnitsStrategy1, PRConstants.VALUE_TYPE_PCT, 20, 20);
		TestHelper.setLigConstraint(maxMarMaintainUnitsStrategy1, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(maxMarMaintainUnitsStrategy1, TestHelper.getRoundingTableTable1());

		highestRevStrategy = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate, recWeekEndDate,
				false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestRevenueDollar(highestRevStrategy);

		TestHelper.setPIGuideline(highestRevStrategy, 80, 90);
		TestHelper.setThreasholdConstraint(highestRevStrategy, PRConstants.VALUE_TYPE_PCT, 20, 20);
		TestHelper.setLigConstraint(highestRevStrategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(highestRevStrategy, TestHelper.getRoundingTableTable1());

		highestMarginDollarStrategy = TestHelper.getStrategy(1, locationLevelId, locationId, productLevelId, productId, curWeekStartDate,
				recWeekEndDate, false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(highestMarginDollarStrategy);

		TestHelper.setPIGuideline(highestMarginDollarStrategy, 80, 90);
		TestHelper.setThreasholdConstraint(highestMarginDollarStrategy, PRConstants.VALUE_TYPE_PCT, 20, 20);
		TestHelper.setLigConstraint(highestMarginDollarStrategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setRoundingConstraint(highestMarginDollarStrategy, TestHelper.getRoundingTableTable1());

		// Set LIG Data
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember1, ligMember1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember2, ligMember2);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember3, ligMember3);

		// Set LIG Constraint
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig1, LIG_CONSTRAINT_SAME);

		// Set item history
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember1, week1StartDate, 1, 2.69, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember2, week1StartDate, 1, 2.69, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember3, week1StartDate, 1, 2.69, 0d, curPriceEffDate1, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, nonLig1, week1StartDate, 1, 2.69, 0d, curPriceEffDate1, 0, 0, 0, "");

		// Mockup
		predictionServiceMock = EasyMock.createMock(PredictionService.class);
	 
	}

	/***
	 * There is only one final price point available Whose sales is higher than current sales. Recommended price will be the final
	 * price point
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.69, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 150d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 80d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 70d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 90d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 260d, null, null, null, null, null, null));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		PRItemDTO ligItem = itemMap.get(lig1Key);
		PRItemDTO ligMember1Item = itemMap.get(ligMember1Key);
		PRItemDTO nonLig1Item = itemMap.get(nonLig1Key);

		// Check lig price
		assertEquals("Mismatch", ligItem.getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check lig member
		assertEquals("Mismatch", ligMember1Item.getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check non lig
		assertEquals("Mismatch", nonLig1Item.getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is only one final price point available Whose sales is lower than current sales. Recommended price will be the final
	 * price point
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.69, maxMarMaintainSalesStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 80d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 80d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 70d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 85d, null, null, null, null, null, null));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		PRItemDTO ligItem = itemMap.get(lig1Key);
		PRItemDTO ligMember1Item = itemMap.get(ligMember1Key);
		PRItemDTO nonLig1Item = itemMap.get(nonLig1Key);

		// Check lig price
		assertEquals("Mismatch", ligItem.getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check lig member
		assertEquals("Mismatch", ligMember1Item.getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check non lig
		assertEquals("Mismatch", nonLig1Item.getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is multiple final price point available none of the price point sales is higher than current sales. Recommended price
	 * will be from one of the final price point with highest sales
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 97d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 80d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 70d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 92d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 75d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 87d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 70d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 45d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 40d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 42d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is multiple final price point available some of the price point sales is higher than current sales. Recommended price
	 * will be from one of the final price point with highest margin
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 70d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 75d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 88d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 42d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.69));

	}

	/***
	 * There is multiple final price point available all of the price point sales is higher than current sales. Recommended price
	 * will be from one of the final price point with highest margin
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 100d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 90d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 88d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 85d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 58d));

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
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is multiple final price point available all of the price point sales is higher than current sales. There is no cost.
	 * Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase6() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, null, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 100d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 90d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 88d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 85d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 58d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.49));

	}

	/***
	 * There is multiple final price point available all of the price point sales is higher than current sales. There is no
	 * current price. Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase7() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, null, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, null, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, null, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, null, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 100d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 90d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 88d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 85d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 58d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), null);

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), null);

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), null);

	}

	/***
	 * There is multiple final price point available all of the price point sales is higher than current sales. Current price
	 * prediction not available. Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase8() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);;

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.49));

	}

	/***
	 * There is multiple final price point available. None of the price point has valid prediction. Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase9() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainSalesStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, null, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainSalesStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), null, null, new MultiplePrice(1, 2.69), null, null, new MultiplePrice(1, 2.79), null, null));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.49));

	}

	/***
	 * There is only one final price point available Whose units is higher than current units. Recommended price will be the final
	 * price point
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase10() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.69, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 150d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 80d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 110d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 70d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 90d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 260d, null, null, null, null, null, null));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is only one final price point available Whose units is lower than current units. Recommended price will be the final
	 * price point
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase11() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.49, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.89, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.69, maxMarMaintainUnitsStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 80d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 80d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.49), PredictionStatus.SUCCESS, 70d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d, null, null, null, null, null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 85d, null, null, null, null, null, null));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is multiple final price point available none of the price point units is higher than current units. Recommended price
	 * will be from one of the final price point with highest units
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase12() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 97d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 80d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 70d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 92d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 75d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 87d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 70d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 47d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 40d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 42d));

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
		
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.59));

	}

	/***
	 * There is multiple final price point available some of the price point units is higher than current units. Recommended price
	 * will be from one of the final price point with highest margin
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase13() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 102d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 70d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 102d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 96d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 75d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 91d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 60d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 51d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 42d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.79));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.79));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.69));

	}

	/***
	 * There is multiple final price point available all of the price point units is higher than current units. Recommended price
	 * will be from one of the final price point with highest margin
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase14() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 102d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 103d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 102d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 96d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 98d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 91d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 92d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 51d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 53d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.99));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.79));

	}

	/***
	 * There is multiple final price point available all of the price point units is higher than current units. There is no cost.
	 * Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase15() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, null, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, null, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 102d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 103d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 102d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 96d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 98d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 91d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 92d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 51d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 53d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.49));

	}

	/***
	 * There is multiple final price point available all of the price point units is higher than current units. There is no
	 * current price. Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase16() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, null, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, null, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, null, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, null, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 110d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 102d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 103d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 102d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 96d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 98d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), PredictionStatus.SUCCESS, 95d, new MultiplePrice(1, 3.79), PredictionStatus.SUCCESS, 91d,
				new MultiplePrice(1, 3.99), PredictionStatus.SUCCESS, 92d));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), PredictionStatus.SUCCESS, 52d, new MultiplePrice(1, 2.69), PredictionStatus.SUCCESS, 51d,
				new MultiplePrice(1, 2.79), PredictionStatus.SUCCESS, 53d));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), null);

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), null);

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), null);

	}

	/***
	 * There is multiple final price point available all of the price point units is higher than current units. Current price
	 * prediction not available. Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase17() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.49));

	}

	/***
	 * There is multiple final price point available. None of the price point has valid prediction. Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase18() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.10, 2.06, COST_INCREASED, compStrId, 3.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, maxMarMaintainUnitsStrategy1);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 2.49, null, 1.29, 1.20, COST_INCREASED, compStrId, 2.29, maxMarMaintainUnitsStrategy1,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 2.49), PredictionStatus.SUCCESS, 50d,
				new MultiplePrice(1, 2.59), null, null, new MultiplePrice(1, 2.69), null, null, new MultiplePrice(1, 2.79), null, null));

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

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 2.49));

	}
	
	/***
	 * There is multiple final price point available. None of the price point has valid prediction.
	 * Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase19() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.06, 2.06, COST_NO_CHANGE, compStrId, 3.29, highestRevStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.06, 2.06, COST_NO_CHANGE, compStrId, 3.29, highestRevStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.06, 2.06, COST_NO_CHANGE, compStrId, 3.29, highestRevStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, highestRevStrategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 8.49, null, 6.45, 6.45, COST_NO_CHANGE, compStrId, 8.69, highestRevStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 8.49), PredictionStatus.SUCCESS, 150d,
				new MultiplePrice(1, 9.69), null, null, new MultiplePrice(1, 9.79), null, null,
				new MultiplePrice(1, 9.99), null, null));

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
		
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 8.49));

	}
	
	/***
	 * There is multiple final price point available. None of the price point has valid prediction.
	 * Current price will be retained
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase20() throws GeneralException, Exception, OfferManagementException {
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Setup data
		PRItemDTO item = TestHelper.getTestItem2(ligMember1, 1, 3.59, null, 2.06, 2.06, COST_NO_CHANGE, compStrId, 3.29, highestMarginDollarStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember2, 1, 3.59, null, 2.06, 2.06, COST_NO_CHANGE, compStrId, 3.29, highestMarginDollarStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.getTestItem2(ligMember3, 1, 3.59, null, 2.06, 2.06, COST_NO_CHANGE, compStrId, 3.29, highestMarginDollarStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember3UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, highestMarginDollarStrategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, item);

		// non lig
		item = TestHelper.getTestItem2(nonLig1, 1, 8.49, null, 6.45, 6.45, COST_NO_CHANGE, compStrId, 8.69, highestMarginDollarStrategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, nonLig1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, ligMember1UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 100d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, ligMember2UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 95d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, ligMember3UPC, new MultiplePrice(1, 3.59), PredictionStatus.SUCCESS, 90d,
				new MultiplePrice(1, 3.69), null, null, new MultiplePrice(1, 3.79), null, null, new MultiplePrice(1, 3.99), null, null));
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 8.49), PredictionStatus.SUCCESS, 150d,
				new MultiplePrice(1, 9.69), null, null, new MultiplePrice(1, 9.79), null, null,
				new MultiplePrice(1, 9.99), null, null));

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
		
		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check lig member
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.59));

		// Check non lig
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 8.49));

	}
	
	
	/**
	 * Current price is retained as there is no valid prediction,
	 * even though the brand/size relation pushes to new price.
	 * Objective: MaximizeMar$ByMaintaningSale$
	 * LIG in size relation with non-lig
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase21() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase21......");
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 3.89, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 3.89, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 2.49);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price (dependent item)
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.89));

		// Check lig member (dependent item)
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.89));

	}
	
	/**
	 * Current price is retained as there is no valid prediction,
	 * even though the brand/size relation pushes to new price.
	 * Objective: MaximizeMar$ByMaintaningUnits
	 * LIG in size relation with non-lig
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase22() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase22......");
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningUnits(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 3.89, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 3.89, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 2.49);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price (dependent item)
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.89));

		// Check lig member (dependent item)
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.89));

	}
	
	/**
	 * Current price is not retained even though there is no valid prediction,
	 * and the brand/size relation pushes to new price.
	 * Objective: UseGuidelinesAndConstraints
	 * LIG in size relation with non-lig 
	 * UseGuidelinesAndConstraints works differently than other objectives
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase23() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase23......");
		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 3.89, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 3.89, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 2.49);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream().collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		// Check lig price (dependent item)
		assertEquals("Mismatch", itemMap.get(lig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

		// Check lig member (dependent item)
		assertEquals("Mismatch", itemMap.get(ligMember2Key).getRecommendedRegPrice(), new MultiplePrice(1, 3.69));

	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size related item price is within the given range
	 * Current price needs to be retained
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase24() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase24......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.99, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();

		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 1.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.39), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		item.setListCost(1.23);
		
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		
		allItems.forEach(testItem -> {
			if (item.getItemCode() == itemCode1) {
				// Match recommended price
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
				assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
			}
		});
			

	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size related item price is not within the given range
	 * Item with size relationship alone and it is violated – new retail is recommended
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase25() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase25......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy,
				50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 3.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 2.91, 3.1, itemCode2,
				false, 10);

		item.setExplainLog(explainLog);

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0,
				0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0,
				0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0,
				0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0,
				0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0,
				0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.39), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		item.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		allItems.forEach(testItem -> {
			if (item.getItemCode() == itemCode1) {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 3.49);
				assertEquals("Mismatch", expectedRecommendedPrice, testItem.getRecommendedRegPrice());
			}
		});

	}
	
	/**
	 * To check if Processing item is on TPR
	 * Brand related item price is within the given range
	 * Item with Brand relationship alone and it is not violated – Same retail needs to be retained
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase26() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase26......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
				PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
				TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
				
				TestHelper.setSizeGuideline(strategy, 1);
				TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

				// Set Item
				PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.99, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
				item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
				item.setIsTPR(1);
				// Set price group
				TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

				// Set Related Item
				PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

				TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
				
				
				// Set Size Relation
//				TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//						PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

				// Set explain log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 2.79, 3.79,
						itemCode2, false);
//				TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
				
				item.setExplainLog(explainLog);
				
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.39), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		item.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		
		allItems.forEach(testItem -> {
			if (item.getItemCode() == itemCode1) {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
				assertEquals("Mismatch", expectedRecommendedPrice, testItem.getRecommendedRegPrice());
			}
		});

	}
	
	/**
	 * To check if Processing item is on TPR
	 * Brand related item price is not within the given range
	 * Item with Brand relationship alone and it is violated – new retail needs to be retained
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase27() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase27......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
				PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
				TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
				TestHelper.setBrandGuideline(strategy);
//				TestHelper.setSizeGuideline(strategy, 1);
				TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

				// Set Item
				PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
				item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
				item.setIsTPR(1);
				// Set price group
				TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

				// Set Related Item
				PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 3.29);
				itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

				TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
						Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
				
				
				// Set Size Relation
//				TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//						PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

				// Set explain log
				PRExplainLog explainLog = new PRExplainLog();
				PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
				TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
						itemCode2, false);
//				TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
				
				item.setExplainLog(explainLog);
				
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
				TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 1.29), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		item.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		
		allItems.forEach(testItem -> {
			if (item.getItemCode() == itemCode1) {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.29);
				assertEquals("Mismatch", expectedRecommendedPrice, testItem.getRecommendedRegPrice());
			}
		});
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size and Brand related item price is not within the given range
	 * Item with Size and Brand relationship is violated – new retail needs to be recommended
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase28() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase28......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 4.99, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 3.49);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, 3.29, 3.79, 3.79, 4.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 1.29), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		item.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		allItems.forEach(testItem -> {
			if (item.getItemCode() == itemCode1) {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 3.49);
				assertEquals("Mismatch", expectedRecommendedPrice, testItem.getRecommendedRegPrice());
			}
		});
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size and Brand related item price is within the given range
	 * Item with Size and Brand relationship and it is not violated – Same retail needs to be retained
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase29() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase29......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 3.49, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 3.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 1.29), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		item.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		
		allItems.forEach(testItem -> {
			if (item.getItemCode() == itemCode1) {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 3.49);
				assertEquals("Mismatch", expectedRecommendedPrice, testItem.getRecommendedRegPrice());
			}
		});
	}
	
	/**
	 * To check if LIG Member items is on TPR
	 * Size and Brand related item price is within the given range
	 * Item with Size and Brand relationship and it is not violated – Same retail needs to be retained
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase30() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase30......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 3.29, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 3.29, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 60, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 3.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		TestHelper.setBrandRelation(item1, nonLig1, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(item2, nonLig1, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(ligItem, nonLig1, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog_4 = new PRGuidelineAndConstraintLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog_5  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item1, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
				itemCode2, false);
		TestHelper.setBrandLog(item2, explainLog, guidelineAndConstraintLog_4, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
				itemCode2, false);
		TestHelper.setBrandLog(ligItem, explainLog, guidelineAndConstraintLog_5, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog_1  = new PRGuidelineAndConstraintLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog_2 = new PRGuidelineAndConstraintLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog_3  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item1, explainLog, guidelineAndConstraintLog_1, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		TestHelper.setSizeLog(item2, explainLog, guidelineAndConstraintLog_2, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		TestHelper.setSizeLog(ligItem, explainLog, guidelineAndConstraintLog_3, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		
		item1.setExplainLog(explainLog);
		item2.setExplainLog(explainLog);
		ligItem.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.29), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 3.49), pricePointDTO1);

		item1.setRegPricePredictionMap(priceMovementPrediction);
		item1.setListCost(1.23);
		item2.setRegPricePredictionMap(priceMovementPrediction);
		item2.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);
		
		allItems.forEach(testItem -> {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 3.29);
				assertEquals("Mismatch for item code: "+testItem.getItemCode(), expectedRecommendedPrice, testItem.getRecommendedRegPrice());
		});
	}
	
	/**
	 * To check if LIG Member items is on TPR
	 * Size and Brand related item price is not within the given range
	 * Item with Size and Brand relationship is violated – new retail needs to be recommended
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase31() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase31......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 2.69, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 2.69, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 4.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		PRItemDTO brandParentItem = TestHelper.getRelatedItem(nonLig2, 2.99);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		TestHelper.setBrandRelation(item1, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(item2, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(ligItem, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 2.79), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 2.69), pricePointDTO1);

		item1.setRegPricePredictionMap(priceMovementPrediction);
		item1.setListCost(1.23);
		item2.setRegPricePredictionMap(priceMovementPrediction);
		item2.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		allItems.forEach(testItem -> {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.69);
				assertEquals("Mismatch for item code: "+testItem.getItemCode(), expectedRecommendedPrice, testItem.getRecommendedRegPrice());
		});
	}
	
	/**
	 * LIG Item (3 members), all 3 members retail was changed on 02/22/2018, current retail is below the cost, 
	 * there are 3 final rounding digits with current price has one of the rounding digit.
	 * 
	 * Cost violated and Brand/Size is within the range
	 * 
	 * Expected output: Recommended Retail: Recommended a new retail
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase32() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase31......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 1.79, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 1.79, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 1.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		PRItemDTO brandParentItem = TestHelper.getRelatedItem(nonLig2, 1.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		TestHelper.setBrandRelation(item1, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(item2, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(ligItem, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember1, week1StartDate, 1, 2.89, 0d, week1StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember1, week2StartDate, 1, 2.59, 0d, week2StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember2, week1StartDate, 1, 2.89, 0d, week1StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember2, week2StartDate, 1, 2.59, 0d, week2StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 2.29), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 1.69), pricePointDTO1);

		item1.setRegPricePredictionMap(priceMovementPrediction);
		item1.setListCost(2.23);
		item2.setRegPricePredictionMap(priceMovementPrediction);
		item2.setListCost(2.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

//		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
//				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, recommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		allItems.forEach(testItem -> {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.29);
				assertEquals("Mismatch for item code: "+testItem.getItemCode(), expectedRecommendedPrice, testItem.getRecommendedRegPrice());
		});
	}
	
	/**
	 * LIG Item (3 members), all 3 members retail was changed on 02/22/2018, current retail is below the cost, 
	 * there are 3 final rounding digits with current price has one of the rounding digit.
	 * 
	 * Cost violated and Brand/Size is within the range
	 * 
	 * Expected output: Recommended Retail: Recommended a new retail
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase33() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase31......");
		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();
		objectiveService = new ObjectiveService();
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		TestHelper.setBrandGuideline(strategy);
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item1 = TestHelper.getTestItem2(ligMember1, 1, 1.99, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item1);
		
		PRItemDTO item2 = TestHelper.getTestItem2(ligMember2, 1, 1.99, null, 1.29, 1.28, COST_NO_CHANGE, compStrId, 3.89, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember2UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item2);
		 
		// Lig item
		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, LAST_X_WEEKS_MOV_1, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);
		
		// Set price group
		TestHelper.setPriceGroup(item1, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroup(item2, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');
		TestHelper.setPriceGroupLig(ligItem, 24, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig1, 1.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		PRItemDTO brandParentItem = TestHelper.getRelatedItem(nonLig2, 1.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, brandParentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item1, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(item2, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		TestHelper.setSizeRelation(ligItem, nonLig1, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		TestHelper.setBrandRelation(item1, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(item2, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setBrandRelation(ligItem, nonLig2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember1, week1StartDate, 1, 2.89, 0d, week1StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember1, week2StartDate, 1, 2.59, 0d, week2StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember2, week1StartDate, 1, 2.89, 0d, week1StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember2, week2StartDate, 1, 2.59, 0d, week2StartDate, 0, 0, 0, "");

		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 2.29), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 1.69), pricePointDTO1);

		item1.setRegPricePredictionMap(priceMovementPrediction);
		item1.setListCost(1.23);
		item2.setRegPricePredictionMap(priceMovementPrediction);
		item2.setListCost(1.23);
		// Mock predictions
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

//		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
//				.setRecommendationRuleMap("R11", 0, true);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
				retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties,
				predictionComponent, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				isOnline, itemPriceHistory, movementData, recommendationRuleMap, saleDetails, adDetails,
				priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		allItems.forEach(testItem -> {
				MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.69);
				assertEquals("Mismatch for item code: "+testItem.getItemCode(), expectedRecommendedPrice, testItem.getRecommendedRegPrice());
		});
	}
}
