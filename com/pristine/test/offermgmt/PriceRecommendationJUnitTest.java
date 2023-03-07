package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

/***
 * Put all items which is questioned by client or during internal discussion
 * These test cases will cover real scenarios
 * 
 * @author dunagaraj
 *
 */
public class PriceRecommendationJUnitTest {
	Connection conn = null;
	boolean isRecommendAtStoreLevel = false;
	boolean isOnline = false;
	PredictionComponent predictionComponent = new PredictionComponent();
	HashMap<Integer, List<PRItemDTO>> finalLigMap = new HashMap<Integer, List<PRItemDTO>>();

	// Lead zone is not covered here
	HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
	// Not needed, as it is used only in prediction and prediction is not called and
	// mocked up here
	HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
	// Not needed, as it is used only in prediction and prediction is not called and
	// mocked up here
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
	List<PredictionItemDTO> predictionItems;

	int locationLevelId = 6, locationId = 6, productLevelId = 4, productId = 5768, strategyId = 1;

	int lig1 = 3000, ligMember1 = 1000, ligMember2 = 1001, ligMember3 = 1002, ligMember4 = 1003;
	int nonLig1 = 1100, nonLig2 = 2200;
	String ligMember1UPC = "010101010", ligMember2UPC = "010101011", ligMember3UPC = "010101012",
			ligMember4UPC = "010101013";
	String nonLig1UPC = "020202020", nonLig2UPC = "030202020";
	ProductKey lig1Key = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lig1);
	ProductKey ligMember1Key = new ProductKey(Constants.ITEMLEVELID, ligMember1);
	ProductKey ligMember2Key = new ProductKey(Constants.ITEMLEVELID, ligMember2);
	ProductKey nonLig1Key = new ProductKey(Constants.ITEMLEVELID, nonLig1);
	ProductKey nonLig2Key = new ProductKey(Constants.ITEMLEVELID, nonLig2);

	int startCalendarId = 3456, endCalendarId = 3459;
	Integer compStrId = 967;
	Long LAST_X_WEEKS_MOV_1 = 4587l;
	Integer COST_NO_CHANGE = 0;
	Integer COST_INCREASED = 1;
	String recommendationWeekStartDate = "01/08/2017";
	String recRunningWeekStartDate = "01/01/2017";
	String recWeekStartDate = "12/23/2018", recWeekEndDate = "12/29/2018", curWeekStartDate = "12/16/2018",
			curWeekEndDate = "12/22/2018";
	char LIG_CONSTRAINT_SAME = 'S';
	String storesInZone = "5712,5713,5716,5717,5718,5719,5720";
	boolean usePrediction = true;

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

	ObjectMapper mapper = new ObjectMapper();

	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClientsPIReverse.properties");

		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId,
				recWeekStartDate, recWeekEndDate, isRecommendAtStoreLevel, -1, -1, -1);
		recommendationRunHeader = TestHelper.getRecommendationRunHeader(recWeekStartDate);
		productGroupProperties = TestHelper.addProductGroupProperty(productGroupProperties, productLevelId, productId,
				usePrediction);
		curCalDTO = TestHelper.getCalendarDetails(curWeekStartDate, curWeekEndDate);
		priceZoneStores = Stream.of(storesInZone.split(",")).map(Integer::parseInt).collect(Collectors.toList());
		recommendationRuleMap = TestHelper.getRecommendationRuleAllEnabled(); // recommendationRuleMap =
		TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);

		// Set LIG Data
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember1, ligMember1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember2, ligMember2);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember3, ligMember3);

		// Set LIG Constraint
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig1,
				LIG_CONSTRAINT_SAME);

		// Mockup
		predictionServiceMock = EasyMock.createMock(PredictionService.class);
	}

	/** TestCases created in 06/2022 **/

	// Initilaze method
	public void Initialize(int locationLevelId, int locationId, int productLevelId, int productId, int strategyId,
			String recWeekStartDate, String recWeekEndDate, boolean isRecommendAtStoreLevel, String curWeekStartDate,
			String curWeekEndDate, String storesInZone, int lig1, int ligMember1, int ligMember2, int ligMember3,
			int ligMember4, Integer compStrId) {

		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");

		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId,
				recWeekStartDate, recWeekEndDate, isRecommendAtStoreLevel, -1, -1, -1);
		recommendationRunHeader = TestHelper.getRecommendationRunHeader(recWeekStartDate);
		productGroupProperties = TestHelper.addProductGroupProperty(productGroupProperties, productLevelId, productId,
				usePrediction);
		curCalDTO = TestHelper.getCalendarDetails(curWeekStartDate, curWeekEndDate);
		priceZoneStores = Stream.of(storesInZone.split(",")).map(Integer::parseInt).collect(Collectors.toList());
		recommendationRuleMap = TestHelper.getRecommendationRuleAZ();
		// recommendationRuleMap = TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);

		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);

		// Set LIG Data
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember1, ligMember1);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember2, ligMember2);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember3, ligMember3);
		retLirMap = TestHelper.setRetLirMap(retLirMap, lig1, ligMember4, ligMember4);

		// Set LIG Constraint
		retLirConstraintMap = TestHelper.setRetLirConstraintMap(retLirConstraintMap, locationId, lig1,
				LIG_CONSTRAINT_SAME);
		// Mockup
		predictionServiceMock = EasyMock.createMock(PredictionService.class);
	}

	/**
	 * Issue during GE testing Final rounding is incorrect non lig vs non lig
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */

	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {

		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1,
				-1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());
		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId,
				1.78, strategy, LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId,
				ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// Set price group
		TestHelper.setPriceGroup(nonLig1Item, 29, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(nonLig2, 2.99);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(nonLig1Item, nonLig2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99),
				PredictionStatus.SUCCESS, 100d, null, null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.getRecommendationRuleAllDisabled();

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
				retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties,
				predictionComponent, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap, saleDetails, adDetails,
				priceZoneStores, recWeekStartDate, predictionServiceMock, false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream()
				.collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 1.99));

	}

	// Test the latest recommendations by using the objective use guidelines and
	// constraints only
	// current price should be reatined if its present in rounding digits(item has
	// multiple price points)

	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {

		int locationLevelId = 6, locationId = 9;
		int nonLig1 = 1100;

		Long X_WEEKS_MOVEMENT_ITEM1 = (long) 28;

		HashMap<Integer, Double> compMap = new HashMap<>();

		HashMap<Integer, String> compCheckDates = new HashMap<>();

		String recWeekStartDate = "06/12/2020";

		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2022", "01/28/2023", false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 20, 75);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 95, 95);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableALL_99_BATT());
		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN,
				PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM, 180);

		for (Map.Entry<Integer, Double> comp : compMap.entrySet()) {
			TestHelper.setMultiCompDetailGuideline(strategy, comp.getKey(), 'D', Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, null, false);
		}

		TestHelper.setCostConstraint(strategy, false);
		TestHelper.setMinMaxConstraint(strategy, 289.99, Constants.DEFAULT_NA, 1);

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.setTestItem(nonLig1, 1, 465.99, null, 279.96, 279.96, COST_NO_CHANGE,
				compMap, strategy, X_WEEKS_MOVEMENT_ITEM1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, null,
				locationId, ligMember1UPC, compCheckDates);

		nonLig1Item.setMinRetail(465.99);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		ProductKey nonLig1Key = new ProductKey(Constants.ITEMLEVELID, nonLig1);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 289.99),
				PredictionStatus.SUCCESS, 100d, null, null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.getRecommendationRuleAllDisabled();

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
				retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties,
				predictionComponent, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap, saleDetails, adDetails,
				priceZoneStores, recWeekStartDate, predictionServiceMock, false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream()
				.collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 465.99));

	}

	// Test the latest recommendations by using the objective use guidelines and
	// constraints only
	// current price should be retained if its present in rounding digits
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {

		int locationLevelId = 6, locationId = 4;
		int nonLig1 = 1100;

		Long X_WEEKS_MOVEMENT_ITEM1 = (long) 11;

		Integer compStrId1 = 967, compStrId2 = 968;
		Double CompPrice1 = 289.99, CompPrice2 = 359.99;

		HashMap<Integer, Double> compMap = new HashMap<>();
		compMap.put(compStrId1, CompPrice1);
		compMap.put(compStrId2, CompPrice2);

		HashMap<Integer, String> compCheckDates = new HashMap<>();
		compCheckDates.put(compStrId1, "05/10/2022");
		compCheckDates.put(compStrId2, "06/01/2022");

		String recWeekStartDate = "06/12/2020";

		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2022", "01/28/2023", false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 15, Constants.DEFAULT_NA);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 95, 95);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableALL_99_BATT());
		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, 180);

		for (Map.Entry<Integer, Double> comp : compMap.entrySet()) {
			TestHelper.setMultiCompDetailGuideline(strategy, comp.getKey(), 'D', Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, null, false);
		}

		TestHelper.setCostConstraint(strategy, false);
		TestHelper.setMinMaxConstraint(strategy, 289.99, Constants.DEFAULT_NA, 1);

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.setTestItem(nonLig1, 1, 289.99, null, 176.51, 175.79, COST_INCREASED,
				compMap, strategy, X_WEEKS_MOVEMENT_ITEM1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, null,
				locationId, ligMember1UPC, compCheckDates);

		nonLig1Item.setMinRetail(289.99);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		ProductKey nonLig1Key = new ProductKey(Constants.ITEMLEVELID, nonLig1);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 289.99),
				PredictionStatus.SUCCESS, 100d, null, null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.getRecommendationRuleAllDisabled();

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
				retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties,
				predictionComponent, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap, saleDetails, adDetails,
				priceZoneStores, recWeekStartDate, predictionServiceMock, false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream()
				.collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 289.99));

	}

	// When an item is having pending retail then pending retail should be
	// recommended
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {

		int locationLevelId = 6, locationId = 4;
		int nonLig1 = 1100;

		Long X_WEEKS_MOVEMENT_ITEM1 = (long) 11;

		Integer compStrId1 = 967, compStrId2 = 968;
		Double CompPrice1 = 289.99, CompPrice2 = 359.99;

		HashMap<Integer, Double> compMap = new HashMap<>();
		compMap.put(compStrId1, CompPrice1);
		compMap.put(compStrId2, CompPrice2);

		HashMap<Integer, String> compCheckDates = new HashMap<>();
		compCheckDates.put(compStrId1, "05/10/2022");
		compCheckDates.put(compStrId2, "06/01/2022");

		String recWeekStartDate = "06/12/2020";

		List<PRItemDTO> allItems = null;
		HashMap<ProductKey, PRItemDTO> itemMap = new HashMap<ProductKey, PRItemDTO>();
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 7, 264, "01/08/2022", "01/28/2023", false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 15, Constants.DEFAULT_NA);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 95, 95);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableALL_99_BATT());
		TestHelper.setMultiCompGuideline(strategy, PRConstants.GROUP_PRICE_TYPE_MIN,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, 180);

		for (Map.Entry<Integer, Double> comp : compMap.entrySet()) {
			TestHelper.setMultiCompDetailGuideline(strategy, comp.getKey(), 'D', Constants.DEFAULT_NA,
					Constants.DEFAULT_NA, null, false);
		}

		TestHelper.setCostConstraint(strategy, false);
		TestHelper.setMinMaxConstraint(strategy, 289.99, Constants.DEFAULT_NA, 1);

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.setTestItem(nonLig1, 1, 289.99, null, 176.51, 175.79, COST_INCREASED,
				compMap, strategy, X_WEEKS_MOVEMENT_ITEM1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, null,
				locationId, ligMember1UPC, compCheckDates);

		nonLig1Item.setMinRetail(289.99);
		nonLig1Item.setPendingRetail(new MultiplePrice(1, 301.99));

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		ProductKey nonLig1Key = new ProductKey(Constants.ITEMLEVELID, nonLig1);

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 289.99),
				PredictionStatus.SUCCESS, 100d, null, null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.getRecommendationRuleAllDisabled();

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
				retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties,
				predictionComponent, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap, saleDetails, adDetails,
				priceZoneStores, recWeekStartDate, predictionServiceMock, false);

		itemMap = (HashMap<ProductKey, PRItemDTO>) allItems.stream()
				.collect(Collectors.toMap(PRItemDTO::getProductKey, Function.identity()));

		// Compare prices
		assertEquals("Mismatch", itemMap.get(nonLig1Key).getRecommendedRegPrice(), new MultiplePrice(1, 301.99));

	}

	// When 1 LIG member is having pending retail then pending retail should be
	// recommended for that item
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {

		int locationLevelId = 6, locationId = 4, productLevelId = 4, productId = 5768, strategyId = 1;

		int lig1 = 3000, ligMember1 = 1000, ligMember2 = 1001, ligMember3 = 1002, ligMember4 = 1003;

		String lig1Member1UPC = "010101010", lig1Member2UPC = "010101011", lig1Member3UPC = "010101012",
				lig1Member4UPC = "010101013";

		final Integer COST_UNCHANGED = 0;

		List<PRItemDTO> allItems = null;
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		boolean isRecommendAtStoreLevel = false;

		Integer compStrId1 = 967, compStrId2 = 968;
		Double CompPrice1 = 289.99, CompPrice2 = 359.99;

		HashMap<Integer, Double> compMap = new HashMap<>();
		compMap.put(compStrId1, CompPrice1);
		compMap.put(compStrId2, CompPrice2);

		HashMap<Integer, String> compCheckDates = new HashMap<>();
		compCheckDates.put(compStrId1, "05/10/2022");
		compCheckDates.put(compStrId2, "06/01/2022");

		String storesInZone = "5712,5713,5716,5717,5718,5719,5720";

		String priceEffdate = "05/15/2022";

		String recWeekStartDate = "06/12/2020", recWeekEndDate = "08/28/2022", curWeekStartDate = "06/05/2022",
				curWeekEndDate = "06/11/2022";

		Initialize(locationLevelId, locationId, productLevelId, productId, strategyId, recWeekStartDate, recWeekEndDate,
				isRecommendAtStoreLevel, curWeekStartDate, curWeekEndDate, storesInZone, lig1, ligMember1, ligMember2,
				ligMember3, ligMember4, compStrId1);

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 9, 7, 2610, "01/08/2022", "01/28/2023", false, -1, -1,
				-1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		TestHelper.setMarginGuideline(strategy, PRConstants.VALUE_TYPE_PCT, 20, 90);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 95, 95);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTablePROD_49_99_ROUNDING_DEF());
		TestHelper.setLigConstraint(strategy, PRConstants.LIG_GUIDELINE_SAME);
		TestHelper.setCostConstraint(strategy, false);

		PRItemDTO lig1Member1Item = TestHelper.getTestItem2(ligMember1, 1, 159.99, null, 59.55, 59.55, COST_UNCHANGED,
				compStrId, 2.56, strategy, 7730, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId,
				lig1Member1UPC);
		lig1Member1Item.setShipperItem(false);
		lig1Member1Item.setAvgMovement(645);
		lig1Member1Item.setXweekMovForLIGRepItem(7730);
		lig1Member1Item.setxWeeksMovForTotimpact(7730);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member1Item);
		PRItemDTO lig1Member2Item = TestHelper.getTestItem2(ligMember2, 1, 159.99, null, 59.55, 59.55, COST_UNCHANGED,
				compStrId, 2.56, strategy, 1134, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId,
				lig1Member2UPC);
		lig1Member2Item.setShipperItem(false);
		lig1Member2Item.setAvgMovement(94.5);
		lig1Member2Item.setXweekMovForLIGRepItem(1134);
		lig1Member2Item.setxWeeksMovForTotimpact(1134);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member2Item);

		PRItemDTO lig1Member3Item = TestHelper.getTestItem2(ligMember3, 1, 159.99, null, 59.55, 59.55, COST_UNCHANGED,
				compStrId, 2.56, strategy, 822, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId,
				lig1Member3UPC);
		lig1Member3Item.setShipperItem(false);
		lig1Member3Item.setAvgMovement(68.5);
		lig1Member3Item.setXweekMovForLIGRepItem(822);
		lig1Member3Item.setxWeeksMovForTotimpact(822);
		lig1Member3Item.setPendingRetail(new MultiplePrice(1, 301.99));

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member3Item);

		PRItemDTO lig1Member4Item = TestHelper.getTestItem2(ligMember4, 1, 159.99, null, 59.55, 59.55, COST_UNCHANGED,
				compStrId, 2.56, strategy, 837, 0, 0d, 0d, "", lig1, false, 0, 0, 0, 0, 0, 0d, 0d, locationId,
				lig1Member4UPC);
		lig1Member4Item.setShipperItem(false);
		lig1Member4Item.setAvgMovement(69.75);
		lig1Member4Item.setXweekMovForLIGRepItem(837);
		lig1Member4Item.setxWeeksMovForTotimpact(837);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, lig1Member4Item);

		PRItemDTO ligItem = TestHelper.setLIGItemDTO(lig1, 10523, locationId, strategy);
		itemDataMap = TestHelper.setLigItemDataMap(itemDataMap, ligItem);

		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember1, curWeekStartDate, 1, 159.99, 0d, priceEffdate, 0,
				0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember2, curWeekStartDate, 1, 159.99, 0d, priceEffdate, 0,
				0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember3, curWeekStartDate, 1, 159.99, 0d, priceEffdate, 0,
				0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, ligMember4, curWeekStartDate, 1, 159.99, 0d, priceEffdate, 0,
				0, 0, "");

		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper
				.getRecommendationRuleAZ();

		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember1, lig1Member1UPC, new MultiplePrice(1, 154.99),
				PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 155.99), PredictionStatus.SUCCESS, 150d,
				new MultiplePrice(1, 156.99), PredictionStatus.SUCCESS, 120d, new MultiplePrice(1, 157.99),
				PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember2, lig1Member2UPC, new MultiplePrice(1, 154.99),
				PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 155.99), PredictionStatus.SUCCESS, 150d,
				new MultiplePrice(1, 156.99), PredictionStatus.SUCCESS, 120d, new MultiplePrice(1, 157.99),
				PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember3, lig1Member3UPC, new MultiplePrice(1, 154.99),
				PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 155.99), PredictionStatus.SUCCESS, 150d,
				new MultiplePrice(1, 156.99), PredictionStatus.SUCCESS, 120d, new MultiplePrice(1, 157.99),
				PredictionStatus.SUCCESS, 110d));
		predictionItems.add(TestHelper.setPredictionItemDTO(ligMember4, lig1Member4UPC, new MultiplePrice(1, 154.99),
				PredictionStatus.SUCCESS, 100d, new MultiplePrice(1, 155.99), PredictionStatus.SUCCESS, 150d,
				new MultiplePrice(1, 156.99), PredictionStatus.SUCCESS, 120d, new MultiplePrice(1, 157.99),
				PredictionStatus.SUCCESS, 110d));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId,
				endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock)
				.anyTimes();
		EasyMock.replay(predictionServiceMock);

		// Recommend price
		allItems = new PricingEngineWS().recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
				retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties,
				predictionComponent, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap, saleDetails, adDetails,
				priceZoneStores, recWeekStartDate, predictionServiceMock, false);

		for (PRItemDTO item : allItems) {
			// Set expected log
			PRExplainLog explainLog = new PRExplainLog();
			PRGuidelineAndConstraintLog guidelineAndConstraintLog;
			if (item.getItemCode() == ligMember1 || item.getItemCode() == ligMember2
					|| item.getItemCode() == ligMember4) {
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 74.44, 595.5, 74.44, 595.5,
						"");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 8, 311.98,
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, 74.44, 311.98, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 59.55,
						Constants.DEFAULT_NA, 74.44, 311.98, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 154.99, 155.99, 156.99, 157.99, 158.99, 159.99, 161.99, 162.99, 163.99,
						164.99, 165.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, false, Arrays.asList(roundingDigits));
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(5));
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.PRICE_POINTS_FILTERED,
						additionalDetails);
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog),
						mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 159.99), item.getRecommendedRegPrice());
			}
			else if(item.getItemCode() == ligMember3)
			{
				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 74.44, 595.5, 74.44, 595.5,
						"");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 8, 311.98,
						Constants.DEFAULT_NA, Constants.DEFAULT_NA, 74.44, 311.98, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				TestHelper.setCostConstraintLog(explainLog, guidelineAndConstraintLog, true, false, 59.55,
						Constants.DEFAULT_NA, 74.44, 311.98, "");

				guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
				Double[] roundingDigits = { 154.99, 155.99, 156.99, 157.99, 158.99, 159.99, 161.99, 162.99, 163.99,
						164.99, 165.99 };
				TestHelper.setRoundingLog(explainLog, guidelineAndConstraintLog, true, Arrays.asList(roundingDigits));
				
				TestHelper.setAdditionalLog(explainLog, ExplainRetailNoteTypeLookup.PENDING_RETAIL_RECOMMENDED,
						new ArrayList<String>());
				assertEquals("JSON Not Matching", mapper.writeValueAsString(explainLog),
						mapper.writeValueAsString(item.getExplainLog()));
				assertEquals("Mimatch", new MultiplePrice(1, 301.99), item.getRecommendedRegPrice());
			}

			if (item.isLir()) {
				assertEquals("Mimatch", ligMember1, item.getLigRepItemCode());
			}

		}
	}

}
