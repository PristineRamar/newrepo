package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemRecErrorService;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.service.offermgmt.mwr.core.MultiWeekConverter;
import com.pristine.service.offermgmt.mwr.core.finalizeprice.MultiWeekPriceFinalizer;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.test.offermgmt.TestHelper;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class MultiWeekRecAutomatedJUnitTest {
	

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
	List<PredictionItemDTO> predictionItems;
	
	int locationLevelId = 6, locationId = 6, productLevelId = 4, productId = 5768, strategyId = 1;

	int lig1 = 3000, ligMember1 = 1000, ligMember2 = 1001, ligMember3 = 1002, ligMember4 = 1003;
	int nonLig1 = 1100, nonLig2 = 2200;
	String ligMember1UPC = "010101010", ligMember2UPC = "010101011", ligMember3UPC = "010101012", ligMember4UPC = "010101013";
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
	String recommendationWeekStartDate = "08/05/2018";
	String recRunningWeekStartDate = "07/22/2018";
	String recWeekStartDate = "08/05/2018", recWeekEndDate = "10/21/2018", curWeekStartDate = "07/22/2018", curWeekEndDate = "07/28/2018";
	char LIG_CONSTRAINT_SAME = 'S';
	String storesInZone = "5712,5713,5716,5717,5718,5719,5720";
	boolean usePrediction = true;
	
	String week1StartDate = "08/05/2018";
	
	String curPriceEffDate1 = "11/04/2018";
	HashMap<String, RetailCalendarDTO> allWeekDetails = new HashMap<>();
	RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();
	PricingEngineWS recommendationService = new PricingEngineWS(); 
	private static Logger logger = Logger.getLogger("MultiWeekRecAutomatedJUnitTest");

	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClientsPIReverse.properties");

		// Common Data
		inputDTO = TestHelper.getStrategy(strategyId, locationLevelId, locationId, productLevelId, productId, recWeekStartDate, recWeekEndDate,
				isRecommendAtStoreLevel, -1, -1, -1);
		
		allWeekDetails.put("08/05/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("08/12/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("08/19/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("08/26/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("09/02/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("09/09/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("09/16/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("09/23/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("09/30/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("10/07/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("10/14/2018", RecommendationInputHelper.getCalendarId(123));
		allWeekDetails.put("10/21/2018", RecommendationInputHelper.getCalendarId(123));
		
		recommendationInputDTO = RecommendationInputHelper.getRecommendationInputDTO("08/05/2018", "10/21/2018", "", "", PRConstants.MW_X_WEEKS_RECOMMENDATION, 2);
		RecommendationInputHelper.setRecommendationInputDTO(recommendationInputDTO, locationLevelId, locationId, productLevelId, productId, 50, 50, 0, 0, "08/05/2018");
		
		recommendationInputDTO.setBaseWeek("07/29/2018");
		
		recommendationRunHeader = TestHelper.getRecommendationRunHeader(recWeekStartDate);
		productGroupProperties = TestHelper.addProductGroupProperty(productGroupProperties, productLevelId, productId, usePrediction);
		curCalDTO = TestHelper.getCalendarDetails(curWeekStartDate, curWeekEndDate);
		priceZoneStores = Stream.of(storesInZone.split(",")).map(Integer::parseInt).collect(Collectors.toList());
		recommendationRuleMap = TestHelper.getRecommendationRuleAllEnabled();
		// recommendationRuleMap = TestHelper.getRecommendationRule();
		predictionServiceMock = new PredictionService(movementData, itemPriceHistory, retLirMap);
		TestHelper.setCompId(compIdMap, PRConstants.COMP_TYPE_1, Constants.STORE_LEVEL_ID, compStrId);
		
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

	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 13 weeks. 
	 * This item doesn't have any relation, cost change or comp change
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase1......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		
		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}


	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 4 weeks. 
	 * This item doesn't have any relation, cost change or comp change. 
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase3......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// Set price group
		TestHelper.setPriceGroup(nonLig1Item, 29, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getTestItem2(nonLig2, 1, 2.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		
		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 4 weeks. 
	 * This item doesn't have any relation, there is change in current cost, no comp change
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase3......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_INCREASED, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// Set price group
		TestHelper.setPriceGroup(nonLig1Item, 29, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getTestItem2(nonLig2, 1, 2.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		
		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 4 weeks. 
	 * This item doesn't have any relation, no cost change, there is change in comp price
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase4......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_INCREASED, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		
		// Comp Change
		nonLig1Item.setCompPriceChgIndicator(1);
		
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// Set price group
		TestHelper.setPriceGroup(nonLig1Item, 29, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getTestItem2(nonLig2, 1, 2.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		
		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
/*		MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 4 weeks. 
	 * This item is in brand/size relation, no cost change, no comp price. 
	 * None of the price points are satisfying the brand/size relation
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase5......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_INCREASED, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		
		// Comp Change
		nonLig1Item.setCompPriceChgIndicator(1);
		
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// Set price group
		TestHelper.setPriceGroup(nonLig1Item, 29, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getTestItem2(nonLig2, 1, 2.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		
		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.89), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));
		
		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 4 weeks. 
	 * This item is in brand/size relation, no cost change, no comp price & item is in on-going promotion. 
	 * There is 2 new price points after applying the guidelines & constraints
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase6() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase6......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.58, COST_INCREASED, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 1, 0.99, 0d, "07/22/2018", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		
		
		nonLig1Item.getCurSaleInfo().setSaleWeekStartDate("09/01/2018");
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// Set price group
		TestHelper.setPriceGroup(nonLig1Item, 29, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getTestItem2(nonLig2, 1, 2.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);
		
		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 13 weeks. 
	 * This item has no relation, no cost change, no comp price & item is in near future promotion. 
	 * There is 2 new price points after applying the guidelines & constraints
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase7() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase7......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 1, 0.99, 0d, "08/26/2018", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		
		nonLig1Item.getCurSaleInfo().setSaleWeekStartDate("09/01/2018");
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 13 weeks. 
	 * This item has no relation, no cost change, no comp price & item is in far future promotion. 
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase8() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase8......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.65, COST_NO_CHANGE, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 1, 0.99, 0d, "10/14/2018", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		
		
		nonLig1Item.getCurSaleInfo().setSaleWeekStartDate("10/20/2018");
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
	
	/**
	 * 
	 * Testing the recommended price of an item whose last price was changed before 13 weeks. 
	 * This item has no relation, cost change and no comp change
	 * 
	 * 
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase9() throws GeneralException, Exception, OfferManagementException {
		logger.debug("testCase9......");
		itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		predictionItems = new ArrayList<PredictionItemDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2018", "", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setPIGuideline(strategy, 0, 135);
		TestHelper.setThreasholdConstraint(strategy, PRConstants.VALUE_TYPE_PCT, 4, 5);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTableGE());

		// Set Item
		PRItemDTO nonLig1Item = TestHelper.getTestItem2(nonLig1, 1, 1.99, null, 1.65, 1.58, COST_INCREASED, compStrId, 1.78, strategy,
				LAST_X_WEEKS_MOV_1, 0, 0d, 0d, "", 0, false, 0, 0, 0, 0, 0, 0d, 0d, locationId, ligMember1UPC);
		
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, nonLig1Item);

		// update error codes
		new ItemRecErrorService().setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
				
		// Mock up Predictions
		predictionItems.add(TestHelper.setPredictionItemDTO(nonLig1, nonLig1UPC, new MultiplePrice(1, 1.99), PredictionStatus.SUCCESS, 100d, null,
				null, null, null, null, null, null, null, null));

		PredictionOutputDTO predictionOutputDTOMock = new PredictionOutputDTO();
		TestHelper.setPredictionOutputDTO(predictionOutputDTOMock, locationLevelId, locationId, startCalendarId, endCalendarId, predictionItems);

		EasyMock.expect(predictionServiceMock.predictMovement(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
				EasyMock.anyBoolean())).andReturn(predictionOutputDTOMock).anyTimes();
		EasyMock.replay(predictionServiceMock);
		HashMap<String, List<RecommendationRuleMapDTO>> tempRecommendationRuleMap = TestHelper.getRecommendationRuleAllDisabled();

		// Recommend price
		recommendationService.recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap, retLirConstraintMap,
				compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent, leadZoneDetails,
				isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData, tempRecommendationRuleMap,
				saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionServiceMock,false);

		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekMap = new MultiWeekConverter()
				.convertToMultiWeekItemDataMap(recommendationInputDTO, itemDataMap, allWeekDetails,
						itemZonePriceHistory, tempRecommendationRuleMap, movementData, new HashMap<>(), saleDetails,
						adDetails, new HashMap<>(), new HashMap<>(), new ArrayList<>(), false,null, null);
		
		
		BaseData baseData = new BaseData();
		baseData.setItemDataMap(itemDataMap);
		baseData.setWeeklyItemDataMap(multiWeekMap);
		baseData.setPriceHistory(itemZonePriceHistory);
		baseData.setRecommendationRuleMap(tempRecommendationRuleMap);
		
		/*MultiWeekPriceFinalizer multiWeekPriceFinalizer = new MultiWeekPriceFinalizer(baseData);
		
		multiWeekPriceFinalizer.applyRecommendationToAllWeeks(recommendationInputDTO);*/
		
		multiWeekMap.forEach((k,v) -> {
			v.forEach((ik, iv) -> {
				if(ik.equals(nonLig1Key))
					assertEquals("Mistmatch", iv.getRecommendedRegPrice(), itemDataMap.get(nonLig1Key).getRecommendedRegPrice());
			});
		});
	}
	
}
