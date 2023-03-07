package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.LocationCompetitorMapDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.CriteriaDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.PRConstraintGuardRailDetail;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.StrategyWhatIfService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.PriceTestUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * BaseDataService is responsible for getting all required base data and setting
 * it in BaseData class
 * 
 * @author Pradeepkumar
 *
 */

public class BaseDataService {

	private static Logger logger = Logger.getLogger("BaseDataService");
	private boolean useCommonAPIForPrice = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_PRICE", "FALSE"));

	private boolean useCommonAPIForCost = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_COST", "FALSE"));

	private boolean useCommonAPIForPrmotion = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_PROMOTION", "FALSE"));

	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));

	/**
	 * Sets up all the required data in BaseData class
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws OfferManagementException
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void setupBaseData(Connection conn, RecommendationInputDTO recommendationInputDTO, BaseData baseData,
			CommonDataHelper commonDataHelper) throws GeneralException, OfferManagementException, Exception {

		GenaralService genaralService = new GenaralService();
		AuthorizedItemService authorizedItemService = new AuthorizedItemService();
		PriceCheckListService priceCheckListService = new PriceCheckListService();
		StrategyService strategyService = new com.pristine.service.offermgmt.mwr.basedata.StrategyService();
		PriceGroupDataService priceGroupDataService = new PriceGroupDataService();
		CompDataService compDataService = new CompDataService();
		PriceDataService priceDataService = new PriceDataService();
		CostDataService costDataService = new CostDataService();
		MovementDataService movementDataService = new MovementDataService();
		AdDataService adDataService = new AdDataService();
		PromoDataService promoDataService = new PromoDataService();
		DisplayDataService displayDataService = new DisplayDataService();

		HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceHistory = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> priceZoneHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costDataMap = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostMap = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();
		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap = new LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = new HashMap<Integer, List<PRItemDisplayInfoDTO>>();

		logger.info("setupBaseData() - Retrieving store list for given product and location...");
		// Step 1: Get List of stores for given product and location
		List<Integer> storeList = genaralService.getStoresForProductAndLoction(conn,
				recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
				recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
				recommendationInputDTO.isGlobalZone(), recommendationInputDTO.isPriceTestZone());

		logger.info("setupBaseData() - Stores fetched: " + storeList.size());

		// Get the zone with maximum stores in storelist for fetching price/cost/promo
		// infformation
		if (recommendationInputDTO.isPriceTestZone()) {
			PriceTestUtil priceTestUtil = new PriceTestUtil();
			String zone = priceTestUtil.getZoneId(conn, storeList);
			recommendationInputDTO.setTempLocationID(Integer.parseInt(zone.split(";")[0]));

		}
		logger.info("setupBaseData() - Retrieving authorized items...");
		// Step 2: Get authorized items
		List<PRItemDTO> authorizedItems = authorizedItemService.getAuthorizedItems(conn, recommendationInputDTO,
				storeList);

		logger.info("setupBaseData() - # of authorized items: " + authorizedItems.size());

		List<PRItemDTO> inputwithLessAStrRevenue = authorizedItems.stream().filter(m -> !m.isSendToPrediction())
				.collect(Collectors.toList());
		logger.info("no of items not to pass to prediction " + inputwithLessAStrRevenue.size());

		logger.info("setupBaseData() - Retrieving price check lists for items...");
		// Step 3: Get price check list info by item and LIG
		HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo = priceCheckListService.getPriceCheckLists(conn,
				recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
				recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
				recommendationInputDTO.isPriceTestZone(), recommendationInputDTO.getStartWeek());

		logger.info("setupBaseData() - Price check list map size: " + priceCheckListInfo.size());

		logger.info("setupBaseData() - Retrieving item data map...");
		// Step 4: Transform authorized items list to item data map
		HashMap<ItemKey, PRItemDTO> itemDataMap = genaralService.getItemDataMap(conn, recommendationInputDTO.getRunId(),
				recommendationInputDTO, authorizedItems);

		logger.info("setupBaseData() - Size of item data map: " + itemDataMap.size());

		logger.info("setupBaseData() - Retrieving all active strategies...");
		// Step 5: Get all active strategies
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = strategyService.getAllActiveStrategies(conn,
				recommendationInputDTO);

		logger.info("setupBaseData() - # of Strategies: " + strategyMap.size());

		// Added for GE to check if the guardrail cosntarint is defined for ZoneId
		Set<Integer> uniqueZoneIDs = new HashSet<Integer>();

		for (Entry<StrategyKey, List<PRStrategyDTO>> stEntry : strategyMap.entrySet()) {

			List<PRStrategyDTO> strDetails = stEntry.getValue();
			for (PRStrategyDTO strategyDTO : strDetails) {
				if (strategyDTO.getConstriants().getGuardrailConstraint() != null) {
					List<PRConstraintGuardRailDetail> guardRaildetails = strategyDTO.getConstriants()
							.getGuardrailConstraint().getCompetitorDetails();

					if (guardRaildetails != null) {
						Set<Integer> uniqueZones = guardRaildetails.stream()
								.map(PRConstraintGuardRailDetail::getPriceZoneID).collect(Collectors.toSet());
						if (uniqueZones.size() > 0) {
							for (Integer zoneId : uniqueZones) {
								if (zoneId != 0)
									uniqueZoneIDs.add(zoneId);
							}
						}
					}

				}
			}

		}
		logger.info("setupBaseData() - # of guardrailConstraint for zoneID : " + uniqueZoneIDs.size());
		if (uniqueZoneIDs.size() > 0) {
			for (Integer zone : uniqueZoneIDs) {
				logger.info("setupBaseData() - Unique  zoneID : " + zone);
			}
		}
		// if uniqueZoneIDs size is populated then get the latest reccs for these
		// zoneIDs;
		if (uniqueZoneIDs.size() > 0) {

			setLatestRecsForZones(itemDataMap, uniqueZoneIDs, genaralService, recommendationInputDTO, conn);
		}

		// Triggered during Strategy what-if. Give preference given to the strategies
		// defined
		// in the strategy what-if
		if (recommendationInputDTO.getRunType() == PRConstants.RUN_TYPE_TEMP) {
			StrategyWhatIfService whatIfService = new StrategyWhatIfService();
			// Get the latest temp strategy if there are multiple strategies present for the
			// same product/location
			List<Long> strategyIds = strategyService.getAllActiveTempStrategies(conn, recommendationInputDTO);
			logger.info("Is runOnly TempStrategy :" + recommendationInputDTO.isRunOnlyTempStrats());
			if (recommendationInputDTO.isRunOnlyTempStrats()) {
				long globalStrategyId = strategyService.getGlobalStrategy(conn);
				if (globalStrategyId > 0) {
					strategyIds.add(globalStrategyId);
				}
			}

			List<PRStrategyDTO> whatIfStrategies = whatIfService.getWhatIfStrategies(conn, strategyIds);
			// If run only temp strategies is selected while WhatiF then consider only temp
			// strategies and not production strategy

			if (recommendationInputDTO.isRunOnlyTempStrats()) {
				strategyMap.clear();

				StrategyKey strategyKey = null;
				for (PRStrategyDTO whatIfStrategy : whatIfStrategies) {
					strategyKey = new StrategyKey(whatIfStrategy.getLocationLevelId(), whatIfStrategy.getLocationId(),
							whatIfStrategy.getProductLevelId(), whatIfStrategy.getProductId());
					// logger.info("Strategy key :" + strategyKey);
					List<PRStrategyDTO> newStrategies = new ArrayList<PRStrategyDTO>();
					if (strategyMap.containsKey(strategyKey)) {
						newStrategies = strategyMap.get(strategyKey);

					}
					newStrategies.add(whatIfStrategy);
					strategyMap.put(strategyKey, newStrategies);
				}

			} else {
				whatIfService.replaceStrategies(strategyMap, whatIfStrategies);
			}
			logger.info("setupBaseData()-Strategy WhatIf Map size: " + strategyMap.size());
		}

		logger.info("setupBaseData() - Retrieving product group info...");
		// Step 6: Get product group information
		HashMap<String, ArrayList<Integer>> productListMap = genaralService.getProductListForProducts(conn,
				recommendationInputDTO);

		logger.info("setupBaseData() - Product group map size: " + productListMap.size());

		logger.info("setupBaseData() - Retrieving product group relation map...");

		// Step 7: get product group relation
		HashMap<Integer, Integer> productParentChildRelationMap = genaralService.getProductLevelRelationMap(conn,
				recommendationInputDTO);

		logger.info("setupBaseData() - Product group relation map size: " + productParentChildRelationMap.size());

		logger.info("setupBaseData() - Retrieving price group info map...");

		// Step 8: Get Price group details
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupDetails = priceGroupDataService
				.getPriceGroupDetails(conn, recommendationInputDTO, itemDataMap);

		logger.info("setupBaseData() - Price group info map size: " + priceGroupDetails.size());

		logger.info("setupBaseData() - Retrieving competitor map...");
		// Step 9: get Competitor map
		HashMap<Integer, LocationKey> competitorMap = compDataService.getCompetitors(conn, strategyMap,
				recommendationInputDTO);

		logger.info("setupBaseData() - Competitor map size: " + competitorMap.size());

		logger.info("setupBaseData() - Retrieving price zones and strategy zones...");
		// Get price and strategy zones from authorization items
		List<String> priceAndStrategyZoneNos = genaralService.getPriceAndStrategyZoneNos(authorizedItems);

		logger.info("setupBaseData() - # of price and strategy zones: " + priceAndStrategyZoneNos.size());

		if (!useCommonAPIForPrice) {

			priceHistory = priceDataService.getPriceHistory(conn, itemDataMap,
					commonDataHelper.getAllWeekCalendarDetails(), storeList, authorizedItems, priceAndStrategyZoneNos,
					recommendationInputDTO);
			logger.info("setupBaseData() - Retrieving price history...");
			// Step 10: Get price data
			priceZoneHistory = priceDataService.getZonePriceHistory(conn, itemDataMap,
					commonDataHelper.getAllWeekCalendarDetails(), storeList, authorizedItems, priceAndStrategyZoneNos,
					recommendationInputDTO, priceHistory);
			logger.info("setupBaseData() - Price history size: " + priceHistory.size());

			logger.info("setupBaseData() - Retrieving latest price data...");
			// Step 11: Get Latest price data
			priceDataMap = priceDataService.getLatestPriceData(conn, itemDataMap, priceAndStrategyZoneNos, storeList,
					recommendationInputDTO);
//				logger.info("priceDataMap" + priceDataMap.toString()); 

			logger.info("setupBaseData() - Retrieving latest price data is completed.");
		}

		if (!useCommonAPIForCost) {
			logger.info("setupBaseData() - Retrieving cost data ...");
			// Step 12: Get Cost data
			costDataMap = costDataService.getCostData(conn, itemDataMap, storeList, priceAndStrategyZoneNos,
					commonDataHelper.getAllWeekCalendarDetails(), recommendationInputDTO);

			logger.info("setupBaseData() - Cost data map size: " + costDataMap.size());
			if (chkFutureCost) {
				logger.info("setupBaseData() - Retrieving future cost...");
				futureCostMap = costDataService.getFutureCostData(conn, itemDataMap, storeList, priceAndStrategyZoneNos,
						commonDataHelper.getAllWeekCalendarDetails(), recommendationInputDTO);
				logger.info("setupBaseData() -Future cost data map size: " + futureCostMap.size());
			}

		}

		logger.info("setupBaseData() - Retrieving movement data map...");
		// Step 13: Get Movement data
		HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementDataMap = movementDataService.getMovementData(
				conn, itemDataMap, storeList, commonDataHelper.getPreviousCalendarDetails(), recommendationInputDTO);

		logger.info("setupBaseData() - Movement data map size: " + movementDataMap.size());

		logger.info("setupBaseData() - Retrieving Last X Weeks movement...");
		// Step 14: Get Last X weeks movement to retain current price,
		HashMap<ProductKey, Long> lastXWeeksMov = movementDataService.getLastXWeeksMovement(conn, itemDataMap,
				storeList, recommendationInputDTO);

		logger.info("setupBaseData() - Retrieving Last X Weeks movement is completed.");

		logger.info("setupBaseData() - Retrieving latest comp price data...");
		// Step 15: Get Comp Price data
		HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompDataMap = compDataService
				.getLatestCompPriceData(conn, itemDataMap, competitorMap, recommendationInputDTO);

		logger.info("setupBaseData() - Retrieving latest comp price data size: " + latestCompDataMap.size());

		logger.info("setupBaseData() - Retrieving previous comp price data...");
		// Step 16: Get Comp Price data
		HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompDataMap = compDataService
				.getPreviousCompPriceData(conn, itemDataMap, competitorMap, recommendationInputDTO);

		logger.info("setupBaseData() - Retrieving latest comp price data size: " + previousCompDataMap.size());

		if (!useCommonAPIForPrmotion) {
			logger.info("setupBaseData() - Retrieving sale data map...");
			// Step 17: Get Sale details
			saleDetails = promoDataService.getSaleDetails(conn, storeList, recommendationInputDTO);

			logger.info("setupBaseData() - Sale data map size: " + saleDetails.size());

			logger.info("setupBaseData() - Retrieving ad data map...");
			// Step 18: Get Ad details
			adDetails = adDataService.getAdDetails(conn, storeList, recommendationInputDTO);

			logger.info("setupBaseData() - Ad data map size: " + adDetails.size());

			logger.info("setupBaseData() - Retrieving display data map...");

			// Step 19: Get Display details
			displayDetails = displayDataService.getDisplayDetails(conn, storeList, recommendationInputDTO);

			logger.info("setupBaseData() - Diplay data map size: " + displayDetails.size());
		}

		// Step 20: Get location lists for given input location
		logger.info("setupBaseData() - Retrieving locationList  ..");
		ArrayList<Integer> locationList = genaralService.getLocationListId(conn, recommendationInputDTO);

		// Step 21: Get latest price for multi comp
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<>();
		logger.info("setupBaseData() - Retrieving multi Competitor Prices  ..");
		compDataService.getMultiCompLatestPriceMap(conn, strategyMap, itemDataMap, recommendationInputDTO);

		// Step 22: Get recommendation rules
		logger.info("setupBaseData() - Retrieving recommendationRuleMap  ..");
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = genaralService
				.getRecommendationRules(conn);

		// Step 23: Get parent rec info
		logger.info("setupBaseData() - Retrieving parentRecInfo  ..");
		HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> parentRecInfo = genaralService.getParentRecommendation(conn,
				recommendationInputDTO.getMwrRunHeader().getParentRunId());

		// Step 24: Get prediction cache
		logger.info("setupBaseData() - Retrieving prediction cache");
		List<MultiWeekPredEngineItemDTO> predictionCache = genaralService.getPredictionCache(conn,
				recommendationInputDTO.getMwrRunHeader());

		// Get product group properties
		logger.info("setupBaseData() - Retrieving productGroupProperties..");
		List<PRProductGroupProperty> productGroupProperties = genaralService.getProductProperties(conn,
				recommendationInputDTO);

		logger.info("setupBaseData() - Retrieving criteriaStrategyDetails..");
		// Step 25: Get criteria used in strategy
		HashMap<Integer, List<CriteriaDTO>> criteriaStrategyDetails = strategyService
				.getCriteriaDetailsFromStrategy(conn, strategyMap);

		HashMap<Integer, Integer> compSettings = new LocationCompetitorMapDAO().getMultiCompSetting(conn);

		// Changes done by Bhargavi on 01/05/2021
		// update the MarkUp and MarkDown values for Rite Aid
		HashMap<Integer, Integer> storeInventory = new HashMap<Integer, Integer>();
		String impactFactor = PropertyManager.getProperty("IMPACT_CALCULATION_FACTOR", Constants.X_WEEKS_MOV);
		if (Constants.STORE_INVENTORY.equals(impactFactor)) {
			logger.info("setupBaseData() - Retrieving storeInventory details ..");
			storeInventory = new PricingEngineDAO().getInventoryDetails(recommendationInputDTO.getBaseWeek(),
					recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
					recommendationInputDTO.getLocationId());
		}

		// Set all the base data in BaseData class
		setupBaseDataObjects(baseData, storeList, authorizedItems, priceCheckListInfo, itemDataMap, strategyMap,
				productListMap, productParentChildRelationMap, priceGroupDetails, competitorMap,
				priceAndStrategyZoneNos, priceZoneHistory, priceDataMap, costDataMap, latestCompDataMap,
				previousCompDataMap, movementDataMap, lastXWeeksMov, saleDetails, adDetails, displayDetails,
				locationList, multiCompLatestPriceMap, recommendationRuleMap, parentRecInfo, predictionCache,
				productGroupProperties, priceHistory, criteriaStrategyDetails, compSettings, storeInventory,
				futureCostMap);
		// Changes-ended
	}

	/**
	 * Sets base data in BaseData class
	 * 
	 * @param storeList
	 * @param authorizedItems
	 * @param priceCheckListInfo
	 * @param itemDataMap
	 * @param strategyMap
	 * @param productListMap
	 * @param productParentChildRelationMap
	 * @param priceGroupDetails
	 * @param competitorMap
	 * @param priceAndStrategyZoneNos
	 * @param priceHistory
	 * @param costDataMap
	 * @param movementDataMap
	 * @param lastXWeeksMov
	 */
	private void setupBaseDataObjects(BaseData baseData, List<Integer> storeList, List<PRItemDTO> authorizedItems,
			HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<String, ArrayList<Integer>> productListMap,
			HashMap<Integer, Integer> productParentChildRelationMap,
			HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupDetails,
			HashMap<Integer, LocationKey> competitorMap, List<String> priceAndStrategyZoneNos,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceZoneHistory,
			LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap,
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costDataMap,
			HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompDataMap,
			HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompDataMap,
			HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementDataMap,
			HashMap<ProductKey, Long> lastXWeeksMov, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails,
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails, ArrayList<Integer> locationList,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> parentRecInfo,
			List<MultiWeekPredEngineItemDTO> predictionCache, List<PRProductGroupProperty> productGroupProperties,
			HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceHistory,
			HashMap<Integer, List<CriteriaDTO>> criteriaStrategyDetails, HashMap<Integer, Integer> compSettings,
			HashMap<Integer, Integer> storeInventory,
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostDataMap) {

		// Store list
		baseData.setStoreList(storeList);

		// Authorized items
		baseData.setAuthorizedItems(authorizedItems);

		// Price check lists
		baseData.setPriceCheckListInfo(priceCheckListInfo);

		// Item data map
		baseData.setItemDataMap(itemDataMap);

		// All active strategies
		baseData.setStrategyMap(strategyMap);

		// Product group map
		baseData.setProductListMap(productListMap);

		// Product group relation
		baseData.setProductParentChildRelationMap(productParentChildRelationMap);

		// Price group details
		baseData.setPriceGroupDetails(priceGroupDetails);

		// Competitors
		baseData.setCompetitorMap(competitorMap);

		// Price and strategy zones
		baseData.setPriceAndStrategyZoneNos(priceAndStrategyZoneNos);

		// Price data map
		baseData.setPriceHistory(priceZoneHistory);

		// Latest price data
		baseData.setPriceDataMap(priceDataMap);

		// Cost data map
		baseData.setCostDataMap(costDataMap);

		// Latest comp data
		baseData.setLatestCompDataMap(latestCompDataMap);

		// previous comp data
		baseData.setPreviousCompDataMap(previousCompDataMap);

		// movement data map
		baseData.setMovementDataMap(movementDataMap);

		// Last X Weeks movement
		baseData.setLastXWeeksMovement(lastXWeeksMov);

		// Sale details
		baseData.setSaleDetails(saleDetails);

		// Ad details
		baseData.setAdDetails(adDetails);

		// Display details
		baseData.setDisplayDetails(displayDetails);

		// Location list
		baseData.setLocationList(locationList);

		// Multi comp latest price
		baseData.setMultiCompLatestPriceMap(multiCompLatestPriceMap);

		// Recommendation rules map
		baseData.setRecommendationRuleMap(recommendationRuleMap);

		// Parent rec info
		baseData.setParentRecInfo(parentRecInfo);

		// Set prediction cache
		baseData.setPredictionCache(predictionCache);

		// Set product group properties
		baseData.setProductGroupProperties(productGroupProperties);

		// Set price historyy
		baseData.setPriceHistoryAll(priceHistory);

		// Set criteria details
		baseData.setCriteriaDetailsFromStrategy(criteriaStrategyDetails);

		// Multi comp settings
		baseData.setCompSettings(compSettings);

		// Inventory settings
		baseData.setStoreInventoryMap(storeInventory);

		// Future Cost Map
		baseData.setFutureCostDataMap(futureCostDataMap);
	}

	/**
	 * Added by Karishma for GE Guardrail req to get the latest recc prices for
	 * allitems from the zoneID's
	 * 
	 * @param itemDataMap
	 * @param uniqueZoneIDs
	 * @param recommendationInputDTO
	 * @param conn
	 * @throws GeneralException
	 */
	public void setLatestRecsForZones(HashMap<ItemKey, PRItemDTO> itemDataMap, Set<Integer> uniqueZoneIDs,
			GenaralService genaralService, RecommendationInputDTO recommendationInputDTO, Connection conn)
			throws GeneralException {

		HashMap<ItemKey, HashMap<Integer, Double>> priceMap = genaralService.setlatestRecforZones(itemDataMap,
				uniqueZoneIDs, recommendationInputDTO, conn);
		ItemKey itemKey = null;
		for (Entry<ItemKey, PRItemDTO> item : itemDataMap.entrySet()) {

			PRItemDTO itemDTO = item.getValue();

			if (!itemDTO.isLir()) {

				if (itemDTO.getRetLirId() != 0) {

					itemKey = new ItemKey(itemDTO.getRetLirId(), PRConstants.LIG_ITEM_INDICATOR);
				} else
					itemKey = new ItemKey(itemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);

				if (itemKey != null) {
					// logger.info("itemcode: "+ itemDTO.getItemCode() +"retailer Id:" +
					// itemDTO.getRetLirId());
					if (priceMap.containsKey(itemKey)) {
						itemDTO.setZonePriceMap(priceMap.get(itemKey));

					}
				}

			}

		}

	}

}
