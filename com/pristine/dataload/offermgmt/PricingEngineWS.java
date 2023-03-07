package com.pristine.dataload.offermgmt;

//import java.io.IOException;
//import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.LocationCompetitorMapDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.DashboardDAO;
import com.pristine.dao.offermgmt.PriceGroupRelationDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.SubstituteDAO;
//import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionOrder;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.PRConstraintGuardRailDetail;
import com.pristine.dto.offermgmt.PRGuidelineComp;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PROrderCode;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRSellCode;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.PRZoneStoreReccommendationFlag;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.webservice.Strategy;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.ProductService;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.ApplyStrategy;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.CheckListService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemRecErrorService;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.MarginOpportunity;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.OverrideService;
import com.pristine.service.offermgmt.PriceAdjustment;
import com.pristine.service.offermgmt.PriceGroupAdjustmentService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.service.offermgmt.PriceRollbackService;
import com.pristine.service.offermgmt.PricingEngineHelper;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.RecommendationAnalysis;
import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.service.offermgmt.RegRecommendationService;
import com.pristine.service.offermgmt.SecondaryZoneRecService;
import com.pristine.service.offermgmt.StrategyService;
import com.pristine.service.offermgmt.StrategyWhatIfService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.mwr.core.PricePointFinder;
import com.pristine.service.offermgmt.mwr.core.RecommendationRulesFilter;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.substitute.SubstituteService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCacheManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PricingEngineWS {
	private static Logger logger = Logger.getLogger("PricingEngineWS");
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private boolean isOnline = true;
	private Connection conn = null;
	private char runType = 'O';
	private String chainId = null;
	private int divisionId = 0;

	private static final String ZONE_ID = "ZONE_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String STRATEGY_ID = "STRATEGY_ID=";
	private static final String DATE = "DATE=";

	private int batchLocationLevelId = -1;
	private int batchLocationId = -1;
	private int batchProductLevelId = -1;
	private int batchProductId = -1;
	private String batchInputDate = null;
//	private String costChangeLogic ;
	private String curWeekStartDate;
	List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
	int executionOrder = 0;
	private ItemService itemService;
	private StrategyService strategyService;
	private MarginOpportunity marginOpportunity;

	private static final boolean checkForGuardRailConstraint = Boolean
			.parseBoolean(PropertyManager.getProperty("CHECK_GUARDRAIL_CONSTRAINT_APPLIED", "FALSE"));
	
	public PricingEngineWS() {
		String runType = PropertyManager.getProperty("PR_RUN_TYPE");
//		logger.debug("Run Type - " + runType);
		if (runType != null && PRConstants.RUN_TYPE_ONLINE == runType.charAt(0)) {
			isOnline = true;
		} else
			isOnline = false;

//		this.costChangeLogic = PropertyManager.getProperty("PR_COST_CHANGE_BEHAVIOR", "");
	}

	public long getPriceRecommendation(int locationLevelId, int locationId, int productLevelId, int productId, String predictedUserId) throws GeneralException {
		if (isOnline) {
			initializeForWS(locationId, productId);
		} else {
			initialize();
		}
		runType = PRConstants.RUN_TYPE_DASHBOARD;
		PRRecommendationRunHeader recommendationRunHeader = insertRecommendationHeader(locationLevelId, locationId, productLevelId, productId,
				predictedUserId);
		logger.info("Run Id - " + recommendationRunHeader.getRunId());
		if (recommendationRunHeader.getIsAutomaticRecommendation() && recommendationRunHeader.getIsPriceZone()) {
			asyncServiceMethod(recommendationRunHeader, locationLevelId, locationId, productLevelId, productId, recommendationRunHeader.getRunId());
		}
		return recommendationRunHeader.getRunId();
	}

	public long getPriceRecommendation(Strategy strategy) throws GeneralException {
		if (isOnline) {
			initializeForWS(strategy.locationId, strategy.productId);
		} else {
			initialize();
		}
		runType = PRConstants.RUN_TYPE_TEMP;
		PRRecommendationRunHeader recommendationRunHeader = insertRecommendationHeader(strategy.locationLevelId, strategy.locationId,
				strategy.productLevelId, strategy.productId, strategy.predictedUserId);
		logger.info("Run Id - " + recommendationRunHeader.getRunId());
		if (recommendationRunHeader.getIsPriceZone()) {
			asyncServiceMethod(recommendationRunHeader, strategy, recommendationRunHeader.getRunId());
		}
		return recommendationRunHeader.getRunId();
	}

	private PRRecommendationRunHeader insertRecommendationHeader(int locationLevelId, int locationId, int productLevelId, int productId, 
			String predictedUserId){
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();		
		recommendationRunHeader.setRunId(-1);
		try{
			DashboardDAO dashboardDAO = new DashboardDAO(getConnection());
			PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			PRStrategyDTO inputDTO = new PRStrategyDTO();
			
			inputDTO.setRunType(runType);
			recommendationRunHeader.setRunType(String.valueOf(runType));
			if (!isOnline) {
				recommendationRunHeader.setIsAutomaticRecommendation(
						dashboardDAO.getIsAutomaticRecommendation(locationLevelId, locationId, productLevelId, productId));
				recommendationRunHeader.setIsPriceZone(pricingEngineDAO.isPriceZone(conn, productLevelId, productId, locationLevelId, locationId));
			}

			// Don't run recommendation if the passed zone is not the price zone
			if (recommendationRunHeader.getIsPriceZone()) {
				// Check if the Product & Location can be run in batch mode
				if (recommendationRunHeader.getIsAutomaticRecommendation()) {
					//String curWkStartDate = DateUtil.getWeekStartDate(-1);
					String curWkStartDate = getBatchInputDate() != null ? getBatchInputDate() : DateUtil.getWeekStartDate(-1);
					RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), curWkStartDate,
							Constants.CALENDAR_WEEK);
					inputDTO.setLocationLevelId(locationLevelId);
					inputDTO.setLocationId(locationId);
					inputDTO.setProductLevelId(productLevelId);
					inputDTO.setProductId(productId);
					inputDTO.setPredictedBy(predictedUserId);
					recommendationRunHeader.setRunId(pricingEngineDAO.insertRecommendationHeader(getConnection(),
							curWkDTO.getCalendarId(), inputDTO));
					PristineDBUtil.commitTransaction(getConnection(), "Initial Commit for Price Recommendation");
					pricingEngineDAO = null;
					retailCalendarDAO = null;

					recommendationRunHeader.setCalendarId(curWkDTO.getCalendarId());
					recommendationRunHeader.setStartDate(curWkDTO.getStartDate());
					recommendationRunHeader.setLocationLevelId(inputDTO.getLocationLevelId());
					recommendationRunHeader.setLocationId(inputDTO.getLocationId());
					recommendationRunHeader.setProductLevelId(inputDTO.getProductLevelId());
					recommendationRunHeader.setProductId(inputDTO.getProductId());
					recommendationRunHeader.setPredictedBy(predictedUserId);
				} else {
					logger.info("Recommendation is not done. Run Type: " + runType
							+ " .Automatic Recommendation is disabled for " + "Product Level Id: " + productLevelId
							+ ",Product Id: " + productId + ",Location Level Id: " + locationLevelId + ",Location Id:"
							+ locationId);
				}
			} else {
				logger.info("Recommendation is not done. Run Type: " + runType
						+ " . Tyring to run Recommendation for non Price Zone." + "Product Level Id: "
						+ productLevelId + ",Product Id: " + productId
						+ ",Location Level Id: " + locationLevelId + ",Location Id:"
						+ locationId);
			}
		}catch(GeneralException exception){
			logger.error("Error when inserting recommendation header - " + exception.toString());
		}
		return recommendationRunHeader;
	}
	
	private void asyncServiceMethod(final PRRecommendationRunHeader recommendationRunHeader, final int locationLevelId, final int locationId, 
			final int productLevelId, final int productId, final long runId){
		List<Long> strategyIds = new ArrayList<Long>();
		Runnable task = new Runnable() {
            @Override
            public void run() {
               getPriceRecommendation(recommendationRunHeader, locationLevelId, locationId, productLevelId, productId, strategyIds, runId);
            }
        };
        new Thread(task, "ServiceThread").start(); 
	}
	
	private void asyncServiceMethod(final PRRecommendationRunHeader recommendationRunHeader, final Strategy strategy, final long runId){
		Runnable task = new Runnable() {
            @Override
            public void run() {
				List<Long> strategyIds = new ArrayList<Long>();
				if (strategy.strategyId != null) {
					strategyIds = strategy.strategyId;
				}
               getPriceRecommendation(recommendationRunHeader, strategy.locationLevelId, strategy.locationId, strategy.productLevelId, 
            		   strategy.productId, strategyIds, runId);
            }
        };
        new Thread(task, "ServiceThread").start(); 
	}
	
	private void getPriceRecommendation(PRRecommendationRunHeader recommendationRunHeader, int locationLevelId, int locationId, 
			int productLevelId, int productId, List<Long> whatIfStrategyIds, long runId) {
		logger.info("Pricing Recommendation Started for RunId - " + runId);
		// Reset execution Order
		@SuppressWarnings("unused")
		ExecutionOrder executionOrder = new ExecutionOrder();
		ExecutionTimeLog executionTimeLog;
		ExecutionTimeLog overallExecutionTime = new ExecutionTimeLog(PRConstants.OVERALL_RECOMMENDATION);
		executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		CheckListService checkListService = new CheckListService();
		SubstituteService substituteService = new SubstituteService(isOnline);
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		PriceGroupRelationDAO pgDAO = new PriceGroupRelationDAO(getConnection());
		PredictionComponent predictionComponent = new PredictionComponent();
		LocationCompetitorMapDAO locCompMapDAO = new LocationCompetitorMapDAO();
		PricingEngineService pricingEngineService = new PricingEngineService();
//		PredictionDAO predictionDAO = new PredictionDAO();
		SubstituteDAO substituteDAO = new SubstituteDAO();
		
		DashboardDAO dashboardDAO = new DashboardDAO(getConnection());
		// HashMap<RET_LIR_ID, HashMap<RET_LIR_ITEM_CODE, ArrayList<ITEM_CODE>>>
		//HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> retLirMap = new HashMap<Integer, HashMap<Integer, ArrayList<Integer>>>();
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		// HashMap<CHILD_LOCATION_ID, HashMap<RET_LIR_ID, S-Same/D-Different LIG Constraint>>
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		// HashMap<ITEM_CODE, ItemInfo>
		//HashMap<Integer, PRItemDTO> itemDataMap = new HashMap<Integer, PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		// HashMap<StoreId, HashMap<ItemCode, ItemInfo at store level>>
		//HashMap<Integer, HashMap<Integer, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<Integer, PRItemDTO>>();
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		// HashMap<StoreId, HashMap<ItemCode, ItemInfo at store level>> - Stores items which are not in cache and
		// properties for which needs to be retrieved from DB.
		//HashMap<Integer, HashMap<Integer, PRItemDTO>> itemDataMapStoreNotInCache = new HashMap<Integer, HashMap<Integer, PRItemDTO>>();
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreNotInCache = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		// HashMap<ChildLocationId, Item List>
		HashMap<Integer, List<PRItemDTO>> finalLigMap = new HashMap<Integer, List<PRItemDTO>>();
		// HashMap<StoreId, HashMap<Comp Type, Comp Store Id>>
		HashMap<Integer, HashMap<Integer, LocationKey>> compIdMapStore = new HashMap<Integer, HashMap<Integer, LocationKey>>();
		//Lead zone details
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		
		PRStrategyDTO recInput = new PRStrategyDTO(); // Used in invoking prediction engine
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		List<PRProductGroupProperty> productGroupProperties = new ArrayList<PRProductGroupProperty>();
		long tStartTime = System.currentTimeMillis();
		long tEndTime = System.currentTimeMillis();

		itemService = new ItemService(executionTimeLogs);
		strategyService = new StrategyService(executionTimeLogs);
		int pctComp = 0;
		int curRetailCalendarId = 0;
		//Runtime runtime = Runtime.getRuntime();  
		//writeMemoryUsage("Initial Memory", runtime);
		
		//NU:: 25th Nov 2016, all week calendar details, Start date as key
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		PricingEngineService pricignEngineService = new PricingEngineService();
		
		//NU:: 13th Jun 2017, applying rules based on setting
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new PricingEngineDAO().getRecommendationRules(conn);
		
		try {
			allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);
			
			PRStrategyDTO inStrategyDTO = null;
			inStrategyDTO = new PRStrategyDTO();
			inStrategyDTO.setLocationLevelId(locationLevelId);
			inStrategyDTO.setLocationId(locationId);
			inStrategyDTO.setProductLevelId(productLevelId);
			inStrategyDTO.setProductId(productId);
			
			inStrategyDTO.setStartCalendarId(recommendationRunHeader.getCalendarId());
			inStrategyDTO.setEndCalendarId(recommendationRunHeader.getCalendarId());
			inStrategyDTO.setStartDate(recommendationRunHeader.getStartDate());
			
			if (isOnline)
				inStrategyDTO.setRunType(PRConstants.RUN_TYPE_ONLINE);
			else
				inStrategyDTO.setRunType(PRConstants.RUN_TYPE_BATCH);

			// Changes for implementing zone lists
			List<PRStrategyDTO> locationList = new ArrayList<PRStrategyDTO>();

			// Get all items under input product
			List<ProductDTO> productList = new ArrayList<ProductDTO>();
			if (inStrategyDTO.getProductLevelId() == PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID) {
				// If the input product level id is product list level id, retrieve all products under that product list
				productList = pricingEngineDAO.getProductsUnderProductList(getConnection(), inStrategyDTO.getProductId());
			} else {
				ProductDTO productDTO = new ProductDTO();
				productDTO.setProductLevelId(inStrategyDTO.getProductLevelId());
				productDTO.setProductId(new Integer(String.valueOf(inStrategyDTO.getProductId())));
				productList.add(productDTO);
			}

			List<Integer> zoneList = new ArrayList<Integer>();
			if (inStrategyDTO.getLocationLevelId() == PRConstants.ZONE_LIST_LEVEL_TYPE_ID) {
				zoneList = pricingEngineDAO.getZonesUnderZoneList(getConnection(), inStrategyDTO.getLocationId());
			} else {
				zoneList.add(inStrategyDTO.getLocationId());
			}

			int productCount = productList.size();

			// Get Product Group Properties
			productGroupProperties = pricingEngineDAO.getProductGroupProperties(getConnection(), productList);

			// Set Chain Id (Used while finding strategy for an item)
			if (chainId == null)
				chainId = new RetailPriceDAO().getChainId(getConnection());

			
			// Iterate through each product in product list
			for (ProductDTO product : productList) {
				PRStrategyDTO strategyDTO = new PRStrategyDTO();
				strategyDTO.copy(inStrategyDTO);
				strategyDTO.setProductLevelId(product.getProductLevelId());
				strategyDTO.setProductId(product.getProductId());

				if (strategyDTO.getStartCalendarId() <= 0 && strategyDTO.getEndCalendarId() <= 0) {
					// DateUtil.getWeekStartDate(-1) - Recommendation will run for next week by default
					RetailCalendarDTO runForWeek = retailCalendarDAO.getCalendarId(getConnection(), DateUtil.getWeekStartDate(-1),
							Constants.CALENDAR_WEEK);
					strategyDTO.setStartCalendarId(runForWeek.getCalendarId());
					strategyDTO.setEndCalendarId(runForWeek.getCalendarId());
					strategyDTO.setStartDate(runForWeek.getStartDate());
					strategyDTO.setEndDate(runForWeek.getEndDate());
				}
				recInput.copy(strategyDTO);

				for (int zoneId : zoneList) {
					PRStrategyDTO tempDTO = new PRStrategyDTO();
					tempDTO.copy(strategyDTO);
					tempDTO.setLocationLevelId(PRConstants.ZONE_LEVEL_TYPE_ID);
					tempDTO.setLocationId(zoneId);
					tempDTO.setRunType(runType);
					locationList.add(tempDTO);
				}

				int zoneCount = locationList.size();
				// Iterate through locations
				for (PRStrategyDTO inputDTO : locationList) {
					logger.info("Running for " + inputDTO.getLocationLevelId() + "\t" + inputDTO.getLocationId() + "\t"
							+ inputDTO.getProductLevelId() + "\t" + inputDTO.getProductId());
					boolean isRecommendAtStoreLevel = false;
					int leadZoneId = 0, leadZoneDivisionId = 0;
					List<PRItemDTO> allStoreItems = new ArrayList<PRItemDTO>();
					List<Integer> priceZoneStores = new ArrayList<Integer>();
					List<String> priceAndStrategyZoneNos = new ArrayList<String>();
					HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo;
//					HashMap<ItemKey, List<PRSubstituteItem>> substituteItems = new HashMap<ItemKey, List<PRSubstituteItem>>();
					List<PRSubstituteItem> substituteItems = new ArrayList<PRSubstituteItem>();
					
					//NU:: 23rd Jun 2017, 
					//TOPS doesn't want to recommend to dependent zone, but in order to show chain level
					//summary we wanted to run recommendation for all dependent zone, but TOPS doesn't want
					//to spend time in assigning strategy for dependent zone, they will set strategy only
					//for lead zones, they asked to use lead zone strategy. But primary competitor may be
					//different for dependent zone
					leadZoneId = new PricingEngineDAO().getLeadAndDependentZone(conn,inputDTO.getLocationId());
					
					
					//NU:: 23rd Jun 2017, if it is dependent zone, pick lead zone strategies
					PRStrategyDTO leadInputDTO = new PRStrategyDTO();
					if(leadZoneId > 0) {
						pricingEngineService.copyForLeadStrategyDTO(inputDTO, leadInputDTO);
						leadInputDTO.setLocationId(leadZoneId);
					}
					
					
					//Step 1: Get Division Id of the Zone
					// Set Division Id (Used while finding strategy for an item)
					if (inputDTO.getLocationLevelId() == PRConstants.ZONE_LEVEL_TYPE_ID) {
						divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConnection(), inputDTO.getLocationId());
						if(leadZoneId > 0) {
							leadZoneDivisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConnection(), leadZoneId);
						}						
					}

					pctComp = pctComp + (5 / zoneCount / productCount);
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Retrieving Items");

					//Step 2: Get stores in the Price Zone
					//Get stores of price zone
					//Separate query it written to get the store ids instead of in the same query, as the performance is good this way
					priceZoneStores = itemService.getPriceZoneStores(conn, inputDTO.getProductLevelId(), inputDTO.getProductId(),
							inputDTO.getLocationLevelId(), inputDTO.getLocationId());
					
					//Step 3: Get Authorized items of Zone and Store
					// Get items authorized for a location-product combination
					allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(getConnection(), inputDTO,priceZoneStores);
					
					if (allStoreItems.size() == 0) {
						logger.info("No Authorized Item Found");
						//If strategy what-if, then don't update run id in dashboard table
						pricingEngineService.updateDashboard(getConnection(), inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
								inputDTO.getProductLevelId(), inputDTO.getProductId(), ((runType == PRConstants.RUN_TYPE_TEMP) ? -1 : runId));
						pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Recommendation is not Done. No Authorized Item Found");
						pricingEngineDAO.updateRecommendationRunHeader(getConnection(), runId, PRConstants.RUN_STATUS_ERROR);
						PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
						PristineDBUtil.close(getConnection());
						return;
					}
					
					
					priceCheckListInfo = pricingEngineDAO.getPriceCheckListInfo(conn, inputDTO.getLocationLevelId(), 
							inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId(),null);
					
					//Step 6: Transform zone authorized item's to object
					itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);

					//Step 4: Get all zone no's
					priceAndStrategyZoneNos = itemService.getPriceAndStrategyZoneNos(allStoreItems);

					addMissingZones(priceAndStrategyZoneNos, itemDataMap);
					
					//Restructured prog to get All active strategies By Dinesh(08/21/2017)
					HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
					if(leadZoneId > 0){
						HashMap<StrategyKey, List<PRStrategyDTO>> leadStrategyMap = strategyService.getAllActiveStrategies(getConnection(),
						leadInputDTO, leadZoneDivisionId);
						HashMap<StrategyKey, List<PRStrategyDTO>> dependentStrategyMap = strategyService.getAllActiveStrategies(getConnection(),
								inputDTO, divisionId);
						for(Map.Entry<StrategyKey, List<PRStrategyDTO>> entry: leadStrategyMap.entrySet()){
							List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
							if(strategyMap.containsKey(entry.getKey())){
								strategyList = strategyMap.get(entry.getKey());
							}
							strategyList.addAll(entry.getValue());
							strategyMap.put(entry.getKey(), strategyList);
						}
						for(Map.Entry<StrategyKey, List<PRStrategyDTO>> entry: dependentStrategyMap.entrySet()){
							List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
							if(strategyMap.containsKey(entry.getKey())){
								strategyList = strategyMap.get(entry.getKey());
							}
							strategyList.addAll(entry.getValue());
							strategyMap.put(entry.getKey(), strategyList);
						}
					}else{
						strategyMap = strategyService.getAllActiveStrategies(getConnection(),
								inputDTO, divisionId);
					}
					retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
					HashMap<String, ArrayList<Integer>> productListMap = new ProductGroupDAO().getProductListForProducts(getConnection(),
							inputDTO.getProductLevelId(), inputDTO.getProductId());
					PRZoneStoreReccommendationFlag zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
//					HashMap<Integer, Integer> productParentChildRelationMap = pricingEngineDAO.getParentChildRelationMap();
					HashMap<Integer, Integer> productParentChildRelationMap = new ProductService().getProductLevelRelationMap(getConnection(), productLevelId);
					//Step 7: Assign price check list to each zone item
					//writeMemoryUsage("After populating Authorized Items of Zone", runtime);
					checkListService.populatePriceCheckListDetailsZone(getConnection(), Integer.valueOf(chainId),
							divisionId, inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
							inputDTO.getProductLevelId(), inputDTO.getProductId(), itemDataMap, priceCheckListInfo,
							leadZoneId, strategyMap, retLirMap, productListMap, zoneStoreRecFlag,
							productParentChildRelationMap, pricingEngineDAO, strategyService, leadInputDTO, inputDTO,
							leadZoneDivisionId);

					//Step 8: Group Lig and it's members 
					// Map containing ret lir id as key and a map containing ret lir item code key and list of item code
					// value as its value
					
					
					//Step 9: Get product hierarchy
					// Generate a map with product level id as key and products under that level as value
					//NU: 4th Jan 2017, commented as this is not used
//					HashMap<Integer, List<Integer>> productsMap = pricingEngineDAO.getProductsAtAllProductLevels(getConnection(), inputDTO,
//							itemDataMap);

					pctComp = pctComp + (10 / zoneCount / productCount);
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Retrieving Strategies");
					
					//Step 12: Get all Strategies
					/*
					 * Retrieves all active strategies on the dates passed in inputDTO, for all levels in location
					 * hierarchy(Zone and Above) for location passed in inputDTO, for all levels in product
					 * hierarchy(Item through Major Category) for product passed in inputDTO
					 * HashMap<locationlevelid-locationid, productlevelid-productid>
					 */
					//NU:: 23rd Jun 2017, if it is dependent zone, pass input dto and division id of lead zone
//					HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = strategyService.getAllActiveStrategies(getConnection(),
//							(leadZoneId > 0 ? leadInputDTO : inputDTO), (leadZoneId > 0 ? leadZoneDivisionId : divisionId));

					//Step 13: Get price group details
					// Retrieves price group details for product-location combination being processed.
					// HashMap<PriceGroupName, HashMap<ItemCode, Item Info in price group>>
					executionTimeLog = new ExecutionTimeLog(PRConstants.GET_PRICE_GROUPS);
					HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupRelation = pgDAO.getPriceGroupDetails(inputDTO, itemDataMap);
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);

					//Step 14: Assign price group details to each item
					// Assign Price Group for Zone Items
					// Copies price group information for items in itemList.
					// If price group relation is defined for LIR, it is copied to all items in the group
					pricingEngineDAO.populatePriceGroupDetails(itemDataMap, priceGroupRelation);
					
					//Triggered during Strategy what-if. Give preference given to the strategies defined
					//in the strategy what-if
					if(isOnline && runType == PRConstants.RUN_TYPE_TEMP) {
						StrategyWhatIfService whatIfService = new StrategyWhatIfService();
						List<PRStrategyDTO> whatIfStrategies = whatIfService.getWhatIfStrategies(getConnection(), whatIfStrategyIds);
						whatIfService.replaceStrategies(strategyMap, whatIfStrategies);
					}
					

					// If there are no strategies retrieved at any of product/location hierarchy, updated necessary
					// status in PR_RECOMMENDATION_RUN_HEADER and return
					if (strategyMap.size() == 0) {
						pricingEngineService.updateDashboard(getConnection(), inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
								inputDTO.getProductLevelId(), inputDTO.getProductId(), ((runType == PRConstants.RUN_TYPE_TEMP) ? -1 : runId));
						logger.info("No Strategies Found");
						pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "No Strategy Found");
						pricingEngineDAO.updateRecommendationRunHeader(getConnection(), runId, PRConstants.RUN_STATUS_ERROR);
						PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
						PristineDBUtil.close(getConnection());
						return;
					}

					//Get the latest week
					// Set the latest week in RetailCalendarDTO
					tStartTime = System.currentTimeMillis();
					// calDTO - Latest Week for which price is available (Go from week for which recommendation is going
					// to run to latest week)
					RetailCalendarDTO calDTO = getLatestWeek(getConnection(), inputDTO, retailCalendarDAO);
					//even if its runs for product list or location list, the cur retail calendar id supposed to remain same
					if(calDTO != null)
						curRetailCalendarId = calDTO.getCalendarId();
					
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Retrieve Latest Week with Price (getLatestWeek) --> " + ((tEndTime - tStartTime) / 1000)
							+ " s ^^^");
					


//					HashMap<String, ArrayList<Integer>> productListMap = new ProductGroupDAO().getProductListForProducts(getConnection(),
//							inputDTO.getProductLevelId(), inputDTO.getProductId());
					zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
////					HashMap<Integer, Integer> productParentChildRelationMap = pricingEngineDAO.getParentChildRelationMap();
//					HashMap<Integer, Integer> productParentChildRelationMap = new ProductService().getProductLevelRelationMap(getConnection(), productLevelId);
					// Find strategy for each item
					executionTimeLog = new ExecutionTimeLog(PRConstants.FIND_ZONE_STRATEGY);
					
					
					strategyService.getStrategiesForEachItem(getConnection(), inputDTO, pricingEngineDAO,
							strategyMap, itemDataMap, productParentChildRelationMap, retLirMap, productListMap, chainId,
							divisionId, true, zoneStoreRecFlag,leadInputDTO,leadZoneDivisionId,leadZoneId);
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);
					isRecommendAtStoreLevel = zoneStoreRecFlag.isRecommendAtStoreLevel;

					//if lead zone guideline is present
					if(pricingEngineService.isLeadZoneGuidelinePresent(itemDataMap)) {
						//Get Lead Zone Item Details of Zone
						leadZoneDetails = pricingEngineService.getLeadZoneDetails(conn, itemDataMap, inputDTO);
					}
					
					
					
					logger.info("Strategy Found - " + strategyService.isStrategyFound);
					// If no item has strategy assigned to it update status accordingly in PR_RECOMMENDATION_RUN_HEADER
					// and return.
					if (!strategyService.isStrategyFound) {
						//If strategy what-if, then don't update run id in dashboard table
						pricingEngineService.updateDashboard(getConnection(), inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
								inputDTO.getProductLevelId(), inputDTO.getProductId(), ((runType == PRConstants.RUN_TYPE_TEMP) ? -1 : runId));
						logger.error("No Strategies Applied");
						pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp,
								"No Strategy Found");
						pricingEngineDAO.updateRecommendationRunHeader(getConnection(), runId,
								PRConstants.RUN_STATUS_ERROR);
						PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
						PristineDBUtil.close(getConnection());
						return;
					}

					/*
					 * Initial plan was to find if there is state or vendor level strategy even without fetching the
					 * store items. But it was not possible to find if any one of the item in the category has state or
					 * zone or store level strategy without fetching the store items
					 */

					// Get store level items
					//List<PRItemDTO> storeItems = itemService.getAuthorizedItemsOfStores(conn, inputDTO, false);					
					//itemService.populateStoreItemMap(conn, inputDTO, itemDataMap, storeItems, itemDataMapStore);
					if(inputDTO.getRunType() == PRConstants.RUN_TYPE_TEMP) {
						itemDataMapStore = itemService.populateAuthorizedItemsOfStore(getConnection(), runId, inputDTO, allStoreItems);	
					}
					//writeMemoryUsage("After populating Authorized Items of Store", runtime);
					
					//TODO:: Update price check list from zone
					
					// Assign Price Group for Store Items
					// Uncomment this to invoke price group for store
					 pricingEngineDAO.populatePriceGroupDetailsForStore(itemDataMapStore, priceGroupRelation);
					 
					//Update vendor id for zone items
					itemService.updateVendorIdForZoneItems(itemDataMap, itemDataMapStore);
					
					//Update price check id for store items
					checkListService.populatePriceCheckListDetailsStore(getConnection(), Integer.valueOf(chainId),
							divisionId, inputDTO.getLocationId(), inputDTO.getProductLevelId(),
							inputDTO.getProductId(), itemDataMapStore, priceCheckListInfo,leadZoneId, strategyMap, retLirMap, productListMap, 
							zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAO,strategyService, leadInputDTO, inputDTO,leadZoneDivisionId);
					
					// Find strategy for store items
					zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();
					
					/*strategyService.getStrategiesAtStoreForEachItem(getConnection(), inputDTO, pricingEngineDAO,
							strategyMap, itemDataMapStore, productParentChildRelationMap, retLirMap, productListMap,
							chainId, divisionId, zoneStoreRecFlag,leadInputDTO,leadZoneDivisionId,leadZoneId);*/

					//Make sure pre-price is same across all items in the lig, even if one of the member is pre-priced then all the member of the
					//lig will be pre-priced
					itemService.copyPrePriceToAllMembers(itemDataMap);
					
					// Copy pre-price, loc-price, min-max from zone to store
					strategyService.copyPreLocMinMaxFromZoneToStore(itemDataMap, itemDataMapStore);

					pctComp = pctComp + (10 / zoneCount / productCount);
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Retrieving Price Data");
					int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_AVG_MOVEMENT"));
					int noOfWeeksIMSData = Integer.parseInt(PropertyManager.getProperty("PR_NO_OF_WEEKS_IMS_DATA"));

					// Gets competitor ids
					//17th May 2016, to support multiple competitors in Price Index for CVS
					HashMap<Integer, LocationKey> compIdMap = locCompMapDAO.getCompetitors(getConnection(), inputDTO.getLocationLevelId(),
							inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId());
					
					//NU::23rd Jun 2017, if the recommendation is for dependent zone and if there
					//is primary competitor defined for that, consider that competitor alone. 
					//This is done as lead zone strategies are used for dependent zone, but dependent
					//zone will have different primary competitor
					//if(isDependentZone && compIdMap != null && compIdMap.size() > 0) {
						//clear str id from index guideline
						//pricingEngineService.resetCompStrIdInIndexGuideline(itemDataMap);
					//}
					
					//Add distinct competitor defined in strategy, so that those competition price also taken
					pricingEngineService.addDistinctCompStrId(itemDataMap, compIdMap);
				//	logger.info("Zone Competitor - " + compIdMap);
					// logger.debug("Reading properties...");
					// logger.debug(Integer.parseInt(PropertyManager.getProperty("PR_COMP_HISTORY")));
					int compHistory = Integer.parseInt(PropertyManager.getProperty("PR_COMP_HISTORY"));

					String key = inputDTO.getLocationLevelId() + "-" + inputDTO.getLocationId() + "-" + inputDTO.getProductLevelId() + "-"
							+ inputDTO.getProductId();
					boolean cacheEnabled = true;

					// logger.debug(PropertyManager.getProperty("EHCACHE_ENABLED"));
					// Do not retrieve from cache is cacheEnabled is false
					if ("FALSE".equalsIgnoreCase(PropertyManager.getProperty("EHCACHE_ENABLED"))) {
						cacheEnabled = false;
					}

					boolean isDataRetrievedFromCache = false;
					if (cacheEnabled && isOnline && PRCacheManager.getCache(key) != null) {
						//logger.info("Data Retrieved From Cache");
						// Zone Level Data is already available in cache

						List<PRItemDTO> listFromCache = PRCacheManager.getCache(key);
						for (PRItemDTO item : listFromCache) {
							ItemKey itemKey = PRCommonUtil.getItemKey(item);
							if (itemDataMap.get(itemKey) != null) {
								PRItemDTO itemInMap = itemDataMap.get(itemKey);
								// Copy Price, Cost, Movement & Comp Price from cache
								//logger.debug("Item taken from cache: ");
								itemInMap.copyFromCache(item);
							} else {
								logger.debug("Already cached Item is not part of latest Item Lookup table: " + item.getItemCode());
							}
						}

						// Iterate through all items for which store level recommendation has to be done
						for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> outEntry : itemDataMapStore.entrySet()) {
							for (Map.Entry<ItemKey, PRItemDTO> inEntry : outEntry.getValue().entrySet()) {
								String storeKey = Constants.STORE_LEVEL_ID + "-" + outEntry.getKey() + "-" + Constants.ITEMLEVELID + "-"
										+ inEntry.getKey();
								// Check if data is available for this store item combination in cache.
								// If not available add the item to itemDataMapStoreNotInCache
								if (PRCacheManager.getCacheStore(storeKey) != null) {
									PRItemDTO itemFromCache = PRCacheManager.getCacheStore(storeKey);
									PRItemDTO itemInMap = inEntry.getValue();
									itemInMap.copyFromCache(itemFromCache);
								} else {
									HashMap<ItemKey, PRItemDTO> tMap = null;
									if (itemDataMapStoreNotInCache.get(outEntry.getKey()) != null) {
										tMap = itemDataMapStoreNotInCache.get(outEntry.getKey());
									} else {
										tMap = new HashMap<ItemKey, PRItemDTO>();
									}
									tMap.put(inEntry.getKey(), inEntry.getValue());
									itemDataMapStoreNotInCache.put(outEntry.getKey(), tMap);
								}
							}
						}
						isDataRetrievedFromCache = true;
					}

					//Get non cached store items
					getNonCachedStoreItems(isDataRetrievedFromCache, itemDataMapStore, itemDataMapStoreNotInCache);
					
					String resetDate = null;
					RetailCalendarDTO resetCalDTO = null;
					if (inputDTO.getProductLevelId() == Constants.CATEGORYLEVELID) {
						// Max of reset date in dashboard or last approval date
						// in recommendation
						executionTimeLog = new ExecutionTimeLog(PRConstants.GET_DASHBOARD_RESET_DATE);
						resetDate = dashboardDAO.getDashboardResetDate(inputDTO.getLocationLevelId(),
								inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId());
						executionTimeLog.setEndTime();
						executionTimeLogs.add(executionTimeLog);
						if (resetDate != null) {
							/*
							 * logger.info("Week End Date - " + calDTO.getEndDate());
							 * logger.info("Reset Date - " + resetDate);
							 */
							resetCalDTO = retailCalendarDAO.getCalendarId(getConnection(), resetDate,
									Constants.CALENDAR_WEEK);
						}
					}

					tStartTime = System.currentTimeMillis();
					// Price, Cost, Movement, Comp Price

					/*
					 * Zone/Store 
					 * Current Price - Price of the Latest Week (RETAIL_PRICE_INFO) (Latest Week - Go from
					 * week for which recommendation is going to running -- to week (configured in
					 * PR_PRICE_THRESHOLD_IN_WEEKS) where latest price available) 
					 * Zone/Store 
					 * Previous Price - Price of  the Reset Week Zone/Store 
					 * Current Cost - Latest Cost(RETAIL_COST_INFO) (Latest from week in which
					 * the recommendation run -- to week (configured in PR_COST_HISTORY) 
					 * Zone/Store Previous Cost -
					 * Latest Cost (Latest from reset week to week (configured in PR_COST_HISTORY) 
					 * Zone(MOVEMENT_WEEKLY
					 * Table)/Store(SYNONYM_IMS_WEEKLY Table) Movement Avg Movement from week in which the
					 * recommendation run -- to week (configured in PR_AVG_MOVEMENT) 
					 * Zone/Store Current Comp Price -
					 * Latest Price (SYNONYM_COMPETITIVE_DATA Table) from week in which the recommendation run -- to
					 * week (configured in PR_COMP_HISTORY) 
					 * Zone/Store Previous Comp Price - Latest Price
					 * (SYNONYM_COMPETITIVE_DATA Table) from reset week in which the recommendation run -- to week
					 * (configured in PR_COMP_HISTORY)
					 */
					
					//Get price history of the items
					// NU:: 25th nov 2016, as previous price is used in determining the
					// recommended price lot more weeks price is required
					boolean fetchStorePrice = (itemDataMapStore != null && itemDataMapStore.size() > 0) ? true : false;
					HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = pricingEngineDAO.getPriceHistory(conn,
							Integer.parseInt(chainId), allWeekCalendarDetails, calDTO, resetCalDTO, itemDataMap, priceAndStrategyZoneNos,
							priceZoneStores, fetchStorePrice);
					
					HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = pricignEngineService
							.getItemZonePriceHistory(Integer.parseInt(chainId), inputDTO.getLocationId(), itemPriceHistory);
					
					//TODO:: make use of above data to find current price and previous price

					//New Optimized Price Fetching Block
					boolean fetchZoneLevelPrice = false;
					if (!isDataRetrievedFromCache) {
						fetchZoneLevelPrice = true;
					}

					// Retrieve cost information. Sets current cost, previous cost, cost change indicator
					// for items in itemDataMap and itemDataMapStore
					tStartTime = System.currentTimeMillis();
					executionTimeLog = new ExecutionTimeLog(PRConstants.GET_STORE_PRICE);
					pricingEngineDAO.getPriceDataOptimized(getConnection(), Integer.parseInt(chainId), inputDTO, calDTO, resetCalDTO, 0, itemDataMap,
							itemDataMapStore, itemDataMapStoreNotInCache, fetchZoneLevelPrice, priceAndStrategyZoneNos, priceZoneStores);
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Get Price Data(getPriceData) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");
					//New Optimized Cost Fetching Block

					
					//Check if prices across all stores are same
					RegRecommendationService regRecService = new RegRecommendationService();
					regRecService.findIsCurRetailIsSameAcrossStores(itemDataMap, itemDataMapStore);
										
					// Remove store items
					// If any one of the item in the category has vendor or state or store level strategy, then
					// recommend Store items irrespective of the isRecommendAtStoreLevel flag
					if (zoneStoreRecFlag.isStateLevelStrategyPresent || zoneStoreRecFlag.isVendorLevelStrategyPresent
							|| zoneStoreRecFlag.isStateLevelStrategyPresent || isRecommendAtStoreLevel) {
						// If state level strategy is not present
						if (!zoneStoreRecFlag.isStateLevelStrategyPresent) {
							// remove warehouse items
							itemDataMapStore = itemService.removeWarehouseItems(itemDataMapStore);
						}
					} else {
						// Remove all store level items
						itemDataMapStore.clear();
					}
					
					//As the store items are removed above, again find not cached items
					itemDataMapStoreNotInCache.clear();
					getNonCachedStoreItems(isDataRetrievedFromCache, itemDataMapStore, itemDataMapStoreNotInCache);
					
					pctComp = pctComp + (20 / zoneCount / productCount);
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Retrieving Cost Data");
					// Get Base Store Cost from retail_cost_info
					RetailCalendarDTO curCalDTO = null;

					// Week in which the batch runs
					curCalDTO = getCurCalDTO(conn,retailCalendarDAO);

					logger.info("Current Calendar Details: Calendar Id:" + curCalDTO.getCalendarId() + ", Start Date: " + curCalDTO.getStartDate());
					
					int costHistory = Integer.parseInt(PropertyManager.getProperty("PR_COST_HISTORY"));
					if (resetDate == null) {
						String startDate = DateUtil.getWeekStartDate(DateUtil.toDate(curCalDTO.getStartDate()), costHistory);
						resetDate = startDate;
						resetCalDTO = retailCalendarDAO.getCalendarId(getConnection(), startDate, Constants.CALENDAR_WEEK);
					}

					//New Optimized Cost Fetching Block
//					boolean fetchZoneLevelCost = false;
//					if (!isDataRetrievedFromCache) {
//						fetchZoneLevelCost = true;
//					}
					
					//writeMemoryUsage("After populating Price", runtime);
					// Retrieve cost information. Sets current cost, previous cost, cost change indicator
					// for items in itemDataMap and itemDataMapStore
					tStartTime = System.currentTimeMillis();
					executionTimeLog = new ExecutionTimeLog(PRConstants.GET_STORE_COST);
					
//					pricingEngineDAO.getCostDataOptimized(getConnection(), Integer.parseInt(chainId), inputDTO, curCalDTO, resetCalDTO, costHistory,
//							itemDataMap, itemDataMapStore, itemDataMapStoreNotInCache, fetchZoneLevelCost, priceAndStrategyZoneNos, priceZoneStores);
					
					/************** Set item's current and previous cost of zone and store ********************/
					
					// TODO:: as re-factoring and changes needed in the caching, passed all items instead of non-cached items
					double costChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PR_DASHBOARD.COST_CHANGE_THRESHOLD", "0"));
					
					Set<Integer> nonCachedItemCodeSet = new HashSet<Integer>();
					for (PRItemDTO item : itemDataMap.values()) {
						if (!item.isLir()) {
							nonCachedItemCodeSet.add(item.getItemCode());
						}
					}
					RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(getConnection());
					
					// Get non-cached item's zone and store cost history
					HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory = retailCostServiceOptimized.getCostHistory(
							Integer.parseInt(chainId), curCalDTO, costHistory, allWeekCalendarDetails, nonCachedItemCodeSet, priceAndStrategyZoneNos,
							priceZoneStores);
					
					// Find latest cost of the item for zone and store
					retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, nonCachedItemCodeSet, itemDataMap, Integer.valueOf(chainId),
							inputDTO.getLocationId(), curCalDTO, costHistory, allWeekCalendarDetails);

					retailCostServiceOptimized.getLatestCostOfStoreItems(itemCostHistory, nonCachedItemCodeSet, itemDataMapStore,
							Integer.valueOf(chainId), inputDTO.getLocationId(), curCalDTO, costHistory, allWeekCalendarDetails);

					// Find if there is cost change for zone
					retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, nonCachedItemCodeSet, itemDataMap,
							Integer.valueOf(chainId), inputDTO.getLocationId(), curCalDTO, costHistory, allWeekCalendarDetails, costChangeThreshold);

					// Find if there is cost change for store
					retailCostServiceOptimized.getPreviousCostOfStoreItems(itemCostHistory, nonCachedItemCodeSet, itemDataMapStore,
							Integer.valueOf(chainId), inputDTO.getLocationId(), curCalDTO, costHistory, allWeekCalendarDetails, costChangeThreshold);

					/************** Set item's current and previous cost of zone and store ********************/
					
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Get Cost Data(getCostData) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");
					//New Optimized Cost Fetching Block
					
					pctComp = pctComp + (5 / zoneCount / productCount);
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Retrieving Avg Revenue/Movements");

					//NU:: 2nd Aug 2016, needed to find movement avg, prediction analysis
					//Get all calendar's below the week on which the recommendation runs
					//Sorted by date in descending order, i.e. first index will have latest week
					LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO.getAllPreviousWeeks(conn,
							curCalDTO.getStartDate(), noOfWeeksIMSData);
					//writeMemoryUsage("After populating Cost", runtime);
					// Get Average revenue and movement from Movement_Weekly
					
					HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = null;
					if (!isDataRetrievedFromCache) {
						// Sets average movement, average revenue for items in itemDataMap
						tStartTime = System.currentTimeMillis();
						executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ZONE_MOV);
						//pricingEngineDAO.getMovementData(getConnection(), inputDTO, curCalDTO, noOfWeeksBehind, itemDataMap);
						movementData = pricingEngineDAO.getMovementDataForZone(conn, inputDTO,
								priceZoneStores, calDTO.getStartDate(), noOfWeeksIMSData, itemDataMap);
						pricingEngineDAO.getMovementDataForZone(movementData, itemDataMap, previousCalendarDetails, noOfWeeksBehind);
						executionTimeLog.setEndTime();
						executionTimeLogs.add(executionTimeLog);
						tEndTime = System.currentTimeMillis();
						logger.info("^^^ Time -- Get Avg Movement-Zone(getMovementData) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");
					}

					// Added for ignoring non-moving Items
					// Added on 12/28/2018 by Pradeep 
					// Added based on Giant Eagle's feed back
					markNonMovingItems(itemDataMap, retLirMap);
					
					// Added for adjusting price groups based on non moving items/inactive items/unauthorized items
					// Added on 06/24/2019 by Pradeep
					// This code is to handle missing relationships issue due to non-moving items for Giant Eagle
					// This will also take care of AutoZone's missing tier requirement
					new PriceGroupAdjustmentService().adjustPriceGroupsByDiscontinuedItems(itemDataMap, retLirMap);
					
					itemDataMap = filterByActiveAuthAndMovingItems(itemDataMap, retLirMap);
					
					retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);
					
					if (itemDataMap.size() == 0) {
						logger.info("All items are non-moving items");
						// If strategy what-if, then don't update run id in dashboard table
						pricingEngineService.updateDashboard(getConnection(), inputDTO.getLocationLevelId(),
								inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId(),
								((runType == PRConstants.RUN_TYPE_TEMP) ? -1 : runId));
						pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp,
								"Recommendation is not Done. All items are non-moving items");
						pricingEngineDAO.updateRecommendationRunHeader(getConnection(), runId,
								PRConstants.RUN_STATUS_ERROR);
						PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
						PristineDBUtil.close(getConnection());
						return;
					}
					
					tStartTime = System.currentTimeMillis();
					// Sets average movement, average revenue at store level for items in itemDataMapStore
					executionTimeLog = new ExecutionTimeLog(PRConstants.GET_STORE_MOV);
					pricingEngineDAO.getMovementDataForStore(getConnection(), inputDTO, curCalDTO, noOfWeeksBehind, itemDataMapStore,
							itemDataMapStoreNotInCache);
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Get Avg Movement-Store(getMovementDataForStore) --> " + ((tEndTime - tStartTime) / 1000)
							+ " s ^^^");

					int movHistoryWeeks = Integer.parseInt(PropertyManager.getProperty("PR_MOV_HISTORY_WEEKS"));
					//16th Jul 2016, retain current price when an item didn't move in last 2 years
					pricingEngineDAO.getLastXWeeksMovForZone(getConnection(), priceZoneStores, curCalDTO.getStartDate(), movHistoryWeeks, 
							itemDataMap, retLirMap, inputDTO.getLocationId()); 
					
					pctComp = pctComp + (10 / zoneCount / productCount);
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Retrieving Competition Data");

					if (resetDate == null) {
						String startDate = DateUtil.getWeekStartDate(DateUtil.toDate(curCalDTO.getStartDate()), compHistory);
						resetCalDTO = retailCalendarDAO.getCalendarId(getConnection(), startDate, Constants.CALENDAR_WEEK);
					}
					//writeMemoryUsage("After populating movement", runtime);
					// Get Competition Price from competitive_data
					tStartTime = System.currentTimeMillis();
					//if (!isDataRetrievedFromCache) {
						for (LocationKey locationKey : compIdMap.values()) {
							PRStrategyDTO tempDTO = new PRStrategyDTO();
							tempDTO.copy(inputDTO);
							tempDTO.setLocationLevelId(locationKey.getLocationLevelId());
							tempDTO.setLocationId(locationKey.getLocationId());
							// Sets current comp price, previous comp price, comp price change indicator for items in
							// itemDataMap
							executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ZONE_COMP);
							//logger.info("Getting competitor data for comp store:" + locationKey.toString() + " is Started...");
							//pricingEngineDAO.getCompPriceDataOld(getConnection(), tempDTO, curCalDTO, resetCalDTO, compHistory, itemDataMap);
							pricingEngineDAO.getCompPriceData(getConnection(), tempDTO, curCalDTO, resetCalDTO, compHistory, itemDataMap);
							//logger.info("Getting competitor data for comp store:" + locationKey.toString() + " is Completed...");
							executionTimeLog.setEndTime();
							executionTimeLogs.add(executionTimeLog);
						}
					//}
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Get Comp Price-Zone(getCompPriceData) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");

					// 16th Dec 2014, Competitor for zone and all its store must be same. no need to take store level
					// competitor for stores
					// Copy zone comp map store to store
					compIdMapStore = copyZoneCompMapToStore(compIdMap, itemDataMapStore, itemDataMapStoreNotInCache);
					// Copy zone comp price to store
					copyZoneCompPriceToStore(itemDataMap, itemDataMapStore);

					/*** Retrieve Competition Data for Multiple Competitor ***/
					logger.info("Retrieving of Competition Data for Multiple Competitor is Started...");
					tStartTime = System.currentTimeMillis();
					
					applyMultiCompRetails(getConnection(),strategyMap, curCalDTO, inputDTO.getProductLevelId(), 
							inputDTO.getProductId(), itemDataMap);
					
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Get Multi Comp Price(getLatestCompDataForMultiComp) --> " + ((tEndTime - tStartTime) / 1000)
							+ " s ^^^");
					/*** Retrieve Competition Data for Multiple Competitor ***/

					logger.info("Competition data retrieved");
					//writeMemoryUsage("After populating competition data", runtime);
					ArrayList<PRItemDTO> prItemList = new ArrayList<PRItemDTO>();
					for (PRItemDTO prItemDTO : itemDataMap.values()) {
						prItemList.add(prItemDTO);
					}
					if (isOnline) {
						// Put items in cache for zone
						PRCacheManager.putCache(key, prItemList);
						// Put items in cache for store
						for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> outEntry : itemDataMapStore.entrySet()) {
							for (Map.Entry<ItemKey, PRItemDTO> inEntry : outEntry.getValue().entrySet()) {
								String storeKey = Constants.STORE_LEVEL_ID + "-" + outEntry.getKey() + "-" + Constants.ITEMLEVELID + "-"
										+ inEntry.getKey();
								PRCacheManager.putCacheStore(storeKey, inEntry.getValue());
							}
						}
					}
					int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
					
					String promoStartDate = curCalDTO.getStartDate();
					String recWeekStartDate = recommendationRunHeader.getStartDate();
//					String promoStartDate = "12/18/2016";
//					String recWeekStartDate = "12/25/2016";
					// Get Sale information of the items

					// 31st Jan 2017, passed price zone stores as previously stores in a zone are taken from the competitor store
					// table. for GE, there is no zone defined for store in competitor store table. So in order to work for all
					// clients the stores are passed as parameter itself
					HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = pricingEngineDAO.getSaleDetails(conn, inputDTO.getProductLevelId(),
							inputDTO.getProductId(), Integer.valueOf(chainId), inputDTO.getLocationId(), promoStartDate, noOfsaleAdDisplayWeeks,
							priceZoneStores, false);

					// Assign Sale price to items
//					pricingEngineService.fillSaleDetails(itemDataMap, saleDetails, promoStartDate, recWeekStartDate, noOfsaleAdDisplayWeeks);

					// Get Ad details
					HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = pricingEngineDAO.getAdDetails(conn, inputDTO.getProductLevelId(),
							inputDTO.getProductId(), Integer.valueOf(chainId), inputDTO.getLocationId(), promoStartDate, noOfsaleAdDisplayWeeks,
							priceZoneStores, false);

					// Assign ad information to items
//					pricingEngineService.fillAdDetails(itemDataMap, adDetails, promoStartDate, recWeekStartDate, noOfsaleAdDisplayWeeks);

					// Get Display info
					HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = pricingEngineDAO.getDisplayDetails(conn,
							inputDTO.getProductLevelId(), inputDTO.getProductId(), Integer.valueOf(chainId), inputDTO.getLocationId(), promoStartDate,
							noOfsaleAdDisplayWeeks, priceZoneStores, false);

					// Assign display info to items
//					pricingEngineService.fillDisplayDetails(itemDataMap, displayDetails, promoStartDate, recWeekStartDate, noOfsaleAdDisplayWeeks);
					
					//Dinesh:: 21-FEB-18, Code changes to handle future promotions and on-going promotions
					pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails, promoStartDate, recWeekStartDate,
							noOfsaleAdDisplayWeeks);
					
					//Update item level errors, whether it can still be recommended
					ItemRecErrorService itemRecErrService = new ItemRecErrorService();
					itemRecErrService.setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
					itemRecErrService.setErrorCodeForStoreItems(itemDataMapStore, leadZoneDetails);
					
					// Store Level Recommendation Starts
					// Apply store level strategies
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Recommending Prices");
					logger.info("Store Level Recommendation is Started....");
					tStartTime = System.currentTimeMillis();
					marginOpportunity = new MarginOpportunity();
					
					boolean doSubsAnalysis = Boolean.parseBoolean(PropertyManager.getProperty("PR_SUBSTITUTE_ANALYSIS", "TRUE"));
					
					if (doSubsAnalysis && itemDataMapStore.size() == 0 && runType != PRConstants.RUN_TYPE_TEMP) {
						substituteItems = substituteDAO.getSubstituteItemsNew(conn, locationLevelId, locationId, productLevelId, productId, retLirMap,
								itemDataMap);
					}
					
					//21st March 2017, update brand and size relation from strategy
					//previous it was done inside the applyStrategy(), but in order to
					//assign default brand precedence, the order of execution changed,
					//so before applying strategy for the item, the price relation and
					//brand precedence will be set here
					PriceGroupService priceGroupService = new PriceGroupService();
					priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
					priceGroupService.setDefaultBrandPrecedence(itemDataMap,retLirMap);
					
					// New Code Block to invoke processItems() for applying strategies
					
					// TODO::SRIC Find lead item in itemDataMap, have 2 flag, 1-which is lead,2-is part of same retailer item code items,
					// do in recommendation also
					
					
					//HashMap<CompStrId, List of items after recommendation>
					HashMap<Integer, List<PRItemDTO>> storeRecMap = new HashMap<Integer, List<PRItemDTO>>();	
					// itemDataMapStore - HashMap<StoreId, HashMap<ItemCode, ItemInfo>
					// prItemListStoreMap - HashMap<StoreId, List of items to recommend price for that store>
					for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> entry : itemDataMapStore.entrySet()) {
						logger.debug("Processing of Store : " + entry.getKey() + " is Started...");
						// Changes to invoke prediction engine
						recInput.setLocationLevelId(Constants.STORE_LEVEL_ID);
						recInput.setLocationId(entry.getKey());
						
						//21st March 2017, price group related changes, see zone level changes for details
						priceGroupService.updatePriceRelationFromStrategy(entry.getValue());
						priceGroupService.setDefaultBrandPrecedence(entry.getValue(),retLirMap);
						
						List<PRItemDTO> storeRecList = processItems(entry.getValue(),
								recommendationRunHeader, retLirMap, retLirConstraintMap,
								compIdMapStore.get(entry.getKey()), multiCompLatestPriceMap, finalLigMap,
								leadZoneDetails, isRecommendAtStoreLevel,
								curCalDTO);

						// Changes to find opportunities
						//commented below function as not needed, since opportunity are not found for store
						//recommendations
						/*List<PRItemDTO> storeItemListForInsert = marginOpportunity.findOpportunities(getConnection(),
								storeRecList, retLirMap, recInput, productGroupProperties, executionTimeLogs,
								recommendationRunHeader, isOnline, priceZoneStores, curCalDTO);*/

						//storeRecMap.put(entry.getKey(), storeItemListForInsert);
						storeRecMap.put(entry.getKey(), storeRecList);
						logger.debug("Processing of Store : " + entry.getKey() + " is Completed...");
					}
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Store Level Recommendation --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");
					
					//clear finalLigMap, so that store level records are cleared
					finalLigMap = new HashMap<Integer, List<PRItemDTO>>();
					
					//Update zone price with common store prices
					updateZoneInfoWithCommonStoreInfo(itemDataMap, storeRecMap, retLirMap, retLirConstraintMap);
					
					
					
					// Added for retaining overrides from past recommendations
					// Added as part of Enhancements for Giant Eagle
					// Added by Pradeep on 03/07/2019 
					new OverrideService().lookupPastOverrides(conn, locationLevelId, locationId, productLevelId,
							productId, curWeekStartDate, itemDataMap, recommendationRuleMap);
					
					
					// Get future retail changes 
					int noOfWeeksInFuture = Integer.parseInt(PropertyManager.getProperty("PR_NO_OF_FUTURE_WEEKS_FOR_RETAIL_CHANGES", "8"));
					
					pricingEngineDAO.getFuturePriceData(getConnection(), Integer.valueOf(chainId), leadInputDTO,
							curCalDTO, resetCalDTO, noOfWeeksInFuture, itemDataMap, itemDataMapStore,
							itemDataMapStoreNotInCache, fetchZoneLevelPrice, priceAndStrategyZoneNos, priceZoneStores);
					
					/****************************************************/
					PredictionService predictionService = new PredictionService(movementData, itemPriceHistory, retLirMap);
					
					PricePointFinder pricePointFinder = new PricePointFinder();
					
					HashMap<String,HashMap<ItemKey, PRItemDTO>> itemsClassified = pricePointFinder.getItemsClassified(itemDataMap, retLirMap);

					HashMap<ItemKey, PRItemDTO> independentItemDataMap = itemsClassified.get(PRConstants.INDEPENDENT_ITEMS);
					HashMap<ItemKey, PRItemDTO> dependentItemDataMap = itemsClassified.get(PRConstants.DEPENDENT_ITEMS);
					
					logger.info("# of independent items: " + independentItemDataMap.size());
					logger.info("# of dependent items: " + dependentItemDataMap.size());
					logger.info("# of items: " + itemDataMap.size());
					List<PRItemDTO> itemListWithLIG = new ArrayList<>();
					// All functionalities which decides the price of an item is put here
					List<PRItemDTO> independentItems = recommendPrice(conn, independentItemDataMap, inputDTO, recommendationRunHeader, retLirMap,
							retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent,
							leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData,
							recommendationRuleMap, saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionService,false);
					
					// All functionalities which decides the price of an item is put here
					List<PRItemDTO> dependentItems = recommendPrice(conn, itemDataMap, inputDTO, recommendationRunHeader, retLirMap,
							retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, productGroupProperties, predictionComponent,
							leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, isOnline, itemPriceHistory, movementData,
							recommendationRuleMap, saleDetails, adDetails, priceZoneStores, recWeekStartDate, predictionService,false);
					
					
					itemListWithLIG.addAll(independentItems);
					itemListWithLIG.addAll(dependentItems);
					
					/****************************************************/
					
					
					// Changes to find opportunities
					tStartTime = System.currentTimeMillis();
					recInput.setLocationLevelId(inputDTO.getLocationLevelId());
					recInput.setLocationId(inputDTO.getLocationId());
					logger.info("Marking opportunity is started...");
					List<PRItemDTO> itemListForInsert = marginOpportunity.markOpportunities(getConnection(), itemListWithLIG, retLirMap, 
							recInput, productGroupProperties, executionTimeLogs, recommendationRunHeader, isOnline, priceZoneStores, curCalDTO);
					logger.info("Marking opportunity is completed...");
					
					/*debug - write lig level details */
//					PRCommonUtil.logLigDetails(itemListForInsert);
					
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Finding Opportunity(findOpportunities) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");

					// Mark items which is same across all stores
					tStartTime = System.currentTimeMillis();
					markItemWhoseZoneAndStorePriceIsSame(itemListForInsert, storeRecMap);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Mark Item whose Store Price is same as Zone Price --> " + ((tEndTime - tStartTime) / 1000)
							+ " s ^^^");

					//NU:: 28th Oct 2016, Set effective date for short term promotions
					//pricingEngineService.setEffectiveDateForShortPromo(itemListForInsert, recommendationRunHeader);
					pricingEngineService.setEffectiveDate(itemListForInsert, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);
			
					//NU:: 15th Feb 2017, Moved code here. Since sale prediction was done before, 
					// the sale prediction with current and rec price becomes incorrect, 
					// when the current prices are retained (here rec + sale will change to cur + sale)
					logger.info("Sale prediction is started..");
					predictionComponent.predictPromoItems(conn, executionTimeLogs, recommendationRunHeader, itemListWithLIG, isOnline, predictionService);
					logger.info("Sale prediction is completed..");
					
					
					// Do subs adjustment			
					/** Change Recommended price using Substitution Effect Starts here**/ 
					logger.info("Substitution effect application is started..");
					//No Store level recommendation
					if(doSubsAnalysis && itemDataMapStore.size() == 0 && runType != PRConstants.RUN_TYPE_TEMP){
						//22nd Sep 2016 adjust movement due to subs effect
//						substituteService.adjustPredWithSubsEffect(conn, locationLevelId, locationId, productLevelId, productId,
//								recommendationRunHeader.getCalendarId(), substituteItems, itemListForInsert, saleDetails, curCalDTO,
//								recommendationRunHeader);
						substituteService.adjustPredWithSubsEffect(conn, recommendationRunHeader, curCalDTO, substituteItems, itemListForInsert,
								saleDetails, executionTimeLogs);
					}
					logger.info("Substitution effect application is completed..");
					/** Change Recommended price using Substitution Effect Ends here**/
			
					
					tStartTime = System.currentTimeMillis();
					executionTimeLog = new ExecutionTimeLog(PRConstants.INSERT_ZONE_RECOMMENDATION);
					pricingEngineDAO.insertRecommendationItems(getConnection(), itemListForInsert);
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Insert Recommendation Item for Zone(insertRecommendationItems) --> "
							+ ((tEndTime - tStartTime) / 1000) + " s ^^^");

					// Recommend Order Code Price
					tStartTime = System.currentTimeMillis();
					recommendOrderCodePrice(getConnection(), runId, productList, PRConstants.ZONE_LEVEL_TYPE_ID, zoneList,
							productGroupProperties, itemListForInsert, curCalDTO, inputDTO);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Recommend Order Code Price(recommendOrderCodePrice) --> " + ((tEndTime - tStartTime) / 1000)
							+ " s ^^^");

					tStartTime = System.currentTimeMillis();
					// Inserts store level recommendation records in PR_RECOMMENDATION_STORE
					executionTimeLog = new ExecutionTimeLog(PRConstants.INSERT_STORE_RECOMMENDATION);
					pricingEngineDAO.insertRecommendationItemStore(getConnection(), storeRecMap);
					executionTimeLog.setEndTime();
					executionTimeLogs.add(executionTimeLog);
					tEndTime = System.currentTimeMillis();
					logger.info("^^^ Time -- Insert Recommendation Item for Store(insertRecommendationItemStore) --> "
							+ ((tEndTime - tStartTime) / 1000) + " s ^^^");

					//Get recommended item details up-front as it is used in many places
					List<Long> runIds = new ArrayList<Long>();
					runIds.add(runId);
					HashMap<Long, List<PRItemDTO>> runAndItsRecommendedItems = pricingEngineDAO.getRecommendationItems(conn, runIds);
					
					//Don't update during strategy what-if
					if(!(runType == PRConstants.RUN_TYPE_TEMP)) {
						//TODO:: Recommended items details taken again while updating dashboard and balancing
						//instead above list can be used(recommendedItems) by generalizing it
						
						// Updates dashboard numbers
						pricingEngineService.updateDashboard(getConnection(), inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
								inputDTO.getProductLevelId(), inputDTO.getProductId(), runId);
						logger.info("Dashboard update complete");
						logger.info("Balancing of PI and Margin started..");
						PriceAdjustment priceAdjustment = new PriceAdjustment();
						priceAdjustment.balancePIAndMargin(conn, runId, runType);
						logger.info("Balancing of PI and Margin completed..");
					}
					
					// Updates Success status and Success message in PR_RECOMMENDATION_RUN_HEADER
					pctComp = 100;
					pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, "Price Recommendation Complete");
					pricingEngineDAO.updateRecommendationRunHeader(getConnection(), runId, PRConstants.RUN_STATUS_SUCCESS);
					//Update the calendar id from which the cur retail price is fetched
					//this will be useful for the front end, when it needs to show the all stores cur retail
					//(where cur retail varies from one store to other)
					pricingEngineDAO.updateCurRetailCalIdInRunHeader(conn, runId, curRetailCalendarId); 
					overallExecutionTime.setEndTime();
					executionTimeLogs.add(overallExecutionTime);
					// Update execution time log
					pricingEngineDAO.insertExecutionTimeLog(getConnection(), recommendationRunHeader, executionTimeLogs);
					PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
					logger.info("Price Recommendation Complete for RunId - " + runId);

					if(!(runType == PRConstants.RUN_TYPE_TEMP)) {
						// update NotificationAnalysis using runID
						logger.info("Notification Analysis is Started...");
						addNotifications(runId, PRConstants.REC_COMPLETED);
						logger.info("Notification Analysis is Completed...");

						// update recommendationAnalysis using runId
						logger.info("Recommendation Analysis is Started...");
						recommendationAnalysis(runId, runAndItsRecommendedItems, itemDataMap, previousCalendarDetails);
						logger.info("Recommendation Analysis is Completed...");

						//call audit trail
						callAuditTrail(locationLevelId, locationId, productLevelId, productId, AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(),
								PRConstants.REC_COMPLETED, runId, recommendationRunHeader.getPredictedBy(),AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),AuditTrailStatusLookup.AUDIT_SUB_TYPE_RECC.getAuditTrailTypeId(),AuditTrailStatusLookup.SUB_STATUS_TYPE_RECC.getAuditTrailTypeId());
						
						//Calling Audit process						
						callAudit(recommendationRunHeader);
						
						PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
					}
					
					//writeMemoryUsage("After balancing", runtime);
				}
			}
			
//			PristineDBUtil.close(getConnection());
			//writeMemoryUsage("After closing connection", runtime);
		} catch (GeneralException | Exception | OfferManagementException exception) {
			exception.printStackTrace();
			logger.error(exception.toString(), exception);
			try {
				String exceptionMessage = "Error during Recommendation. Error Code: ";
				if (exception instanceof OfferManagementException) {
					exceptionMessage = exceptionMessage
							+ ((OfferManagementException) exception).getRecommendationErrorCode().getErrorCode();
				} else {
					exceptionMessage = exceptionMessage + RecommendationErrorCode.GENERAL_EXCEPTION.getErrorCode();
				}
				// Updates Error status and Error message in PR_RECOMMENDATION_RUN_HEADER
				pricingEngineDAO.updateRecommendationStatus(getConnection(), runId, pctComp, exceptionMessage);
				pricingEngineDAO.updateRecommendationRunHeader(getConnection(), runId, PRConstants.RUN_STATUS_ERROR);
				//If strategy what-if, then don't update run id in dashboard table
				pricingEngineService.updateDashboard(getConnection(), locationLevelId, locationId, productLevelId, productId,
						((runType == PRConstants.RUN_TYPE_TEMP) ? -1 : runId));
				if (!(runType == PRConstants.RUN_TYPE_TEMP)) {
					addNotifications(runId, PRConstants.ERROR_REC);
					//call audit
					callAuditTrail(locationLevelId, locationId, productLevelId, productId, AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(),
							PRConstants.ERROR_REC, runId, recommendationRunHeader.getPredictedBy()
							,AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_ERROR.getAuditTrailTypeId(),
							AuditTrailStatusLookup.SUB_STATUS_TYPE_ERROR.getAuditTrailTypeId());
				}
				
				PristineDBUtil.commitTransaction(getConnection(), "Commit Price Recommendation");
			} catch (GeneralException | OfferManagementException | SQLException ge) {
				logger.error("Exception when updating dashboard and recommendation status - " + ge);
			} finally {
//				PristineDBUtil.close(getConnection());
			}
			logger.error("Exception in getPriceRecommendation - " + exception.toString(), exception);
		} finally {
			PristineDBUtil.close(getConnection());
		}
	}
	
	
	public List<PRItemDTO> recommendPrice(Connection conn, HashMap<ItemKey, PRItemDTO> itemDataMap, PRStrategyDTO inputDTO,
			PRRecommendationRunHeader recommendationRunHeader, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap, HashMap<Integer, LocationKey> compIdMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, HashMap<Integer, List<PRItemDTO>> finalLigMap,
			List<PRProductGroupProperty> productGroupProperties, PredictionComponent predictionComponent,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails, boolean isRecommendAtStoreLevel,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory, RetailCalendarDTO curCalDTO, boolean isOnline,
			HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory,
			HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails,
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails, List<Integer> priceZoneStores, String recWeekStartDate,
			PredictionService predictionService, boolean isGlobalZone) throws GeneralException, Exception, OfferManagementException {
		
		// New code block to execute processItems() for applying strategies
		List<PRItemDTO> itemListWithLIG = processItems(itemDataMap, recommendationRunHeader, retLirMap, retLirConstraintMap, compIdMap,
				multiCompLatestPriceMap, finalLigMap, leadZoneDetails, isRecommendAtStoreLevel, curCalDTO);
		
		// After this. All items are recommended with "Use guidelines and Constraints" objective.
		// No prediction or average movement is considered till here for price determination

		// clear all other price points and have current price point by checking recommendation rule
		clearPricePointsWhenCurrentPriceRetained(itemListWithLIG, recommendationRuleMap, itemZonePriceHistory, recWeekStartDate);

		// Add price Points rule filter to reduce price points
		// filterPricePointsbyCheckingRules(itemListWithLIG, recommendationRuleMap, curCalDTO, itemZonePriceHistory);
		
		double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));

		// Call prediction, it find's prediction for all the price points
		// (including current price) of all the items
		// for price group related items, it finds all possible price point that can
		// become recommended price based on its parent price range
		// and find prediction for those price points
		logger.info("Reg price prediction is started..");
		// Zone level prediction
		predictionComponent.predictRegPricePoints(conn, itemDataMap, itemListWithLIG, inputDTO, productGroupProperties, recommendationRunHeader,
				executionTimeLogs, isOnline, retLirMap, predictionService);
		logger.info("Reg price prediction is completed..");

		// Find sale predictions
		// NU:: 18th Oct 2016, Call prediction for sale prices
		// logger.info("Sale prediction is started..");
		// predictionComponent.predictPromoItems(conn, executionTimeLogs, recommendationRunHeader, itemListWithLIG, isOnline);
		// logger.info("Sale prediction is completed..");

		logger.info("Apply actual objective is started..");
		// Now pick price points again based on the actual objective
		new ObjectiveService().applyObjectiveAndSetRecPrice(itemListWithLIG, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		logger.info("Apply actual objective is completed..");

		
		// Apply lig constraint
		HashMap<Integer, List<PRItemDTO>> ligMap = new PricingEngineService().formLigMap(itemDataMap);
		new LIGConstraint().applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);

		// Recommended again all the related items - Apply LIG Constraint invoked inside for the third time.
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithLIG, itemDataMap, compIdMap, retLirConstraintMap,
				multiCompLatestPriceMap, curWeekStartDate, leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// update item level attributes like current and recommended price prediction
		new PricingEngineService().updateItemAttributes(itemListWithLIG, recommendationRunHeader);

	
		// apply lig constraint
		new LIGConstraint().applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);

		// NU:: 13th Dec 2016, cur price has to be retained, in certain cases. - Apply LIG Constraint invoked inside for the fifth time.
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		logger.debug("Retaining current prices starts...");
		priceRollbackService.retainCurrentRetail(itemListWithLIG, recommendationRunHeader, saleDetails, adDetails, priceZoneStores.size(),
				itemDataMap, compIdMap, retLirConstraintMap, multiCompLatestPriceMap, recWeekStartDate, leadZoneDetails, isRecommendAtStoreLevel,
				itemZonePriceHistory, curCalDTO, maxUnitPriceDiff, recommendationRuleMap);
		logger.debug("Retaining current prices ends...");

		// Added for AZ to recommend pending retails if it is present
		logger.info("Retaining pending retails starts...");
		priceRollbackService.setPendingRetail(itemListWithLIG);

		updateLIGAsRecommended(itemDataMap, retLirMap);

		// mark final objective applied to False so that LIg constraint is not applied
		// again
		ligMap.forEach((locationId, members) -> {
			members.forEach(item -> {
				item.setFinalObjectiveApplied(false);
			});

		});
		logger.info("Final Objective reset completed");
		// Calculate price change impact
		calculatePriceChangeImpact(itemDataMap, retLirMap, isGlobalZone);

		new SecondaryZoneRecService().applyRecommendationForSecondaryZones(itemDataMap, retLirMap, false);

		// apply lig constraint
		new LIGConstraint().applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);

		return itemListWithLIG;
	}

	private void addNotifications(long runId, Integer notificationTypeId) throws SQLException, GeneralException {
		NotificationService notificationService = new NotificationService();
		List<NotificationDetailInputDTO> notificationDetailDTOs = new ArrayList<NotificationDetailInputDTO>();
		NotificationDetailInputDTO notificationDetailInputDTO = new NotificationDetailInputDTO();
		notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
		notificationDetailInputDTO.setNotificationTypeId(notificationTypeId);
		notificationDetailInputDTO.setNotificationKey1(runId);
		notificationDetailDTOs.add(notificationDetailInputDTO);
		notificationService.addNotificationsBatch(getConnection(), notificationDetailDTOs, true);
	}

	private void recommendationAnalysis(long runId, HashMap<Long, List<PRItemDTO>> runAndItsRecommendedItems, HashMap<ItemKey, PRItemDTO> itemDataMap,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails) throws SQLException, GeneralException {
		RecommendationAnalysis recommendationAnalysis = new RecommendationAnalysis();
		recommendationAnalysis.recommendationAnalysis(getConnection(), runId, runAndItsRecommendedItems, itemDataMap, previousCalendarDetails);
	}

	private void callAuditTrail(int locationLevelId, int locationId, int productLevelId, int productId, int auditTrailTypeId, int recStatusId, long runId,
			String userId,int auditType, int auditSubType, int auditSubStatus) throws GeneralException {
		AuditTrailService auditTrailService = new AuditTrailService();
		auditTrailService.auditRecommendation(conn, locationLevelId, locationId, productLevelId, productId, auditTrailTypeId, recStatusId, runId,
				userId,auditType,auditSubType,auditSubStatus,0,0,0);
	}

	private void callAudit(PRRecommendationRunHeader recommendationRunHeader) throws GeneralException {
		// Calling Audit process after pricing recommendation is successful
		AuditEngineWS auditEngine = new AuditEngineWS();
		auditEngine.isIntegratedProcess = true;

		long reportId = auditEngine.getAuditHeader(conn, recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(),
				recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(), recommendationRunHeader.getPredictedBy(),"");
		logger.info("Audit is completed for report id - " + reportId);

	}

	private void getNonCachedStoreItems(boolean isDataRetrievedFromCache, HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreNotInCache) {
		if (!isDataRetrievedFromCache) {
			for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> outEntry : itemDataMapStore.entrySet()) {
				for (Map.Entry<ItemKey, PRItemDTO> inEntry : outEntry.getValue().entrySet()) {
					HashMap<ItemKey, PRItemDTO> tMap = null;
					if (itemDataMapStoreNotInCache.get(outEntry.getKey()) != null) {
						tMap = itemDataMapStoreNotInCache.get(outEntry.getKey());
					} else {
						tMap = new HashMap<ItemKey, PRItemDTO>();
					}
					tMap.put(inEntry.getKey(), inEntry.getValue());
					itemDataMapStoreNotInCache.put(outEntry.getKey(), tMap);
				}
			}
		}
	}

	public void setLigProcessedStatus(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap, HashMap<Integer, List<PRItemDTO>> finalLigMap,
			List<PRItemDTO> processedItems, List<PRItemDTO> nonProcessedLigList) {
		// if (retLirMap.size() > 0 && retLirConstraintMap.size() > 0) {
		HashMap<Integer, List<PRItemDTO>> ligMap = new HashMap<Integer, List<PRItemDTO>>();
		// Loop items whose price is recommended
		for (PRItemDTO itemDTO : processedItems) {
			// logger.debug("Item code:" + itemDTO.getItemCode());
			int retLirId = itemDTO.getRetLirId();
			boolean lirItemNotProcessed = false;
			if (retLirMap.get(retLirId) != null) {
				List<PRItemDTO> lirItemList = retLirMap.get(retLirId);
				// for (ArrayList<Integer> tlirItemList : lirItemList) {
				// Loop each lig member of a lig
				for (PRItemDTO lirItem : lirItemList) {
					ItemKey itemKey = PRCommonUtil.getItemKey(lirItem);
					// logger.debug("lig member:" + lirItem);
							//logger.debug("Processed Status:" + (itemDataMap.get(itemKey) != null ? itemDataMap.get(itemKey).isProcessed() : ""));
					// Check if that item is sold in store /zone
					if (itemDataMap.get(itemKey) != null && !itemDataMap.get(itemKey).isProcessed()) {
						// If any one item in a group is not processed
						logger.debug("Ret Lir Id " + retLirId + "Item Code " + lirItem.getItemCode() + "not processed");
						lirItemNotProcessed = true;
					} else if (itemDataMap.get(itemKey) == null) {
						logger.debug("Unauthorized item in lir list - " + lirItem.getItemCode());
					}
				}
				// }
			}

			if (!lirItemNotProcessed) {
				// LIG constraint is applied for items in ligMap.
				// Recommended price at LIG level
				// might be used in case of dependent items
				if (ligMap.get(itemDTO.getChildLocationId()) != null) {
					List<PRItemDTO> tList = ligMap.get(itemDTO.getChildLocationId());
					tList.add(itemDTO);
					ligMap.put(itemDTO.getChildLocationId(), tList);
				} else {
					List<PRItemDTO> tList = new ArrayList<PRItemDTO>();
					tList.add(itemDTO);
					ligMap.put(itemDTO.getChildLocationId(), tList);
				}

				// finalLigMap gets populated simultaneously. Used with applyLIGConstraint is called for the last time
				if (finalLigMap.get(itemDTO.getChildLocationId()) != null) {
					List<PRItemDTO> tList = finalLigMap.get(itemDTO.getChildLocationId());
					tList.add(itemDTO);
					finalLigMap.put(itemDTO.getChildLocationId(), tList);
				} else {
					ArrayList<PRItemDTO> tList = new ArrayList<PRItemDTO>();
					tList.add(itemDTO);
					finalLigMap.put(itemDTO.getChildLocationId(), tList);
				}
			} else {
				// If any one item in a group is not processed add the item to the list
				nonProcessedLigList.add(itemDTO);
			}
		}
		// applyLIGConstraint(ligMap, itemDataMap, retLirMap, retLirConstraintMap);
		applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);
	}

	public List<PRItemDTO> processItems(HashMap<ItemKey, PRItemDTO> itemDataMap, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<Integer, LocationKey> compIdMap, HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap,
			HashMap<Integer, List<PRItemDTO>> finalLigMap, HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails,
			boolean isRecommendAtStoreLevel, RetailCalendarDTO curCalDTO)
			throws GeneralException, Exception, OfferManagementException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		ObjectiveService objectiveService = new ObjectiveService();
		List<PRItemDTO> nonProcessedLigList = new ArrayList<PRItemDTO>();
		int loopThresholdCnt = 0;
		int loopThreshold = Integer.parseInt(PropertyManager.getProperty("PR_WHILE_LOOP_THRESHOLD"));
		while (true) {
			loopThresholdCnt++;
			HashMap<ItemKey, PRItemDTO> procItemDataMap = new HashMap<ItemKey, PRItemDTO>();
			for (Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()) {
				if (!entry.getValue().isProcessed()) {
					// Add not processed items
					procItemDataMap.put(entry.getKey(), entry.getValue());
				}
			}
			// When all items are processed
			if (procItemDataMap.size() == 0)
				break;

			logger.info("Number of Items to process - " + procItemDataMap.size());

			long tStartTime = System.currentTimeMillis();
			// Apply Guidelines and Constraints for all items
			ArrayList<PRItemDTO> itemPriceRangeList = applyStrategies(procItemDataMap, itemDataMap, compIdMap, recommendationRunHeader.getRunId(),
					retLirConstraintMap, multiCompLatestPriceMap, curCalDTO.getStartDate(), leadZoneDetails, isRecommendAtStoreLevel,
					recommendationRunHeader);
			long tEndTime = System.currentTimeMillis();
			logger.info("^^^ Time -- Applying Strategies(applyStrategies) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");

			// Invokes prediction component and applies objectives to come up with recommended price for items
			tStartTime = System.currentTimeMillis();
//			predictionComponent.predictPrice(getConnection(), itemPriceRangeList, inputDTO, productGroupProperties,
//					executionTimeLogs, recommendationRunHeader, isOnline, retLirMap, itemZonePriceHistory, curCalDTO);

			//initially followrules objective is applied, later actual objectives will be applied
			logger.info("applying guidelines is started...");
			objectiveService.applyFollowRulesAndSetRecPrice(itemPriceRangeList);
			logger.debug("applying guidelines is completed...");

			tEndTime = System.currentTimeMillis();
			logger.info("^^^ Time -- Prediction(predictPrice) --> " + ((tEndTime - tStartTime) / 1000) + " s ^^^");

			/* nonProcessedLigList contains LIG items for which guidelines and constraints are applied but other members in its group are yet to be processed.
			 * This will happen when few items from a LIG occur before dependent item and few items occur after dependent item in the list being processed.
			 * This might not be needed when map being processed is sorted by ret_lir_id (Needs testing to confirm)
			 */
			itemPriceRangeList.addAll(nonProcessedLigList);
			nonProcessedLigList = new ArrayList<PRItemDTO>();

			logger.debug("Finding if any lig member yet to be processed starts");
			logger.debug("RetLirMap Size:" + retLirMap.size() + ",RetLirConstraintMap Size:" + retLirConstraintMap.size());

			logger.debug("finalLigMap Size before applying LIG: " + finalLigMap.size());
			setLigProcessedStatus(itemDataMap, retLirMap, retLirConstraintMap, finalLigMap, itemPriceRangeList, nonProcessedLigList);

			logger.debug("Non Processed List Count:" + nonProcessedLigList.size());
			logger.debug("Finding if any lig member yet to be processed completes");

			// This is added to avoid the while loop keep running
			if (loopThresholdCnt > loopThreshold) {
				throw new OfferManagementException("Infinite While Loop, Program Terminated ", RecommendationErrorCode.INFINITE_LOOP);
			}

		} // while loop

		for (Map.Entry<Integer, List<PRItemDTO>> outEntry : finalLigMap.entrySet()) {
			// Set conflicts based on recommended price
			for (PRItemDTO item : outEntry.getValue()) {
				// Ignore LIG Item, as it representation any way would have
				// updated the conflicts
				if (!item.isLir()) {
					pricingEngineService.updateConflicts(item);
				}
			}
		}

		// Handle items with same retailer item code
		// Group by retailer item code
		//Consider retailer item code appears more than once and there is different recommended price
		// pick one random item which has price group relation
		//If none of the item has price group relation, then pick item with highest 13 week avg movement 
		//(if more than one item has same 13 week avg movement, then pick random item). If no item has 13 week avg movement, 
		// then also pick random item

		List<PRItemDTO> finalRecList = applyLIGConstraint(finalLigMap, itemDataMap, retLirConstraintMap);
		finalLigMap.clear();
		return finalRecList;
	}

	private void updateZoneInfoWithCommonStoreInfo(HashMap<ItemKey, PRItemDTO> itemDataMap, 
			HashMap<Integer, List<PRItemDTO>> storeAndItsItems,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap){
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		//ArrayList<PRItemDTO> processedZoneItems = new ArrayList<PRItemDTO>();
		if (storeAndItsItems.size() > 0) {
			// loop through each zone item(lig members and non lig)
			for (PRItemDTO zoneItem : itemDataMap.values()) {
				
//				if (zoneItem.getItemCode() == 2591 || zoneItem.getItemCode() == 113704) {
//					logger.debug("stop log:" + zoneItem.getItemCode() + "-" + zoneItem.getRetLirId() + "-" + zoneItem.getRetLirItemCode());
//				}
				
				if (!zoneItem.isLir()) {
					List<PRItemDTO> correspondingStoreItems = new ArrayList<PRItemDTO>();
					Collection<PRItemDTO> storeItemsWithMostCommonRecRegPrice = new ArrayList<PRItemDTO>();
					PRItemDTO choosenStoreItemForZoneRepresentation = null;
//					double avgMovement = 0;
//					double avgRevenue = 0;
//					double predictedMovement = 0;
//					double curRegPricePredictedMovement = 0;

					// Get the corresponding item from all stores
					for (List<PRItemDTO> storeItems : storeAndItsItems.values()) {
						for (PRItemDTO storeItem : storeItems) {
							if (zoneItem.getItemCode() == storeItem.getItemCode() && zoneItem.isLir() == storeItem.isLir()) {
								correspondingStoreItems.add(storeItem);
								break;
							}
						}
					}
					/**
					 * How Zone Level Data is Picked 1. Find max occurrence price (First price of distinct price counts,
					 * sorted by its total movement in descending) 2. Get all items with max occurrence price 3. Other
					 * Data's Get First X of distinct X counts from above item list(2), sorted by its total movement in
					 * descending 4. Still it there the value of X is null, then Get First X of distinct X counts from
					 * full item list, sorted by its total movement in descending
					 */

					if (correspondingStoreItems.size() > 0) {
						//double price = (double) mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "RecRegPrice");
						Object object =  mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "RecRegPrice");
						double price = Constants.DEFAULT_NA;
						if(object != null){
							MultiplePrice multiplePrice = (MultiplePrice) object;							
							price = multiplePrice.price;
						}
							

						for (PRItemDTO storeItem : correspondingStoreItems) {
//							if (storeItem.getRecommendedRegPrice() != null && storeItem.getRecommendedRegPrice() == price) {
							if (storeItem.getRecommendedRegPrice() != null && storeItem.getRecommendedRegPrice().price == price) {
								// Take last occurrence
								choosenStoreItemForZoneRepresentation = storeItem;
								storeItemsWithMostCommonRecRegPrice.add(storeItem);
							}
							
							//10th Mar 2017, sum of store avg mov or rev, will not match with zone avg or rev
							//so don't sum it up
							//avgMovement = avgMovement + storeItem.getAvgMovement();
//							avgRevenue = avgRevenue + storeItem.getAvgRevenue();
							
							//10th Mar 2017, don't sum store prediction to come up with zone prediction
							//as there is no store level prediction, let zone level prediction
							//be used (either prediction or avg mov)
//							if (storeItem.getCurRegPricePredictedMovement() != null)
//								curRegPricePredictedMovement = curRegPricePredictedMovement + storeItem.getCurRegPricePredictedMovement();
//							if (storeItem.getPredictedMovement() != null)
//								predictedMovement = predictedMovement + storeItem.getPredictedMovement();
						}

						//Still If there is no representing item (this may happen when none of the store has recommended price)
						if(choosenStoreItemForZoneRepresentation == null){
							//pick the first item
							choosenStoreItemForZoneRepresentation = correspondingStoreItems.get(0);
						}
						
						//If all the store recommendation is null, the update zone as null
//						if(price != Constants.DEFAULT_NA)
//							zoneItem.setRecommendedRegPrice(price);
//						else
//							zoneItem.setRecommendedRegPrice(null);
//						
//						zoneItem.setRecommendedRegMultiple(choosenStoreItemForZoneRepresentation.getRecommendedRegMultiple());
						
						if(price != Constants.DEFAULT_NA) {
//							zoneItem.setRecommendedRegMultiple(choosenStoreItemForZoneRepresentation.getRecommendedRegMultiple());
							zoneItem.setRecommendedRegPrice(
									new MultiplePrice(choosenStoreItemForZoneRepresentation.getRecommendedRegPrice().multiple, price));
						} else {
							zoneItem.setRecommendedRegPrice(null);
						}
							
						
						
						
						zoneItem.setExplainLog(choosenStoreItemForZoneRepresentation.getExplainLog());
						zoneItem.setStrategyId(choosenStoreItemForZoneRepresentation.getStrategyId());
						zoneItem.setIsConflict(choosenStoreItemForZoneRepresentation.getIsConflict());
						zoneItem.setIsPrePriced(choosenStoreItemForZoneRepresentation.getIsPrePriced());
						zoneItem.setShipperItem(choosenStoreItemForZoneRepresentation.isShipperItem());
						zoneItem.setIsLocPriced(choosenStoreItemForZoneRepresentation.getIsLocPriced());
						zoneItem.setDistFlag(choosenStoreItemForZoneRepresentation.getDistFlag());
//						zoneItem.setPredictionStatus(choosenStoreItemForZoneRepresentation.getPredictionStatus());
//						zoneItem.setCurRegPricePredictedMovement(curRegPricePredictedMovement);
//						zoneItem.setPredictedMovement(predictedMovement);
//						zoneItem.setAvgMovement(avgMovement);
//						zoneItem.setAvgRevenue(avgRevenue);

						updateZoneItemAttributeWithStoreItem(mostOccurrenceData, zoneItem, correspondingStoreItems,
								storeItemsWithMostCommonRecRegPrice);
						zoneItem.setIsMostCommonStorePriceRecAsZonePrice(true);
					}					
				}
			}
		}
	}
	
	private void updateZoneItemAttributeWithStoreItem(MostOccurrenceData mostOccurrenceData, PRItemDTO zoneItem, 
			List<PRItemDTO> correspondingStoreItems, Collection<PRItemDTO> storeItemsWithMostCommonRecRegPrice){
		//Size
		if((Double)mostOccurrenceData.getMaxOccurance(storeItemsWithMostCommonRecRegPrice, "ItemSize") == null){
			if((Double)mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "ItemSize") != null){
				zoneItem.setItemSize((double)mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "ItemSize"));
			}
		}else{
			zoneItem.setItemSize((double)mostOccurrenceData.getMaxOccurance(storeItemsWithMostCommonRecRegPrice, "ItemSize"));
		}
		 
		//UOM			
		zoneItem.setUOMName((String)mostOccurrenceData.getMaxOccurance(storeItemsWithMostCommonRecRegPrice, "UOM"));
		//If still null
		if(zoneItem.getUOMName() == null){
			zoneItem.setUOMName((String)mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "UOM"));
		}
		
		//Cur Reg Price
		zoneItem.setRegPrice((Double)mostOccurrenceData.getMaxOccurance(storeItemsWithMostCommonRecRegPrice, "CurRegPrice"));
		//If still null
		if(zoneItem.getRegPrice() == null){
			zoneItem.setRegPrice((Double)mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "CurRegPrice"));
		}
		
		//Pre Reg Price
		zoneItem.setPreRegPrice((Double)mostOccurrenceData.getMaxOccurance(storeItemsWithMostCommonRecRegPrice, "PreRegPrice"));
		//If still null
		if(zoneItem.getPreRegPrice() == null){
			zoneItem.setPreRegPrice((Double)mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "PreRegPrice"));
		}
		
		//Current Reg Eff Date
		zoneItem.setCurRegPriceEffDate((String)mostOccurrenceData.getMaxOccurance(storeItemsWithMostCommonRecRegPrice, "RegPriceEffDate"));
		//If still null
		if(zoneItem.getCurRegPriceEffDate() == null){
			zoneItem.setCurRegPriceEffDate((String)mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "RegPriceEffDate"));
		}
		
		LIGConstraint ligConstraint = new LIGConstraint();
		ligConstraint.updateErrorCode(zoneItem, correspondingStoreItems);
	}
	
	private void copyZoneCompPriceToStore(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore) {
		// Loop each store
		for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> stores : itemDataMapStore.entrySet()) {
			// Loop each item inside the store
			for (Map.Entry<ItemKey, PRItemDTO> storeItems : stores.getValue().entrySet()) {
				ItemKey itemKey = PRCommonUtil.getItemKey(storeItems.getValue());
				PRItemDTO zoneItem = itemDataMap.get(itemKey);
				// Check if item present in zone
				if (zoneItem != null) {
					PRItemDTO storeItem = storeItems.getValue();
					storeItem.setAllCompPrice(zoneItem.getAllCompPrice());
					storeItem.setAllCompPriceCheckDate(zoneItem.getAllCompPriceCheckDate());
					storeItem.setAllCompPreviousPrice(zoneItem.getAllCompPreviousPrice());
					storeItem.setAllCompPriceChgIndicator(zoneItem.getAllCompPriceChgIndicator());
					//logger.debug("Store price:" + (storeItem.getCompPrice() != null ? storeItem.getCompPrice().price : ""));
				}
				
			}
		}
	}
	
	//Copy the zone competitor map to store competitor map
	private HashMap<Integer, HashMap<Integer, LocationKey>> copyZoneCompMapToStore(HashMap<Integer, LocationKey> compIdMapOfZone,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreNotInCache) {
		HashMap<Integer, HashMap<Integer, LocationKey>> compIdMapStore = new HashMap<Integer, HashMap<Integer, LocationKey>>();
		// Loop each store item
		for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> entry : itemDataMapStore.entrySet()) {
			// check if comp details is already found for a store
			if (compIdMapStore.get(entry.getKey()) == null) {
				// Copy zone comp map
				compIdMapStore.put(entry.getKey(), compIdMapOfZone);
			}
		}
		// Loop each store item (not cached)
		for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> entry : itemDataMapStoreNotInCache.entrySet()) {
			// check if comp details is already found for a store
			if (compIdMapStore.get(entry.getKey()) == null) {
				// Copy zone comp map
				compIdMapStore.put(entry.getKey(), compIdMapOfZone);
			}
		}

		// Debug log
//		for(Map.Entry<Integer, HashMap<Character, Integer>> stores : compIdMapStore.entrySet()){
		// logger.debug("Competitors for Store: " + stores.getKey());
//			for(Map.Entry<Character, Integer> competitors : stores.getValue().entrySet()){
//				logger.debug("Competitor Store: " + competitors.getKey() + "-" + competitors.getValue());
		// }
		// }

		return compIdMapStore;
	}
	
	private void markItemWhoseZoneAndStorePriceIsSame(List<PRItemDTO> zoneItems, 
			HashMap<Integer, List<PRItemDTO>> storeItems) throws OfferManagementException {

		HashMap<ItemKey, PRItemDTO> zoneItemMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<ItemKey, List<Double>> storeItemMap = new HashMap<ItemKey, List<Double>>();
		
		try {
			ItemKey itemKey = null;
			// Convert zoneItems to a hashmap with itemcode as key and its ItemDTO as value
			for (PRItemDTO item : zoneItems) {
				itemKey = PRCommonUtil.getItemKey(item.getItemCode(), item.isLir());
				zoneItemMap.put(itemKey, item);
			}
			List<Double> storeItemPrices = new ArrayList<Double>();
			// Convert storeItems to a hashmap with itemcode as key and all its
			// store level price as value in a list
			for (List<PRItemDTO> itemList : storeItems.values()) {
				for (PRItemDTO item : itemList) {
					itemKey = PRCommonUtil.getItemKey(item.getItemCode(), item.isLir());
					//If item is already present
					if(storeItemMap.get(itemKey) != null){
						storeItemPrices = storeItemMap.get(itemKey);
//						storeItemPrices.add(item.getRecommendedRegPrice());
						//storeItemPrices.add(item.getRecommendedRegPrice().price);
						storeItemPrices.add((item.getRecommendedRegPrice() != null ? item.getRecommendedRegPrice().price : null));
					}else{
						storeItemPrices = new ArrayList<Double>();
//						storeItemPrices.add(item.getRecommendedRegPrice());
//						storeItemPrices.add(item.getRecommendedRegPrice().price);
						storeItemPrices.add((item.getRecommendedRegPrice() != null ? item.getRecommendedRegPrice().price : null));
						storeItemMap.put(itemKey, storeItemPrices);
					}
				}
			}

			// Loop store items
			for (Map.Entry<ItemKey, List<Double>> storeItem : storeItemMap.entrySet()) {
				// Get corresponding zone item
				PRItemDTO zoneItem = zoneItemMap.get(storeItem.getKey());
				// If item is present in zone 
				if (zoneItem != null) {
					// if price is recommended 
					// zone item will be null, only when all the store is null
//					Double zonePrice = zoneItem.getRecommendedRegPrice();
					Double zonePrice = (zoneItem.getRecommendedRegPrice() != null ? zoneItem.getRecommendedRegPrice().price : null);
					if (zonePrice != null) {
						// logger.debug("**** Item Code:" +
						// zoneItem.getItemCode() + ",Zone Price: " + zonePrice
						// + " ****");
						boolean isPriceSame = true;
						// Loop all store prices of the item
						for (Double storePrice : storeItem.getValue()) {
							// Check if those prices are same as zone
							if (storePrice != null) {
								// logger.debug("Item Code:" + zoneItem.getItemCode() + ",Store Price: " + storePrice);
								if (!zonePrice.equals(storePrice)) {
									// logger.debug("Zone and Store Price is Different");
									isPriceSame = false;
									break;
								} else {
									// logger.debug("Zone and Store Price is Same");
								}
							}
						}
						zoneItem.setIsZoneAndStorePriceSame(isPriceSame);
					}
					else{
						//Both zone and store prices will be null
						zoneItem.setIsZoneAndStorePriceSame(true);
					}
				}
			}
		} catch (Exception e) {
			throw new OfferManagementException("Error in markItemWhoseZoneAndStorePriceIsSame() - " + e, 
					RecommendationErrorCode.MARK_ITEM_WHOSE_ZONE_STORE_PRICE_SAME);
		}
	}
	
	//Recommend Order Code Price for each product --> location
	private void recommendOrderCodePrice(Connection conn, long runId, List<ProductDTO> productList,
			int locationLevelId, List<Integer> locationIds, List<PRProductGroupProperty> productGroupProperties,
			List<PRItemDTO> itemListWithRecPrice, RetailCalendarDTO curCalDTO, PRStrategyDTO inputDTO) throws OfferManagementException{
		logger.info("Order Code Price Recommendation is Started...");
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		try {
			List<PROrderCode> orderCodesToInsert = new ArrayList<PROrderCode>();
			List<PROrderCode> allOrderCodes = new ArrayList<PROrderCode>();
			// Loop through each product
			for (ProductDTO product : productList) {
				// Get Product Property
				PRProductGroupProperty prProductGroupProperty = PricingEngineHelper.getProductGroupProperty(productGroupProperties,
						product.getProductLevelId(), product.getProductId());
				// Check if it is perishable and order_sell_code is enabled
				//15th Jan 2014, removed to check isOrderSellCode as Anand suggested
				//if (prProductGroupProperty.getIsPerishable() && prProductGroupProperty.getIsOrderSellCode()) {
				if (prProductGroupProperty.getIsPerishable()) {
					// Get order code and sell code of the product
					//allOrderCodes = pricingEngineDAO.getOrderAndSellCode(conn, product.getProductLevelId(), product.getProductId());
					// Uncomment this to return only sell codes of order codes which has minimum sell code value under this product
					
					
					//Dinesh:: 10th Apr 2018, changes to consider only latest table structure and to get order and sell code 
					//based on Relation Header table
					/*//Dinesh:: 04th Dec 2017. Changes done to latest table structure and to get order and sell code based on Relation Header table
					boolean getLatestOrderAndSellCode = Boolean
							.parseBoolean(PropertyManager.getProperty("GET_ORDER_AND_SELL_CODE_GENERIC_TABLES", "FALSE"));
					if (getLatestOrderAndSellCode) {
						allOrderCodes = pricingEngineDAO.getOrderAndSellCode(conn, product.getProductLevelId(), product.getProductId(), curCalDTO,
								inputDTO.getLocationId());
					} else {
						allOrderCodes = pricingEngineDAO.getOrderAndSellCode(conn, product.getProductLevelId(), product.getProductId());
					}*/
					
					allOrderCodes = pricingEngineDAO.getOrderAndSellCode(conn, product.getProductLevelId(), product.getProductId(), curCalDTO,
							inputDTO.getLocationId());
					// Call Recommend order code price
					calculateOrderCodePrice(orderCodesToInsert, locationLevelId, locationIds, allOrderCodes, itemListWithRecPrice);
				}			
			}
			
			if (orderCodesToInsert.size() > 0) {				
				// Insert order code price
				pricingEngineDAO.insertOrderCodeRecommendation(conn, runId, orderCodesToInsert);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.equals("Error in recommendOrderCodePrice"+ex);
			throw new OfferManagementException("Error in recommendOrderCodePrice() - " + ex, 
					RecommendationErrorCode.PROCESS_ORDER_CODE_REC);
		}
		logger.info("Order Code Price Recommendation is Completed...");
	}

	// Recommend order code price
	private void calculateOrderCodePrice(List<PROrderCode> orderCodesToInsert, int locationLevelId, 
			List<Integer> locationIds, List<PROrderCode> allOrderCodes, List<PRItemDTO> itemListWithRecPrice){	 

		// Loop through each location
		for (Integer locationId : locationIds) {
			// Loop through each Order Code
			for (PROrderCode inputOrderCode : allOrderCodes) {
				
				//Whether to process this order code or not
				double orderCodePrice = 0d;
				//double yieldTotal = 0d;
				double sellCodePrice = 0d;
				//Double yield = null;
				Double yield = 0d;
				//logger.debug("Order Item Code:" + inputOrderCode.getOrderItemCode());
				// Loop through each Sell Code
				for (PRSellCode sellCode : inputOrderCode.getSellCodes()) {
					//logger.debug("  Sell Code:" + sellCode.getSellCode() + ",Yield: " + sellCode.getYield());
					// Check if Sell Code has recommendation and its location is matching
					for (PRItemDTO itemDTO : itemListWithRecPrice) {
						if (itemDTO.getItemCode() == sellCode.getItemCode() && itemDTO.getChildLocationLevelId() == locationLevelId
								&& itemDTO.getChildLocationId() == locationId && itemDTO.getRecommendedRegPrice() != null) {
							// Find order price price based on recommended price and yield %
//							double sellMulByYield = (itemDTO.getRecommendedRegPrice() * 
//									((sellCode.getYield() == null ? 0 : sellCode.getYield()) / 100));
							double sellMulByYield = (itemDTO.getRecommendedRegPrice().price * 
									((sellCode.getYield() == null ? 0 : sellCode.getYield()) / 100));
							//yieldTotal = yieldTotal + (sellCode.getYield() / 100);
							orderCodePrice = orderCodePrice + sellMulByYield;
							/*
							 * logger.debug("  	Rec Price:" +
							 * PRCommonUtil.getPriceForLog(itemDTO.getRecommendedRegPrice()) +
							 * ",Sell Multiplied By Yield:" + sellMulByYield);
							 */
							
							//Update 1st occurrence of sell code price
							if(sellCodePrice == 0)
//								sellCodePrice = itemDTO.getRecommendedRegPrice();
								sellCodePrice = itemDTO.getRecommendedRegPrice().price;
							
							//Find if all the yield is null
							if(yield == 0)
								yield = sellCode.getYield();
							break;
						}					
					}					
				}
				
				//If all the yield of sell code is null, then updated order code price 
				//as one of the sell code's recommended price (first occurrence of sell code price)
				if(yield == 0)
					orderCodePrice = sellCodePrice;
				
				//logger.debug("orderCodePrice:" + orderCodePrice);
				// Add to orderCodes list if order code price is > 0
				if(orderCodePrice > 0){
					PROrderCode orderCode = new PROrderCode();
					orderCode.setLocationLevelId(locationLevelId);
					orderCode.setLocationId(locationId);
					orderCode.setOrderItemCode(inputOrderCode.getOrderItemCode());
					orderCode.setFreshRelationHeaderId(inputOrderCode.getFreshRelationHeaderId());
					orderCode.setRecommendedRegMultiple(1);
					orderCode.setRecommendedRegPrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(orderCodePrice)));
					orderCodesToInsert.add(orderCode);
				}
			}
		}
	}
		
	public void applyMultiCompRetails(Connection conn, HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap,
			RetailCalendarDTO calDTO, int productLevelId, int productId, HashMap<ItemKey, PRItemDTO> itemDataMap)
					throws OfferManagementException {
		// Get unique comp id's and max day
		HashMap<Integer, String> distinctCompIds = new HashMap<Integer, String>();
		int maxLastObsDay;
		HashMap<Integer, HashMap<Integer, CompetitiveDataDTO>> multiCompDataMap = new HashMap<>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

		try {
			getDistinctCompId(strategyMap, distinctCompIds);
			// getDistinctCompId(strategyMapStore, distinctCompIds);
			if(distinctCompIds.size() > 0) {
				maxLastObsDay = getMaxLastObsDay(strategyMap);

				HashMap<Integer, Integer> compChainIdMap = new LocationCompetitorMapDAO().getCompChainId(conn,
						distinctCompIds.keySet());

				// Get competition data
				multiCompDataMap = pricingEngineDAO.getLatestCompPrice(conn, distinctCompIds, productLevelId, productId,
						calDTO.getStartDate(), maxLastObsDay);

				for (Map.Entry<Integer, HashMap<Integer, CompetitiveDataDTO>> compStoreEntry : multiCompDataMap
						.entrySet()) {

					int compChainId = compChainIdMap.get(compStoreEntry.getKey());
					pricingEngineDAO.setCompPriceData(itemDataMap, compStoreEntry.getValue(), compStoreEntry.getKey(),
							compChainId);

				}
			}
		} catch (GeneralException | Exception e) {
			throw new OfferManagementException("Error in getLatestCompDataForMultiComp() - " + e,
					RecommendationErrorCode.DB_MULTI_COMPETITOR_DATA);
		}
	}
	 
	private void getDistinctCompId(HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<Integer, String> distinctCompIds) {
		if (strategyMap != null) {
			// Loop Strategies
			for (List<PRStrategyDTO> strategies : strategyMap.values()) {
				for (PRStrategyDTO strategyDTO : strategies) {
					// Is there competitor guideline
					if (strategyDTO.getGuidelines().getCompGuideline() != null) {
						List<PRGuidelineCompDetail> competitorDetails = strategyDTO.getGuidelines().getCompGuideline()
								.getCompetitorDetails();
						// Is there any competitor
						if (competitorDetails.size() > 0) {
							// Loop all competitor
							for (PRGuidelineCompDetail compDetail : competitorDetails) {
								// Is Valid competition
								if (compDetail.getCompStrId() > 0) {
									// Not already there
									if (distinctCompIds.get(compDetail.getCompStrId()) == null) {
										distinctCompIds.put(compDetail.getCompStrId(), "");
									}
								}
							}
						}
					}
					
					if (strategyDTO.getConstriants().getGuardrailConstraint() != null) {

						List<PRConstraintGuardRailDetail> compDetails = strategyDTO.getConstriants()
								.getGuardrailConstraint().getCompetitorDetails();
						for (PRConstraintGuardRailDetail comp : compDetails) {
							if (comp.getCompStrId() > 0) {
								if (distinctCompIds.get(comp.getCompStrId()) == null) {
									distinctCompIds.put(comp.getCompStrId(), "");
								}
							}
						}
					}
					
					// Is there any PI Guideline
					if (strategyDTO.getGuidelines().getPiGuideline() != null) {
						List<PRGuidelinePI>  piGuidelines=strategyDTO.getGuidelines().getPiGuideline();
								
						
						if (piGuidelines .size() > 0) {
							// Loop all competitor
							for (PRGuidelinePI piDetail : piGuidelines) {
								// Is Valid competition
								if (piDetail.getCompStrId() > 0) {
									// Not already there
									if (distinctCompIds.get(piDetail.getCompStrId()) == null) {
										distinctCompIds.put(piDetail.getCompStrId(), "");
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	private int getMaxLastObsDay(HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap) {
		int maxLastObsDay = 0;

		// Loop Strategies
		for (List<PRStrategyDTO> strategies : strategyMap.values()) {
			for (PRStrategyDTO strategyDTO : strategies) {
				// Is there competitor guideline
				if (strategyDTO.getGuidelines().getCompGuideline() != null) {
					 PRGuidelineComp guidelelineComp = strategyDTO.getGuidelines().getCompGuideline();
					 if(guidelelineComp.getLatestPriceObservationDays() > maxLastObsDay)						 
						 maxLastObsDay = guidelelineComp.getLatestPriceObservationDays();
				}
			}
		}
		return maxLastObsDay;
	}
	
	/**
	 * Initializes connection. Used when program is accessed through webservice
	 * @throws GeneralException 
	 */
	protected void initializeForWS(int locationId, int productId) throws GeneralException {
		setConnection(getDSConnection());
		setLog4jProperties(locationId, productId);
		logger.info("Connection : " + getConnection());
	}
	
	/**
	 * Returns Connection from datasource
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));;
		try{
			if(ds == null){
				initContext = new InitialContext();
				envContext  = (Context)initContext.lookup("java:/comp/env");
				ds = (DataSource)envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		}catch(NamingException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(SQLException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(Exception ex){
			logger.error("Error when creating connection from datasource " + ex.toString());
		}
		return connection;
	}
	
	protected Connection getConnection(){
		return conn;
	}
	
	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection(){
		if(conn == null){
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	protected void setConnection(Connection conn){
		this.conn = conn;
	}
	
	/**
	 * Initializes connection
	 * @throws GeneralException 
	 */
	protected void initialize() throws GeneralException{
		setConnection();
		setLog4jProperties(getBatchLocationId(), getBatchProductId());
	}
	
	/**
	 * Returns calendar id of end week if we have crossed the end week specified in the input
	 * Returns last week calendar id if end week is not specified in the input. This will be the calendar id for which
	 * price, cost, movement information will be available
	 * @param conn
	 * @param inputDTO
	 * @return
	 * @throws GeneralException
	 */
	public RetailCalendarDTO getLatestWeek(Connection conn, PRStrategyDTO inputDTO, RetailCalendarDAO retailCalendarDAO) throws GeneralException{
		RetailCalendarDTO calDTO = null;
		
		long minPriceRecordCnt = Integer.parseInt(PropertyManager.getProperty("PR_MIN_PRICE_RECORD", "200000"));
		
		//Start and end calendar id will be > 0 when recommendation is running for next week by default
		if(inputDTO.getStartCalendarId() > 0){
			RetailCalendarDTO startCalDTO = retailCalendarDAO.getCalendarDetail(conn, inputDTO.getStartCalendarId());
			RetailCalendarDTO endCalDTO = null;
			if(inputDTO.getEndCalendarId() > 0){
				endCalDTO = retailCalendarDAO.getCalendarDetail(conn, inputDTO.getEndCalendarId());
				endCalDTO.setCalendarId(inputDTO.getEndCalendarId());
			}
			
			//Start Date of Week for which recommendation is running
			Date inputStartDate = DateUtil.toDate(startCalDTO.getStartDate());
			//Previous week from current week
			Date lastWeekStartDate = DateUtil.toDate(DateUtil.getWeekStartDate(1));
			if(endCalDTO != null){
				Date inputEndDate = DateUtil.toDate(endCalDTO.getEndDate());
				//if Recommendation week start date is lesser than or equal to last week start date
				//i.e. if the recommendation is for past week
				if(PrestoUtil.compareTo(inputStartDate, lastWeekStartDate) <=0){
					//If recommendation week end date is great than or equal to last week start date
					if(PrestoUtil.compareTo(lastWeekStartDate, inputEndDate) >= 0){
						calDTO = endCalDTO;
					}
				}
			}
		}

		if(calDTO == null){
			// Returns last week calendar id
			if(getBatchInputDate() == null)
				//Changed on 2nd July 2015, to first consider current week
				//calDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(1), Constants.CALENDAR_WEEK);
				calDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
			else
				calDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(DateUtil.toDate(getBatchInputDate()), 0), Constants.CALENDAR_WEEK);
		}
		
		logger.info("Latest Calendar Week " + calDTO.getStartDate() + "\t" + calDTO.getEndDate());
		
		// If retail price is not available in the latest week, change the latest week
		PricingEngineDAO peDAO = new PricingEngineDAO();
		int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_PRICE_THRESHOLD_IN_WEEKS"));
		List<RetailCalendarDTO> rcDTOList = retailCalendarDAO.getCalendarList(conn, calDTO.getCalendarId(), noOfWeeksBehind);
		long count = -1;
		int calendarIdWithPrice = -1;
		for(RetailCalendarDTO rcDTO : rcDTOList){
			int calendarId = rcDTO.getCalendarId();
			count = peDAO.isPriceExists(conn, calendarId, minPriceRecordCnt);
			if(count >= minPriceRecordCnt){
				calendarIdWithPrice = calendarId;
				break;
			}
		}
		
		logger.debug("calendar id with price " + calendarIdWithPrice);
		if(calendarIdWithPrice != calDTO.getCalendarId()){
			calDTO = retailCalendarDAO.getCalendarDetail(conn, calendarIdWithPrice);
			if(calDTO != null)
				logger.info("Latest Calendar Week With Price" + calDTO.getStartDate() + "\t" + calDTO.getEndDate());
		}
		
		return calDTO;
	}

	/**
	 * Iterates through list of items for which strategies have to be applied and invokes the actual method
	 * that applies strategy on an item
	 * @param procItemDataMap		Map containing items to be processed. Map containing Item code as key and item dto as its value
	 * @param itemDataMap			Master map containing all items. Map containing Item code as key and item dto as its value
	 * Change made to key on 06/02 for AZ
	 * @param compIdMap				Map containing competitor type (1,2,3,4) as key and Comp_Str_Id as value
	 * @param runId					Run Id
	 * @param pricingEngineDAO		Pricing Engine DAO
	 * @param retLirGuidelineMap	HashMap<Child_Location_Id, HashMap<Ret_Lir_Id, LIG Constraint(S-Same/D-Different)>>
	 * @return
	 * @throws Exception 
	 */
	private ArrayList<PRItemDTO> applyStrategies(HashMap<ItemKey, PRItemDTO> procItemDataMap, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, LocationKey> compIdMap, long runId, HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, String curWeekStartDate,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails, boolean isRecommendAtStoreLevel,
			PRRecommendationRunHeader recommendationRunHeader) throws Exception {
		ArrayList<PRItemDTO> prItemList = new ArrayList<PRItemDTO>();

		// Loop through each items
		for (PRItemDTO item : procItemDataMap.values()) {
//			logger.debug("*** Applying Strategy for Item: " + item.getItemCode() + " is Started ***");
			applyStrategies(item, prItemList, itemDataMap, compIdMap, runId, retLirConstraintMap, multiCompLatestPriceMap, curWeekStartDate,
					leadZoneDetails, isRecommendAtStoreLevel, recommendationRunHeader);
//			logger.debug("*** Applying Strategy for Item: " + item.getItemCode() + " is Completed***");
		}

		return prItemList;
	}
	
	public void applyStrategies(PRItemDTO item, List<PRItemDTO> prItemList, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, LocationKey> compIdMap, long runId, HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, String curWeekStartDate,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails, boolean isRecommendAtStoreLevel,
			PRRecommendationRunHeader recommendationRunHeader) throws Exception {

		ApplyStrategy applyStrategy = new ApplyStrategy();
		applyStrategy.applyStrategies(item, prItemList, itemDataMap, compIdMap, runId, retLirConstraintMap, multiCompLatestPriceMap, curWeekStartDate,
				leadZoneDetails, isRecommendAtStoreLevel, recommendationRunHeader);

	}
			

	/**
	 * Applies LIG constraint
	 * @param itemPriceRangeMap		Map containing Child Location Id as key and list of items for that location as value
	 * @param itemDataMap			Map containing all items under input product
	 * @param retLirMap				Map containing RET_LIR_ID as key and map containing RET_LIR_ITEM_CODE as key and items under that group as its value
	 * @param retLirConstraintMap	Map containing Child Location Id as key and map containing RET_LIR_ID and S-Same/D-Different LIG constraint as its value 
	 * @return
	 */
	/*
	 * Child Location Id was added as part of processing for prediction from strategy definition screen at location list level.
	 */
	private List<PRItemDTO> applyLIGConstraint(HashMap<Integer, List<PRItemDTO>> itemPriceRangeMap, 
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap) {
		List<PRItemDTO> finalItemList = new ArrayList<PRItemDTO>();
		LIGConstraint ligConstraint = new LIGConstraint();
		// apply LIG constraint first time as part of apply guidelines and constraints
		finalItemList = ligConstraint.applyLIGConstraint(itemPriceRangeMap, itemDataMap, retLirConstraintMap);
		return finalItemList;
	}

		
	/**
	 * Main Method - Gets invoked when the program runs in Batch mode
	 * @param args
	 */
	public static void main(String[] args) {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
		PropertyManager.initialize("recommendation.properties");
		PricingEngineWS engine = new PricingEngineWS();
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith(ZONE_ID)){
					engine.setBatchLocationLevelId(Constants.ZONE_LEVEL_ID);
					engine.setBatchLocationId(Integer.parseInt(arg.substring(ZONE_ID.length())));
				}else if(arg.startsWith(PRODUCT_LEVEL_ID)){
					engine.setBatchProductLevelId(Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				}else if(arg.startsWith(PRODUCT_ID)){
					engine.setBatchProductId(Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				}else if(arg.startsWith(STRATEGY_ID)){
					engine.setBatchProductId(Integer.parseInt(arg.substring(STRATEGY_ID.length())));
				}else if(arg.startsWith(DATE)){
					engine.setBatchInputDate(arg.substring(DATE.length()));
				}
			}
		}
		
		try{
			engine.initialize();			
			engine.runRecommendation();
		}catch(GeneralException | SQLException ge){
			logger.error("Error when setting location/product list");
		}
	}

	/**
	 * Run Recommendation From Batch
	 * @throws GeneralException
	 * @throws SQLException
	 */
	private void runRecommendation() throws GeneralException, SQLException {
		runType = PRConstants.RUN_TYPE_BATCH;
		isOnline = false;
		List<Long> strategyIds = new ArrayList<Long>();
		if (getBatchLocationId() > 0 || getBatchProductId() > 0) {
//			initialize();
			// Retrieves list of locations for which recommendation batch has to run. Retrieves all zones when ZONE_ID is not
			// passed as input parameter.
			ArrayList<Integer> locationList = getLocationList();

			/*
			 * Retrieves list of products for which recommendation batch has to run. Retrieves all categories when
			 * PRODUCT_LEVEL_ID and PRODUCT_ID is not passed as input parameter.
			 */
			ArrayList<Integer> productList = getProductList();

			for (Integer locationId : locationList) {
				for (Integer productId : productList) {
					if (getConnection().isClosed()) {
						conn = null;
						initialize();
					}
					logger.info("*** Running price recommendation for location " + locationId + " product " + productId + " ***");
					// Inserts into PR_RECOMMENDATION_RUN_HEADER table
					PRRecommendationRunHeader recommendationRunHeader = insertRecommendationHeader(Constants.ZONE_LEVEL_ID, locationId,
							Constants.CATEGORYLEVELID, productId, PRConstants.BATCH_USER);
					if (recommendationRunHeader.getIsAutomaticRecommendation() && recommendationRunHeader.getIsPriceZone()) {
						if (getBatchProductLevelId() >= Constants.CATEGORYLEVELID) {
							// Run recommendations at category level if the input product level is
							// category/portfolio/major category
							getPriceRecommendation(recommendationRunHeader, Constants.ZONE_LEVEL_ID, locationId, Constants.CATEGORYLEVELID, productId,
									strategyIds, recommendationRunHeader.getRunId());
						} else
							// Run recommendations at input product level if it is sub category/segment/item level
							getPriceRecommendation(recommendationRunHeader, Constants.ZONE_LEVEL_ID, locationId, getBatchProductLevelId(), productId,
									strategyIds, recommendationRunHeader.getRunId());
						logger.info("Run Id returned " + recommendationRunHeader.getRunId());
					}
				}
			}
		}
	}

	/**
	 * Retrieves list of locations/price zone ids for which recommendation batch has to run
	 * @return
	 */
	private ArrayList<Integer> getLocationList() throws GeneralException {
		RetailPriceZoneDAO rpzDAO = new RetailPriceZoneDAO();
		ArrayList<Integer> locationList = null;
		if (getBatchLocationId() == -1)
			// Get all zones
			locationList = rpzDAO.getZonesUnderSubscriber(conn);
		else {
			// Add only the location passed in the input
			locationList = new ArrayList<Integer>();
			locationList.add(getBatchLocationId());
		}
		rpzDAO = null;
		return locationList;
	}

	/**
	 * Returns list of all categories for which recommendation batch has to run
	 * @return
	 * @throws GeneralException
	 */
	private ArrayList<Integer> getProductList() throws GeneralException {
		ArrayList<Integer> productList = null;
		ProductGroupDAO pgDAO = new ProductGroupDAO();
		if (getBatchProductLevelId() == -1 && getBatchProductId() == -1)
			// Get all categories
			productList = pgDAO.getCategories(conn);
		else if (getBatchProductLevelId() <= Constants.CATEGORYLEVELID) {
			// If product level id is category level or below use product id as defined in the input
			productList = new ArrayList<Integer>();
			productList.add(getBatchProductId());
		} else
			// If product level id is above category level or below use product id of child categories
			productList = pgDAO.getCategories(conn, getBatchProductLevelId(), getBatchProductId());
		return productList;
	}

	public RetailCalendarDTO getCurCalDTO(Connection conn, RetailCalendarDAO retailCalendarDAO) throws GeneralException{
		RetailCalendarDTO curCalDTO;
		if (getBatchInputDate() == null)
			// Current Week
			curCalDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
		else
			curCalDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(DateUtil.toDate(getBatchInputDate()), 0), Constants.CALENDAR_WEEK);

		this.curWeekStartDate = curCalDTO.getStartDate();
		return curCalDTO;
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @param priceHistory
	 * @param inpBaseWeekStartDate
	 * @return recommend new price or not
	 * @throws Exception
	 * @throws GeneralException
	 */
	private void clearPricePointsWhenCurrentPriceRetained(List<PRItemDTO> itemList,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory, String inpBaseWeekStartDate)
			throws Exception, GeneralException {
		RecommendationRulesFilter recommendationRulesFilter = new RecommendationRulesFilter();

		for (PRItemDTO itemDTO : itemList) {

			// Changes done for RA-43 to skip post recommendation rules if guardrail
			// constraint is set for a lead zone.
			// Property added for FF since its not applicable for it and moved the existing logic to a checkIfNewReccRequired
			// condition addeded  by Karishma on 09/06 for
			// checking if guardrail constraint is null
			if (checkForGuardRailConstraint && (itemDTO.getStrategyDTO().getConstriants()
					.getGuardrailConstraint() == null
					|| (itemDTO.getStrategyDTO().getConstriants().getGuardrailConstraint() != null
							&& !itemDTO.getStrategyDTO().getConstriants().getGuardrailConstraint().isZonePresent()))) {
				checkIfNewReccRequired(itemDTO, recommendationRulesFilter, recommendationRuleMap, priceHistory,
						inpBaseWeekStartDate);
			} else {
				checkIfNewReccRequired(itemDTO, recommendationRulesFilter, recommendationRuleMap, priceHistory,
						inpBaseWeekStartDate);
			}

		}
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRulesFilter
	 * @param recommendationRuleMap
	 * @param priceHistory
	 * @param inpBaseWeekStartDate
	 * @throws Exception
	 * @throws GeneralException
	 */
	private void checkIfNewReccRequired(PRItemDTO itemDTO, RecommendationRulesFilter recommendationRulesFilter,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory, String inpBaseWeekStartDate)
			throws Exception, GeneralException {

		if (!itemDTO.isLir()) {
			boolean isNewPriceRequired = recommendationRulesFilter.isNewPriceToBeRecommended(itemDTO);

			if (!isNewPriceRequired) {
				boolean isCurrPriceRetainRequired = recommendationRulesFilter.isCurrPriceRetainRequired(itemDTO,
						recommendationRuleMap, priceHistory, inpBaseWeekStartDate);

				if (isCurrPriceRetainRequired) {

						logger.debug("Current Price Retain: " + isCurrPriceRetainRequired + " Item code: " + itemDTO.getItemCode());
						MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
								itemDTO.getRegMPrice());
					itemDTO.setPriceRange(new Double[] { PRCommonUtil.getUnitPrice(curRegPrice, false) });
					itemDTO.setCurPriceRetained(true);
				}
			}
		}
	}

	/**
	 * 
	 * @param PRItemDTO
	 * @param recommendationRuleMap
	 * @param RetailCalendarDTO
	 * @param itemZonePriceHistory
	 */

	/*private void filterPricePointsbyCheckingRules(List<PRItemDTO> itemList,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, RetailCalendarDTO curCalDTO,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory) throws GeneralException, Exception {
		ObjectiveService objService = new ObjectiveService();
		for (PRItemDTO itemDTO : itemList) {

			if (!itemDTO.isLir()) {

				Double[] filterFinalPricePoints = objService.filterFinalPricePoints(itemDTO, itemZonePriceHistory,
						curCalDTO, recommendationRuleMap);
				itemDTO.setPriceRange(filterFinalPricePoints);
				
			}
		}

	}	*/
	
	
	
	@SuppressWarnings("unused")
	private void writeMemoryUsage(String msg, Runtime runTime) {
		logger.debug("***" + msg + "***");
		logger.debug("Total Memory: " + humanReadableByteCount(runTime.totalMemory()));
		logger.debug("Free Memory: " + humanReadableByteCount(runTime.freeMemory()));
		logger.debug("Used Memory: " + humanReadableByteCount(runTime.totalMemory() - runTime.freeMemory()));
		logger.debug("******");
	}

	private String humanReadableByteCount(long bytes) {
		int unit = 1000;
	    if (bytes < unit) return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = ("kMGTPE").charAt(exp - 1) + ("");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	@SuppressWarnings("unused")
	private int getBatchLocationLevelId() {
		return batchLocationLevelId;
	}

	private void setBatchLocationLevelId(int batchLocationLevelId) {
		this.batchLocationLevelId = batchLocationLevelId;
	}

	private int getBatchLocationId() {
		return batchLocationId;
	}

	private void setBatchLocationId(int batchLocationId) {
		this.batchLocationId = batchLocationId;
	}

	private int getBatchProductLevelId() {
		return batchProductLevelId;
	}

	private void setBatchProductLevelId(int batchProductLevelId) {
		this.batchProductLevelId = batchProductLevelId;
	}

	private int getBatchProductId() {
		return batchProductId;
	}

	private void setBatchProductId(int batchProductId) {
		this.batchProductId = batchProductId;
	}

	private String getBatchInputDate() {
		return batchInputDate;
	}

	private void setBatchInputDate(String batchInputDate) {
		this.batchInputDate = batchInputDate;
	}

	public void setLog4jProperties(int locationId, int productId) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");

		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager.getProperty("log4j.appender.console.layout.ConversionPattern");

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();

		//RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), DateUtil.getWeekStartDate(-1), Constants.CALENDAR_WEEK);		

		String curWkStartDate = getBatchInputDate() != null ? getBatchInputDate() : DateUtil.getWeekStartDate(-1);
		RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), curWkStartDate, Constants.CALENDAR_WEEK);

//		LocalDate recWeekStartDate = DateUtil.toDateAsLocalDate(curWkDTO.getStartDate());
		Date recWeekStartDate = DateUtil.toDate(curWkDTO.getStartDate());
		SimpleDateFormat nf = new SimpleDateFormat("MM-dd-yyy");
		String dateInLog = nf.format(recWeekStartDate);
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String catAndZoneNum = String.valueOf(locationId) + "_" + String.valueOf(productId);
		logPath = logPath + "/" + catAndZoneNum + "_" + dateInLog + "_" + timeStamp + ".log";

		Properties props = new Properties();
//		try {
//			InputStream configStream = getClass().getResourceAsStream("/log4j-pricing-engine.properties");
//			props.load(configStream);
//			configStream.close();
//		} catch (IOException e) {
//			System.out.println("Error: Cannot laod configuration file ");
//		}
		props.setProperty("log4j.rootLogger", logTypes);
		props.setProperty("log4j.appender.logFile", appender);
		props.setProperty("log4j.appender.logFile.File", logPath);
		props.setProperty("log4j.appender.logFile.MaxFileSize", maxFileSize);
		props.setProperty("log4j.appender.logFile.layout", patternLayout);
		props.setProperty("log4j.appender.logFile.layout.ConversionPattern", conversionPattern);

		props.setProperty("log4j.appender.console", appenderConsole);
		props.setProperty("log4j.appender.console.layout", appenderConsoleLayout);
		props.setProperty("log4j.appender.console.layout.ConversionPattern", appenderConsoleLayoutPattern);
		PropertyConfigurator.configure(props);
	}

	
	/**
	 * 
	 * @param itemDataMap
	 */
	public void markNonMovingItems(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		boolean isNonMovingItemsFilterEnabled = Boolean
				.parseBoolean(PropertyManager.getProperty("IGNORE_NON_MOVING_ITEMS_FROM_REC", "FALSE"));

		boolean filterNonMovingItems = Boolean
				.parseBoolean(PropertyManager.getProperty("MARK_NON_MOVING_ITEMS", "FALSE"));


		if (isNonMovingItemsFilterEnabled || filterNonMovingItems) {

			markItemsWithNoMovment(itemDataMap);

			markLigWithNoMovment(itemDataMap, retLirMap);

		}
	}

	/**
	 * 
	 * @param itemDataMap
	 */
	private void markItemsWithNoMovment(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		for (Map.Entry<ItemKey, PRItemDTO> itemEntry : itemDataMap.entrySet()) {
			PRItemDTO prItemDTO = itemEntry.getValue();
			if (!prItemDTO.isLir()) {
				if (prItemDTO.getAvgMovement() == 0) {
					logger.debug("markItemsWithNoMovment() - Item with zero mov in last X weeks: " + prItemDTO.getItemCode());
					prItemDTO.setNonMovingItem(true);
				}
			}
		}
	}

	/**
	 * 
	 * @param itemsForInsert
	 * @return items with movement
	 */
	private void markLigWithNoMovment(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		for (Map.Entry<ItemKey, PRItemDTO> itemEntry : itemDataMap.entrySet()) {
			PRItemDTO itemDTO = itemEntry.getValue();
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				ligMembers = ligMembers.stream().filter(p -> !p.isNonMovingItem()).collect(Collectors.toList());

				// If all the LIG members are not moving in an LIG, skip the LIG and its members from recommendation
				// If anyone of member is moving, then consider all members in the LIG
				if (ligMembers.size() == 0) {
					logger.debug("filterMovingItems() - Non moving LIG: " + itemDTO.toString());
					itemDTO.setNonMovingItem(true);
				}
			}
		}
	}

	
	/**
	 * 
	 * @param itemDataMap
	 * @return items filtered by active/authorized/moving
	 */
	int nonMovingItemsCounter = 0;
	public HashMap<ItemKey, PRItemDTO> filterByActiveAuthAndMovingItems(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		HashMap<ItemKey, PRItemDTO> itemDataMapFiltered = new HashMap<>();

	
		boolean isNonMovingItemsFilterEnabled = Boolean
				.parseBoolean(PropertyManager.getProperty("IGNORE_NON_MOVING_ITEMS_FROM_REC", "FALSE"));
		boolean markNonMovingItemsToNotSendToPrediction = Boolean
				.parseBoolean(PropertyManager.getProperty("MARK_NON_MOVING_ITEMS_TO_NOT_SEND_TO_PREDICTION", "FALSE"));

		//logger.debug("Total items and ligs in collection: " + itemDataMap.size());

		/*logger.info("filterByActiveAuthAndMovingItems() - # of Authorized items: "
				+ itemDataMap.values().stream().filter(p -> p.isAuthorized() && !p.isLir()).count());
		logger.info("filterByActiveAuthAndMovingItems() - # of Active items: "
				+ itemDataMap.values().stream().filter(p -> p.isActive() && !p.isLir()).count());*/

		logger.info("filterByActiveAuthAndMovingItems() - Total # of items: "
				+ itemDataMap.values().stream().filter(p -> !p.isLir()).count());

		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (!itemDTO.isLir() && itemDTO.getRetLirId() == 0) {
				// Added for FF to mark non moving items to not to send to Prediction
				if (markNonMovingItemsToNotSendToPrediction) {
					if (!itemDTO.isNonMovingItem()) {
						itemDTO.setSendToPrediction(true);
					} else {
						nonMovingItemsCounter++;
						itemDTO.setSendToPrediction(false);
					}
				}

				if (isNonMovingItemsFilterEnabled) {
					if (!itemDTO.isNonMovingItem()) {
						itemDataMapFiltered.put(itemKey, itemDTO);
					}
				} else {
					if (itemDTO.isActive() && itemDTO.isAuthorized()) {
						itemDataMapFiltered.put(itemKey, itemDTO);
					}
				}
			}
		});
		logger.info("filterByActiveAuthAndMovingItems() - Total # of non LIG items not to pass to prediction : "+ nonMovingItemsCounter);
		nonMovingItemsCounter = 0;

		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				if (isNonMovingItemsFilterEnabled) {
					List<PRItemDTO> movingMembers = ligMembers.stream()
							.filter(p -> (!p.isNonMovingItem()))
							.collect(Collectors.toList());
					if (movingMembers.size() > 0) {
						itemDataMapFiltered.put(itemKey, itemDTO);
						ligMembers.forEach(member -> {
							itemDataMapFiltered.put(PRCommonUtil.getItemKey(member), member);
						});
					}
				} else {
					ligMembers = ligMembers.stream()
							.filter(p -> (p.isActive() && p.isAuthorized()))
							.collect(Collectors.toList());
					if (ligMembers.size() > 0) {
						itemDataMapFiltered.put(itemKey, itemDTO);
						ligMembers.forEach(member -> {
							if (markNonMovingItemsToNotSendToPrediction) {
								if (!member.isNonMovingItem()) {
									member.setSendToPrediction(true);
								} else {
									nonMovingItemsCounter++;
									member.setSendToPrediction(false);
								}
							}

							itemDataMapFiltered.put(PRCommonUtil.getItemKey(member), member);
						});
					}
				}
			}
		});
		logger.info("filterByActiveAuthAndMovingItems() - Total # of LIG items  not to pass to prediction : "
				+ nonMovingItemsCounter);

		logger.info("filterByActiveAuthAndMovingItems() - # of items considered for recommendation: "
				+ itemDataMapFiltered.values().stream().filter(p -> !p.isLir()).count());

		return itemDataMapFiltered;
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 */
	private void updateLIGAsRecommended(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				boolean isPriceRecommended = false;
				for (PRItemDTO member : ligMembers) {
					if (member.getIsNewPriceRecommended() == 1) {
						isPriceRecommended = true;
						break;
					}
				}

				if (isPriceRecommended) {
					itemDTO.setIsNewPriceRecommended(1);
				}
			}
		});
	}

	private void addMissingZones(List<String> priceZoneStores, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		itemDataMap.forEach((key, value) -> {
			if (value.getPriceZoneNo() != null && !Constants.EMPTY.equals(value.getPriceZoneNo())
					&& !priceZoneStores.contains(value.getPriceZoneNo())) {
				priceZoneStores.add(value.getPriceZoneNo());
			}
		});
	}

	public void calculatePriceChangeImpact(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, boolean isGlobalZone) throws GeneralException {

		boolean calculateglobalZoneImpact = Boolean
				.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE", "FALSE"));

		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (!itemDTO.isLir()) {
				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
						itemDTO.getRegMPrice());

				// Changes done by Bhargavi on 01/05/2021
				// update the MarkUp and MarkDown values for Rite Aid
				String impactFactor = PropertyManager.getProperty("IMPACT_CALCULATION_FACTOR", Constants.X_WEEKS_MOV);
				boolean priceSet = false;

				// Added by Karishma on 11/15/2021 for AZ
				// For global zone impact will be calculates as follows/
				// get the current retail and L13 units of item from all other zones and use the
				// recc retail of Z1000
				if (isGlobalZone && calculateglobalZoneImpact && !priceSet) {
					double impact = 0;
					// Added on 05/04 for using the approved impact for items which have pending
					// retail and no new recommendation
					if (itemDTO.getPendingRetail() != null &&( itemDTO.getOverriddenRegularPrice() == null ||  
							(itemDTO.getOverriddenRegularPrice()!=null && itemDTO.getOverriddenRegularPrice().getUnitPrice()==0))) {
						// impact is only for display purpose if there is a pending retail, in summary
						// calculation it should not included
						// used the approved impact field for inserting in final table
						impact = 0;
						/*
						 * logger.debug("Imp.Ret lir id: " + itemDTO.getRetLirId() + "Item code: " +
						 * itemDTO.getItemCode() + ",52weeksmov:" + itemDTO.getxWeeksMovForTotimpact() +
						 * " impact: " + impact + "pending retail :" +
						 * itemDTO.getPendingRetail().getUnitPrice());
						 */
						
					} else if (itemDTO.getCurrRetailslOfAllZones() != null && curRegPrice != null) {

						for (Map.Entry<Double, List<Double>> priceMovObj : itemDTO.getCurrRetailslOfAllZones()
								.entrySet()) {
							double newUnitPrice = 0;

							if (itemDTO.getOverriddenRegularPrice() != null
									&& itemDTO.getOverriddenRegularPrice().getUnitPrice() > 0)
								newUnitPrice = itemDTO.getOverriddenRegularPrice().getUnitPrice();
							else
								newUnitPrice = itemDTO.getRecommendedRegPrice().getUnitPrice();

							for (Double units : priceMovObj.getValue()) {
								impact = impact + (newUnitPrice - priceMovObj.getKey()) * units;
							}
						}
						/*
						 * logger.debug("Imp.Ret lir id : " + itemDTO.getRetLirId() + "Item code: " +
						 * +itemDTO.getItemCode() + ",52weeksmov:" + itemDTO.getxWeeksMovForTotimpact()
						 * + " impact: " + impact);
						 */
					}
					itemDTO.setPriceChangeImpact(impact);
				}
					/** PROM-2223 changes end **/
				else

				if (curRegPrice != null && !priceSet) {
					double currentUnitPrice = curRegPrice.getUnitPrice();
					double newUnitPrice = 0;
					if (itemDTO.getOverriddenRegularPrice() != null
							&& itemDTO.getOverriddenRegularPrice().getUnitPrice() > 0)
						newUnitPrice = itemDTO.getOverriddenRegularPrice().getUnitPrice();
					else
						newUnitPrice = itemDTO.getRecommendedRegPrice().getUnitPrice();
					
					double impact = 0;
					if (currentUnitPrice > 0 && newUnitPrice > 0) {
						// Changes done by Bhargavi on 01/05/2021
						// update the MarkUp and MarkDown values for Rite Aid
						
						if (Constants.STORE_INVENTORY.equals(impactFactor)) {

							impact = itemDTO.getInventory() * (newUnitPrice - currentUnitPrice);
							itemDTO.setPriceChangeImpact(impact);

							/*
							 * logger.debug("Rite Aid Impact : Imp.Ret lir id: " + itemDTO.getRetLirId() +
							 * "Item code: " + itemDTO.getItemCode() + ",quantity in hand:" +
							 * itemDTO.getInventory() + ", curr. price: " + currentUnitPrice +
							 * ", new price: " + newUnitPrice + ", impact: " + impact);
							 */
							// Changes-ended
						}
						  else
						{
							// Added on 05/04/22 for using the approved impact for items which have pending
							// retail
							if (itemDTO.getPendingRetail() != null && (itemDTO.getOverriddenRegularPrice() == null
									|| (itemDTO.getOverriddenRegularPrice() != null
											&& itemDTO.getOverriddenRegularPrice().getUnitPrice() == 0))) {
	 
								// impact is only for display purpose if there is a pending retail, in summary
								// calculation it should not included
								// use the approved impact field for inserting in final table
								impact = 0;
								/*
								 * logger.debug("Imp.Ret lir id :  " + itemDTO.getRetLirId() + "Item code: " +
								 * itemDTO.getItemCode() + ",52weeksmov:" + itemDTO.getxWeeksMovForTotimpact() +
								 * ", curr. price: " + currentUnitPrice + ", new price: " + newUnitPrice +
								 * ", impact: " + impact + " pending retail: " +
								 * itemDTO.getPendingRetail().getUnitPrice());
								 */
							} else {
								impact = itemDTO.getxWeeksMovForTotimpact() * (newUnitPrice - currentUnitPrice);
								/*
								 * logger.debug("Imp.Ret lir id:" + itemDTO.getRetLirId() + "Item code: " +
								 * itemDTO.getItemCode() + ",52weeksmov:" + itemDTO.getxWeeksMovForTotimpact() +
								 * ", curr. price: " + currentUnitPrice + ", new price: " + newUnitPrice +
								 * ", impact: " + impact);
								 */
							}
							itemDTO.setPriceChangeImpact(impact);
							
						}
					}
				}
			}

		});

		itemDataMap.forEach((itemKey, itemDTO) -> {

			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				double priceChangeImpact = ligMembers.stream().mapToDouble(PRItemDTO::getPriceChangeImpact).sum();
				itemDTO.setPriceChangeImpact(priceChangeImpact);
			}
		});

	}

	

		
}


