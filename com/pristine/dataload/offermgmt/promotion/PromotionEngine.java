package com.pristine.dataload.offermgmt.promotion;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.dao.LocationGroupDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.promotion.AdDetail;
//import com.pristine.dto.offermgmt.promotion.AdLevelMetrics;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PromoEngineDataDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.dto.salesanalysis.ProductDTO;
//import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.RetailCalendarService;
import com.pristine.service.offermgmt.promotion.AdFinalizationService;
import com.pristine.service.offermgmt.promotion.PromotionEngineService;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/**
 * Recommends items for weekly ad and display
 * 
 * @author Nagarajan
 *
 */


public class PromotionEngine extends BasePromotionEngine {

	private static Logger logger = Logger.getLogger("PromotionEngine");

	// private int noOfWeeksCompHistory = 13;
	private int noOfWeeksPriceHistory = 13;
	private int noOfWeeksCostHistory = 52;
	private int noOfWeeksCompHistory = 13;

	private Connection conn = null;

	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_IDS = "PRODUCT_IDS=";
	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private static final String PRIMARY_COMP_STR_ID = "PRIMARY_COMP_STR_ID=";
	private static final String PAGE_NO = "PAGE_NO=";
	private static final String PPG_ENABLED = "PPG_ENABLED="; // 1 : Enabled
	private static final String DEBUG_ITEMS = "DEBUG_ITEMS=";
	private static final String MIN_MARGIN_PCT = "MIN_MARGIN_PCT=";

	private int inputLocationLevelId = -1;
	private int inputLocationId = -1;
	private int inputProductLevelId = 5;
	private String inputProductIds = "";
	Set<Integer> inputProductIdSet = new HashSet<Integer>();
	
	private String inputWeekStartDate = null;
	private int inputPrimaryCompStrId = -1;
	private int inputPageNo = -1;
	private double inputMinMarginPCT = -1;
	// private int processPPGData = 1; // as of now a) Enabled (default) : 1 b) Disabled : 0

	private String inputDebugItems = "";
	Set<Integer> debugItems = new HashSet<Integer>();
	private boolean isDebugMode = false;

	private int chainId = 0;

	PromoEngineDataDTO promoEngineDataDTO = PromoEngineDataDTO.getPromoEngineDataDTO();
	PromotionEngineService promotionEngineService = null;
	HashMap<Long, PromoProductGroup> ppgGroupDetails = new HashMap<Long, PromoProductGroup>();
	HashMap<ProductKey, ProductDTO> departDetails = new HashMap<ProductKey, ProductDTO>();
	
	public static void main(String[] args) {
		PropertyManager.initialize("recommendation.properties");
		PromotionEngine promotionEngine = new PromotionEngine();

		// Set input arguments
		promotionEngine.setArguments(args);

		// Validate input arguments
		promotionEngine.validateArguments();

		// Start recommendation process
		promotionEngine.doRecommendation();
	}

	private void setArguments(String[] args) {
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(LOCATION_LEVEL_ID)) {
					inputLocationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
				} else if (arg.startsWith(LOCATION_ID)) {
					inputLocationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					inputProductLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
				} else if (arg.startsWith(PRODUCT_IDS)) {
					inputProductIds = arg.substring(PRODUCT_IDS.length());
					inputProductIdSet = Stream.of(inputProductIds.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
				} else if (arg.startsWith(WEEK_START_DATE)) {
					inputWeekStartDate = arg.substring(WEEK_START_DATE.length());
				} else if (arg.startsWith(PRIMARY_COMP_STR_ID)) {
					inputPrimaryCompStrId = Integer.parseInt(arg.substring(PRIMARY_COMP_STR_ID.length()));
				} else if (arg.startsWith(PAGE_NO)) {
					inputPageNo = Integer.parseInt(arg.substring(PAGE_NO.length()));
				} else if (arg.startsWith(PPG_ENABLED)) {
					// processPPGData = Integer.parseInt(arg.substring(PPG_ENABLED.length()));
				} else if (arg.startsWith(DEBUG_ITEMS)) {
					inputDebugItems = arg.substring(DEBUG_ITEMS.length());
					debugItems = Stream.of(inputDebugItems.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
					isDebugMode = true;
				} else if (arg.startsWith(MIN_MARGIN_PCT)) {
					inputMinMarginPCT = Integer.parseInt(arg.substring(MIN_MARGIN_PCT.length()));
				}
				
				
			}
			logger.debug("PromotionEngineService: Input Arguments - inputLocationLevelId : " + inputLocationLevelId + " inputLocationId : "
					+ inputLocationId + " inputWeekStartDate : " + inputWeekStartDate + " inputPrimaryCompStrId : " + inputPrimaryCompStrId);
		}
	}

	private void validateArguments() {
		if (inputLocationLevelId == -1 || inputLocationId == -1 || inputWeekStartDate == null || (inputPageNo == -1)) {
			logger.error("One or more mandatory input arguments (inputLocationLevelId, inputLocationId, "
					+ " inputWeekStartDate, inputPageNo) missing. Program Terminated!!!");
			return;
		}
	}

	private void doRecommendation() {
		try {
			// initialize
			initialize();

			// Get actual Ad
			getActualAd();

			// Get base data
			getBaseData();

			// Close the connection as it is not needed from here
			PristineDBUtil.close(getConnection());

			// Movement Prediction for actual Ad items
			actualAdPrediction();

			// Recommending higher margin higher HH impact items
			promoRecommendation();

		} catch (GeneralException | Exception | OfferManagementException e) {
			e.printStackTrace();
			logger.error(e.toString(), e);
		} finally {
			try {
				if (getConnection() != null & !getConnection().isClosed()) {
					PristineDBUtil.close(getConnection());
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void initialize() throws GeneralException, Exception {
		logger.debug("PromotionEngineService: initializing connection and properties");

		setConnection();

		initializePrimaryData();

		initializeLoggerInfo();

	}

	private void getBaseData() throws GeneralException, OfferManagementException {

		logger.info("PromotionEngineService: fetching Base data");

		// Add previous calendar Id and last 3 years calendar id
		getPreviousCalendarDetails();

		departDetails = new ProductGroupDAO().getDepartments(getConnection());
		
		// Get authorized items
		getAuthorizedItems();

		// Getting store list price
		getPrice();

		// Getting store list cost
		getCost();

		// update lig level price and cost
		promotionEngineService.updateLigLevelData(promoEngineDataDTO.authroizedItemMap, promoEngineDataDTO.ligAndItsMember);
				
		// Getting competitor price
		getCompPrice();

		// Getting store list Sale price
		getSaleInfo();

		// Getting Ad details
		getAdInfo();

		// Getting display details
		// getDisplayInfo();

		// Getting item classification data
		getItemClassifictionData();

		// Get PPG Data
		ppgGroupDetails = promotionEngineService.getPPGBaseData(conn, promoEngineDataDTO.getDepartmentListAsString());
	}

	private void promoRecommendation() throws GeneralException, OfferManagementException {
		
		HashMap<ProductKey, List<PromoItemDTO>> individualItemsPromoCombinations = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		// Full PPG promo combinations
		HashMap<Long, List<PromoProductGroup>> ppgItemsPromoCombinations = new HashMap<Long, List<PromoProductGroup>>();
		
		// PPG details received from DAO layer
		HashMap<Long, PromoProductGroup> ppgGroupDetailsAfterFirstFilter = new HashMap<Long, PromoProductGroup>();
		
		// HashMap<Categry Key, List<Items>> as input for prediction engine
		HashMap<ProductKey, List<PromoItemDTO>> itemGroupedByCategory = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		// Prediction Engine output
		HashMap<ProductKey, List<PromoItemDTO>> predictedItems = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		// All promo combinations including PPG groups
		HashMap<ProductKey, List<PromoItemDTO>> individualAndPPGItemsPromoCombinations = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		// Input map with all promo combinations as input to Group by Category function
		// Past ad items + item classification items + PPG's at lig and item level
//		HashMap<ProductKey, List<PromoItemDTO>> finalItemListWithAllPriceCombinations = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMapAfterFirstFilter = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		//Candidate items after merging past Ad items, HH recommendation and item classification
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap = new HashMap<ProductKey, List<PromoItemDTO>>();
		
		// Merge items (Items from Past Ad and Item Classification)
		candidateItemMap = promotionEngineService.mergeItems(inputWeekStartDate, promoEngineDataDTO.retailCalendarCache,
				promoEngineDataDTO.authroizedItemMap, promoEngineDataDTO.adDetailMap, promoEngineDataDTO.displayDetailMap,
				promoEngineDataDTO.saleDetailMap, promoEngineDataDTO.itemClassificationMap, promoEngineDataDTO.hhRecommendedItemMap,
				promoEngineDataDTO.ligAndItsMember);

		// Assign price, cost and other attributes of items
		promotionEngineService.assingItemAttributes(promoEngineDataDTO.authroizedItemMap, candidateItemMap, departDetails);
		
		//Past sale price of items
		promoEngineDataDTO.pastSalePrices = promotionEngineService.getPastSalePrices(promoEngineDataDTO.adDetailsHistory);

		// Find category level average margin percentage when promoted on first page
		promotionEngineService.getPastSaleMarginStats(promoEngineDataDTO.adDetailsHistory, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.avgSaleMarPCTOfItemOnFirstPage, promoEngineDataDTO.avgSaleMarPCTOfCategoryOnFirstPage,
				promoEngineDataDTO.minDealCostOfItemOnAd, promoEngineDataDTO.ligAndItsMember);

		// Logging:: Log items from past promo , item classification with all item attributes
		promotionEngineService.writeToCSVLog(candidateItemMap, "");
		
		HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary = promotionEngineService
				.groupActualAdItemsByPageBlock(promoEngineDataDTO.actualAdItemsInBlocks, promoEngineDataDTO.itemClassificationFullData);

		// Apply first level filter (Remove some items)
		candidateItemMapAfterFirstFilter = promotionEngineService.applyFirstLevelFilter(inputWeekStartDate,
				promoEngineDataDTO.retailCalendarCache, promoEngineDataDTO.adDetailMap, promoEngineDataDTO.saleDetailMap,
				candidateItemMap, false);
		
		// Find all promo combinations
		individualItemsPromoCombinations = promotionEngineService.findAllPromotionCombination(candidateItemMapAfterFirstFilter,
				promoEngineDataDTO.deptBlockMap, inputPageNo, promoEngineDataDTO.ligAndItsMember, false, promoEngineDataDTO.pastSalePrices,
				promoEngineDataDTO.authroizedItemMap, promoEngineDataDTO.avgSaleMarPCTOfItemOnFirstPage,
				promoEngineDataDTO.avgSaleMarPCTOfCategoryOnFirstPage, promoEngineDataDTO.minDealCostOfItemOnAd,
				actualAdItemsLIGNonLigSummary);
		
		logger.info("***ItemDetails: 5. No of PPG's (#):" + ppgGroupDetails.size() + " ***");
		
		// Apply first level filter for ppg items
		ppgGroupDetailsAfterFirstFilter = promotionEngineService.applyFirstLevelFilterOnPPGItems(inputWeekStartDate,
				promoEngineDataDTO.retailCalendarCache, promoEngineDataDTO.adDetailMap, promoEngineDataDTO.saleDetailMap, ppgGroupDetails);

		// Find possible price combination for PPG
		ppgItemsPromoCombinations = promotionEngineService.getPPGPromoCombinations(inputPageNo, ppgGroupDetailsAfterFirstFilter,
				promoEngineDataDTO.deptBlockMap, promoEngineDataDTO.ligAndItsMember, promoEngineDataDTO.authroizedItemMap, departDetails,
				promoEngineDataDTO.pastSalePrices, promoEngineDataDTO.avgSaleMarPCTOfItemOnFirstPage,
				promoEngineDataDTO.avgSaleMarPCTOfCategoryOnFirstPage, promoEngineDataDTO.minDealCostOfItemOnAd,
				actualAdItemsLIGNonLigSummary);

		
		
		// Apply second level filter, based on sale prices for ppg and ind item combination
		individualItemsPromoCombinations = promotionEngineService.ignoreHigherPricePointThanCompIndItems(individualItemsPromoCombinations);
		
		logger.info("***ItemDetails: 4. Item after first & second filter (#):" + individualItemsPromoCombinations.size() + "("
				+ promotionEngineService.getLigMemNonLIGCount(individualItemsPromoCombinations.keySet(), promoEngineDataDTO.authroizedItemMap,
						promoEngineDataDTO.ligAndItsMember)
				+ ") ***");
		
		ppgItemsPromoCombinations = promotionEngineService.ignoreHigherPricePointThanCompPPGItems(ppgItemsPromoCombinations,
				promoEngineDataDTO.ligAndItsMember);

		logger.info("***ItemDetails: 6. PPG's after first filter (#):" + ppgItemsPromoCombinations.size() + " ***");
		
		
		// Combine individual and PPG item promo combinations
		individualAndPPGItemsPromoCombinations = promotionEngineService.mergePPGItemsWithCandidateItems(individualItemsPromoCombinations,
				ppgItemsPromoCombinations);


		// Logging:: Log all price combinations
//		promotionEngineService.writeToCSVLog(finalItemListWithAllPriceCombinations, "all price combinations(Reg+Sale)");
		promotionEngineService.writeToCSVLog(individualAndPPGItemsPromoCombinations, "all price combinations(Reg+Sale)");

		// Group items by category
		itemGroupedByCategory = promotionEngineService.groupItemsByCategory(promoEngineDataDTO.retailCalendarCache, inputWeekStartDate,
				promoEngineDataDTO.ligAndItsMember, individualAndPPGItemsPromoCombinations);

		// Getting prediction for Promo combinations
		predictedItems = promotionEngineService.callPredictionEngine(promoEngineDataDTO.retailCalendarCache, inputLocationLevelId, inputLocationId,
				inputWeekStartDate, itemGroupedByCategory, false);

		
		//update brand id and brand name at item level
		promotionEngineService.assingItemAttributes(promoEngineDataDTO.authroizedItemMap, predictedItems, departDetails);
		
		// Logging:: Write LIG and Non LIG Summary
		promotionEngineService.writeToCSVLog(predictedItems, "Items with prediction");

		List<PromoItemDTO> indItemSummary = promotionEngineService.individualItemLevelSummary(inputLocationLevelId, inputLocationId,
				individualItemsPromoCombinations, predictedItems, promoEngineDataDTO.ligAndItsMember);

		// Write individual item summary
		promotionEngineService.writeToCSVLog(indItemSummary, "");

		// Find ppg level summary data
		List<PromoItemDTO> ppgAndItsItemSummary = promotionEngineService.ppgLevelAndItsBreakDownSummary(inputLocationLevelId, inputLocationId,
				ppgItemsPromoCombinations, predictedItems, promoEngineDataDTO.ligAndItsMember);

		// Logging:: Write ppg level summary
		promotionEngineService.writeToCSVLog(ppgAndItsItemSummary, "");

		// Group by page, block of actual ad for the rec week


		HashMap<PageBlockNoKey, BlockDetail> actualAdBlockLevelSummary = promotionEngineService.getBlockLevelSummary(actualAdItemsLIGNonLigSummary);
		
		logger.debug("actualAdBlockLevelSummary:" + actualAdBlockLevelSummary.toString());
		
		// Finalize the items
		HashMap<PageBlockNoKey, List<PromoItemDTO>> recAdItemsLIGNonLigSummary = new AdFinalizationService()
				.finalizeItems(actualAdBlockLevelSummary, actualAdItemsLIGNonLigSummary, indItemSummary, ppgAndItsItemSummary, inputMinMarginPCT);
		
		HashMap<PageBlockNoKey, BlockDetail> recAdBlockLevelSummary = promotionEngineService.getRecBlockLevelSummary(actualAdItemsLIGNonLigSummary,
				recAdItemsLIGNonLigSummary);
		
		
		logger.debug("recAdBlockLevelSummary:" + recAdBlockLevelSummary);
		
		// Find ad level metrics & convert to AdDetail Object
		AdDetail finalActualAdDetail  = promotionEngineService.getAdLevelSummary(actualAdItemsLIGNonLigSummary, actualAdBlockLevelSummary);
		
		logger.debug("finalActualAdDetail:" + finalActualAdDetail);
		
		AdDetail finalRecAdDetail  = promotionEngineService.getAdLevelSummary(recAdItemsLIGNonLigSummary, recAdBlockLevelSummary);
		
		logger.debug("finalRecAdDetail:" + finalRecAdDetail);
		
		//Generate excel report
		promotionEngineService.generateFinalReport(inputWeekStartDate, finalActualAdDetail, finalRecAdDetail);
	}

	private void initializePrimaryData() throws GeneralException {

		// If Dept given in input then pick that dept
		if (inputProductIdSet.size() > 0) {
			promoEngineDataDTO.productIdsToProcess.clear();
			promoEngineDataDTO.productIdsToProcess.addAll(inputProductIdSet);
		}

		promotionEngineService = new PromotionEngineService(inputLocationLevelId, inputLocationId, inputProductLevelId,
				promoEngineDataDTO.productIdsToProcess, inputWeekStartDate, inputPageNo);

		// Cache retail Calendar
		promoEngineDataDTO.retailCalendarCache = promotionEngineService.getRetailCalendar(getConnection());

		// Get chain Id
		chainId = Integer.valueOf(new RetailPriceDAO().getChainId(getConnection()));

		// Get all weekly calendar details
		promoEngineDataDTO.allWeekPromoCalendarDetails = new RetailCalendarService().getAllWeeks(promoEngineDataDTO.retailCalendarCache);

		// Rec week calendar details
		promoEngineDataDTO.recWeekCalDTO = new RetailCalendarService().getWeekCalendarDetail(promoEngineDataDTO.retailCalendarCache,
				inputWeekStartDate);

		// Get stores of store list
		LocationGroupDAO locationGroupDAO = new LocationGroupDAO(getConnection());
		promoEngineDataDTO.allStores = locationGroupDAO.getStoresOfLocation(inputLocationLevelId, inputLocationId);
	}

	private void initializeLoggerInfo() throws GeneralException {
		// Set log name
		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(inputWeekStartDate);
		String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate, "MM-dd-yyy");
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String logName = "";
		if (inputProductIdSet.size() > 1) {
			logName = String.valueOf(inputLocationLevelId) + "_" + String.valueOf(inputLocationId) + "_" + tempWeekStartDate + "_" + timeStamp;
		} else {
			logName = String.valueOf(inputLocationLevelId) + "-" + String.valueOf(inputLocationId) + "_" + inputProductLevelId + "-"
					+ String.valueOf(inputProductIdSet.iterator().next()) + "_" + tempWeekStartDate + "_" + timeStamp;
		}
		
		setLog4jProperties(logName);
	}

	/**
	 * Sets database connection. Used when program runs in batch mode
	 * 
	 * @throws Exception
	 */
	protected void setConnection() throws Exception {
		if (conn == null || conn.isClosed()) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException e) {
				throw new Exception("Error in setConnection() - " + e);
			}
		}
	}

	protected Connection getConnection() {
		return conn;
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	// getting current Ad details
	private void getActualAd() throws GeneralException {

		// Get all product entries (from all depts) currently on promotion

		logger.info("Getting actual ad details is started...");
		promoEngineDataDTO.actualAdDetail = promotionEngineService.getActualAdDetails(getConnection(), inputLocationLevelId, inputLocationId,
				inputProductLevelId, inputProductIdSet, promoEngineDataDTO.recWeekCalDTO.getStartDate(), 1, inputPageNo);
		logger.info("Getting actual ad details is completed...");

		if (inputProductIdSet.size() > 0) {
			promoEngineDataDTO.productIdsToProcess.clear();
			promoEngineDataDTO.productIdsToProcess.addAll(inputProductIdSet);
		} else if (promoEngineDataDTO.actualAdDetail != null) {
			// Getting the list of non-perishable departments
			promoEngineDataDTO.productIdsToProcess = promotionEngineService.getNonPerishableDepts(promoEngineDataDTO.actualAdDetail);
		}

		promoEngineDataDTO.actualAdItemsInBlocks = promotionEngineService.findNPItemsCurrentlyOnPromo(promoEngineDataDTO.actualAdDetail);

		promoEngineDataDTO.deptBlockMap = promotionEngineService.findDeptBlockMapforActualAd(promoEngineDataDTO.productIdsToProcess,
				promoEngineDataDTO.actualAdDetail);

	}

	private void getAuthorizedItems() throws GeneralException, OfferManagementException {
		logger.info("Getting authorized items started...");

		promoEngineDataDTO.authroizedItemMapDeptWise = promotionEngineService.getAuthorizedItems(getConnection(), inputLocationLevelId,
				inputLocationId, inputProductLevelId, promoEngineDataDTO.allStores, promoEngineDataDTO.productIdsToProcess, debugItems);

		logger.debug(" :::: getAuthorizedItems() : Dept Ids " + promoEngineDataDTO.authroizedItemMapDeptWise.keySet());

		// Merging all dept items
		for (Map.Entry<Integer, HashMap<ProductKey, PRItemDTO>> itemMap : promoEngineDataDTO.authroizedItemMapDeptWise.entrySet()) {
			logger.debug(" getAuthorizedItems() : Authorized items count : " + itemMap.getValue().size() + " for Department : " + itemMap.getKey());

			promoEngineDataDTO.authroizedItemMap.putAll(itemMap.getValue());

		}

		// Getting Lig and its member items map
		promoEngineDataDTO.ligAndItsMember = promotionEngineService.populateLigDetailsInMap(promoEngineDataDTO.authroizedItemMap);

		// Getting item Data Map
		promoEngineDataDTO.itemDataMap = promotionEngineService.populateItemDataMap(promoEngineDataDTO.authroizedItemMap);
		
		logger.info("***ItemDetails: 1. No of Authorized Items :"
				+ (promoEngineDataDTO.ligAndItsMember.size()
						+ promoEngineDataDTO.itemDataMap.values().stream().filter(p -> p.getRetLirId() == 0).count())
				+ "(" + promoEngineDataDTO.itemDataMap.values().stream().filter(p -> !p.isLir()).count() + ") ***");
		
		// Get item store map
		promoEngineDataDTO.itemStoreMap = promotionEngineService.getItemStoreMap(getConnection(), promoEngineDataDTO.allStores,
				promoEngineDataDTO.getDepartmentListAsString());

		// promoDTO.authroizedItemMap.putAll(itemMap.getValue());
		logger.info("Getting authorized items completed...");

	}

	private void getPreviousCalendarDetails() {
		// Past promotion weeks
		promoEngineDataDTO.calendarsForBaseData = promotionEngineService.getPastPromotionCalendarDetail(inputWeekStartDate,
				promoEngineDataDTO.retailCalendarCache);

		// Previous week
		promoEngineDataDTO.calendarsForBaseData
				.add(promotionEngineService.getPreviousWeekCalendarDetail(inputWeekStartDate, promoEngineDataDTO.retailCalendarCache));

		// recommendation week
		promoEngineDataDTO.calendarsForBaseData.add(promoEngineDataDTO.recWeekCalDTO);

	}

	private void getPrice() throws GeneralException {
		logger.info("Getting price is started...");

		promotionEngineService.getPriceOfAuthorizedItems(conn, chainId, promoEngineDataDTO.recWeekCalDTO, noOfWeeksPriceHistory,
				promoEngineDataDTO.allWeekPromoCalendarDetails, promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.itemDataMap, promoEngineDataDTO.itemStoreMap);
		
		logger.info("Getting price is completed...");
	}

	private void getCost() throws GeneralException {
		logger.info("Getting cost is started...");
		
		promotionEngineService.getCostOfAuthorizedItems(conn, chainId, promoEngineDataDTO.recWeekCalDTO, noOfWeeksCostHistory,
				promoEngineDataDTO.allWeekPromoCalendarDetails, promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.itemDataMap, promoEngineDataDTO.itemStoreMap);
		
		logger.info("Getting cost is completed...");
	}

	private void getCompPrice() throws GeneralException {
		promotionEngineService.getCompetitorPrice(conn, promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap, inputProductLevelId,
				promoEngineDataDTO.getDepartmentListAsString(), inputWeekStartDate, promoEngineDataDTO.ligAndItsMember,
				promoEngineDataDTO.itemStoreMap, inputPrimaryCompStrId, noOfWeeksCompHistory);

	}

	private void getSaleInfo() throws GeneralException {
		logger.info("Getting sale price is started...");
		// Get previous week promotion
		promoEngineDataDTO.saleDetailMap = promotionEngineService.getSaleInfo(getConnection(), promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.calendarsForBaseData, chainId, inputLocationLevelId, inputLocationId, inputProductLevelId,
				promoEngineDataDTO.getDepartmentListAsString(), promoEngineDataDTO.allStores, isDebugMode);

		// update lig level data
		promotionEngineService.populateLigSaleData(promoEngineDataDTO.saleDetailMap, promoEngineDataDTO.ligAndItsMember);
		logger.info("Getting sale price is completed...");
	}


	private void getAdInfo() throws GeneralException {
		logger.info("Getting ad is started...");

		// Get previous and future week weekly ad details
		promoEngineDataDTO.adDetailMap = promotionEngineService.getAdInfo(getConnection(), chainId, inputLocationLevelId,
				inputLocationId, inputProductLevelId, promoEngineDataDTO.getDepartmentListAsString(), promoEngineDataDTO.allStores,
				promoEngineDataDTO.authroizedItemMap, isDebugMode, inputWeekStartDate, promoEngineDataDTO.retailCalendarCache);

		// update lig level data
		promotionEngineService.populateLigAdData(promoEngineDataDTO.adDetailMap, promoEngineDataDTO.ligAndItsMember);

		//Get last 52 weeks ad details
		promoEngineDataDTO.adDetailsHistory = promotionEngineService.getLastXWeeksAdDetail(getConnection(),
				promoEngineDataDTO.recWeekCalDTO, 52, chainId, inputLocationLevelId, inputLocationId, inputProductLevelId,
				promoEngineDataDTO.getDepartmentListAsString(), promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap, isDebugMode);
		
		// Get min sale prices and bogo prices
//		promotionEngineService.getMinXPricePoints(promoEngineDataDTO.adDetailsHistory, promoEngineDataDTO.minXSalePercentages,
//				promoEngineDataDTO.itemsOnBOGOinLastXWeeks, promoEngineDataDTO.minXMultiplePrices);

	
		
		logger.info("Getting ad is completed...");
	}

	private void getItemClassifictionData() throws GeneralException {
		logger.info("Getting item classifcation data is started...");
		// Get item classification data
		promoEngineDataDTO.itemClassificationMap = promotionEngineService.getHighImpactHouseHoldItems(getConnection(), inputProductLevelId,
				promoEngineDataDTO.productIdsToProcess, inputLocationLevelId, inputLocationId, promoEngineDataDTO.itemClassificationFullData);

		// Update Authorized items List with HouseHold Count
		promotionEngineService.updateHHCountInAuthorizedItemMap(promoEngineDataDTO.itemClassificationFullData, promoEngineDataDTO.authroizedItemMap);

		logger.info("Getting item classifcation data is completed...");
	}

	private void actualAdPrediction() throws GeneralException, OfferManagementException {

		logger.info("actualAdRecommendation started...");

		// Update reg, sale price and cost of actual ad items
		promotionEngineService.updateAttributesOfActualAdItems(promoEngineDataDTO.actualAdDetail, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.saleDetailMap, promoEngineDataDTO.displayDetailMap, promoEngineDataDTO.recWeekCalDTO.getStartDate());

		// Get Prediction for Actual items
		HashMap<ProductKey, List<PromoItemDTO>> actualAdPredResults = promotionEngineService.getPredictionOfActualAd(inputLocationLevelId,
				inputLocationId, promoEngineDataDTO.retailCalendarCache, promoEngineDataDTO.ligAndItsMember,
				promoEngineDataDTO.recWeekCalDTO.getStartDate(), promoEngineDataDTO.actualAdDetail);

		// Update Margin , revenue and HH# for actual Ad
		promotionEngineService.updatePredictionsOfActualAd(inputLocationLevelId, inputLocationId, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.actualAdDetail, actualAdPredResults, promoEngineDataDTO.ligAndItsMember);
		logger.info("actualAdRecommendation completed...");
	}
}
