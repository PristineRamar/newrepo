package com.pristine.dataload.offermgmt.promotion;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.promotion.PromoExplainationDAO;
import com.pristine.dao.offermgmt.promotion.PromoStrategyDAO;
import com.pristine.dao.offermgmt.promotion.PromotionEngineDAO;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRZoneStoreReccommendationFlag;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoEngineDataDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.ProductService;
import com.pristine.service.RetailCalendarService;
import com.pristine.service.offermgmt.CheckListService;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.StrategyService;
import com.pristine.service.offermgmt.promotion.ItemSelectionService;
import com.pristine.service.offermgmt.promotion.PromotionEngineService;
import com.pristine.service.offermgmt.promotion.PromotionRecommendationService;
import com.pristine.service.offermgmt.promotion.PromotionSelectionService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

/**
 * Promotion recommendation engine recommends promotions for items in a category/department etc.,
 * 
 * @author Pradeepkumar
 *
 */
public class PromotionRecommendationEngine extends BasePromotionEngine {

	private static Logger logger = Logger.getLogger("PromotionEngine");

	// Input parameters
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_IDS = "PRODUCT_IDS=";
	private static final String CAL_TYPE = "CAL_TYPE=";
	private static final String START_DATE = "START_WEEK=";
	private static final String END_DATE = "END_WEEK=";
	private static final String LEAD_LOCATION_LEVEL_ID = "LEAD_LOCATION_LEVEL_ID=";
	private static final String LEAD_LOCATION_ID = "LEAD_LOCATION_ID=";
	// Input Variables
	private int locationLevelId = -1;
	private int locationId = -1;
	private int productLevelId = -1;
	private String productIds = "";
	private Set<Integer> productIdSet = new HashSet<Integer>();
	private String calType;
	private String startWeek;
	private String endWeek;
	private int leadLocationLevelId = -1;
	private int leadLocationId = -1;
	
	
	// Connection variables
	private Connection conn = null;

	int chainId;

	PromoEngineDataDTO promoEngineDataDTO = PromoEngineDataDTO.getPromoEngineDataDTO();
	PromotionEngineService promotionEngineService = null;
	HashMap<Long, PromoProductGroup> ppgGroupDetails = new HashMap<Long, PromoProductGroup>();
	RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();
	CommonDataHelper commonDataHelper = null;
	public static void main(String[] args) {

		PropertyManager.initialize("recommendation.properties");

		PromotionRecommendationEngine promotionRecommendationEngine = new PromotionRecommendationEngine();

		// Set arguments
		promotionRecommendationEngine.setArguments(args);

		// Validate input arguments
		promotionRecommendationEngine.validateArguments();
		
		// Recommendation
		promotionRecommendationEngine.startRecommendationProcess();
		
	}

	private void setArguments(String args[]) {

		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(LOCATION_LEVEL_ID)) {
					setLocationLevelId(Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length())));
				} else if (arg.startsWith(LOCATION_ID)) {
					setLocationId(Integer.parseInt(arg.substring(LOCATION_ID.length())));
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					setProductLevelId(Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				} else if (arg.startsWith(PRODUCT_IDS)) {
					setProductIds(arg.substring(PRODUCT_IDS.length()));
				} else if (arg.startsWith(CAL_TYPE)) {
					setCalType(arg.substring(CAL_TYPE.length()));
				} else if (arg.startsWith(START_DATE)) {
					setStartWeek(arg.substring(START_DATE.length()));
				} else if (arg.startsWith(END_DATE)) {
					setEndWeek(arg.substring(END_DATE.length()));
				} else if (arg.startsWith(LEAD_LOCATION_ID)) {
					setLeadLocationId(Integer.parseInt(arg.substring(LEAD_LOCATION_ID.length())));
				} else if (arg.startsWith(LEAD_LOCATION_LEVEL_ID)) {
					setLeadLocationLevelId(Integer.parseInt(arg.substring(LEAD_LOCATION_LEVEL_ID.length())));
				} 
			}
		}
	}
	
	private void validateArguments() {
		if (locationLevelId == -1 || locationId == -1 || startWeek == null || (productLevelId == -1)) {
			logger.error("One or more mandatory input arguments (locationLevelId, locationId, "
					+ " inputWeekStartDate, inputPageNo) missing. Program Terminated!!!");
			System.exit(1);
		}
	}
	
	private void startRecommendationProcess() {
		try {
			// 1. Initialize
			initialize();

			// 2. Get base data
			prepareBaseData();
			
			// 3. Identify candidate items
			callItemSelection();
			
			// 4. Identify promo types
			callPromoSelection();
			
			// 5. Recommed promotions
			callPromoRecommendation();
			
			PristineDBUtil.commitTransaction(conn, "Rec successful");

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
			
			System.exit(0);
		}
	}

	
	private void initialize() throws GeneralException, Exception {
		logger.debug("PromotionEngineService: initializing connection and properties");

		setConnection();

		initializePrimaryData();

		initializeLoggerInfo();

	}
	
	private void initializePrimaryData() throws GeneralException {

		String[] productsArr = productIds.split(",");
		for(String productId: productsArr) {
			productIdSet.add(Integer.parseInt(productId));
		}
		
		// If Dept given in input then pick that dept
		if (productIdSet.size() > 0) {
			promoEngineDataDTO.productIdsToProcess.clear();
			promoEngineDataDTO.productIdsToProcess.addAll(productIdSet);
		}

		promotionEngineService = new PromotionEngineService(locationLevelId, locationId, productLevelId,
				promoEngineDataDTO.productIdsToProcess, startWeek, 0);

		// Cache retail Calendar
		promoEngineDataDTO.retailCalendarCache = promotionEngineService.getRetailCalendar(getConnection());

		// Get chain Id
		chainId = Integer.valueOf(new RetailPriceDAO().getChainId(getConnection()));

		// Get all weekly calendar details
		promoEngineDataDTO.allWeekPromoCalendarDetails = new RetailCalendarService().getAllWeeks(promoEngineDataDTO.retailCalendarCache);

		// Rec week calendar details
		promoEngineDataDTO.recWeekCalDTO = new RetailCalendarService().getWeekCalendarDetail(promoEngineDataDTO.retailCalendarCache,
				startWeek);

		// Get stores of store list
		
		recommendationInputDTO.setLocationLevelId(locationLevelId);
		recommendationInputDTO.setLocationId(locationId);
		recommendationInputDTO.setLeadLocationLevelId(leadLocationLevelId);
		recommendationInputDTO.setLeadLocationId(leadLocationId);
		recommendationInputDTO.setProductLevelId(productLevelId);
		recommendationInputDTO.setProductId(productIdSet.iterator().next());
		recommendationInputDTO.setChainId(chainId);
		recommendationInputDTO.setStartWeek(startWeek);
		recommendationInputDTO.setEndWeek(endWeek);
		recommendationInputDTO.setBaseWeek(startWeek);
		recommendationInputDTO.setStartCalendarId(promoEngineDataDTO.allWeekPromoCalendarDetails.get(startWeek).getCalendarId());
		recommendationInputDTO.setEndCalendarId(promoEngineDataDTO.allWeekPromoCalendarDetails.get(endWeek).getCalendarId());
		recommendationInputDTO.setActualStartCalendarId(recommendationInputDTO.getActualStartCalendarId());
		recommendationInputDTO.setActualEndCalendarId(recommendationInputDTO.getActualEndCalendarId());
		MWRRunHeader mwrRunHeader = MWRRunHeader.getRunHeaderDTO(recommendationInputDTO, 0, 0);
		recommendationInputDTO.setMwrRunHeader(mwrRunHeader);
		
		// TODO Change PRICE_ZONE_ID_3
		int divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConnection(),
				promoEngineDataDTO.inputDTO.getLocationId());
		
		recommendationInputDTO.setDivisionId(divisionId);
		
		promoEngineDataDTO.inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		
		
		promoEngineDataDTO.allStores = new ItemService(new ArrayList<>()).getPriceZoneStores(conn, promoEngineDataDTO.inputDTO.getProductLevelId(),
				promoEngineDataDTO.inputDTO.getProductId(), promoEngineDataDTO.inputDTO.getLocationLevelId(),
				promoEngineDataDTO.inputDTO.getLocationId());
		
		commonDataHelper = new CommonDataHelper();
		commonDataHelper.getCommonData(getConnection(), recommendationInputDTO);
		
		promoEngineDataDTO.allWeekCalendarDetails = commonDataHelper.getAllWeekCalendarDetails();
	}
	
	private void initializeLoggerInfo() throws GeneralException {
		// Set log name
		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(startWeek);
		String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate, "MM-dd-yyy");
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String logName = "";
		if (productIdSet.size() > 1) {
			logName = String.valueOf(locationLevelId) + "_" + String.valueOf(locationId) + "_" + tempWeekStartDate + "_" + timeStamp;
		} else {
			logName = String.valueOf(locationLevelId) + "-" + String.valueOf(locationId) + "_" + productLevelId + "-"
					+ String.valueOf(productIdSet.iterator().next()) + "_" + tempWeekStartDate + "_" + timeStamp;
		}
		
		setLog4jProperties(logName);
	}
	
	
	
	private void prepareBaseData() throws GeneralException, OfferManagementException {
		logger.info("PromotionEngineService: fetching Base data");

		// Get authorized items
		getAuthorizedItems();

		// Get Strategy details
		getStrategy();
		
		// Getting store list price
		getPrice();

		// Getting store list cost
		getCost();

		// update lig level price and cost
		promotionEngineService.updateLigLevelData(promoEngineDataDTO.authroizedItemMap, promoEngineDataDTO.ligAndItsMember);
				
		// Getting competitor price
		//getCompPrice();

		
		//getSaleInfo();
		
		// Populate item map
		populateCandidateItemMap();
		
	}
	
	
	/**
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	private void getStrategy() throws GeneralException, OfferManagementException {
		PromoStrategyDAO promoStrategyDAO = new PromoStrategyDAO();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		StrategyService strategyService = new StrategyService(new ArrayList<>());
		PRZoneStoreReccommendationFlag pRZoneStoreReccommendationFlag = new PRZoneStoreReccommendationFlag();
		PRStrategyDTO leadInputDTO = new PRStrategyDTO();

		promoEngineDataDTO.strategyMap = promoStrategyDAO.getAllActivePromoStrategies(getConnection(),
				promoEngineDataDTO.inputDTO, recommendationInputDTO.getDivisionId());

		
		promoEngineDataDTO.priceCheckListInfo = new PricingEngineDAO().getPriceCheckListInfo(conn,
				promoEngineDataDTO.inputDTO.getLocationLevelId(), promoEngineDataDTO.inputDTO.getLocationId(),
				promoEngineDataDTO.inputDTO.getProductLevelId(), promoEngineDataDTO.inputDTO.getProductId(),null);

		HashMap<String, ArrayList<Integer>> productListMap = new ProductGroupDAO().getProductListForProducts(
				getConnection(), promoEngineDataDTO.inputDTO.getProductLevelId(),
				promoEngineDataDTO.inputDTO.getProductId());

		HashMap<Integer, Integer> productParentChildRelationMap = new ProductService()
				.getProductLevelRelationMap(getConnection(), productLevelId);

		new CheckListService().populatePriceCheckListDetailsZone(getConnection(), Integer.valueOf(chainId),
				recommendationInputDTO.getDivisionId(), promoEngineDataDTO.inputDTO.getLocationLevelId(),
				promoEngineDataDTO.inputDTO.getLocationId(), promoEngineDataDTO.inputDTO.getProductLevelId(),
				promoEngineDataDTO.inputDTO.getProductId(), promoEngineDataDTO.itemDataMap,
				promoEngineDataDTO.priceCheckListInfo, leadInputDTO.getLocationId(), promoEngineDataDTO.strategyMap,
				promoEngineDataDTO.ligAndItsMember, productListMap, pRZoneStoreReccommendationFlag,
				productParentChildRelationMap, pricingEngineDAO, strategyService, leadInputDTO,
				promoEngineDataDTO.inputDTO, 0);

		strategyService.getStrategiesForEachItem(getConnection(), promoEngineDataDTO.inputDTO, pricingEngineDAO,
				promoEngineDataDTO.strategyMap, promoEngineDataDTO.itemDataMap, productParentChildRelationMap,
				promoEngineDataDTO.ligAndItsMember, productListMap, String.valueOf(chainId),
				recommendationInputDTO.getDivisionId(), true, pRZoneStoreReccommendationFlag, leadInputDTO, 0,
				leadInputDTO.getLocationId());

	}
	
	
	
	
	/**
	 * Gets authorized items for given products and location combination
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	private void getAuthorizedItems() throws GeneralException, OfferManagementException {
		logger.info("Getting authorized items started...");

		promoEngineDataDTO.authroizedItemMapDeptWise = promotionEngineService.getAuthorizedItems(getConnection(), locationLevelId,
				locationId, productLevelId, promoEngineDataDTO.allStores, promoEngineDataDTO.productIdsToProcess, new HashSet<>());

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
		
		
		// Populate lig related info
		new ItemService(new ArrayList<>()).addLigItemInItemMap(promoEngineDataDTO.itemDataMap);
		
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
	
	
	
	/**
	 * Gets price history
	 * @throws GeneralException
	 */
	private void getPrice() throws GeneralException {
		logger.info("Getting price is started...");

		int priceHistory = Integer.parseInt(PropertyManager.getProperty("PR_PRICE_HISTORY"));
		
		
		int daysToBeAdded = Integer.parseInt(PropertyManager.getProperty("DAYS_TO_ADD_FOR_BUSINESS_CAL", "0"));
		
		LocalDate startDate = LocalDate.parse(startWeek, PRCommonUtil.getDateFormatter());
		LocalDate businessCalStartDate = startDate.plus(daysToBeAdded, ChronoUnit.DAYS);
		String businessStartWeek = PRCommonUtil.getDateFormatter().format(businessCalStartDate);
		
		RetailCalendarDTO retailCalendarDTO = promoEngineDataDTO.allWeekCalendarDetails.get(businessStartWeek);
		
		promotionEngineService.getPriceOfAuthorizedItems(conn, chainId, retailCalendarDTO, priceHistory,
				promoEngineDataDTO.allWeekCalendarDetails, promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.itemDataMap, promoEngineDataDTO.itemStoreMap);
		
		logger.info("Getting price is completed...");
	}

	
	/**
	 * Gets cost history
	 * @throws GeneralException
	 */
	private void getCost() throws GeneralException {
		logger.info("Getting cost is started...");
		
		int costHistory = Integer.parseInt(PropertyManager.getProperty("PR_COST_HISTORY"));
		
		int daysToBeAdded = Integer.parseInt(PropertyManager.getProperty("DAYS_TO_ADD_FOR_BUSINESS_CAL", "0"));
		
		LocalDate startDate = LocalDate.parse(startWeek, PRCommonUtil.getDateFormatter());
		LocalDate businessCalStartDate = startDate.plus(daysToBeAdded, ChronoUnit.DAYS);
		String businessStartWeek = PRCommonUtil.getDateFormatter().format(businessCalStartDate);

		
		RetailCalendarDTO retailCalendarDTO = promoEngineDataDTO.allWeekCalendarDetails.get(businessStartWeek);
		
		promotionEngineService.getCostOfAuthorizedItems(conn, chainId, retailCalendarDTO, costHistory,
				promoEngineDataDTO.allWeekCalendarDetails, promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap,
				promoEngineDataDTO.itemDataMap, promoEngineDataDTO.itemStoreMap);
		
		logger.info("Getting cost is completed...");
	}

	/**
	 * Gets comp price history
	 * @throws GeneralException
	 */
	private void getCompPrice() throws GeneralException {
		promotionEngineService.getCompetitorPrice(conn, promoEngineDataDTO.allStores, promoEngineDataDTO.authroizedItemMap, productLevelId,
				promoEngineDataDTO.getDepartmentListAsString(), startWeek, promoEngineDataDTO.ligAndItsMember,
				promoEngineDataDTO.itemStoreMap, 0, 52);

	}

	
	/**
	 * 
	 * @throws GeneralException
	 */
	private void callItemSelection() throws GeneralException {
		ItemSelectionService itemSelectionService = new ItemSelectionService();
		
		for (Map.Entry<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weekEntry : promoEngineDataDTO.candidateItemMap
				.entrySet()) {

			logger.info("Total # of items: " + weekEntry.getValue().size());
			
			HashMap<ProductKey, List<PromoItemDTO>> candidates = itemSelectionService.selectCandidateItems(
					getConnection(), recommendationInputDTO, promoEngineDataDTO.allStores, weekEntry.getValue(),
					weekEntry.getKey(), promoEngineDataDTO.ligAndItsMember);

			logger.info("# of items selected: " + candidates.size());
			
			promoEngineDataDTO.candidateItemMap.put(weekEntry.getKey(), candidates);
			
			logger.info("# of items selected: " + weekEntry.getValue().size());
		}
	}
	
	
	/**
	 * 
	 * @throws GeneralException
	 */
	private void callPromoSelection() throws GeneralException {
		PromotionSelectionService promotionSelectionService = new PromotionSelectionService();
		for (Map.Entry<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weekEntry : promoEngineDataDTO.candidateItemMap
				.entrySet()) {
			promotionSelectionService.selectPromotions(conn, weekEntry.getValue(), promoEngineDataDTO.ligAndItsMember,
					recommendationInputDTO);
		}
	}
	
	/**
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception 
	 */
	private void callPromoRecommendation() throws GeneralException, OfferManagementException, Exception {

		PromotionRecommendationService promotionRecommendationService = new PromotionRecommendationService(conn,
				promoEngineDataDTO.allWeekPromoCalendarDetails);

		PromoExplainationDAO promoExplainationDAO = new PromoExplainationDAO();
		PromotionEngineDAO promotionEngineDAO = new PromotionEngineDAO();

		// 1. Call recommendation service
		promotionRecommendationService.recommendPromotions(recommendationInputDTO, promoEngineDataDTO.candidateItemMap,
				promoEngineDataDTO.ligAndItsMember);

		// 2. Store promo explanation
		promoExplainationDAO.savePromoExplanationData(conn, recommendationInputDTO, promoEngineDataDTO.candidateItemMap);

		// 3. Save recommendation data
		promotionEngineDAO.savePromoRecommendationData(getConnection(), recommendationInputDTO,
				promoEngineDataDTO.candidateItemMap);
	}
	
	
	/*private void getSaleInfo() throws GeneralException {
		logger.info("Getting sale price is started...");
		
		// Get previous week promotion
		promoEngineDataDTO.saleDetailMap = promotionEngineService.getSaleInfo(getConnection(),
				promoEngineDataDTO.authroizedItemMap, getSaleWeeks(), chainId,
				recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
				recommendationInputDTO.getProductLevelId(), promoEngineDataDTO.getDepartmentListAsString(),
				promoEngineDataDTO.allStores, false);
	}*/
	
	/**
	 * data population for each week
	 */
	private void populateCandidateItemMap() {
		HashMap<RecWeekKey,HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap = new HashMap<>();
		List<RetailCalendarDTO> weeksToProcess = getWeeksToProcess();
		logger.info("populateCandidateItemMap() - # of weeks to process: " + weeksToProcess.size());
		
		logger.info("populateCandidateItemMap() - Total # of items: " + promoEngineDataDTO.authroizedItemMap.size());
		
		logger.info("populateCandidateItemMap() - # of items without price: " + promoEngineDataDTO.authroizedItemMap
				.values().stream().filter(p -> p.getRecommendedRegPrice() == null).count());
		
		logger.info("populateCandidateItemMap() - # of items without cost: " + promoEngineDataDTO.authroizedItemMap
				.values().stream().filter(p -> p.getListCost() == null).count());
		
		
		weeksToProcess.forEach(rc -> {
			RecWeekKey recWeekKey = new RecWeekKey(rc.getCalendarId(), rc.getStartDate());
			HashMap<ProductKey, List<PromoItemDTO>> itemMap = new HashMap<>();
			promoEngineDataDTO.authroizedItemMap.forEach((productKey, prItemDTO) -> {
				if (prItemDTO.getRecommendedRegPrice() != null && prItemDTO.getListCost() != null) {
					List<PromoItemDTO> promoItems = new ArrayList<>();
					PromoItemDTO promoItemDTO = new PromoItemDTO();
					promoItemDTO.setProductKey(productKey);
					promoItemDTO.setStrategyDTO(prItemDTO.getStrategyDTO());
					if (prItemDTO.getStrategyDTO() != null) {
						promoItemDTO.setObjectiveTypeId(prItemDTO.getStrategyDTO().getObjective().getObjectiveTypeId());
					}
					promoItemDTO.setRegPrice(prItemDTO.getRecommendedRegPrice());
					promoItemDTO.setItemName(prItemDTO.getItemName());
					promoItemDTO.setRetLirName(prItemDTO.getRetLirName());
					promoItemDTO.setRetLirId(prItemDTO.getRetLirId());
					promoItemDTO.setCategoryId(prItemDTO.getCategoryProductId());
					promoItemDTO.setCompPrice(prItemDTO.getCompPrice());
					promoItemDTO.setUpc(prItemDTO.getUpc());
					promoItemDTO.setActive(true);
					promoItemDTO.setDeptId(prItemDTO.getDeptIdPromotion());
					promoItemDTO.setNoOfHHRecommendedTo(prItemDTO.getUniqueHHCount());
					promoItemDTO.setBrandId(prItemDTO.getBrandId());
					promoItemDTO.setBrandName(prItemDTO.getBrandName());
					promoItemDTO.setListCost(prItemDTO.getListCost());
					promoItemDTO.setDealCost(prItemDTO.getDealCost());
					promoItems.add(promoItemDTO);
					itemMap.put(productKey, promoItems);
				}else {
					logger.debug("Price/Cost not found: " + productKey.toString());
				}
			});
			//itemGroup.put(1, itemMap);
			//promoEngineDataDTO.itemGroups.put(recWeekKey, itemGroup);
			candidateItemMap.put(recWeekKey, itemMap);
		});
		
		promoEngineDataDTO.candidateItemMap = candidateItemMap;
	}

	/**
	 * 
	 * @return weeks to process
	 */
	private List<RetailCalendarDTO> getWeeksToProcess(){
		
		LocalDate startDate = LocalDate.parse(startWeek, PRCommonUtil.getDateFormatter());
		List<RetailCalendarDTO> weeksToProcess = promoEngineDataDTO.retailCalendarCache.stream()
				.filter(rc -> rc.getRowType().equals(Constants.CALENDAR_WEEK)
						&& (rc.getStartDateAsDate().equals(startDate)))
				.collect(Collectors.toList());
		
		return weeksToProcess;
		
	}
	
/*	private List<RetailCalendarDTO> getSaleWeeks(){
		
		LocalDate endDate = LocalDate.parse(startWeek, PRCommonUtil.getDateFormatter()).minus(1, ChronoUnit.DAYS);
		LocalDate startDate = LocalDate.parse(startWeek, PRCommonUtil.getDateFormatter()).minus(13, ChronoUnit.WEEKS);
		
		List<RetailCalendarDTO> weeksToProcess = promoEngineDataDTO.retailCalendarCache.stream()
				.filter(rc -> rc.getRowType().equals(Constants.CALENDAR_WEEK)
						&& (rc.getStartDateAsDate().isAfter(startDate)
								|| rc.getStartDateAsDate().isEqual(startDate))
						&& (rc.getEndDateAsDate().isBefore(endDate)
								|| rc.getEndDateAsDate().isEqual(endDate)))
				.collect(Collectors.toList());

		return weeksToProcess;
	}*/
	
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
	
	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public String getProductIds() {
		return productIds;
	}

	public void setProductIds(String productIds) {
		this.productIds = productIds;
	}

	public Set<Integer> getProductIdSet() {
		return productIdSet;
	}

	public void setProductIdSet(Set<Integer> productIdSet) {
		this.productIdSet = productIdSet;
	}

	public String getCalType() {
		return calType;
	}

	public void setCalType(String calType) {
		this.calType = calType;
	}

	public String getStartWeek() {
		return startWeek;
	}

	public void setStartWeek(String startWeek) {
		this.startWeek = startWeek;
	}

	public String getEndWeek() {
		return endWeek;
	}

	public void setEndWeek(String endWeek) {
		this.endWeek = endWeek;
	}

	public int getLeadLocationLevelId() {
		return leadLocationLevelId;
	}

	public void setLeadLocationLevelId(int leadLocationLevelId) {
		this.leadLocationLevelId = leadLocationLevelId;
	}

	public int getLeadLocationId() {
		return leadLocationId;
	}

	public void setLeadLocationId(int leadLocationId) {
		this.leadLocationId = leadLocationId;
	}

}
