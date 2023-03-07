package com.pristine.dataload.offermgmt.promotion;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.dao.LocationGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.promotion.PromoItemGroupFinderDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.promotion.PromoItemGroupFinderService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PromoItemGroupFinder extends BasePromotionEngine{
	private static Logger logger = Logger.getLogger("PromoItemGroupFinder");

	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String WEEK_START_DATE_FOR_ANALYSIS = "WEEK_START_DATE_FOR_ANALYSIS=";
	private static final String NO_OF_WEEKS_FOR_ANALYSIS = "NO_OF_WEEKS_FOR_ANALYSIS=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";

	private int inputLocationLevelId = -1;
	private int inputLocationId = -1;
	private int inputProductLevelId = -1;
	private int inputProductId = -1;
	private int noOfWeeksForAnalysis = -1;
	private String weekStartDateForAnalysis = "";
	private int noOfWeeksPriceHistory = 13;
	private int noOfWeeksItemMovementHistory = 53;
	
	private Connection conn = null;
	PromoItemGroupFinderService promoItemGroupFinderService = new PromoItemGroupFinderService();
	List<PromoItemDTO> adDetails = new ArrayList<PromoItemDTO>();
	HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> itemsByCategory = new HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>>();
	private int chainId = 0;
	private List<Integer> allStores  = new ArrayList<Integer>();
	List<PRItemDTO> authorizedItems = new ArrayList<PRItemDTO>();
	
	public static void main(String[] args) {
		PropertyManager.initialize("recommendation.properties");
		PromoItemGroupFinder promoItemGroupFinder = new PromoItemGroupFinder();

		// Set input arguments
		promoItemGroupFinder.setArguments(args);
		
		promoItemGroupFinder.mainEntry();
	}

	private void setArguments(String[] args) {
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(LOCATION_LEVEL_ID)) {
					inputLocationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
				} else if (arg.startsWith(LOCATION_ID)) {
					inputLocationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
				} else if (arg.startsWith(NO_OF_WEEKS_FOR_ANALYSIS)) {
					noOfWeeksForAnalysis = Integer.parseInt(arg.substring(NO_OF_WEEKS_FOR_ANALYSIS.length()));
				} else if (arg.startsWith(WEEK_START_DATE_FOR_ANALYSIS)) {
					weekStartDateForAnalysis = arg.substring(WEEK_START_DATE_FOR_ANALYSIS.length());
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					inputProductLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
				} else if (arg.startsWith(PRODUCT_ID)) {
					inputProductId = Integer.parseInt(arg.substring(PRODUCT_ID.length()));
				}
			}
			logger.debug("PromoItemGroupFinder: Input Arguments - inputLocationLevelId : " + inputLocationLevelId + " inputLocationId : "
					+ inputLocationId + " noOfWeeksForAnalysis : " + noOfWeeksForAnalysis);
		}
	}
	
	private void mainEntry() {
		try {
			
			initialize();
			
			//Get base data
			getBaseData();
			
			findItemGroups(adDetails);
			
			// Commit transaction
			PristineDBUtil.commitTransaction(getConnection(), "Commit Recommendation");
			
		} catch (GeneralException | Exception | OfferManagementException e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
	}
	
	private void initialize() throws GeneralException, Exception {
		logger.debug("PromotionEngineService: initializing connection and properties");
		
		setConnection();
		
		initializeLoggerInfo();
		
		// Get chain Id
		chainId = Integer.valueOf(new RetailPriceDAO().getChainId(getConnection()));
		
		LocationGroupDAO locationGroupDAO = new LocationGroupDAO(getConnection());
		allStores = locationGroupDAO.getStoresOfLocation(inputLocationLevelId, inputLocationId);
	}
	
	private void initializeLoggerInfo() throws GeneralException{
		// Set log name
		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(weekStartDateForAnalysis);
		String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate, "MM-dd-yyy");
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String logName = "PPG_" + String.valueOf(inputLocationLevelId) + "_" + String.valueOf(inputLocationId) 
				+ "_" + tempWeekStartDate + "_" + timeStamp;
		setLog4jProperties(logName);
	}
	
	/**
	 * Sets database connection. Used when program runs in batch mode
	 * 
	 * @throws Exception
	 */
	protected void setConnection() throws Exception {
		if (conn == null) {
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
	
	private void getBaseData() throws GeneralException, OfferManagementException {
		ItemService itemService = new ItemService(new ArrayList<ExecutionTimeLog>());
		
		logger.debug("Get Authorized items : location level : " + inputLocationLevelId + " location : " + inputLocationId
				+ " product level : " + inputProductLevelId + " product : " + inputProductId);
		
		// get authorized item
		authorizedItems = itemService.getAuthorizedItems(getConnection(), inputLocationLevelId, inputLocationId, inputProductLevelId, inputProductId);
		
		// Convert to hashmap
		HashMap<ProductKey, PRItemDTO> authorizedItemMap = promoItemGroupFinderService.convertAuthorizedItemsToMap(authorizedItems);
		
		// get recent regular price
		getPrice(authorizedItemMap);
		
		// group authorized item by category Id, also convert PRItemDTO to PromoItemDTO
		itemsByCategory = promoItemGroupFinderService.groupItemsByCategory(authorizedItemMap);
		
		// Get last x weeks weekly ad details
		adDetails = promoItemGroupFinderService.getActualAdDetails(getConnection(), inputLocationLevelId, inputLocationId, inputProductLevelId,
				inputProductId, weekStartDateForAnalysis, noOfWeeksForAnalysis);
	}
	
	private void findItemGroups(List<PromoItemDTO> adDetails) throws GeneralException {
		HashMap<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>> itemsGroupedByWeekAndPageBlock = new HashMap<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>>();
		List<PromoProductGroup> productGroups = new ArrayList<PromoProductGroup>();
		PromoItemGroupFinderDAO promoItemGroupFinderDAO = new PromoItemGroupFinderDAO();
		
		// Group items by week
		itemsGroupedByWeekAndPageBlock = promoItemGroupFinderService.groupByAdWeekPageBlock(adDetails);
		
		// Analysis block
		List<BlockDetail> blockDetails = promoItemGroupFinderService.analysisBlock(itemsGroupedByWeekAndPageBlock, itemsByCategory);
		
		HashMap<Set<ProductKey>, List<BlockDetail>> groupedBySameSetOfCategories = promoItemGroupFinderService.groupByCategories(blockDetails);
		
		HashMap<Set<Integer>, List<BlockDetail>> groupedBySameSetOfBrands = promoItemGroupFinderService.groupByBrands(blockDetails);
		
		productGroups.addAll(promoItemGroupFinderService.findGroups(groupedBySameSetOfCategories, groupedBySameSetOfBrands, itemsByCategory));
		
		// Find item that can be grouped across the categories
//		productGroups.addAll(promoItemGroupFinderService.groupItemAcrossCategories(groupedBySameSetOfCategories, itemsByCategory));
		
		// Find multiple brands that can be grouped together
//		productGroups.addAll(promoItemGroupFinderService.groupItemAcrossBrands(groupedBySameSetOfBrands, itemsByCategory));
		
		// Roll to LIG level and non-lig 
		List<PromoProductGroup> finalProductGroups = promoItemGroupFinderService.rollupToLigAndNonLig(productGroups);
		
		//For debugging
		logger.debug("finalProductGroups.size():" + finalProductGroups.size());
		
		// Remove duplicate groups
		List<PromoProductGroup> distinctGroups = promoItemGroupFinderService.removeDuplicateGroups(finalProductGroups);
		
		logger.debug("distinctGroups.size():" + distinctGroups.size());
		
		// Find Total items movement for authorized items in last X weeks 
		HashMap<ProductKey, Long> lastXWeeksTotalMov = promoItemGroupFinderService.getLastXWeeksMovForStoreList(getConnection(), inputLocationLevelId, 
				inputLocationId, weekStartDateForAnalysis, noOfWeeksItemMovementHistory, authorizedItems);
		
		// Get the lead item for each group
		List<PromoProductGroup> distinctGroupsWithLeadItems = promoItemGroupFinderService.getLeadItemForGroup(distinctGroups, lastXWeeksTotalMov, authorizedItems);
		
		logger.debug("distinctGroupsWithLeadItems.size():" + distinctGroupsWithLeadItems.size());
		
		//Further filter the same or subsets groups with same lead items
//		List<PromoProductGroup> finalizedGroupsWithLeadItems = promoItemGroupFinderService.filterMatchingGroups(distinctGroupsWithLeadItems);
//		
		// Insert in to database
		promoItemGroupFinderDAO.insertPPGHeader(getConnection(), distinctGroupsWithLeadItems, inputProductId);
		promoItemGroupFinderDAO.insertPPGItems(getConnection(), distinctGroupsWithLeadItems);
	}
	
	private void getPrice(HashMap<ProductKey, PRItemDTO> authorizedItemMap) throws GeneralException {
		logger.info("Getting price is started...");
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		List<String> priceAndStrategyZoneNos = new ArrayList<String>();
		
		
		RetailCalendarDTO curCalDTO = retailCalendarDAO.getCalendarId(getConnection(), DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
		
		// TODO:: Currently chain level price is taken

		HashMap<ItemKey, PRItemDTO> tempAuthorizedItemMap = new HashMap<ItemKey, PRItemDTO>();
		for (Map.Entry<ProductKey, PRItemDTO> item : authorizedItemMap.entrySet()) {
			int lirIndicator = (item.getKey().getProductLevelId() == Constants.ITEMLEVELID ? PRConstants.NON_LIG_ITEM_INDICATOR
					: PRConstants.LIG_ITEM_INDICATOR);
			ItemKey itemKey = new ItemKey(item.getKey().getProductId(), lirIndicator);
			tempAuthorizedItemMap.put(itemKey, item.getValue());
		}

		priceAndStrategyZoneNos.add("0");
		pricingEngineDAO.getPriceDataOptimized(getConnection(), chainId, new PRStrategyDTO(), curCalDTO, null, noOfWeeksPriceHistory,
				tempAuthorizedItemMap, null, new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>(), true, priceAndStrategyZoneNos, allStores);

		logger.info("Getting price is completed...");
	}
}
