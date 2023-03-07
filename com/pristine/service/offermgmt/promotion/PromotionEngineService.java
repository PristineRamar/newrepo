package com.pristine.service.offermgmt.promotion;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.LocationCompetitorMapDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.ItemDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.promotion.PromotionEngineDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.itemClassification.ItemClassificationDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.dto.offermgmt.promotion.AdDetail;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PageDetail;
import com.pristine.dto.offermgmt.promotion.PromoAdSaleInfo;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.dto.offermgmt.promotion.SalePriceKey;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.RetailCalendarService;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

/***
 * All db calls go through this class
 * 
 * @author Nagarajan
 *
 */
public class PromotionEngineService {

	private static Logger logger = Logger.getLogger("PromotionEngineService");

	// TODO:: put in configuration
	private int pastPromotionsDurationInYear = 3;
	private int maxPromoCombination = Integer.valueOf(PropertyManager.getProperty("PROMO_COMBINATIONS_PER_ITEM"));;
	// private Double minMarginPCT = 10.0, maxDiscountPCT = 25.0;

	// private int noOfTopMarginItem = Integer.valueOf(PropertyManager.getProperty("MAX_ITEM_PER_BLOCK"));
	private String itemAnalysisLogPath = PropertyManager.getProperty("AD_REC_ITEM_ANALYSIS_LOG_PATH");
	// private String excelReportPath = PropertyManager.getProperty("AD_REC_ITEM_ANALYSIS_LOG_PATH");

	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	// private int productId;
	private String weekStartDate;
	 private Set<Integer> departmentList;
	// private int adPageNo;
	private ItemLogService itemLogService = new ItemLogService();
	// private AdRecReportService reportGenService = new AdRecReportService();

	public PromotionEngineService(int locationLevelId, int locationId, int productLevelId, Set<Integer> departmentList, String weekStartDate,
			int adPageNo) throws GeneralException {
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.productLevelId = productLevelId;
		 this.departmentList = departmentList;
		// this.productId = productId;
		this.weekStartDate = weekStartDate;
		// this.adPageNo = adPageNo;
		setLogPath(departmentList);
	}

	/**
	 * Get calendar details of all weeks
	 * 
	 * @return
	 * @throws GeneralException
	 */
	public List<RetailCalendarDTO> getRetailCalendar(Connection conn) throws GeneralException {
		return new RetailCalendarDAO().getFullRetailCalendar(conn);
	}

	public HashMap<String, RetailCalendarDTO> getAllWeeksCalendarDetail(Connection conn) throws GeneralException {
		return new RetailCalendarDAO().getAllWeeks(conn);
	}

	public HashMap<Integer, HashMap<ProductKey, PRItemDTO>> getAuthorizedItems(Connection conn, int locationLevelId, int locationId,
			int productLevelId, List<Integer> stores, Set<Integer> npDepartments, Set<Integer> debugItems) throws GeneralException {

		HashMap<Integer, HashMap<ProductKey, PRItemDTO>> authroizedItemMap = new HashMap<Integer, HashMap<ProductKey, PRItemDTO>>();
		for (Integer deptId : npDepartments) {

			HashMap<ProductKey, PRItemDTO> authroizedItemMapTemp = getAuthorizedItemsOneDept(conn, locationLevelId, locationId, productLevelId,
					deptId, stores, debugItems);
			authroizedItemMap.put(deptId, authroizedItemMapTemp);
		}
		return authroizedItemMap;
	}

	public HashMap<ProductKey, PRItemDTO> getAuthorizedItemsOneDept(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId, List<Integer> stores, Set<Integer> debugItems) throws GeneralException {

		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		HashMap<ProductKey, PRItemDTO> authroizedItemMap = new HashMap<ProductKey, PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		PRStrategyDTO inputDTO = new PRStrategyDTO();

		try {
			logger.debug("getAuthorizedItems(): Start...");
			// Setting input
			inputDTO.setLocationId(locationId);
			inputDTO.setLocationLevelId(locationLevelId); // storeList level
			inputDTO.setProductId(productId); // Major category
			inputDTO.setProductLevelId(productLevelId);

			ItemService itemService = new ItemService(executionTimeLogs);
			ItemDAO itemDAO = new ItemDAO();
			List<PRItemDTO> allStoreItems = null;

			if (debugItems.size() > 0) {
				List<Integer> debugItemsList = new ArrayList<Integer>(debugItems);
				allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, productLevelId, productId, debugItemsList, stores);
			} else {
				allStoreItems = itemDAO.getItemsForPromoRecommendation(conn, inputDTO, stores, false, null);
				// allStoreItems = itemService.getAuthorizedItems(conn, productLevelId, productId, stores);
			}

			logger.debug("allStoreItems count : " + allStoreItems.size());

			List<PRItemDTO> allItems = null;
			boolean considerAuthorization = Boolean.parseBoolean(PropertyManager.getProperty("OR_CONSIDER_AUTH_ITEMS", "FALSE"));
			if(considerAuthorization) {
				allItems = allStoreItems.stream().filter(p -> p.isAuthorized() && p.isActive()).collect(Collectors.toList());
			}else {
				allItems = allStoreItems;
			}
			
			
			logger.debug("allItems count : " + allItems.size());
			// updating the item and LIG details
			itemDataMap = itemService.populateAuthorizedItemsOfZone(inputDTO, allItems);
			logger.debug("authorized lig and non lig items count : " + itemDataMap.size());

			// Populating the authorized item map
			for (Map.Entry<ItemKey, PRItemDTO> item : itemDataMap.entrySet()) {
				item.getValue().setDeptIdPromotion(productId);
				if (item.getKey().getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
					authroizedItemMap.put(new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, item.getValue().getItemCode()), item.getValue());
				} else {
					authroizedItemMap.put(new ProductKey(Constants.ITEMLEVELID, item.getValue().getItemCode()), item.getValue());
				}
			}
		} catch (Exception | OfferManagementException e) {
			logger.error("getAuthorizedItems() : Exception occurred : " + e.getMessage());
			throw new GeneralException(e.getMessage());
		}
		logger.debug("getAuthorizedItems(): End...");
		return authroizedItemMap;
	}

	public HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> getSaleInfo(Connection conn, HashMap<ProductKey, PRItemDTO> authroizedItemMap,
			List<RetailCalendarDTO> weekList, int chainId, int locationLevelId, int locationId, int productLevelId, String productIdList,
			List<Integer> allStores, boolean isDebugMode) throws GeneralException {

		HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap = new HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>>();

		try {
			logger.debug("getSaleInfo() : Start...");

			PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
			// PromoDetailsCompare promoDetailsCompare = new PromoDetailsCompare(Constants.DTO_TYPE_SALE);

			if (isDebugMode) {
				productLevelId = Constants.ITEMLEVELID;
				productIdList = authorizedMapToString(authroizedItemMap);
			}

			for (RetailCalendarDTO calendarWeek : weekList) {

				HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = pricingEngineDAO.getSaleDetails(conn, productLevelId, productIdList, chainId,
						locationLevelId, locationId, calendarWeek.getStartDate(), 1, allStores);

				logger.debug("No of Sale items :" + saleDetails.size() + " for the calendar:" + calendarWeek.getStartDate());

				for (Integer itemCode : saleDetails.keySet()) {

					HashMap<String, PRItemSaleInfoDTO> itemSaleInfo = new HashMap<String, PRItemSaleInfoDTO>();
					// Set sale details of the item
					ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemCode.intValue());
					List<PRItemSaleInfoDTO> salePrices = saleDetails.get(itemCode);

					// TODO:: this section might need re-visting of logic
					// pick price according to reg price, as items may have different reg price across the stores
					// the sale prices also changing accordingly, for e.g. item chain price 1/5.99 and few other zones at 1/4.99
					// for 1/5.99 - the sale goes in BOGO, but for 1/4.99 it goes with 2/6. So if 2/6 is picked
					// then combination of reg price 1/5.99 with 2/6 becomes incorrect

					PRItemSaleInfoDTO mostOccuringSale = null;
					// For debugging alone
					for (PRItemSaleInfoDTO salePrice : saleDetails.get(itemCode)) {
						PRItemDTO itemDTO = authroizedItemMap.get(productKey);
						if (itemDTO != null && itemDTO.getRecommendedRegPrice() != null && itemDTO.getRecommendedRegPrice().multiple == 1) {
							// Check if bogo sale price available, then pick it
							MultiplePrice tempPrice = new MultiplePrice(2, itemDTO.getRecommendedRegPrice().price);
							if (salePrice.getSalePrice().equals(tempPrice)) {
								mostOccuringSale = salePrice;
								break;
							}
							// logger.debug("Item Code: " + itemCode + ",saleDetails:" + salePrice.toString());
						}
					}

					if (mostOccuringSale == null) {
						// Calculating the most common Sale details
						mostOccuringSale = (PRItemSaleInfoDTO) mostCommonPromoObject(salePrices, "SALE");
					}

					if (mostOccuringSale != null) {
						// adding most occurring sale to saleDetailMap output
						if (saleDetailMap.get(productKey) != null && saleDetailMap.get(productKey).size() > 0) {
							itemSaleInfo = saleDetailMap.get(productKey);
						}
						// itemSaleInfo.put(mostOccuringSale.getSaleWeekStartDate(), mostOccuringSale);
						itemSaleInfo.put(calendarWeek.getStartDate(), mostOccuringSale);
						// logger.debug("Item Code: " + itemCode + ",start date:" + calendarWeek.getStartDate() + ",most occurance
						// saleDetails:"
						// + mostOccuringSale.toString());
						saleDetailMap.put(productKey, itemSaleInfo);
					}
				}
			}

			// logger.debug(" saleDetailMap size : " + saleDetailMap.size());
			logger.debug("getSaleInfo() : End...");
		} catch (Exception e) {
			logger.error("getSaleInfo() : Exception occurred " + e.getMessage());
			throw new GeneralException(e.getMessage());
		}
		return saleDetailMap;
	}

	public HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> getAdInfo(Connection conn, int chainId, int locationLevelId, int locationId,
			int productLevelId, String productIdList, List<Integer> allStores, HashMap<ProductKey, PRItemDTO> authroizedItemMap, boolean isDebugMode,
			String inputWeekStartDate, List<RetailCalendarDTO> retailCalendarCache) throws GeneralException {

		// HashMap<ItemCode, HashMap<WeekStartDate, PRItemSaleInfoDTO>>
		HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap = new HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>>();

		try {
			logger.debug("getAdInfo() : Start...");
			PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

			List<RetailCalendarDTO> fetchAdDataForCalendars = new ArrayList<RetailCalendarDTO>();

			// Past promotion weeks
			fetchAdDataForCalendars.addAll(getPastPromotionCalendarDetail(inputWeekStartDate, retailCalendarCache));

			// Previous week
			fetchAdDataForCalendars.add(getPreviousWeekCalendarDetail(inputWeekStartDate, retailCalendarCache));

			// Future weeks
			fetchAdDataForCalendars.addAll(getNextWeeksCalendarDetail(retailCalendarCache, inputWeekStartDate));

			logger.debug("fetchAdDataForCalendars:" + fetchAdDataForCalendars.toString());

			if (isDebugMode) {
				productLevelId = Constants.ITEMLEVELID;
				productIdList = authorizedMapToString(authroizedItemMap);
			}

			for (RetailCalendarDTO calendarWeek : fetchAdDataForCalendars) {

				HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = pricingEngineDAO.getAdDetails(conn, productLevelId, productIdList, chainId,
						locationLevelId, locationId, calendarWeek.getStartDate(), 1, allStores);

				logger.debug("No of Ad items :" + adDetails.size() + " for the calendar:" + calendarWeek.getStartDate());

				for (Integer itemCode : adDetails.keySet()) {

					HashMap<String, PRItemAdInfoDTO> itemAdInfo = new HashMap<String, PRItemAdInfoDTO>();

					ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemCode.intValue());
					List<PRItemAdInfoDTO> adList = adDetails.get(itemCode);

					// Calculating the most common Ad details
					PRItemAdInfoDTO mostOccuringAd = (PRItemAdInfoDTO) mostCommonPromoObject(adList, "AD");

					if (mostOccuringAd != null) {
						// adding most occurring ad to adDetailMap output
						if (adDetailMap.get(productKey) != null && adDetailMap.get(productKey).size() > 0) {
							itemAdInfo = adDetailMap.get(productKey);
						}
						itemAdInfo.put(mostOccuringAd.getWeeklyAdStartDate(), mostOccuringAd);
						adDetailMap.put(productKey, itemAdInfo);
					}

				}
			}
			// logger.debug(" adDetailMap : " + adDetailMap);
			logger.debug("getAdInfo() : End...");
		} catch (Exception e) {
			logger.error("getAdInfo() : Exception occurred : " + e.getMessage());
			throw new GeneralException(e.getMessage());
		}
		return adDetailMap;
	}

	public HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> getDisplayInfo(Connection conn, List<RetailCalendarDTO> weekList, int chainId,
			int locationLevelId, int locationId, int productLevelId, String productIdList, List<Integer> allStores) throws GeneralException {

		// HashMap<ItemCode, HashMap<WeekStartDate, PRItemSaleInfoDTO>>
		HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> displayDetailMap = new HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>>();

		try {
			logger.debug("getDisplayInfo() : Start...");
			PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

			for (RetailCalendarDTO calendarWeek : weekList) {

				HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = pricingEngineDAO.getDisplayDetails(conn, productLevelId, productIdList,
						chainId, locationLevelId, locationId, calendarWeek.getStartDate(), 1, allStores);

				logger.debug("No of display items :" + displayDetails.size() + " for the calendar:" + calendarWeek.getStartDate());

				for (Integer itemCode : displayDetails.keySet()) {

					HashMap<String, PRItemDisplayInfoDTO> itemDisplayInfo = new HashMap<String, PRItemDisplayInfoDTO>();
					ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemCode.intValue());
					List<PRItemDisplayInfoDTO> displayList = displayDetails.get(itemCode);

					// Calculating the most common Display details
					PRItemDisplayInfoDTO mostOccuringDisplay = (PRItemDisplayInfoDTO) mostCommonPromoObject(displayList, "DISPLAY");

					if (mostOccuringDisplay != null) {
						// adding most occurring display to displayDetailMap
						// output
						if (displayDetailMap.get(productKey) != null && displayDetailMap.get(productKey).size() > 0) {
							itemDisplayInfo = displayDetailMap.get(productKey);
						}

						itemDisplayInfo.put(mostOccuringDisplay.getDisplayWeekStartDate(), mostOccuringDisplay);
						displayDetailMap.put(productKey, itemDisplayInfo);
					}
				}
			}
			// logger.debug("displayDetailMap size : " +
			// displayDetailMap.size());
			logger.debug("getDisplayInfo() : End...");
		} catch (Exception e) {
			logger.error("getDisplayInfo() : Exception occurred " + e.getMessage());
			throw new GeneralException(e.getMessage());
		}
		return displayDetailMap;
	}

	/**
	 * @param conn
	 * @param productLevelId
	 * @param deptIdList
	 * @param locationLevelId
	 * @param locationId
	 * @param promoDTO
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<ProductKey, ItemClassificationDTO> getHighImpactHouseHoldItems(Connection conn, int productLevelId, Set<Integer> deptIdList,
			int locationLevelId, int locationId, HashMap<ProductKey, ItemClassificationDTO> itemClassificationFullData) throws GeneralException {
		PromotionEngineDAO promotionEngineDAO = new PromotionEngineDAO();
		logger.debug("HouseHold impact items fetching start .... ");
		HashMap<ProductKey, ItemClassificationDTO> highImpactHouseHoldItems = new HashMap<ProductKey, ItemClassificationDTO>();

		for (Integer deptId : deptIdList) {
			HashMap<ProductKey, ItemClassificationDTO> highImpactHouseHoldItemsTemp = promotionEngineDAO.getHighImpactHouseHoldItems(conn,
					productLevelId, deptId, locationLevelId, locationId, itemClassificationFullData);

			if (highImpactHouseHoldItemsTemp != null && highImpactHouseHoldItemsTemp.size() > 0) {
				highImpactHouseHoldItems.putAll(highImpactHouseHoldItemsTemp);
			}
		}

		logger.debug("***************TOP household Item details *************** ");
		return highImpactHouseHoldItems;
	}

	/**
	 * @param conn
	 * @param recWeekStartDate
	 * @param productLevelId
	 * @param productIdList
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<ProductKey, ItemClassificationDTO> getHHRecommendationItems(Connection conn, String recWeekStartDate, int productLevelId,
			String productIdList) throws GeneralException {
		// TODO::location is not considered, as it is done only for 108 store
		// for testing purpose
		PromotionEngineDAO promotionEngineDAO = new PromotionEngineDAO();

		HashMap<ProductKey, ItemClassificationDTO> hhRecommendationItems = promotionEngineDAO.getHHRecommendationItems(conn, recWeekStartDate,
				productLevelId, productIdList);

		return hhRecommendationItems;
	}

	/**
	 * @param objectList
	 * @param objectType
	 * @return
	 * @throws GeneralException
	 */
	private Object mostCommonPromoObject(List<?> objectList, String objectType) throws GeneralException {

		Object mostOccuringObject = null;

		HashMap<Object, Integer> objectOccurance = new HashMap<Object, Integer>();

		try {// Process only when object list is not null
			if (objectList != null && objectList.size() > 0) {
				// if object list is having 2 or 1 element then return first
				// element
				if (objectList.size() < 3) {
					mostOccuringObject = objectList.get(0);
				} else {
					// if object list is having more than 2 element then return
					// most common element

					// Aggregating the entities with their frequency of
					// occurrence
					for (Object object : objectList) {
						int occurance = 1;
						for (Map.Entry<Object, Integer> entry : objectOccurance.entrySet()) {
							// Comparing the two saleInfoDTO
							if (compare(entry.getKey(), object, objectType)) {
								objectOccurance.remove(entry.getKey());
								occurance = occurance + entry.getValue();
								break;
							}
						}
						objectOccurance.put(object, occurance);
					}

					// Calculating most occurring entity

					int previousFrequency = 0;
					for (Map.Entry<Object, Integer> entry : objectOccurance.entrySet()) {
						if (previousFrequency < entry.getValue()) {
							previousFrequency = entry.getValue();
							mostOccuringObject = entry.getKey();
						}
					}
				}
			}
		} catch (Exception ex) {
			throw new GeneralException("Error in mostCommonPromoObject() - " + ex.getMessage());
		}
		return mostOccuringObject;
	}

	/**
	 * @param object1
	 * @param object2
	 * @param objectType
	 * @return
	 */
	public boolean compare(Object object1, Object object2, String objectType) {
		boolean output = false;
		if (objectType.equals("SALE")) {
			PRItemSaleInfoDTO sale1 = (PRItemSaleInfoDTO) object1;
			PRItemSaleInfoDTO sale2 = (PRItemSaleInfoDTO) object2;
			// System.out.println("Comparing sale objects");
			if (sale1.getSalePrice().equals(sale2.getSalePrice())
					&& sale1.getPromoTypeId() == sale2.getPromoTypeId()) {
				output = true;
				// System.out.println("Output is true");
			}
		} else if (objectType.equals("AD")) {
			PRItemAdInfoDTO ad1 = (PRItemAdInfoDTO) object1;
			PRItemAdInfoDTO ad2 = (PRItemAdInfoDTO) object2;

			if (ad1.getAdPageNo() == ad2.getAdPageNo() && ad1.getAdBlockNo() == ad2.getAdBlockNo()) {
				output = true;
			}
		} else if (objectType.equals("DISPLAY")) {
			PRItemDisplayInfoDTO display1 = (PRItemDisplayInfoDTO) object1;
			PRItemDisplayInfoDTO display2 = (PRItemDisplayInfoDTO) object2;

			if (display1.getDisplayTypeLookup().getDisplayTypeId() == display2.getDisplayTypeLookup().getDisplayTypeId()) {
				output = true;
			}
		}
		return output;
	}

	/**
	 * @param saleDetailMap
	 * @param ligMap
	 * @throws GeneralException
	 */
	public void populateLigSaleData(HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap, HashMap<Integer, List<PRItemDTO>> ligMap)
			throws GeneralException {

		try {
			logger.debug("populateLigSaleData() : Start...");
			// Processing for each LIG
			// Aggregating sale data for all members in LIG then processing
			for (Integer lirId : ligMap.keySet()) {
				if (ligMap.get(lirId) != null && ligMap.get(lirId).size() > 0) {
					List<PRItemDTO> itemsInLig = ligMap.get(lirId);
					ProductKey ligProductKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lirId);
					// Common Hashmap<startDate , List<PRItemSaleInfoDTO>>() for
					// one lig members
					HashMap<String, List<PRItemSaleInfoDTO>> ligSaleDetailMap = new HashMap<String, List<PRItemSaleInfoDTO>>();
					for (PRItemDTO item : itemsInLig) {
						ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, item.getItemCode());

						// getting the product detail from Sale map
						if (saleDetailMap.get(productKey) != null) {

							HashMap<String, PRItemSaleInfoDTO> itemSaleDetails = saleDetailMap.get(productKey);

							for (String startDate : itemSaleDetails.keySet()) {
								PRItemSaleInfoDTO itemSaleInfo = itemSaleDetails.get(startDate);

								List<PRItemSaleInfoDTO> itemSaleList = new ArrayList<PRItemSaleInfoDTO>();

								if (ligSaleDetailMap.containsKey(startDate)) {
									itemSaleList = ligSaleDetailMap.get(startDate);
								}
								itemSaleList.add(itemSaleInfo);
								ligSaleDetailMap.put(startDate, itemSaleList);
							}
						}
					}
					// Collected Sale details for all items in LIG
					// Processing the LIG Sale data
					if (ligSaleDetailMap != null && ligSaleDetailMap.size() > 0) {
						HashMap<String, PRItemSaleInfoDTO> ligSaleInfo = getLigSaleDetail(ligSaleDetailMap);
						// Adding Lig sale entry to map
						saleDetailMap.put(ligProductKey, ligSaleInfo);
					}
				}
			}
			logger.debug("saleDetailMap size : " + saleDetailMap.size());
			logger.debug("populateLigSaleData() : End...");
		} catch (Exception e) {
			logger.error("populateLigSaleData() : Exception occurred : " + e.getMessage());
			throw new GeneralException("Error in populateLigSaleData() - " + e.getMessage());
		}
	}

	/**
	 * Method to get LIG Sale data on the basis of most occurring record
	 * 
	 * @param ligDisplayDetailMap
	 * @return
	 * @throws GeneralException
	 */
	private HashMap<String, PRItemSaleInfoDTO> getLigSaleDetail(HashMap<String, List<PRItemSaleInfoDTO>> ligSaleDetailMap) throws GeneralException {

		// HashMap<StartDate, Lig>
		HashMap<String, PRItemSaleInfoDTO> saleOutPut = new HashMap<String, PRItemSaleInfoDTO>();
		try {
			for (String startDate : ligSaleDetailMap.keySet()) {

				// Calculating most occurring Sale
				if (ligSaleDetailMap.get(startDate) != null && ligSaleDetailMap.get(startDate).size() > 0) {
					List<PRItemSaleInfoDTO> itemSaleList = ligSaleDetailMap.get(startDate);

					// Calculating the most common Ad details
					PRItemSaleInfoDTO mostOccuringSale = (PRItemSaleInfoDTO) mostCommonPromoObject(itemSaleList, "SALE");

					// Putting record with highest occurrence
					saleOutPut.put(startDate, mostOccuringSale);
				}
			}
		} catch (Exception ex) {
			logger.error("getLigSaleDetail() : Exception occurred " + ex.getMessage());
			throw new GeneralException("Error in getLigSaleDetail() - " + ex.getMessage());
		}
		return saleOutPut;
	}

	/**
	 * Method to add LIG to the map for display details
	 * 
	 * @param displayDetailMap
	 * @param ligMap
	 * @throws GeneralException
	 */
	public void populateLigDisplayData(HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> displayDetailMap,
			HashMap<Integer, List<PRItemDTO>> ligMap) throws GeneralException {
		// HashMap<ItemCode, HashMap<WeekStartDate, PRItemSaleInfoDTO>>
		// HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>>
		// displayDetailMap = new HashMap<ProductKey, HashMap<String,
		// PRItemDisplayInfoDTO>>();

		try {
			logger.debug("populateLigDisplayData() : Start...");
			// Processing for each LIG
			for (Integer lirId : ligMap.keySet()) {
				if (ligMap.get(lirId) != null && ligMap.get(lirId).size() > 0) {
					List<PRItemDTO> itemsInLig = ligMap.get(lirId);
					ProductKey lirProductKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lirId);
					// Common Hashmap<startDate , List<PRItemDisplayInfoDTO>>()
					// for one lig members
					HashMap<String, List<PRItemDisplayInfoDTO>> ligDisplayDetailMap = new HashMap<String, List<PRItemDisplayInfoDTO>>();
					for (PRItemDTO item : itemsInLig) {
						ProductKey productKey = new ProductKey(1, item.getItemCode());

						// getting the product detail from display map
						if (displayDetailMap.get(productKey) != null) {

							HashMap<String, PRItemDisplayInfoDTO> itemDisplayDetails = displayDetailMap.get(productKey);

							for (String startDate : itemDisplayDetails.keySet()) {
								PRItemDisplayInfoDTO itemDisplayInfo = itemDisplayDetails.get(startDate);

								List<PRItemDisplayInfoDTO> itemDisplayList = new ArrayList<PRItemDisplayInfoDTO>();

								if (ligDisplayDetailMap.containsKey(startDate)) {
									itemDisplayList = ligDisplayDetailMap.get(startDate);
								}
								itemDisplayList.add(itemDisplayInfo);
								ligDisplayDetailMap.put(startDate, itemDisplayList);

							}
						}
					}
					// Collected display details for all items in LIG
					// Processing the LIG display data
					if (ligDisplayDetailMap != null && ligDisplayDetailMap.size() > 0) {
						HashMap<String, PRItemDisplayInfoDTO> ligDisplayInfo = getLigDisplayDetail(ligDisplayDetailMap);
						// Adding the LIG display detail to existing map
						displayDetailMap.put(lirProductKey, ligDisplayInfo);
					}
				}
			}
			logger.debug("populateLigDisplayData() : End....");
		} catch (Exception e) {
			logger.error("populateLigDisplayData() : Exception occurred " + e.getMessage());
			throw new GeneralException("Error in populateLigDisplayData() - " + e.getMessage());
		}
	}

	/**
	 * Method to get LIG display data on the basis of most occurring record
	 * 
	 * @param ligDisplayDetailMap
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, PRItemDisplayInfoDTO> getLigDisplayDetail(HashMap<String, List<PRItemDisplayInfoDTO>> ligDisplayDetailMap)
			throws GeneralException {

		// HashMap<StartDate, Lig>
		HashMap<String, PRItemDisplayInfoDTO> displayOutPut = new HashMap<String, PRItemDisplayInfoDTO>();
		try {
			for (String startDate : ligDisplayDetailMap.keySet()) {
				// Calculating most occurring display
				if (ligDisplayDetailMap.get(startDate) != null && ligDisplayDetailMap.get(startDate).size() > 0) {
					List<PRItemDisplayInfoDTO> itemDisplayList = ligDisplayDetailMap.get(startDate);

					// Calculating the most common Display details
					PRItemDisplayInfoDTO mostOccuringDisplay = (PRItemDisplayInfoDTO) mostCommonPromoObject(itemDisplayList, "DISPLAY");

					// putting the record with max occurrence
					displayOutPut.put(startDate, mostOccuringDisplay);
				}
			}
		} catch (Exception e) {
			logger.error("getLigDisplayDetail() : Exception occurred :" + e.getMessage());
			throw new GeneralException("Error in getLigDisplayDetail() - " + e.getMessage());
		}
		return displayOutPut;
	}

	/**
	 * Method to add LIG to the map for Ad details
	 * 
	 * @param adDetailMap
	 * @param ligMap
	 * @throws GeneralException
	 */
	public void populateLigAdData(HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap, HashMap<Integer, List<PRItemDTO>> ligMap)
			throws GeneralException {

		try {
			logger.debug("populateLigAdData() : Start...");
			// Processing for each LIG
			// Collecting the ad details of all items in lig then get the lig ad
			// details
			for (Integer lirId : ligMap.keySet()) {
				if (ligMap.get(lirId) != null && ligMap.get(lirId).size() > 0) {
					List<PRItemDTO> itemsInLig = ligMap.get(lirId);
					ProductKey lirProductKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, lirId);
					// Common Hashmap<startDate , List<PRItemAdInfoDTO>>() for
					// one lig members
					HashMap<String, List<PRItemAdInfoDTO>> ligAdDetailMap = new HashMap<String, List<PRItemAdInfoDTO>>();
					for (PRItemDTO item : itemsInLig) {
						ProductKey productKey = new ProductKey(1, item.getItemCode());

						// getting the product detail from Ad map
						if (adDetailMap.get(productKey) != null) {

							HashMap<String, PRItemAdInfoDTO> itemAdDetails = adDetailMap.get(productKey);

							for (String startDate : itemAdDetails.keySet()) {
								PRItemAdInfoDTO itemAdInfo = itemAdDetails.get(startDate);

								List<PRItemAdInfoDTO> itemAdList = new ArrayList<PRItemAdInfoDTO>();

								if (ligAdDetailMap.containsKey(startDate)) {
									itemAdList = ligAdDetailMap.get(startDate);
								}
								itemAdList.add(itemAdInfo);
								ligAdDetailMap.put(startDate, itemAdList);

							}
						}
					}
					// Collected Ad details for all items in LIG
					// Processing the LIG Ad data
					if (ligAdDetailMap != null && ligAdDetailMap.size() > 0) {
						HashMap<String, PRItemAdInfoDTO> ligAdInfo = getLigAdDetail(ligAdDetailMap);
						adDetailMap.put(lirProductKey, ligAdInfo);
					}
				}
			}
			// logger.debug("adDetailMap : " + (adDetailMap != null ? adDetailMap.toString() : " NULL "));
			logger.debug("populateLigAdData() : End....");
		} catch (Exception e) {
			logger.error("populateLigAdData() : Exception occurred : " + e.getMessage());
			throw new GeneralException("Error in populateLigAdData() - " + e.getMessage());
		}
	}

	/**
	 * Method to get LIG Ad data on the basis of most occurring record
	 * 
	 * @param ligDisplayDetailMap
	 * @return
	 * @throws GeneralException
	 */
	private HashMap<String, PRItemAdInfoDTO> getLigAdDetail(HashMap<String, List<PRItemAdInfoDTO>> ligAdDetailMap) throws GeneralException {

		// HashMap<StartDate, Lig>
		HashMap<String, PRItemAdInfoDTO> adOutPut = new HashMap<String, PRItemAdInfoDTO>();
		try {
			for (String startDate : ligAdDetailMap.keySet()) {
				// Calculating most occurring Ad
				if (ligAdDetailMap.get(startDate) != null && ligAdDetailMap.get(startDate).size() > 0) {
					List<PRItemAdInfoDTO> itemAdList = ligAdDetailMap.get(startDate);

					// Calculating the most common Ad details
					PRItemAdInfoDTO mostOccuringAd = (PRItemAdInfoDTO) mostCommonPromoObject(itemAdList, "AD");

					// Putting most occurring record with highest occurrence
					adOutPut.put(startDate, mostOccuringAd);
				}
			}
		} catch (Exception e) {
			logger.error("getLigAdDetail() : Exception occurred : " + e.getMessage());
			throw new GeneralException("Error in getLigAdDetail() - " + e.getMessage());
		}
		return adOutPut;
	}

	/**
	 * Method to retrieve the map of LIG and its items from authorized items list
	 * 
	 * @param displayDetailMap
	 * @param ligMap
	 * @throws GeneralException
	 */
	public HashMap<Integer, List<PRItemDTO>> populateLigDetailsInMap(HashMap<ProductKey, PRItemDTO> authorizedItems)
			throws OfferManagementException, GeneralException {
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();

		try {
			logger.debug("populateLigDetailsInMap() : Start....");
			for (PRItemDTO prItem : authorizedItems.values()) {
				if (prItem.getRetLirId() > 0 && !prItem.isLir()) {
					List<PRItemDTO> tList = new ArrayList<PRItemDTO>();
					if (retLirMap.get(prItem.getRetLirId()) != null) {

						tList = retLirMap.get(prItem.getRetLirId());
					}
					tList.add(prItem);
					retLirMap.put(prItem.getRetLirId(), tList);
				}
			}
			logger.debug("populateLigDetailsInMap() : End....");
		} catch (Exception ex) {
			logger.error("populateLigDetailsInMap() : Exception occurred : " + ex.getMessage());
			throw new GeneralException("Error in populateLigDetailsInMap() - " + ex.getMessage());
		}
		return retLirMap;
	}

	public void updateLigLevelData(HashMap<ProductKey, PRItemDTO> authroizedItemMap, HashMap<Integer, List<PRItemDTO>> ligMap) {
		MostOccurrenceData mostOccurenceData = new MostOccurrenceData();
		boolean useMinDealCostFromHistory = Boolean.parseBoolean(PropertyManager.getProperty("OR_USE_MIN_DEAL_FROM_HISTORY", "FALSE"));
		// Loop each LIG
		logger.debug("updateLigLevelPriceAndCost() : Start....");

		// logger.debug("authroizedItemMap size : " + entry.getKey().SIZE);
		logger.debug("ligMap size : " + ligMap.size());

		for (Map.Entry<ProductKey, PRItemDTO> authorizedItem : authroizedItemMap.entrySet()) {
			if (authorizedItem.getKey().getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				
				HashMap<Integer, String> brandsInLig = new HashMap<Integer, String>();
				PRItemDTO ligItem = authorizedItem.getValue();
				// get lig members
				List<PRItemDTO> ligMembers = ligMap.get(authorizedItem.getKey().getProductId());
				// Find most occurrence price and cost
				MultiplePrice regPrice = (MultiplePrice) mostOccurenceData.getMaxOccurance(ligMembers, "RecRegPrice");
				Double listCost = (Double) mostOccurenceData.getMaxOccurance(ligMembers, "CurListCost");
				Double dealCost = (Double) mostOccurenceData.getMaxOccurance(ligMembers, "DealCost");
//				Double minDealCost = (Double) mostOccurenceData.getMaxOccurance(ligMembers, "minDealCost");
				Integer categoryProductId = (Integer) mostOccurenceData.getMaxOccurance(ligMembers, "CatProductId");
				
				Double recWeekDealCost = (Double) mostOccurenceData.getMaxOccurance(ligMembers, "RecWeekDealCost");
				
//				ligItem.setBrands((HashMap<Integer, String>) ligMembers.stream()
//						.collect(Collectors.toMap(Integer.g, PromoItemDTO::getBrandName, (p1, p2) -> p1)));
//				 
				
				OptionalDouble minDealCost = ligMembers.stream().filter(p -> p.getDealCost() != null && p.getDealCost() > 0)
						.mapToDouble(PRItemDTO::getDealCost).min();
				
				if(useMinDealCostFromHistory) {
					if(minDealCost.isPresent()) {
						logger.debug("Deal cost: " + minDealCost.getAsDouble() + ", LIG: "
								+ authorizedItem.getKey().toString());
						ligItem.setDealCost(minDealCost.getAsDouble());
					}
				} else {
					ligItem.setDealCost(dealCost);
				}
				
				// update lig level price and cost
				ligItem.setRecommendedRegPrice(regPrice);
				// ligItem.setCompPrice(compPrice);
				ligItem.setCategoryProductId(categoryProductId);
				
				ligItem.setListCost(listCost);
				ligItem.setRecWeekDealCost(recWeekDealCost);
//				ligItem.setDealCost(dealCost);
//				ligItem.setMinDealCost(minDealCost);
				
				for (PRItemDTO itemDTO : ligMembers) {
					itemDTO.setRecommendedRegPrice(regPrice);
					itemDTO.setListCost(listCost);
					itemDTO.setDealCost(dealCost);
				}
			}
		}

		logger.debug("updateLigLevelPriceAndCost() : End....");
	}

	public HashMap<ProductKey, List<PromoItemDTO>> mergeItems(String recWeekStartDate, List<RetailCalendarDTO> retailCalendar,
			HashMap<ProductKey, PRItemDTO> authroizedItemMap, HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap,
			HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> displayDetailMap,
			HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap, HashMap<ProductKey, ItemClassificationDTO> itemClassificationMap,
			HashMap<ProductKey, ItemClassificationDTO> hhRecommendedItemMap, HashMap<Integer, List<PRItemDTO>> ligAndItsMember)
			throws GeneralException {

		logger.debug("mergeItems() : Start ... ");
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap = new HashMap<ProductKey, List<PromoItemDTO>>();

		// Pick items from past weekly ad
		addPastPromotedItems(recWeekStartDate, retailCalendar, authroizedItemMap, adDetailMap, displayDetailMap, saleDetailMap, candidateItemMap);

		logger.info("***ItemDetails: 2. No of past promotion items(#):" + candidateItemMap.size() + "("
				+ getLigMemNonLIGCount(candidateItemMap.keySet(), authroizedItemMap, ligAndItsMember) + ") ***");

		// Add items from household
		for (ItemClassificationDTO itemClassificationDTO : itemClassificationMap.values()) {
			ProductKey productKey = itemClassificationDTO.getProductKey();

			// If it already in past promotion, give preference to that
			if (candidateItemMap.get(productKey) == null) {
				List<PromoItemDTO> items = new ArrayList<PromoItemDTO>();
				PromoItemDTO promoItemDTO = new PromoItemDTO();
				promoItemDTO.setProductKey(productKey);
				promoItemDTO.setAdditionalDetailForLog("Item Classification Items");
				items.add(promoItemDTO);
				candidateItemMap.put(productKey, items);
			}
		}

		logger.info("***ItemDetails: 3. No of item classification items(#):" + itemClassificationMap.size() + "("
				+ getLigMemNonLIGCount(itemClassificationMap.keySet(), authroizedItemMap, ligAndItsMember) + ") ***");

		logger.debug("No of past promotion + item classification items:" + candidateItemMap.size());

		return candidateItemMap;
	}

	public void assingItemAttributes(HashMap<ProductKey, PRItemDTO> authroizedItemMap, HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<ProductKey, ProductDTO> deptDetails) {
		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItems : candidateItemMap.entrySet()) {
			for (PromoItemDTO promoItemDTO : candidateItems.getValue()) {
				populatePromoItemDetails(authroizedItemMap, promoItemDTO, deptDetails);
			}
		}
	}

	private void populatePromoItemDetails(HashMap<ProductKey, PRItemDTO> authroizedItemMap, PromoItemDTO promoItemDTO,
			HashMap<ProductKey, ProductDTO> deptDetails) {
		PRItemDTO authorizedItem = authroizedItemMap.get(promoItemDTO.getProductKey());
		if (authorizedItem != null) {
			promoItemDTO.setRegPrice(authorizedItem.getRecommendedRegPrice());
			promoItemDTO.setItemName(authorizedItem.getItemName());
			promoItemDTO.setRetLirName(authorizedItem.getRetLirName());
			promoItemDTO.setRetLirId(authorizedItem.getRetLirId());
			promoItemDTO.setCategoryId(authorizedItem.getCategoryProductId());
			promoItemDTO.setCompPrice(authorizedItem.getCompPrice());
			promoItemDTO.setUpc(authorizedItem.getUpc());
			promoItemDTO.setActive(true);
			promoItemDTO.setDeptId(authorizedItem.getDeptIdPromotion());
			promoItemDTO.setNoOfHHRecommendedTo(authorizedItem.getUniqueHHCount());
			promoItemDTO.setBrandId(authorizedItem.getBrandId());
			promoItemDTO.setBrandName(authorizedItem.getBrandName());
			promoItemDTO.setListCost(authorizedItem.getListCost());
//			promoItemDTO.setDealCost(authorizedItem.getRecWeekDealCost());
//			promoItemDTO.setMinDealCost(authorizedItem.getMinDealCost());

			ProductKey deptKey = new ProductKey(Constants.DEPARTMENTLEVELID, promoItemDTO.getDeptId());
			promoItemDTO.setDeptName(deptDetails.get(deptKey) != null ? deptDetails.get(deptKey).getProductName() : "");

		} else {
			promoItemDTO.setActive(false);
		}
	}

//	public void updateBrandDetails(HashMap<ProductKey, PRItemDTO> authroizedItemMap, HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {
//		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItems : candidateItemMap.entrySet()) {
//			for (PromoItemDTO promoItemDTO : candidateItems.getValue()) {
//				PRItemDTO authorizedItem = authroizedItemMap.get(promoItemDTO.getProductKey());
//				if (authorizedItem != null) {
//					promoItemDTO.setBrandId(authorizedItem.getBrandId());
//					promoItemDTO.setBrandName(authorizedItem.getBrandName());
//				}
//			}
//		}
//	}

	private void addPastPromotedItems(String recWeekStartDate, List<RetailCalendarDTO> retailCalendar,
			HashMap<ProductKey, PRItemDTO> authroizedItemMap, HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap,
			HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> displayDetailMap,
			HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap, HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap)
			throws GeneralException {

		logger.debug("addPastPromotedItems() : Start....");

		List<RetailCalendarDTO> pastPromoCalendarDetail = getPastPromotionCalendarDetail(recWeekStartDate, retailCalendar);
		logger.debug("pastPromoCalendarDetail " + (pastPromoCalendarDetail != null ? pastPromoCalendarDetail.toString() : " NULL "));

		HashMap<ProductKey, HashMap<String, PromoItemDTO>> tempCandidateItemMap = new HashMap<ProductKey, HashMap<String, PromoItemDTO>>();

		// Get ad and display info for above weeks and convert to PromoItemDTO
		// and keep it in candidateItemMap
		// Loop each past weeks from which the candidate items are going to be
		// picked
		for (RetailCalendarDTO retailCalendarDTO : pastPromoCalendarDetail) {
			// in some cases for future or past week there won't be calendar
			// details
			if (retailCalendarDTO != null) {

				// Loop each item in ad details
				for (Map.Entry<ProductKey, HashMap<String, PRItemAdInfoDTO>> itemAdDetail : adDetailMap.entrySet()) {

					ProductKey productKey = itemAdDetail.getKey();
					HashMap<String, PRItemAdInfoDTO> itemAdWeekDetail = itemAdDetail.getValue();
					PromoItemDTO promoItemDTO = null;

					boolean isLigMember = false;
					if (productKey.getProductLevelId() == Constants.ITEMLEVELID) {
						PRItemDTO itemDTO = authroizedItemMap.get(productKey);
						if (itemDTO != null && itemDTO.getRetLirId() > 0) {
							isLigMember = true;
						}
					}

					// Ignoring lig members as itemADDetail has both lig members
					// and lig data
					// Get past week ad detail on 1st page
					if (itemAdWeekDetail.get(retailCalendarDTO.getStartDate()) != null
							&& itemAdWeekDetail.get(retailCalendarDTO.getStartDate()).getAdPageNo() == 1 && !isLigMember) {
						promoItemDTO = new PromoItemDTO();
						promoItemDTO.setProductKey(productKey);
						promoItemDTO.setAdInfo(itemAdWeekDetail.get(retailCalendarDTO.getStartDate()));

						// update sale detail
						if (saleDetailMap.get(productKey) != null && saleDetailMap.get(productKey).get(retailCalendarDTO.getStartDate()) != null) {
							promoItemDTO.setSaleInfo(saleDetailMap.get(productKey).get(retailCalendarDTO.getStartDate()));
						}

						HashMap<String, PromoItemDTO> weeklyAdMap = new HashMap<String, PromoItemDTO>();
						if (tempCandidateItemMap.get(productKey) != null) {
							weeklyAdMap = tempCandidateItemMap.get(productKey);
						}

						weeklyAdMap.put(retailCalendarDTO.getStartDate(), promoItemDTO);

						tempCandidateItemMap.put(productKey, weeklyAdMap);

					}
				}

				// Loop each item in display detail
				logger.debug("addPastPromotedItems() : displayDetailMap size is : " + displayDetailMap.size());
				for (Map.Entry<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> itemDisplayDetail : displayDetailMap.entrySet()) {
					PromoItemDTO promoItemDTO = null;
					ProductKey productKey = itemDisplayDetail.getKey();
					HashMap<String, PRItemDisplayInfoDTO> itemDisplayWeekDetail = itemDisplayDetail.getValue();

					// Get past week display detail
					if (itemDisplayWeekDetail.get(retailCalendarDTO.getStartDate()) != null) {
						promoItemDTO = new PromoItemDTO();
						promoItemDTO.setProductKey(productKey);
						promoItemDTO.setDisplayInfo(itemDisplayWeekDetail.get(retailCalendarDTO.getStartDate()));

						HashMap<String, PromoItemDTO> weeklyDisplayMap = new HashMap<String, PromoItemDTO>();
						if (tempCandidateItemMap.get(productKey) != null) {
							weeklyDisplayMap = tempCandidateItemMap.get(productKey);
						}

						PromoItemDTO adItem = promoItemDTO;
						// Check if it is already added in ad
						if (weeklyDisplayMap.get(retailCalendarDTO.getStartDate()) != null) {
							adItem = weeklyDisplayMap.get(retailCalendarDTO.getStartDate());
							adItem.setDisplayInfo(promoItemDTO.getDisplayInfo());
							weeklyDisplayMap.put(retailCalendarDTO.getStartDate(), adItem);
							tempCandidateItemMap.put(productKey, weeklyDisplayMap);
						}
					}
				}
			}
		}

		// Convert to candidate map
		// Header records for Past promoted info
		for (Map.Entry<ProductKey, HashMap<String, PromoItemDTO>> candidateItems : tempCandidateItemMap.entrySet()) {
			List<PromoItemDTO> promoItems = new ArrayList<PromoItemDTO>();
			for (PromoItemDTO promoItemDTO : candidateItems.getValue().values()) {
				promoItemDTO.setAdditionalDetailForLog("From Past Ad");
				promoItems.add(promoItemDTO);
			}
			if (promoItems.size() > 0) {
				candidateItemMap.put(candidateItems.getKey(), promoItems);
			}
		}
		logger.debug("addPastPromotedItems() : End....");
	}

	public List<RetailCalendarDTO> getPastPromotionCalendarDetail(String recWeekStartDate, List<RetailCalendarDTO> retailCalendarCache) {

		List<RetailCalendarDTO> pastPromoCalendarDetail = new ArrayList<RetailCalendarDTO>();
		RetailCalendarService retailCalendarService = new RetailCalendarService();
		RetailCalendarDTO tempRetailCalendarDTO = new RetailCalendarDTO();

		logger.debug("getPastPromotionCalendarDetail() : Start....");
		// Get recommended week's week no
		int recWeekNo = retailCalendarService.getActualNo(retailCalendarCache, recWeekStartDate);

		LocalDate localDate = DateUtil.stringToLocalDate(recWeekStartDate, Constants.APP_DATE_FORMAT);

		// Check if it is a special week
		boolean isSpecialWeek = retailCalendarService.isSpecialWeek(retailCalendarCache, recWeekStartDate);
		int recWeekYear = localDate.getYear();

		// Get max actual no of recommendation week year
		int maxActualNoOfRecWeekYear = retailCalendarService.getWeekMaxActualNo(retailCalendarCache, recWeekYear);

		// Special week, Get calendar id of week before (2 weeks) and same of
		// recommended week's week no in last 3 years
		// Regular Week, Get calendar id of week before (1 week), same and after
		// of recommended week's week no in last 3 years
		for (int i = 1; i <= pastPromotionsDurationInYear; i++) {
			int processingYear = recWeekYear - i;
			int tempProcessingYear = processingYear;

			int tempRecWeekNo = recWeekNo;

			// get calendar detail of actual no of year
			tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, tempRecWeekNo, processingYear);
			pastPromoCalendarDetail.add(tempRetailCalendarDTO);

			tempRecWeekNo--;

			// Past week, if past week is 1 then push back to previous year
			// for e.g. if recommendation running for week1 of 2017, then add
			// week1 of 2016, week 52 of 2015
			if (tempRecWeekNo < 1) {
				tempProcessingYear = processingYear - 1;
				// get max actual no of previous year
				int maxActualNoYear = retailCalendarService.getWeekMaxActualNo(retailCalendarCache, tempProcessingYear);

				// get calendar detail of max actual no of previous year
				tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, maxActualNoYear, tempProcessingYear);
				pastPromoCalendarDetail.add(tempRetailCalendarDTO);
				tempRecWeekNo = maxActualNoYear;
			} else {
				// get calendar detail of actual no of year
				tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, tempRecWeekNo, tempProcessingYear);
				pastPromoCalendarDetail.add(tempRetailCalendarDTO);
			}

			tempRecWeekNo--;
			if (isSpecialWeek) {
				// past week
				// for e.g. if recommendation running for week1 of 2017, then
				// add week2 of 2016, week1 of 2016, week 52 of 2015
				if (tempRecWeekNo < 1) {
					tempProcessingYear = processingYear - 1;
					// get max actual no of previous year
					int maxActualNoYear = retailCalendarService.getWeekMaxActualNo(retailCalendarCache, tempProcessingYear);

					// get calendar detail of actual no of year
					tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, maxActualNoYear, tempProcessingYear);
					pastPromoCalendarDetail.add(tempRetailCalendarDTO);
				} else {
					// get calendar detail of actual no of year
					tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, tempRecWeekNo, tempProcessingYear);
					pastPromoCalendarDetail.add(tempRetailCalendarDTO);
				}
			} else {
				tempRecWeekNo = recWeekNo + 1;
				// Future week
				if (tempRecWeekNo >= maxActualNoOfRecWeekYear) {
					tempProcessingYear = processingYear + 1;

					tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, 1, tempProcessingYear);
					pastPromoCalendarDetail.add(tempRetailCalendarDTO);
				} else {
					tempRetailCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, tempRecWeekNo, tempProcessingYear);
					pastPromoCalendarDetail.add(tempRetailCalendarDTO);
				}
			}

		}

		// For debugging alone
		// Past calendar details
		logger.debug("Past promotions details...");
		for (RetailCalendarDTO retailCalendar : pastPromoCalendarDetail) {
			logger.debug(retailCalendar.toString());
		}
		logger.debug("Past promotions details...");

		return pastPromoCalendarDetail;
	}

	public HashMap<ProductKey, List<PromoItemDTO>> applyFirstLevelFilter(String recWeekStartDate, List<RetailCalendarDTO> retailCalendarCache,
			HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap, HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap,
			HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap, boolean isPPGGroup) {

		List<PromoItemDTO> ignoredItems = new ArrayList<PromoItemDTO>();
		logger.debug("applyFirstLevelFilter(): Start...");
		HashMap<ProductKey, List<PromoItemDTO>> filteredCandidateItemMap = new HashMap<ProductKey, List<PromoItemDTO>>();
		// logger.debug("filteredCandidateItemMap : " +
		// (filteredCandidateItemMap != null ?
		// filteredCandidateItemMap.toString() : " NULL "));

		// Get calendar id of previous week of recommendation week
		RetailCalendarDTO previousWeekCalendarDTO = getPreviousWeekCalendarDetail(recWeekStartDate, retailCalendarCache);
		logger.debug("previousWeekCalendarDTO : " + (previousWeekCalendarDTO != null ? previousWeekCalendarDTO.toString() : " NULL "));

		logger.debug("candidateItemMap : " + (candidateItemMap != null ? candidateItemMap.toString() : " NULL "));
		logger.debug("********** Complete Candidate Item List before first level filter ******************");

		// Add items which satisfies the condition
		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItem : candidateItemMap.entrySet()) {
			ProductKey productKey = candidateItem.getKey();
			logger.debug(productKey.toString());

			// Ignore Inactive and unauthorized items
			boolean isActiveItem = true;
			boolean isPresentInPreviousWeekAd = false;
			boolean isItemInTPR = false;
			// boolean isItemOnCurrentAd =
			// candidateItem.getValue().get(0).isItemCurrentlyOnPromo();
			String saleSDate = "", saleEDate = "";

			if (!candidateItem.getValue().get(0).isActive()) {
				isActiveItem = false;
			} else if (previousWeekCalendarDTO != null && adDetailMap.get(productKey) != null
					&& adDetailMap.get(productKey).get(previousWeekCalendarDTO.getStartDate()) != null
					&& adDetailMap.get(productKey).get(previousWeekCalendarDTO.getStartDate()).getAdPageNo() > 0) {
				// Ignore Items promoted on entire Ad on previous week
				//Commented to ignore previous ad check temporarily
//				isPresentInPreviousWeekAd = true;
				saleSDate = previousWeekCalendarDTO.getStartDate();
			} else if (previousWeekCalendarDTO != null && saleDetailMap.get(productKey) != null
					&& saleDetailMap.get(productKey).get(previousWeekCalendarDTO.getStartDate()) != null) {
				// Ignore Items in TPR on recommendation week
				PRItemSaleInfoDTO saleInfoDTO = saleDetailMap.get(productKey).get(previousWeekCalendarDTO.getStartDate());

				// no end date
				if (saleInfoDTO.getSaleEndDate() == null) {
					isItemInTPR = true;
					saleSDate = saleInfoDTO.getSaleStartDate();
				} else {
					LocalDate saleEndDate = DateUtil.stringToLocalDate(saleInfoDTO.getSaleEndDate(), Constants.APP_DATE_FORMAT);
					LocalDate recWeekStart = DateUtil.stringToLocalDate(recWeekStartDate, Constants.APP_DATE_FORMAT);

					if (saleEndDate.isAfter(recWeekStart)) {
						isItemInTPR = true;
						saleSDate = saleInfoDTO.getSaleStartDate();
						saleEDate = saleInfoDTO.getSaleEndDate();
					}
				}
			}

			// logger.debug("ProductKey:" + candidateItem.getKey().toString() +
			// " is filtered in first level." + "isActiveItem:" + isActiveItem
			// + " :: isPresentInPreviousWeekAd:" + isPresentInPreviousWeekAd +
			// " :: isItemInTPR:" + isItemInTPR);

			// 17-Oct-2017 : Including the TPR items as Promotion candidates
			if (isActiveItem && !isPresentInPreviousWeekAd) { // && !isItemInTPR) {
				filteredCandidateItemMap.put(candidateItem.getKey(), candidateItem.getValue());
			} else {
				// for logging
				for (PromoItemDTO promoItemDTO : candidateItem.getValue()) {
					if (isPresentInPreviousWeekAd) {
						promoItemDTO.setPresentInPreviousWeekAd(true);
					}
					if (isItemInTPR) {
						promoItemDTO.setOnTPR(true);
					}

					promoItemDTO.setAdditionalDetailForLog(
							(!isActiveItem ? ".Ignored Item. isActiveItem:" + isActiveItem : "") + "isPPGGroup:" + isPPGGroup
									+ (isPresentInPreviousWeekAd
											? " :: isPresentInPreviousWeekAd:" + isPresentInPreviousWeekAd + "(" + saleSDate + "-" + saleEDate + ")"
											: "")
									+ (isItemInTPR ? " :: isItemInTPR:" + isItemInTPR + "(" + saleSDate + "-" + saleEDate + ")" : ""));
					ignoredItems.add(promoItemDTO);
				}
				// filteredCandidateItemMap.put(candidateItem.getKey(),
				// candidateItem.getValue());
			}
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, ignoredItems);

		// logger.debug("Output filteredCandidateItemMap : " + (filteredCandidateItemMap != null ?
		// filteredCandidateItemMap.toString() : " NULL "));
		logger.debug("Filtered Candidate Items are : " + (filteredCandidateItemMap != null ? filteredCandidateItemMap.keySet().toString() : ""));

		// filtering items on future Ad
		// temporarily commented for client demo
		//Commented to ignore previous ad check temporarily
//		filterItemsOnfutureAd(filteredCandidateItemMap, recWeekStartDate, retailCalendarCache, adDetailMap);

		logger.debug("applyFirstLevelFilter() : End...");
		return filteredCandidateItemMap;
	}

	public HashMap<Long, PromoProductGroup> applyFirstLevelFilterOnPPGItems(String recWeekStartDate, List<RetailCalendarDTO> retailCalendarCache,
			HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap, HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap,
			HashMap<Long, PromoProductGroup> productGroupDetails) {
		HashMap<Long, PromoProductGroup> productGroupDetailsAfterFilter = new HashMap<Long, PromoProductGroup>();
		int totalPPGItem = 0;

		// Each product group
		for (Map.Entry<Long, PromoProductGroup> tempPPGGroup : productGroupDetails.entrySet()) {
			HashMap<ProductKey, List<PromoItemDTO>> tempItemMap = new HashMap<ProductKey, List<PromoItemDTO>>();

			for (PromoItemDTO promoItemDTO : tempPPGGroup.getValue().getItems().values()) {
				List<PromoItemDTO> tempList = new ArrayList<PromoItemDTO>();
				tempList.add(promoItemDTO);
				tempItemMap.put(promoItemDTO.getProductKey(), tempList);
			}

			HashMap<ProductKey, List<PromoItemDTO>> filteredCandidateItemMap = applyFirstLevelFilter(recWeekStartDate, retailCalendarCache,
					adDetailMap, saleDetailMap, tempItemMap, true);

			// Ignore entire group if lead item itself ignored, otherwise ignore members alone
			if (filteredCandidateItemMap.get(tempPPGGroup.getValue().getLeadItem()) != null) {
				PromoProductGroup promoProductGroup = new PromoProductGroup();
				HashMap<ProductKey, PromoItemDTO> items = new HashMap<ProductKey, PromoItemDTO>();

				copyPromoProductGroup(tempPPGGroup.getValue(), promoProductGroup);

				// Add filtered ppg items alone
				for (List<PromoItemDTO> tempItemDTO : filteredCandidateItemMap.values()) {
					for (PromoItemDTO tempDTO : tempItemDTO) {
						tempDTO.setPpgGroupId(tempPPGGroup.getKey());
						items.put(tempDTO.getProductKey(), tempDTO);

						totalPPGItem = totalPPGItem + 1;
					}
				}

				promoProductGroup.setItems(items);
				productGroupDetailsAfterFilter.put(tempPPGGroup.getKey(), promoProductGroup);
			}
		}

		// logger.info("***ItemDetails: Total items in PPG's after first filter (#):" + totalPPGItem);

		return productGroupDetailsAfterFilter;
	}

	public RetailCalendarDTO getPreviousWeekCalendarDetail(String weekStartDate, List<RetailCalendarDTO> retailCalendarCache) {
		RetailCalendarService retailCalendarService = new RetailCalendarService();
		LocalDate weekStart = DateUtil.stringToLocalDate(weekStartDate, Constants.APP_DATE_FORMAT);
		LocalDate dt = weekStart.minusDays(7);
		String previousWeekStartDate = DateUtil.localDateToString(dt, Constants.APP_DATE_FORMAT);
		RetailCalendarDTO previousWeekCalendarDTO = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, previousWeekStartDate);
		return previousWeekCalendarDTO;
	}

//	public HashMap<ProductKey, List<PromoItemDTO>> findAllPromotionCombination(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
//			HashMap<Integer, List<PageBlockNoKey>> deptBlockMap, int adPageNo, HashMap<Integer, List<PRItemDTO>> ligAndItsMembers,
//			boolean isPPGProcess, Set<Integer> itemsOnBOGOinLastXWeeks, HashMap<Integer, List<Double>> minXSaleDiscountPCT,
//			HashMap<ProductKey, Double> catLevelMarginPCT, HashMap<Integer, List<MultiplePrice>> minXMultiplePrices,
//			HashMap<Integer, Set<SalePriceKey>>  pastSalePrices, HashMap<ProductKey, PRItemDTO> authroizedItemMap) {

	public HashMap<ProductKey, List<PromoItemDTO>> findAllPromotionCombination(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<Integer, List<PageBlockNoKey>> deptBlockMap, int adPageNo, HashMap<Integer, List<PRItemDTO>> ligAndItsMembers,
			boolean isPPGProcess, HashMap<Integer, Set<SalePriceKey>> pastSalePrices, HashMap<ProductKey, PRItemDTO> authroizedItemMap,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfItemOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfCategoryOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> minDealCostOfItemOnAd,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary) {
		
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemWithAllPromoMap = new HashMap<ProductKey, List<PromoItemDTO>>();
		HashMap<ProductKey, Set<SalePriceKey>> candidateItemPromoCombinationsMap = new HashMap<ProductKey, Set<SalePriceKey>>();

		logger.debug("findAllPromotionCombination(): Start...");

		String[] saleDiscounts = PropertyManager.getProperty("PROMO_SALE_DISCOUNTS").split(",");
		logger.debug("Sale Price defined in property file are : " + saleDiscounts.toString());
		// Loop each item
		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItem : candidateItemMap.entrySet()) {

			// Take the past 4 minimum sale price points

			logger.debug("processing item" + candidateItem.getKey().toString());

			PromoItemDTO promoItem = candidateItem.getValue().get(0);

			List<Integer> itemCodes = new ArrayList<Integer>();

			if (candidateItem.getKey().getProductLevelId() == Constants.ITEMLEVELID) {
				itemCodes.add(candidateItem.getKey().getProductId());
			} else if (candidateItem.getKey().getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				if (ligAndItsMembers.get(candidateItem.getKey().getProductId()) != null) {
					for (PRItemDTO prItem : ligAndItsMembers.get(candidateItem.getKey().getProductId())) {
						itemCodes.add(prItem.getItemCode());
					}
				}
			}

			if (itemCodes.size() > 0) {

				Set<SalePriceKey> promoCombinations = new HashSet<SalePriceKey>();

				// Adding the past sale price points

				for (Integer itemcode : itemCodes) {
					// ProductKey itemKey = new ProductKey(Constants.ITEMLEVELID, itemcode);
					if (pastSalePrices.get(itemcode) != null) {
						Set<SalePriceKey> pastSalePriceSet = pastSalePrices.get(itemcode);
						// loop all past sale price of item
						for (SalePriceKey salePriceKey : pastSalePriceSet) {
							SalePriceKey temppSalePriceKey = new SalePriceKey();
							temppSalePriceKey.setRegPrice(promoItem.getRegPrice());
							temppSalePriceKey.setPromoTypeId(salePriceKey.getPromoTypeId());

							// If in bogo
							if (salePriceKey.getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()) {
								temppSalePriceKey
										.setSalePrice(new MultiplePrice(promoItem.getRegPrice().multiple * 2, promoItem.getRegPrice().price));
								promoCombinations.add(temppSalePriceKey);
							} else if (salePriceKey.getPromoTypeId() == PromoTypeLookup.STANDARD.getPromoTypeId()) {
								// logger.debug("salePriceKey.getRegPrice():" + salePriceKey.getRegPrice() +
								// "promoItem.getRegPrice():" + promoItem.getRegPrice());
								// Get matching sale price for the current reg price
								if (salePriceKey.getRegPrice().equals(promoItem.getRegPrice())) {
									temppSalePriceKey.setSalePrice(salePriceKey.getSalePrice());
									promoCombinations.add(temppSalePriceKey);
								}
							}
						}
					}

					logger.debug("Past sale prices of itemCode:" + itemcode + "," + promoCombinations);

				}

				int requiredSalePricePoints = maxPromoCombination - promoCombinations.size();

				// If this count is less than maxPromoCombination than add discounted prices

				logger.debug("Required Sale Price points are : " + requiredSalePricePoints);
				if (requiredSalePricePoints > 0) {
					int requiredSalePrice = 0, index = 0;
					while (requiredSalePrice < requiredSalePricePoints && index < saleDiscounts.length) {

						// logger.debug("Current Index value is : " + index);
						if (promoItem.getRegPrice() != null) {
							MultiplePrice salePrice = getRoundedSalePrice(promoItem.getRegPrice(), Double.valueOf(saleDiscounts[index++]));

							SalePriceKey salePriceKey = new SalePriceKey();
							salePriceKey.setSalePrice(salePrice);
							salePriceKey.setPromoTypeId(PromoTypeLookup.STANDARD.getPromoTypeId());

							promoCombinations.add(salePriceKey);
							requiredSalePrice++;

							logger.debug("Price points based on %" + salePrice.toString());

						} else {
							logger.debug("findAllPromotionCombination(): Regular Price is null for item : " + promoItem.getProductKey());
							break;
						}
					}
				} else if (requiredSalePricePoints < 0) {
					String salePriceForLog = "";

					HashMap<MultiplePrice, SalePriceKey> salePriceMap = new HashMap<MultiplePrice, SalePriceKey>();

					for (SalePriceKey salePriceKey : promoCombinations) {
						salePriceMap.put(salePriceKey.getSalePrice(), salePriceKey);
					}
					List<MultiplePrice> minXMultiplePriceCombo = getXMinSalePricePoints(new ArrayList<MultiplePrice>(salePriceMap.keySet()));

					promoCombinations.clear();
					for (MultiplePrice salePrice : minXMultiplePriceCombo) {
						promoCombinations.add(salePriceMap.get(salePrice));
						salePriceForLog = salePriceForLog + "," + salePrice.toString();
					}

					logger.debug("Many price points pick least 4" + salePriceForLog.toString());
				}

				// Adding the BOGO sale promotion for items with BOGO history
				// logger.debug("Adding BOGO combinations. ");

				logger.debug("itemCode:" + candidateItem.getKey() + "," + promoCombinations.toString());

				candidateItemPromoCombinationsMap.put(candidateItem.getKey(), promoCombinations);
			} else {
				logger.debug("findAllPromotionCombination() :: Error: No items found for LIG Item : " + candidateItem.getKey());
			}
		}

		candidateItemWithAllPromoMap = convertAllCombinationToPromoItem(candidateItemMap, candidateItemPromoCombinationsMap, deptBlockMap, adPageNo,
				avgSaleMarPCTOfItemOnFirstPage, avgSaleMarPCTOfCategoryOnFirstPage, minDealCostOfItemOnAd, actualAdItemsLIGNonLigSummary);
		
		logger.debug("findAllPromotionCombination(): End...");

		if (!isPPGProcess) {
			List<PromoItemDTO> itemWithAllCombination = new ArrayList<PromoItemDTO>();
			// for logging
			for (Map.Entry<ProductKey, List<PromoItemDTO>> promoItems : candidateItemWithAllPromoMap.entrySet()) {
				for (PromoItemDTO promoItem : promoItems.getValue()) {
					promoItem.setAdditionalDetailForLog("Individual item Promo Combinations");
					itemWithAllCombination.add(promoItem);
				}
			}
			itemLogService.writeToCSVFile(itemAnalysisLogPath, itemWithAllCombination);
		}

		return candidateItemWithAllPromoMap;
	}

	public MultiplePrice getRoundedSalePrice(MultiplePrice regPrice, Double discount) {

		if (regPrice != null) {
			// Calculate sale price with given discount
			Double salePrice = (regPrice.price * (100.0 - discount)) / 100.0;

			Double roundedSalePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(salePrice);

			Double finalSalePrice = roundedSalePrice;

			// make last digit as 9
			double rounded = PRFormatHelper.roundToOneDecimalDigitAsDouble(salePrice);
			double doubleDigit = PRFormatHelper.roundToTwoDecimalDigitAsDouble(salePrice);
			if (doubleDigit - (int) doubleDigit < 0.15) {
				if (doubleDigit - (int) doubleDigit < 0.09) {
					finalSalePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble((double) ((int) doubleDigit) - 0.01d);
				} else {
					finalSalePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble((double) ((int) doubleDigit) + 0.19d);
				}
			} else {
				if (doubleDigit > rounded) {

					if (PRFormatHelper.roundToTwoDecimalDigitAsDouble(doubleDigit - rounded) < 0.04d) {
						finalSalePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(rounded - 0.01d);
					} else {
						finalSalePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(rounded + 0.09d);
					}
				} else {
					finalSalePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(rounded - 0.01d);
				}
			}

			// logger.debug("sale price converted from/to:" + salePrice + "," +
			// PRFormatHelper.roundToTwoDecimalDigitAsDouble(finalSalePrice));

			return new MultiplePrice(regPrice.multiple, finalSalePrice);
		} else {
			return null;
		}

	}

	private HashMap<ProductKey, List<PromoItemDTO>> convertAllCombinationToPromoItem(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<ProductKey, Set<SalePriceKey>> candidateItemPromoCombinationsMap, HashMap<Integer, List<PageBlockNoKey>> deptBlockMap,
			int adPageNo, HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfItemOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfCategoryOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> minDealCostOfItemOnAd,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary) {

		logger.debug("convertAllCombinationToPromoItem(): Start...");
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemWithAllPromoMap = new HashMap<ProductKey, List<PromoItemDTO>>();
		if (adPageNo == -1) {
			adPageNo = 1;
		} // if adPage is not given then take ad page as 1

		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItems : candidateItemMap.entrySet()) {
			ProductKey productKey = candidateItems.getKey();
			List<PromoItemDTO> allPromosOfItem = new ArrayList<PromoItemDTO>();
			PromoItemDTO candidateItem = candidateItems.getValue().get(0);
			// for (PromoItemDTO candidateItem : candidateItems.getValue()) {
			if (deptBlockMap.get(candidateItem.getDeptId()) != null) {
				for (PageBlockNoKey pageBlockNoKey : deptBlockMap.get(candidateItem.getDeptId())) {

					if (candidateItemPromoCombinationsMap.get(productKey) != null) {
						for (SalePriceKey salePriceKey : candidateItemPromoCombinationsMap.get(productKey)) {
							PromoItemDTO promoItemDTO = new PromoItemDTO();
							PRItemAdInfoDTO adInfoDTO = new PRItemAdInfoDTO();
							PRItemDisplayInfoDTO displayInfoDTO = new PRItemDisplayInfoDTO();
							PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();

							copyPromoItem(productKey.getProductLevelId(), productKey.getProductId(), candidateItem.getUpc(),
									candidateItem.getItemName(), candidateItem, promoItemDTO);

							// sale price
							saleInfoDTO.setPromoTypeId(salePriceKey.getPromoTypeId());
							saleInfoDTO.setSalePrice(salePriceKey.getSalePrice());

							// setting Ad page no
							adInfoDTO.setAdPageNo(adPageNo);
							// setting Ad block no
							adInfoDTO.setAdBlockNo(pageBlockNoKey.getBlockNumber());

							// Display
							displayInfoDTO.setDisplayTypeLookup(DisplayTypeLookup.NONE);

							promoItemDTO.setSaleInfo(saleInfoDTO);
							promoItemDTO.setAdInfo(adInfoDTO);
							promoItemDTO.setDisplayInfo(displayInfoDTO);

							boolean ignoreItem = false;
							// if no cost (only list cost as deal cost is derived later) and standard promotion ignore it, as we
							// can't come with sale price
							if (promoItemDTO.getListCost() == null
									&& salePriceKey.getPromoTypeId() == PromoTypeLookup.STANDARD.getPromoTypeId()) {
								ignoreItem = true;
							} else if (salePriceKey.getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()
									&& promoItemDTO.getRegPrice() != null && promoItemDTO.getRegPrice().multiple > 1) {
								// if it is bogo, but current price is in multiples already
								ignoreItem = true;
							}

							// Set derived deal cost
//							promoItemDTO.setDerivedDealCost(findDerivedDealCost(promoItemDTO, catLevelMarginPCT));

							promoItemDTO.setFinalCost(findFinalCostOfItem(promoItemDTO, avgSaleMarPCTOfItemOnFirstPage,
									avgSaleMarPCTOfCategoryOnFirstPage, minDealCostOfItemOnAd, actualAdItemsLIGNonLigSummary));

							if (!ignoreItem) {
								allPromosOfItem.add(promoItemDTO);
							}
						}
					} else {
						logger.debug("Promo Combination not found for item : " + productKey);
					}
				}
			}
			// }
			if (allPromosOfItem.size() > 0) {
				candidateItemWithAllPromoMap.put(productKey, allPromosOfItem);
			}
		}

		logger.debug("convertAllCombinationToPromoItem(): End...");
		return candidateItemWithAllPromoMap;
	}

	public HashMap<ProductKey, List<PromoItemDTO>> ignoreHigherPricePointThanCompIndItems(
			HashMap<ProductKey, List<PromoItemDTO>> individualItemsPromoCombinations) {
		HashMap<ProductKey, List<PromoItemDTO>> individualItemsPromoCombinationsFiltered = new HashMap<ProductKey, List<PromoItemDTO>>();

		int totalPricePoints = 0;

		for (Map.Entry<ProductKey, List<PromoItemDTO>> itemsWithPromoCombinations : individualItemsPromoCombinations.entrySet()) {
			List<PromoItemDTO> filteredItems = new ArrayList<PromoItemDTO>();
			filteredItems = filterItemsHigerPriceThanCompetitor(itemsWithPromoCombinations.getValue());
			if (filteredItems.size() > 0) {
				individualItemsPromoCombinationsFiltered.put(itemsWithPromoCombinations.getKey(), filteredItems);
				totalPricePoints = totalPricePoints + filteredItems.size();
			}
		}

		// logger.info("***ItemDetails: Ind Items Promo Combinations (#):" + individualItemsPromoCombinations.values().size() + "
		// ***");

		return individualItemsPromoCombinationsFiltered;
	}

	public HashMap<Long, List<PromoProductGroup>> ignoreHigherPricePointThanCompPPGItems(
			HashMap<Long, List<PromoProductGroup>> ppgItemsPromoCombinations, HashMap<Integer, List<PRItemDTO>> ligAndItsMember) {
		HashMap<Long, List<PromoProductGroup>> ppgItemsPromoCombinationsFiltered = new HashMap<Long, List<PromoProductGroup>>();

		HashSet<ProductKey> distinctAllItems = new HashSet<ProductKey>();
		HashSet<ProductKey> distinctItems = new HashSet<ProductKey>();

		for (Entry<Long, List<PromoProductGroup>> ppgGroups : ppgItemsPromoCombinations.entrySet()) {
			List<PromoProductGroup> temppgGroups = new ArrayList<PromoProductGroup>();

			// Each product group
			for (PromoProductGroup promoProductGroup : ppgGroups.getValue()) {
				List<PromoItemDTO> filteredItems = new ArrayList<PromoItemDTO>();
				HashMap<ProductKey, PromoItemDTO> items = new HashMap<ProductKey, PromoItemDTO>();

				filteredItems = filterItemsHigerPriceThanCompetitor(new ArrayList<PromoItemDTO>(promoProductGroup.getItems().values()));

				items = (HashMap<ProductKey, PromoItemDTO>) filteredItems.stream()
						.collect(Collectors.toMap(PromoItemDTO::getProductKey, Function.identity()));

				promoProductGroup.setItems(items);

				// Ignore group if lead item is > comp price
				PromoItemDTO leadItem = getLeadItem(promoProductGroup);
				if (leadItem != null && filteredItems.size() > 0) {
					temppgGroups.add(promoProductGroup);

					// For anaylysis
					for (PromoItemDTO tempItemDTO : filteredItems) {
						distinctItems.add(tempItemDTO.getProductKey());

						if (tempItemDTO.getProductKey().getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
							if (ligAndItsMember.get(tempItemDTO.getProductKey().getProductId()) != null) {
								for (PRItemDTO itemDTO : ligAndItsMember.get(tempItemDTO.getProductKey().getProductId())) {
									distinctAllItems.add(new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode()));
								}
							}
						} else {
							distinctAllItems.add(tempItemDTO.getProductKey());
						}
					}
				}
				// logger.debug("ppgGroups.getKey():" + ppgGroups.getKey() + ",items.size():" + items.size());
			}

			if (temppgGroups.size() > 0) {
				ppgItemsPromoCombinationsFiltered.put(ppgGroups.getKey(), temppgGroups);
			}
		}

		logger.info("***ItemDetails: 7. Distinct Items in PPG (#):" + distinctItems.size() + "(" + distinctAllItems.size() + ")" + " ***");

		return ppgItemsPromoCombinationsFiltered;
	}

	// Remove combination whose sale unit price is > comp price
	private List<PromoItemDTO> filterItemsHigerPriceThanCompetitor(List<PromoItemDTO> itemWithPromoCombination) {

		List<PromoItemDTO> ignoredItems = new ArrayList<PromoItemDTO>();
		List<PromoItemDTO> filteredItems = new ArrayList<PromoItemDTO>();

		// Add items which satisfies the condition
		for (PromoItemDTO promoItemDTO : itemWithPromoCombination) {

			double compMinUnitPrice = 0d;
			double itemMinUnitPrice = 0d;

			if (promoItemDTO.getCompPrice() != null && promoItemDTO.getSaleInfo() != null) {

				compMinUnitPrice = PRCommonUtil.getUnitPrice(promoItemDTO.getCompPrice(), true);
				itemMinUnitPrice = PRCommonUtil.getUnitPrice(promoItemDTO.getSaleInfo().getSalePrice(), true);

				if (itemMinUnitPrice > compMinUnitPrice) {
					promoItemDTO.setAdditionalDetailForLog("Price Point Ignored as price > comp price");
					ignoredItems.add(promoItemDTO);
				} else {
					filteredItems.add(promoItemDTO);
				}

			} else {
				filteredItems.add(promoItemDTO);
			}

		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, ignoredItems);

		return filteredItems;
	}

	@SuppressWarnings("unused")
	private void filterItemsOnfutureAd(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap, String recWeekStartDate,
			List<RetailCalendarDTO> retailCalendarCache, HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap) {
		// TODO:: 4.Remove combination whose sale unit price is > comp price
		logger.debug("filterItemsOnfutureAd(): Start...");

		List<PromoItemDTO> ignoredItems = new ArrayList<PromoItemDTO>();
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMapInternal = new HashMap<ProductKey, List<PromoItemDTO>>();
		candidateItemMapInternal.putAll(candidateItemMap);

		// Getting future weeks first
		List<RetailCalendarDTO> futureWeeks = getNextWeeksCalendarDetail(retailCalendarCache, recWeekStartDate);

		// Add items which satisfies the condition
		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItem : candidateItemMapInternal.entrySet()) {
			ProductKey productKey = candidateItem.getKey();
			// logger.debug("Applying filter for item : " +
			// productKey.toString());
			// boolean isItemOnCurrentAd =
			// candidateItem.getValue().get(0).isItemCurrentlyOnPromo();

			// Ignore Inactive and unauthorized items
			boolean isActiveItem = true;
			boolean isOnAdInFutureWeeks = false;
			String saleSDate = "";

			if (!candidateItem.getValue().get(0).isActive()) {
				isActiveItem = false;
				logger.debug("Item is not active.");
			}
			if (isActiveItem) {
				// logger.debug("Filtering items having ads in future weeks.");
				for (RetailCalendarDTO futureRetailCalendarDTO : futureWeeks) {
					if (futureRetailCalendarDTO != null && adDetailMap.get(productKey) != null
							&& adDetailMap.get(productKey).get(futureRetailCalendarDTO.getStartDate()) != null
							&& adDetailMap.get(productKey).get(futureRetailCalendarDTO.getStartDate()).getAdPageNo() > 0) {
						// Ignore Items promoted on entire Ad on previous week
						// logger.debug("Item is on Ad in future week start date : " + futureRetailCalendarDTO.getStartDate());
						isOnAdInFutureWeeks = true;
						saleSDate = futureRetailCalendarDTO.getStartDate();
					}
				}
			}

			if (isOnAdInFutureWeeks) {
				candidateItemMap.remove(candidateItem.getKey());
				// logger.debug("Marking item as ignored due to Ad in future.");
				for (PromoItemDTO promoItemDTO : candidateItem.getValue()) {
					promoItemDTO.setPresentInFutureWeekAd(true);
					promoItemDTO.setAdditionalDetailForLog(
							"SecondFilter : " + (isOnAdInFutureWeeks ? ("Item on Ad in future weeks starting from " + saleSDate) : ""));
					ignoredItems.add(promoItemDTO);
				}
			}
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, ignoredItems);

		logger.debug("Filtered Candidate Items on future week Ad are : " + (candidateItemMap != null ? candidateItemMap.keySet().toString() : ""));

		logger.debug("filterItemsOnfutureAd(): End...");
	}

	//
	public HashMap<ProductKey, List<PromoItemDTO>> groupItemsByCategory(List<RetailCalendarDTO> retailCalendarCache, String recWeekStartDate,
			HashMap<Integer, List<PRItemDTO>> ligAndItsMembers, HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {

		int totalPriceCombination = 0;

		HashMap<ProductKey, List<PromoItemDTO>> groupByCategory = new HashMap<ProductKey, List<PromoItemDTO>>();

		// Convert lig to lig-members, keep only lig members and non-lig as
		// prediction works only at item level
		HashMap<ProductKey, List<PromoItemDTO>> ligMembersAndNonLig = getLigMembersAndNonLig(ligAndItsMembers, candidateItemMap);

		// Group items by category
		for (Map.Entry<ProductKey, List<PromoItemDTO>> ligMemberAndNonLigItem : ligMembersAndNonLig.entrySet()) {
			for (PromoItemDTO promoItemDTO : ligMemberAndNonLigItem.getValue()) {
				List<PromoItemDTO> promoItems = new ArrayList<PromoItemDTO>();
				ProductKey productKey = new ProductKey(Constants.CATEGORYLEVELID, promoItemDTO.getCategoryId());
				if (groupByCategory.get(productKey) != null) {
					promoItems = groupByCategory.get(productKey);
				}
				promoItems.add(promoItemDTO);
				groupByCategory.put(productKey, promoItems);

				totalPriceCombination = totalPriceCombination + 1;
			}
		}

		String categoriesToProcess = "";
		for (Map.Entry<ProductKey, List<PromoItemDTO>> itemsByCategory : groupByCategory.entrySet()) {
			categoriesToProcess = categoriesToProcess + "," + itemsByCategory.getKey().toString();
		}

		logger.info("Categories for Prediction:" + categoriesToProcess);

		logger.info("***ItemDetails: 8. Total Distinct Items to Pred (#):" + candidateItemMap.size() + "(" + ligMembersAndNonLig.size() + ") ***");

		return groupByCategory;
	}

	public HashMap<ProductKey, List<PromoItemDTO>> callPredictionEngine(List<RetailCalendarDTO> retailCalendarCache, int locationLevelId,
			int locationId, String recWeekStartDate, HashMap<ProductKey, List<PromoItemDTO>> groupByCategoryItems, boolean isActualAdItem)
			throws GeneralException, OfferManagementException {

		logger.debug("callPredictionEngine(): Start...");
		PredictionService predictionService = new PredictionService();
		RetailCalendarDTO retailCalendarDTO = new RetailCalendarService().getWeekCalendarDetail(retailCalendarCache, recWeekStartDate);

		// Run prediction for each 6 item per category to avoid DB connection
		// timeout -- Currently this feature is commented

		HashMap<ProductKey, PredictionInputDTO> categoryWisePredictionInput = new HashMap<ProductKey, PredictionInputDTO>();
		HashSet<PredictionDetailKey> distinctPricePoints = new HashSet<PredictionDetailKey>();

		for (Map.Entry<ProductKey, List<PromoItemDTO>> itemsByCategory : groupByCategoryItems.entrySet()) {

			// Form input for the prediction
			PredictionInputDTO predictionInputDTO = formPredictionInput(locationLevelId, locationId, Constants.CATEGORYLEVELID,
					itemsByCategory.getKey().getProductId(), retailCalendarDTO.getCalendarId(), itemsByCategory.getValue());

			filterDuplicatesPredictionEntries(predictionInputDTO);

			categoryWisePredictionInput.put(itemsByCategory.getKey(), predictionInputDTO);

			// For Analysis
			for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
				for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
					PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, predictionItemDTO.itemCodeOrLirId,
							pricePointDTO.getRegQuantity(), pricePointDTO.getRegPrice(), pricePointDTO.getSaleQuantity(),
							pricePointDTO.getSalePrice(), pricePointDTO.getAdPageNo(), pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(),
							pricePointDTO.getDisplayTypeId());
					distinctPricePoints.add(predictionDetailKey);
				}
			}
		}

		if (!isActualAdItem) {
			logger.info("***ItemDetails: 9. Total Price Combination (LIG Mem+NonLIG) to Pred (#):" + distinctPricePoints.size() + " ***");
		}

		for (Map.Entry<ProductKey, List<PromoItemDTO>> itemsByCategory : groupByCategoryItems.entrySet()) {

			// Call prediction engine
			PredictionOutputDTO predictionOutput = predictionService.predictMovement(categoryWisePredictionInput.get(itemsByCategory.getKey()),
					"PREDICTION_PROMOTION");

			// PredictionOutputDTO predictionOutput = new PredictionOutputDTO();
			// Update predictions
			updatePrediction(locationLevelId, locationId, itemsByCategory.getValue(), predictionOutput);

		}

		logger.debug("callPredictionEngine(): End...");

//		HashMap<ProductKey, List<PromoItemDTO>> groupByCategoryItemsUpdated = updateRegularPricePredictions(groupByCategoryItems);

		HashMap<ProductKey, List<PromoItemDTO>> itemsWithPrediction = new HashMap<ProductKey, List<PromoItemDTO>>();
		// Convert to item and its prediction
		List<PromoItemDTO> tempItemsWithPrediction = new ArrayList<PromoItemDTO>();
		for (List<PromoItemDTO> allItems : groupByCategoryItems.values()) {
			for (PromoItemDTO promoItem : allItems) {
				List<PromoItemDTO> items = new ArrayList<PromoItemDTO>();

				if (itemsWithPrediction.get(promoItem.getProductKey()) != null) {
					items = itemsWithPrediction.get(promoItem.getProductKey());
				}

				items.add(promoItem);
				promoItem.setAdditionalDetailForLog(isActualAdItem ? "Actual Ad Items Prediction" : "Prediction Recommended Items");

				itemsWithPrediction.put(promoItem.getProductKey(), items);
				tempItemsWithPrediction.add(promoItem);
			}
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, tempItemsWithPrediction);

		return itemsWithPrediction;
	}

	private void filterDuplicatesPredictionEntries(PredictionInputDTO predictionInputDTO) {

		List<PredictionItemDTO> predictionItems = predictionInputDTO.predictionItems;

		for (PredictionItemDTO predItem : predictionItems) {
			List<PricePointDTO> pricePoints = predItem.pricePoints;

			List<PricePointDTO> pricePointsUnique = new ArrayList<PricePointDTO>();
			
			HashSet<PredictionDetailKey> distinctPricePoints = new HashSet<PredictionDetailKey>();
			for (PricePointDTO pricePoint : pricePoints) {
				PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, predItem.itemCodeOrLirId,
						pricePoint.getRegQuantity(), pricePoint.getRegPrice(), pricePoint.getSaleQuantity(), pricePoint.getSalePrice(),
						pricePoint.getAdPageNo(), pricePoint.getAdBlockNo(), pricePoint.getPromoTypeId(), pricePoint.getDisplayTypeId());
				
				if(!distinctPricePoints.contains(predictionDetailKey)) {
					pricePointsUnique.add(pricePoint);
				}
				
				distinctPricePoints.add(predictionDetailKey);
			}

//			for (PricePointDTO pricePoint : pricePoints) {
//				boolean isUniquePricePoint = true;
//				for (PricePointDTO uniquePricePoint : pricePointsUnique) {
//					if ((uniquePricePoint.getFormattedRegPrice().equals(pricePoint.getFormattedRegPrice())
//							&& uniquePricePoint.getRegQuantity() == pricePoint.getRegQuantity()
//							&& uniquePricePoint.getFormattedSalePrice().equals(pricePoint.getFormattedSalePrice())
//							&& uniquePricePoint.getSaleQuantity() == pricePoint.getSaleQuantity()
//							&& uniquePricePoint.getAdBlockNo() == pricePoint.getAdBlockNo()
//							&& uniquePricePoint.getAdPageNo() == pricePoint.getAdPageNo()
//							&& uniquePricePoint.getDisplayTypeId() == pricePoint.getDisplayTypeId())
//							|| uniquePricePoint.getFormattedRegPrice().equals(new Double(0.0d))
//							|| uniquePricePoint.getFormattedSalePrice().equals(new Double(0.0d))) {
//
//						isUniquePricePoint = false;
//					}
//				}
//
//				if (isUniquePricePoint) {
//					pricePointsUnique.add(pricePoint);
//				}
//			}
			predItem.pricePoints = pricePointsUnique;
		}
	}

//	private HashMap<ProductKey, List<PromoItemDTO>> updateRegularPricePredictions(HashMap<ProductKey, List<PromoItemDTO>> groupByCategoryItems) {
//
//		HashMap<ProductKey, List<PromoItemDTO>> finalGroupByCategoryItems = new HashMap<ProductKey, List<PromoItemDTO>>();
//
//		for (Map.Entry<ProductKey, List<PromoItemDTO>> predcitionOutput : groupByCategoryItems.entrySet()) {
//
//			HashMap<ProductKey, PromoItemDTO> promoItemRegPricePrediction = new HashMap<ProductKey, PromoItemDTO>();
//
//			List<PromoItemDTO> promoItemWithoutRegularPriceScenario = new ArrayList<PromoItemDTO>();
//
//			for (PromoItemDTO promoItem : predcitionOutput.getValue()) {
//				if (promoItem.getSaleInfo() == null || promoItem.getSaleInfo().getSalePromoTypeLookup().getPromoTypeId() == 0) {
//					promoItemRegPricePrediction.put(promoItem.getProductKey(), promoItem);
//				} else if (promoItem.getRegPrice() != null) {
//					promoItemWithoutRegularPriceScenario.add(promoItem);
//				}
//			}
//
//			// logger.debug("Promo Items counts for product id : " + predcitionOutput.getKey() + " Total count : " +
//			// predcitionOutput.getValue().size()
//			// + " Count without Sale : " + promoItemWithoutRegularPriceScenario.size());
//
//			if (promoItemRegPricePrediction != null && promoItemRegPricePrediction.size() > 0) {
//				for (Map.Entry<ProductKey, PromoItemDTO> regPriceItem : promoItemRegPricePrediction.entrySet()) {
//					for (PromoItemDTO promoItem : promoItemWithoutRegularPriceScenario) {
//						if (promoItem.getSaleInfo() != null && promoItem.getSaleInfo().getSalePromoTypeLookup().getPromoTypeId() != 0
//								&& promoItem.getProductKey().equals(regPriceItem.getKey())) {
//
//							promoItem.setPredMovReg(regPriceItem.getValue().getPredMov() > 0 ? regPriceItem.getValue().getPredMov() : 0);
//							promoItem.setPredStatusReg(regPriceItem.getValue().getPredStatus());
//						}
//					}
//				}
//			}
//
//			finalGroupByCategoryItems.put(predcitionOutput.getKey(), promoItemWithoutRegularPriceScenario);
//		}
//		return finalGroupByCategoryItems;
//	}

	private void copyPromoItem(int productLevelId, int productId, String upc, String itemName, PromoItemDTO source, PromoItemDTO dest) {
		dest.setProductKey(new ProductKey(productLevelId, productId));
		dest.setUpc(upc);
		dest.setRegPrice(source.getRegPrice());
		dest.setSaleInfo(source.getSaleInfo());
		dest.setAdInfo(source.getAdInfo());
		dest.setDisplayInfo(source.getDisplayInfo());
		dest.setCategoryId(source.getCategoryId());
		dest.setRetLirId(source.getRetLirId());
		dest.setListCost(source.getListCost());
		dest.setDealCost(source.getDealCost());
		dest.setMinDealCost(source.getMinDealCost());
		dest.setDerivedDealCost(source.getDerivedDealCost());
		dest.setFinalCost(source.getFinalCost());
		dest.setCompPrice(source.getCompPrice());
		dest.setPastSaleInfo(source.getPastSaleInfo());
		dest.setActive(source.isActive());
		dest.setNoOfHHRecommendedTo(source.getNoOfHHRecommendedTo());
		if (productLevelId == Constants.ITEMLEVELID) {
			if (itemName == null || itemName.length() == 0) {
				if (source.getItemName() != null && source.getItemName().length() > 0) {
					dest.setItemName(source.getItemName());
				}
			} else {
				dest.setItemName(itemName);
			}
		} else {
			dest.setItemName("");
		}
		dest.setRetLirName(source.getRetLirName());
		dest.setItemCurrentlyOnPromo(source.isItemCurrentlyOnPromo());
		dest.setPresentInFutureWeekAd(source.isPresentInFutureWeekAd());
		dest.setOnTPR(source.isOnTPR());
		dest.setPresentInPreviousWeekAd(source.isPresentInPreviousWeekAd());
		dest.setPriceGreaterThanCompPrice(source.isPriceGreaterThanCompPrice());
		dest.setDeptId(source.getDeptId());
		dest.setDeptName(source.getDeptName());
		dest.setPredStatus(source.getPredStatus());
		dest.setPredStatusReg(source.getPredStatusReg());
		dest.setPpgGroupIds(new HashSet<Long>(source.getPpgGroupIds()));
		dest.setPpgGroupId(source.getPpgGroupId());
		dest.setPPGLeadItem(source.isPPGLeadItem());
		dest.setBrandId(source.getBrandId());
		dest.setBrandName(source.getBrandName());
	}

	private PredictionInputDTO formPredictionInput(int locationLevelId, int locationId, int productLevelId, int productId, int recWeekCalendarId,
			List<PromoItemDTO> categoryItems) {

		logger.debug("formPredictionInput(): Start...");
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.locationId = locationId;
		predictionInputDTO.locationLevelId = locationLevelId;
		predictionInputDTO.productId = productId;
		predictionInputDTO.productLevelId = productLevelId;
		predictionInputDTO.startCalendarId = recWeekCalendarId;
		predictionInputDTO.endCalendarId = recWeekCalendarId;
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();

		// group by items
		HashMap<ProductKey, List<PromoItemDTO>> groupByItems = new HashMap<ProductKey, List<PromoItemDTO>>();

		for (PromoItemDTO promoItemDTO : categoryItems) {
			List<PromoItemDTO> items = new ArrayList<PromoItemDTO>();
			if (groupByItems.get(promoItemDTO.getProductKey()) != null) {
				items = groupByItems.get(promoItemDTO.getProductKey());
			}
			items.add(promoItemDTO);
			groupByItems.put(promoItemDTO.getProductKey(), items);
		}

		for (Map.Entry<ProductKey, List<PromoItemDTO>> items : groupByItems.entrySet()) {
			PredictionItemDTO predictionItemDTO = formPredictionItemDTO(items.getKey().getProductId(), items.getValue().get(0).getUpc(),
					items.getValue());
			
			if (predictionItemDTO != null) {
				predictionInputDTO.predictionItems.add(predictionItemDTO);
			}
		}

		// logger.debug("predictionInputDTO values are : " +
		// predictionInputDTO.toString());
		logger.debug("formPredictionInput(): End...");
		return predictionInputDTO;
	}

	private PredictionItemDTO formPredictionItemDTO(int itemCode, String upc, List<PromoItemDTO> promoItems) {
		// logger.debug("formPredictionItemDTO(): Start...");
		PredictionItemDTO predictionItemDTO = null;

		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.lirInd = false;
		predictionItemDTO.itemCodeOrLirId = itemCode;
		predictionItemDTO.upc = upc;
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();

		for (PromoItemDTO promoItemDTO : promoItems) {
			if (promoItemDTO.getRegPrice() != null) {
				PricePointDTO pricePointDTO = formPricePoint(promoItemDTO);
				predictionItemDTO.pricePoints.add(pricePointDTO);
				
				// Add regular price point
				PricePointDTO regPricePointDTO = formRegPricePoint(promoItemDTO);
				predictionItemDTO.pricePoints.add(regPricePointDTO);
			}
		}
		
		
		

		// logger.debug("predictionItemDTO : " + predictionItemDTO.toString());
		// logger.debug("predictionItemDTO : " + (predictionItemDTO != null ?
		// predictionItemDTO.itemCodeOrLirId : " Null object "));
		// logger.debug("formPredictionItemDTO(): End...");
		return predictionItemDTO;
	}

	private PricePointDTO formPricePoint(PromoItemDTO promoItemDTO) {
		PricePointDTO pricePointDTO = new PricePointDTO();

		pricePointDTO.setRegPrice(promoItemDTO.getRegPrice().price);
		pricePointDTO.setRegQuantity(promoItemDTO.getRegPrice().multiple);

		if (promoItemDTO.getSaleInfo() != null) {
			pricePointDTO.setSaleQuantity(promoItemDTO.getSaleInfo().getSalePrice().multiple);
			pricePointDTO.setSalePrice(promoItemDTO.getSaleInfo().getSalePrice().price);
			pricePointDTO.setPromoTypeId(promoItemDTO.getSaleInfo().getPromoTypeId());
		}

		if (promoItemDTO.getAdInfo() != null) {
			pricePointDTO.setAdPageNo(promoItemDTO.getAdInfo().getAdPageNo());
			pricePointDTO.setAdBlockNo(promoItemDTO.getAdInfo().getAdBlockNo());
		}

		if (promoItemDTO.getDisplayInfo() != null) {
			pricePointDTO.setDisplayTypeId(promoItemDTO.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId());
		}

		return pricePointDTO;
	}
	
	private PricePointDTO formRegPricePoint(PromoItemDTO promoItemDTO) {
		PricePointDTO pricePointDTO = new PricePointDTO();

		pricePointDTO.setRegPrice(promoItemDTO.getRegPrice().price);
		pricePointDTO.setRegQuantity(promoItemDTO.getRegPrice().multiple);

		pricePointDTO.setSaleQuantity(0);
		pricePointDTO.setSalePrice(0d);
		pricePointDTO.setPromoTypeId(0);

		pricePointDTO.setAdPageNo(0);
		pricePointDTO.setAdBlockNo(0);

		pricePointDTO.setDisplayTypeId(0);

		return pricePointDTO;
	}

	private void updatePrediction(int locationLevelId, int locationId, List<PromoItemDTO> items, PredictionOutputDTO predictionOutput) {

		logger.debug("updatePrediction(): Start...");
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = convertPredictionOutputDTOToPredictionOutputMap(predictionOutput);
		if (predictionOutputMap != null && predictionOutputMap.size() > 0) {
			for (PromoItemDTO itemDTO : items) {
				// logger.debug("1.Inside updatePrediction: itemCode:" + itemDTO.getProductKey().getProductId());
				List<PricePointDTO> pricePointListFromPrediction = predictionOutputMap.get(itemDTO.getProductKey().getProductId());
				if (pricePointListFromPrediction != null) {

					PredictionDetailKey pdk = generatePredictionDetailKey(locationLevelId, locationId, itemDTO);
					PredictionDetailKey pdkRegPrice = generatePredictionDetailKeyOfRegPrice(locationLevelId, locationId, itemDTO);

					for (PricePointDTO pricePointFromPrediction : pricePointListFromPrediction) {
						PredictionDetailKey pdkPricePoint = new PredictionDetailKey(locationLevelId, locationId,
								itemDTO.getProductKey().getProductId(), pricePointFromPrediction.getRegQuantity(),
								pricePointFromPrediction.getRegPrice(), pricePointFromPrediction.getSaleQuantity(),
								pricePointFromPrediction.getSalePrice(), pricePointFromPrediction.getAdPageNo(),
								pricePointFromPrediction.getAdBlockNo(), pricePointFromPrediction.getPromoTypeId(),
								pricePointFromPrediction.getDisplayTypeId());

						if (pdk.equals(pdkPricePoint)) {
							// logger.debug("2.Inside updatePrediction: itemCode:" + itemDTO.getProductKey().getProductId());
							// update current sale prediction
							itemDTO.setPredMov(pricePointFromPrediction.getPredictedMovement());
							itemDTO.setPredStatus(pricePointFromPrediction.getPredictionStatus());
						}
						
						if (pdkRegPrice.equals(pdkPricePoint)) {
							itemDTO.setPredMovReg(pricePointFromPrediction.getPredictedMovement());
							itemDTO.setPredStatusReg(pricePointFromPrediction.getPredictionStatus());
						}
					}
				} else {
					itemDTO.setPredMov(0d);
					itemDTO.setPredStatus(PredictionStatus.UNDEFINED);
				}
			}
		} else {
			// assign error message
			for (PromoItemDTO itemDTO : items) {
				itemDTO.setPredMov(0d);
				itemDTO.setPredStatus(PredictionStatus.UNDEFINED);
			}
		}
		logger.debug("updatePrediction(): End...");
	}

	private HashMap<Integer, List<PricePointDTO>> convertPredictionOutputDTOToPredictionOutputMap(PredictionOutputDTO predictionOutput) {
		logger.debug("convertPredictionOutputDTOToPredictionOutputMap(): Start...");
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = null;
		if (predictionOutput.predictionItems != null) {
			predictionOutputMap = new HashMap<Integer, List<PricePointDTO>>();
			for (PredictionItemDTO pItem : predictionOutput.predictionItems) {
				predictionOutputMap.put(pItem.itemCodeOrLirId, pItem.pricePoints);
			}
		} else {
			logger.warn("No output from prediction service");
		}
		// logger.debug("predictionOutputMap : " + (predictionOutputMap != null ? predictionOutputMap.toString() : " NULL "));
		logger.debug("convertPredictionOutputDTOToPredictionOutputMap(): End...");

		return predictionOutputMap;
	}

	private void setLogPath(Set<Integer> departmentList) throws GeneralException {

		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(weekStartDate);
		String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate, "MM-dd-yyy");

		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String logName = String.valueOf(locationLevelId) + "_" + String.valueOf(locationId) + "_" + tempWeekStartDate + "_" + timeStamp;
		if (departmentList.size() > 1) {
			logName = String.valueOf(locationLevelId) + "_" + String.valueOf(locationId) + "_" + tempWeekStartDate + "_" + timeStamp;
		} else {
			// logName = String.valueOf(locationLevelId) + "_" + String.valueOf(locationId) + "_" + tempWeekStartDate + "_" +
			// timeStamp;
			logName = String.valueOf(locationLevelId) + "-" + String.valueOf(locationId) + "_" + productLevelId + "-"
					+ String.valueOf(departmentList.iterator().next()) + "_" + tempWeekStartDate + "_" + timeStamp;
		}

		itemAnalysisLogPath = itemAnalysisLogPath + "/" + logName + "_itemAnalysis.csv";
	}

	// get distinct zones nos list
	public List<String> getZoneNos(HashMap<ProductKey, PRItemDTO> authroizedItemMap) {

		List<PRItemDTO> allStoreItems = new ArrayList<PRItemDTO>();

		for (Map.Entry<ProductKey, PRItemDTO> item : authroizedItemMap.entrySet()) {
			if (item.getKey().getProductLevelId() == Constants.ITEMLEVELID) {
				allStoreItems.add(item.getValue());
			}
		}

		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);
		List<String> priceAndStrategyZoneNos = new ArrayList<String>();
		priceAndStrategyZoneNos = itemService.getPriceAndStrategyZoneNos(allStoreItems);
		return priceAndStrategyZoneNos;
	}

	//// HashMap<PriceZone, HashMap<ItemCode, CompetitiveDTO>>
	public HashMap<Integer, HashMap<Integer, CompetitiveDataDTO>> getCompetitorPriceList(Connection conn,
			HashMap<ProductKey, PRItemDTO> authorizedItems, int productLevelId, String productIdList, String weekStartDate,
			List<String> priceAndStrategyZoneNos, int noOfWeeksCompHistory) throws GeneralException {

		LocationCompetitorMapDAO locCompMapDAO = new LocationCompetitorMapDAO();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		// HashMap<PriceZone, HashMap<ItemCode, CompetitiveDTO>>
		HashMap<Integer, HashMap<Integer, CompetitiveDataDTO>> fullCompData = new HashMap<Integer, HashMap<Integer, CompetitiveDataDTO>>();

		try {
			ArrayList<LocationCompetitorMapDTO> locationCompetitorMapDTOList = locCompMapDAO.getLocationCompetitorDetails(conn);

			// iterating for each distinct price zone separately
			for (String priceZone : priceAndStrategyZoneNos) {

				LocationCompetitorMapDTO relatedLocationCompetitorDTO = null;
				for (LocationCompetitorMapDTO locationCompetitorMapDTO : locationCompetitorMapDTOList) {
					if (locationCompetitorMapDTO.getBaseLocationId() == Integer.parseInt(priceZone)
							&& locationCompetitorMapDTO.getBaseLocationLevelId() == Constants.ZONE_LEVEL_ID
							&& locationCompetitorMapDTO.getCompLocationTypeId() == Constants.COMP_LOCATION_TYPE_PRIMARY) {
						relatedLocationCompetitorDTO = locationCompetitorMapDTO;
						logger.debug("Competitor is : " + relatedLocationCompetitorDTO.getCompLocationId() + "  for Zone : "
								+ relatedLocationCompetitorDTO.getBaseLocationId());
						break;
					}
				}
				// Fetching comp data for related comp location
				if (relatedLocationCompetitorDTO != null) {
					logger.debug("Fetching competitor data for price zone: " + priceZone);
					PRStrategyDTO inputDTO = new PRStrategyDTO();
					inputDTO.setLocationId(relatedLocationCompetitorDTO.getCompLocationId());
					inputDTO.setLocationLevelId(relatedLocationCompetitorDTO.getCompLocationLevelId());
					inputDTO.setProductId(-1);
					inputDTO.setProductLevelId(productLevelId);
					// HashMap<ItemCode, CompetitiveDTO>
					HashMap<Integer, CompetitiveDataDTO> compData = pricingEngineDAO.getLatestCompPriceData(conn, inputDTO, weekStartDate,
							noOfWeeksCompHistory * 7, productIdList);

					if (compData != null && compData.size() > 0) {
						fullCompData.put(Integer.parseInt(priceZone), compData);
					}
				} else {
					logger.debug("Competitor location was not found for price Zone : " + priceZone);
				}
			}
		} catch (Exception e) {
			logger.error("getCompetitorPrice() Exception : " + e.getMessage());
		}

		return fullCompData;
	}

	// Updating authorized items List with Comp Price

	// Updating authorized items List with Comp Price
	public void updateAuthorizedItemsList(HashMap<ProductKey, PRItemDTO> authorizedItems,
			HashMap<Integer, HashMap<Integer, CompetitiveDataDTO>> fullCompData, HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap) {

		for (Map.Entry<ProductKey, PRItemDTO> itemEntry : authorizedItems.entrySet()) {
			// Step 1: Get all CompetitiveDataDTOs for a non LIG item
			List<CompetitiveDataDTO> competitiveDataList = new ArrayList<CompetitiveDataDTO>();
			Integer itemCode = itemEntry.getKey().getProductId();

			MultiplePrice minCompPrice = null;

			// processing for non LIG items
			if (itemEntry.getKey().getProductLevelId() == 1 && itemStoreMap.get(itemCode) != null) {
				// stores and zones for the item
				HashMap<Integer, Integer> storesZonesOfItem = itemStoreMap.get(itemCode);

				// for each store collect the competitor price of respective
				// price Zone
				for (Map.Entry<Integer, Integer> storeEntry : storesZonesOfItem.entrySet()) {
					if (fullCompData.get(storeEntry.getValue()) != null && fullCompData.get(storeEntry.getValue()).size() > 0) {
						HashMap<Integer, CompetitiveDataDTO> compDataDTOMap = fullCompData.get(storeEntry.getValue());

						if (compDataDTOMap.get(itemCode) != null) {

							competitiveDataList.add(compDataDTOMap.get(itemCode));
							// logger.debug("Comp regPrice : " +
							// compDataDTOMap.get(itemCode).regPrice
							// + " Sale Price : " +
							// compDataDTOMap.get(itemCode).fSalePrice);
						}
					}
				}
			}

			// Step 2: Get min price CompetitiveDataDTO for a non LIG item
			// adding all comp DTOs for item in map
			if (competitiveDataList.size() == 0) {
				// logger.debug("No competitor data found for item : " + itemCode);
				continue;
			} else {
				minCompPrice = minCompetitorPrice(competitiveDataList);

				// logger.debug("Min Comp price regPrice : " +
				// minCompPrice.toString());
				// Step 3: Update competitor price to authorizedItems
				// logger.debug("Comp Price for item code: " + itemCode + " :: "
				// + minCompPrice.toString());
				if (minCompPrice != null) {
					authorizedItems.get(itemEntry.getKey()).setCompPrice(minCompPrice);
				}
			}
		}
	}

	// Main method to get the competitor price for each item
	public void getCompetitorPrice(Connection conn, List<Integer> stores, HashMap<ProductKey, PRItemDTO> authorizedItems, int productLevelId,
			String productIdList, String weekStartDate, HashMap<Integer, List<PRItemDTO>> ligMap,
			HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap, int inputPrimaryCompStrId, int noOfWeeksCompHistory) throws GeneralException {

		if (inputPrimaryCompStrId <= 0) {

			// Getting all distinct zones for all authorized items
			ArrayList<String> zoneList = getZoneList(itemStoreMap);

			// Getting comp data for all price zones
			HashMap<Integer, HashMap<Integer, CompetitiveDataDTO>> fullCompData = getCompetitorPriceList(conn, authorizedItems, productLevelId,
					productIdList, weekStartDate, zoneList, noOfWeeksCompHistory);

			// updating authorized items list with comp data
			if (fullCompData != null && fullCompData.size() > 0) {
				updateAuthorizedItemsList(authorizedItems, fullCompData, itemStoreMap);
			} else {
				logger.debug("Not able to retrieve competition data.");
			}

		} else {
			// if primary competitor is provided as input, pick only that competitor price

			PRStrategyDTO inputDTO = new PRStrategyDTO();
			inputDTO.setLocationId(inputPrimaryCompStrId);
			inputDTO.setLocationLevelId(Constants.STORE_LEVEL_ID);
			inputDTO.setProductId(-1);
			inputDTO.setProductLevelId(productLevelId);

			HashMap<Integer, CompetitiveDataDTO> compData = new PricingEngineDAO().getLatestCompPriceData(conn, inputDTO, weekStartDate,
					noOfWeeksCompHistory * 7, productIdList);

			// update comp price to all authorized items
			for (Map.Entry<ProductKey, PRItemDTO> itemEntry : authorizedItems.entrySet()) {
				if (compData.get(itemEntry.getKey().getProductId()) != null) {

					List<CompetitiveDataDTO> competitiveDataList = new ArrayList<CompetitiveDataDTO>();

					competitiveDataList.add(compData.get(itemEntry.getKey().getProductId()));

					MultiplePrice compPrice = minCompetitorPrice(competitiveDataList);
					authorizedItems.get(itemEntry.getKey()).setCompPrice(compPrice);
				}
			}
		}

		populateLigCompetitrPrice(authorizedItems, ligMap);
	}

	// function to update the LIG level competitor price
	public void populateLigCompetitrPrice(HashMap<ProductKey, PRItemDTO> authorizedItems, HashMap<Integer, List<PRItemDTO>> ligMap)
			throws GeneralException {

		try {
			logger.debug("populateLigCompetitrPrice() : Start...");
			// Processing for each LIG
			// Aggregating sale data for all members in LIG then processing

			for (Map.Entry<Integer, List<PRItemDTO>> ligEntry : ligMap.entrySet()) {

				ProductKey ligProductKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, ligEntry.getKey());

				if (authorizedItems.get(ligProductKey) != null) {
					PRItemDTO ligItem = authorizedItems.get(ligProductKey);

					ArrayList<MultiplePrice> compPriceList = new ArrayList<MultiplePrice>();
					for (PRItemDTO prItemDTO : ligEntry.getValue()) {

						ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, prItemDTO.getItemCode());
						if (authorizedItems.get(productKey).getCompPrice() != null) {
							compPriceList.add(authorizedItems.get(productKey).getCompPrice());
						}
					}
					ligItem.setCompPrice(getMostOccurringMultiplePrice(compPriceList));
				} else {
					logger.debug("populateLigCompetitrPrice() : LIG " + ligEntry.getKey() + " was not found in authrized items map.");
				}
			}

			logger.debug("populateLigCompetitrPrice() : End...");
		} catch (Exception e) {
			logger.error("populateLigCompetitrPrice() : Exception occurred : " + e.getMessage());
			throw new GeneralException("Error in populateLigCompetitrPrice() - " + e.getMessage());
		}
	}

	// method to get the zone list
	public ArrayList<String> getZoneList(HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap) {
		ArrayList<String> zoneList = new ArrayList<String>();

		if (itemStoreMap != null && itemStoreMap.size() > 0) {

			HashSet<Integer> zoneMap = new HashSet<Integer>();
			// HashMap<Item, HashMap<Store, Zone>>
			for (HashMap<Integer, Integer> storeZoneMap : itemStoreMap.values()) {
				zoneMap.addAll(storeZoneMap.values());
			}
			// Converting set<Integer> to List<String>
			for (Integer zone : zoneMap) {
				zoneList.add(zone.toString());
			}
		} else {
			logger.debug("getZoneList(): Item store map is null or empty.");
		}
		return zoneList;
	}

	// Getting min competitor price for item
	private MultiplePrice minCompetitorPrice(List<CompetitiveDataDTO> compList) {

		// Min Competitor price (it can be sale or regular price during the
		// period)
		MultiplePrice multipleMinCompPrice = null;

		for (CompetitiveDataDTO compDTO : compList) {

			MultiplePrice multipleRegPrice = PRCommonUtil.getMultiplePrice(compDTO.regMPack, (double) compDTO.regPrice, (double) compDTO.regMPrice);

			MultiplePrice multipleSalePrice = PRCommonUtil.getMultiplePrice(compDTO.saleMPack, (double) compDTO.fSalePrice,
					(double) compDTO.fSaleMPrice);

			// Add regular price only if the price is greater than 0.0
			if (multipleRegPrice != null && multipleRegPrice.price > 0.0d) {
				if (multipleMinCompPrice == null) {
					// initializing if null
					multipleMinCompPrice = multipleRegPrice;
				}
				if (multipleMinCompPrice != null
						&& (PRCommonUtil.getUnitPrice(multipleMinCompPrice, true) > PRCommonUtil.getUnitPrice(multipleRegPrice, true))) {
					multipleMinCompPrice = multipleRegPrice;
				}
			}
			// Add sale price only if the sale price is greater than 0.0
			if (multipleSalePrice != null && multipleSalePrice.price > 0.0d) {
				if (multipleMinCompPrice == null) {
					// initializing if null
					multipleMinCompPrice = multipleSalePrice;
				}
				if (multipleMinCompPrice != null
						&& (PRCommonUtil.getUnitPrice(multipleMinCompPrice, true) > PRCommonUtil.getUnitPrice(multipleSalePrice, true))) {
					multipleMinCompPrice = multipleSalePrice;
				}
			}
		}
		// returning min of sale and regular Price
		return multipleMinCompPrice;
	}

	// Funtion to get item store map HashMap<Item, HashMap<Store, Zone>>
	// This function ensures that all stores are considered in store list
	public HashMap<Integer, HashMap<Integer, Integer>> getItemStoreMap(Connection conn, List<Integer> stores, String productId)
			throws GeneralException {
		// fetching the mapping of authorized items and their respective
		// store/zone list
		PromotionEngineDAO promoDAO = new PromotionEngineDAO();

		StringBuffer priceZoneStoreIds = new StringBuffer();
		if (stores != null && stores.size() > 0) {
			for (Integer storeId : stores) {
				priceZoneStoreIds.append(storeId.toString());
				priceZoneStoreIds.append(",");
			}
			priceZoneStoreIds.deleteCharAt(priceZoneStoreIds.length() - 1);
		}

		HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap = new HashMap<Integer, HashMap<Integer, Integer>>();
		itemStoreMap = promoDAO.getItemStoreMap(conn, productLevelId, productId, priceZoneStoreIds.toString());
		return itemStoreMap;
	}

	public List<RetailCalendarDTO> getNextWeeksCalendarDetail(List<RetailCalendarDTO> retailCalendarCache, String recWeekStartDate) {

		List<RetailCalendarDTO> futureRecommendationWeeks = new ArrayList<RetailCalendarDTO>();
		RetailCalendarService retailCalendarService = new RetailCalendarService();

		RetailCalendarDTO recWeek = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, recWeekStartDate);
		// System.out.println("recWeek " + recWeek.getStartDate());
		// Get recommended week's week no

		int maxWeekNoOfYear = retailCalendarService.getWeekMaxActualNo(retailCalendarCache, recWeek.getCalYear());

		int futureWeeksCount = Integer.valueOf(PropertyManager.getProperty("PROMO_FUTURE_X_WEEKS"));
		int weekSelect = 0;
		int nextRecWeekNo = recWeek.getActualNo() + 1;
		int recYear = recWeek.getCalYear();
		while (weekSelect < futureWeeksCount) {
			for (RetailCalendarDTO r : retailCalendarCache) {
				if (nextRecWeekNo <= maxWeekNoOfYear && r.getRowType().equals("W") && r.getActualNo() == nextRecWeekNo && r.getCalYear() == recYear) {
					futureRecommendationWeeks.add(r);
					nextRecWeekNo++;
					break;
				} else if (nextRecWeekNo > maxWeekNoOfYear && r.getRowType().equals("W") && r.getActualNo() == 1 && r.getCalYear() == (recYear + 1)) {
					futureRecommendationWeeks.add(r);
					nextRecWeekNo = 2; // second week of next year
					recYear = recYear + 1;
					break;
				}
			}
			weekSelect++;
		}
		if (futureRecommendationWeeks.size() < futureWeeksCount) {
			logger.debug("getNextWeeksCalendarDetail() : Future weeks datails not available in Retail Calendar table.");
		}
		return futureRecommendationWeeks;
	}

	public List<RetailCalendarDTO> getPreviousXWeeksCalendarDetail(List<RetailCalendarDTO> retailCalendarCache, String recWeekStartDate) {

		List<RetailCalendarDTO> previousXWeeks = new ArrayList<RetailCalendarDTO>();
		RetailCalendarService retailCalendarService = new RetailCalendarService();

		RetailCalendarDTO recWeek = retailCalendarService.getWeekCalendarDetail(retailCalendarCache, recWeekStartDate);

		int maxWeekNoOfPreviousYear = retailCalendarService.getWeekMaxActualNo(retailCalendarCache, recWeek.getCalYear() - 1);

		int pastWeeksCount = 52;// Integer.valueOf(PropertyManager.getProperty("PROMO_PAST_X_WEEKS_SALE_PRICE_POINTS"));
		int weekSelect = 0;
		int prevRecWeekNo = recWeek.getActualNo() - 1;
		int recYear = recWeek.getCalYear();
		while (weekSelect < pastWeeksCount) {
			for (RetailCalendarDTO r : retailCalendarCache) {
				if (prevRecWeekNo > 0 && r.getRowType().equals("W") && r.getActualNo() == prevRecWeekNo && r.getCalYear() == recYear) {
					previousXWeeks.add(r);
					prevRecWeekNo--;
					break;
				} else if (prevRecWeekNo == 0 && r.getRowType().equals("W") && r.getActualNo() == maxWeekNoOfPreviousYear
						&& r.getCalYear() == (recYear - 1)) {
					previousXWeeks.add(r);
					prevRecWeekNo = maxWeekNoOfPreviousYear; // last week of previous year
					recYear = recYear - 1;
					break;
				}
			}
			weekSelect++;
		}
		if (previousXWeeks.size() < pastWeeksCount) {
			logger.debug("getPreviousXWeeksCalendarDetail() : Previous weeks datails not available in Retail Calendar table.");
		}
		return previousXWeeks;
	}

	// Getting most common competitor price
	private MultiplePrice getMostOccurringMultiplePrice(List<MultiplePrice> priceList) {
		MultiplePrice mostOccurringMultiplePrice = null;

		if (priceList != null && priceList.size() > 0) {
			if (priceList.size() < 3) {
				return priceList.get(0);
			} else {
				// <MultiplePrice, Occurrence>
				HashMap<MultiplePrice, Integer> priceOccurrenceMap = new HashMap<MultiplePrice, Integer>();

				for (MultiplePrice compPrice : priceList) {
					int occurrance = 1;
					if (priceOccurrenceMap.get(compPrice) != null) {
						occurrance = occurrance + priceOccurrenceMap.get(compPrice);
					}
					priceOccurrenceMap.put(compPrice, occurrance);
				}

				// Calcualating most common multiple price
				int maxFrequecy = 0;
				for (Map.Entry<MultiplePrice, Integer> entry : priceOccurrenceMap.entrySet()) {
					if (mostOccurringMultiplePrice != null) {
						if (entry.getValue() > maxFrequecy) {
							mostOccurringMultiplePrice = entry.getKey();
							maxFrequecy = entry.getValue();
						}
					} else {
						mostOccurringMultiplePrice = entry.getKey();
						maxFrequecy = entry.getValue();
					}
				}
			}
		} else {
			// logger.debug("Price List is null or empty.");
		}
		return mostOccurringMultiplePrice;

	}
	
	public void getPriceOfAuthorizedItems(Connection connection, Integer chainId, RetailCalendarDTO startWeekCalDTO, int noOfPriceHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, List<Integer> priceZoneStores, HashMap<ProductKey, PRItemDTO> authorizedItems,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap) throws GeneralException {

		logger.info("getPriceOfAuthorizedItems(): Getting price is started...");

		Set<Integer> itemCodeList = new HashSet<Integer>();

		for (Map.Entry<ProductKey, PRItemDTO> item : authorizedItems.entrySet()) {
			if (item.getKey().getProductLevelId() == Constants.ITEMLEVELID) {
				itemCodeList.add(item.getValue().getItemCode());
			}
		}

		LocalDate currentDate = LocalDate.now();
		
		// Getting all distinct zones for all authorized items
		ArrayList<String> zoneList = getZoneList(itemStoreMap);
 
		logger.debug("Fetching price history is started... ");
		// TODO::Fetching full price data is a single run to reduce database IO
		// HashMap<ItemCode, HashMap<WeekStartDate, RetailPriceDTO>>
		HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = new PromotionEngineDAO().getPriceHistory(connection, chainId,
				startWeekCalDTO, noOfPriceHistory, allWeekCalendarDetails, itemCodeList, zoneList, priceZoneStores);
		logger.debug("Fetching price history is completed...");

		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemEachWeekFinalPrice = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		logger.debug("Find price of each item of each week is started...");
		
		// Find price of the item (most common price across the stores)
		// Find item price for each week
		for (Map.Entry<Integer, HashMap<String, List<RetailPriceDTO>>> itemWeeklyPrice : itemPriceHistory.entrySet()) {
			int itemCode = itemWeeklyPrice.getKey();
			HashMap<String, RetailPriceDTO> weekFinalPrice = new HashMap<String, RetailPriceDTO>();
			
			// Loop each week
			for (Map.Entry<String, List<RetailPriceDTO>> weeklyPriceEntry : itemWeeklyPrice.getValue().entrySet()) {
				
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();

				boolean isFuture = false;
				String weekStart = weeklyPriceEntry.getKey();
				LocalDate startDate = LocalDate.parse(weekStart, PRCommonUtil.getDateFormatter());
				if(startDate.isAfter(currentDate)) {
					isFuture = true;
				}
				
				// Find each store price for each item
				HashMap<Integer, Integer> storeAndZoneIdMap = itemStoreMap.get(itemCode);
				
				List<MultiplePrice> regPriceCol = new ArrayList<MultiplePrice>();
				for (Map.Entry<Integer, Integer> storeAndZoneIdMapEntry : storeAndZoneIdMap.entrySet()) {
					int storeId = storeAndZoneIdMapEntry.getKey();
					int zoneId = storeAndZoneIdMapEntry.getValue();
					RetailPriceDTO tempPriceDTO = findPriceForStore(chainId, zoneId, storeId,
							weeklyPriceEntry.getValue(), isFuture);
					if(tempPriceDTO != null) {
						regPriceCol.add(tempPriceDTO.getRegularPrice());	
					}
				}
				

				// Find common price across the store and put it as item price
				MultiplePrice mostCommonRegPrice = (MultiplePrice) mostOccurrenceData.getMaxOccurance(regPriceCol);

				retailPriceDTO.setRegularPrice(mostCommonRegPrice);

				weekFinalPrice.put(weeklyPriceEntry.getKey(), retailPriceDTO);
				itemEachWeekFinalPrice.put(itemCode, weekFinalPrice);
			}
		}
		logger.debug("Find price of each item of each week is completed...");
		
		
		logger.debug("Updating item price is started...");
		// Find latest price for each item
		updateItemWithLatestPrice(itemEachWeekFinalPrice, authorizedItems, startWeekCalDTO, noOfPriceHistory, allWeekCalendarDetails);
		logger.debug("Updating item price is completed...");
		
		
		logger.info("Getting price is completed...");
	}

	// Function to get the most common cost of authorized items for all
	// stores/zones
	public void getCostOfAuthorizedItems(Connection connection, Integer chainId, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, List<Integer> priceZoneStores, HashMap<ProductKey, PRItemDTO> authorizedItems,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap) throws GeneralException {
		
		boolean useMinDealCostFromHistory = Boolean.parseBoolean(PropertyManager.getProperty("OR_USE_MIN_DEAL_FROM_HISTORY", "FALSE"));
		
		logger.debug("Using min deal from history: " + useMinDealCostFromHistory);
		logger.info("getCostOfAuthorizedItems(): Getting cost is started...");

		Set<Integer> itemCodeList = new HashSet<Integer>();

		for (Map.Entry<ProductKey, PRItemDTO> item : authorizedItems.entrySet()) {
			if (item.getKey().getProductLevelId() == Constants.ITEMLEVELID) {
				itemCodeList.add(item.getValue().getItemCode());
			}
		}

		LocalDate currentDate = LocalDate.now();
		
		// Getting all distinct zones for all authorized items
		ArrayList<String> zoneList = getZoneList(itemStoreMap);

		logger.debug("Fetching cost history is started...");
		// Fetching full cost data is a single run to reduce database IO
		// HashMap<ItemCode, HashMap<WeekStartDate, RetailCostDTO>>
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory = new PromotionEngineDAO().getCostHistory(connection, chainId,
				startWeekCalDTO, noOfCostHistory, allWeekCalendarDetails, itemCodeList, zoneList, priceZoneStores);
		logger.debug("Fetching cost history is completed...");

		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		HashMap<Integer, HashMap<String, RetailCostDTO>> itemEachWeekFinalCost = new HashMap<Integer, HashMap<String, RetailCostDTO>>();

		logger.debug("Find cost of each item of each week is started...");
		
		// Find cost of the item (most common cost across the stores)
		// Find item cost for each week
		for (Map.Entry<Integer, HashMap<String, List<RetailCostDTO>>> itemWeeklyCost : itemCostHistory.entrySet()) {
			int itemCode = itemWeeklyCost.getKey();
			HashMap<String, RetailCostDTO> weekFinalCost = new HashMap<String, RetailCostDTO>();
			List<Float> dealCostList = new ArrayList<>();
			// Loop each week
			for (Map.Entry<String, List<RetailCostDTO>> weeklyCostEntry : itemWeeklyCost.getValue().entrySet()) {
				
				RetailCostDTO retailCostDTO = new RetailCostDTO();

				boolean isFuture = false;
				String weekStart = weeklyCostEntry.getKey();
				LocalDate startDate = LocalDate.parse(weekStart, PRCommonUtil.getDateFormatter());
				if(startDate.isAfter(currentDate)) {
					isFuture = true;
				}
				
				// Find each store cost for each item
				HashMap<Integer, Integer> storeAndZoneIdMap = itemStoreMap.get(itemCode);
				List<Double> listCostCol = new ArrayList<Double>();
				List<Double> dealCostCol = new ArrayList<Double>();

				for (Map.Entry<Integer, Integer> storeAndZoneIdMapEntry : storeAndZoneIdMap.entrySet()) {
					int storeId = storeAndZoneIdMapEntry.getKey();
					int zoneId = storeAndZoneIdMapEntry.getValue();
					RetailCostDTO tempCostDTO = findCostForStore(chainId, zoneId, storeId, weeklyCostEntry.getValue(),
							isFuture);

					if(tempCostDTO != null) {
						listCostCol.add((double) tempCostDTO.getListCost());
						dealCostCol.add((double) tempCostDTO.getDealCost());
					}
				}
				
				// Find common cost across the store and put it as item cost
				Double mostCommonListCost = (Double) mostOccurrenceData.getMaxOccurance(listCostCol);
				Double mostCommonDealCost = (Double) mostOccurrenceData.getMaxOccurance(dealCostCol);

				retailCostDTO.setListCost((float) (mostCommonListCost != null ? mostCommonListCost : 0));
				retailCostDTO.setDealCost((float) (mostCommonDealCost != null ? mostCommonDealCost : 0));
				
				if(retailCostDTO.getDealCost() > 0) {
					dealCostList.add(retailCostDTO.getDealCost());
				}
				
				weekFinalCost.put(weeklyCostEntry.getKey(), retailCostDTO);
				itemEachWeekFinalCost.put(itemCode, weekFinalCost);
			}
			
			if (useMinDealCostFromHistory && dealCostList.size() > 0) {
				logger.debug("Min deal cost...");
				float minDealCost = Collections.min(dealCostList);
				if (itemEachWeekFinalCost.containsKey(itemCode)) {
					logger.debug("Min deal cost..." + minDealCost);
					HashMap<String, RetailCostDTO> weeklyCost = itemEachWeekFinalCost.get(itemCode);
					for (Map.Entry<String, RetailCostDTO> weekEntry : weeklyCost.entrySet()) {
						RetailCostDTO retailCostDTO = weekEntry.getValue();
						retailCostDTO.setDealCost(minDealCost);
					}
				}
			}
		}
		logger.debug("Find cost of each item of each week is completed...");
		
		
		logger.debug("Updating item cost is started...");
		// Find latest cost for each item
		updateItemWithLatestCost(itemEachWeekFinalCost, authorizedItems, startWeekCalDTO, noOfCostHistory, allWeekCalendarDetails);
		logger.debug("Updating item cost is completed...");
		
		logger.debug("Finding min deal cost is started...");
		// Find min deal cost in last one year
//		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
//				noOfCostHistory);
//		// Each authorized Item
//		for (PRItemDTO itemDTO : authorizedItems.values()) {
//			HashMap<String, RetailCostDTO> itemWeekWiseCost = itemEachWeekFinalCost.get(itemDTO.getItemCode());
//			List<Double> dealCostList = new ArrayList<Double>();
//			// only lig members and non lig
//			if (!itemDTO.isLir() && itemWeekWiseCost != null) {
//				// go from latest week
//				for (RetailCalendarDTO curCalDTO : retailCalendarList) {
//					RetailCostDTO retailCostDTO = itemWeekWiseCost.get(curCalDTO.getStartDate());
//					if (retailCostDTO != null) {
//						if(retailCostDTO.getDealCost() > 0) {
//							dealCostList.add((double) retailCostDTO.getDealCost());
//						}
//					}
//				}
//			}
//
//			Collections.sort(dealCostList);
//			if(dealCostList.size() > 0) {
//				itemDTO.setMinDealCost(dealCostList.get(0));
//			}
//		}
		logger.debug("Finding min deal cost is completed...");
		
		logger.info("Getting cost is completed...");
	}
	

	private RetailCostDTO findCostForStore(int chainId, int zoneId, int storeId, List<RetailCostDTO> costList, boolean isFuture) {

		boolean useChainLevelFutureData = Boolean.parseBoolean(PropertyManager.getProperty("USE_CHAIN_LEVEL_FUTURE_DATA", "FALSE"));
		
		RetailCostDTO costDTO = new RetailCostDTO();
		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, zoneId);
		RetailPriceCostKey storeKey = new RetailPriceCostKey(Constants.STORE_LEVEL_TYPE_ID, storeId);
		
		if (costList != null && costList.size() > 0) {
			HashMap<RetailPriceCostKey, RetailCostDTO> tempCostKeyMap = new HashMap<RetailPriceCostKey, RetailCostDTO>();
			for (RetailCostDTO retailCostDTO : costList) {
				RetailPriceCostKey retailPriceCostKey = new RetailPriceCostKey(retailCostDTO.getLevelTypeId(),
						Integer.valueOf(retailCostDTO.getLevelId()));
				
				tempCostKeyMap.put(retailPriceCostKey, retailCostDTO);
			}

			if (tempCostKeyMap.get(storeKey) != null) {
				costDTO.copy(tempCostKeyMap.get(storeKey));
			} else if (tempCostKeyMap.get(zoneKey) != null) {
				costDTO.copy(tempCostKeyMap.get(zoneKey));
			} else if (tempCostKeyMap.get(chainKey) != null && (!isFuture || useChainLevelFutureData)) {
				costDTO.copy(tempCostKeyMap.get(chainKey));
			} else {
				costDTO = null;
			}
		}

		return costDTO;
	}
	
	private RetailPriceDTO findPriceForStore(int chainId, int zoneId, int storeId, List<RetailPriceDTO> priceList, boolean isFuture) {
		
		boolean useChainLevelFutureData = Boolean.parseBoolean(PropertyManager.getProperty("USE_CHAIN_LEVEL_FUTURE_DATA", "FALSE"));
		
		RetailPriceDTO priceDTO = new RetailPriceDTO();
		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, zoneId);
		RetailPriceCostKey storeKey = new RetailPriceCostKey(Constants.STORE_LEVEL_TYPE_ID, storeId);

		if (priceDTO != null) {
			HashMap<RetailPriceCostKey, RetailPriceDTO> tempPriceKeyMap = new HashMap<RetailPriceCostKey, RetailPriceDTO>();
			for (RetailPriceDTO retailPriceDTO : priceList) {
				RetailPriceCostKey retailPriceCostKey = new RetailPriceCostKey(retailPriceDTO.getLevelTypeId(),
						Integer.valueOf(retailPriceDTO.getLevelId()));
				tempPriceKeyMap.put(retailPriceCostKey, retailPriceDTO);
			}

			if (tempPriceKeyMap.get(storeKey) != null) {
				priceDTO.copy(tempPriceKeyMap.get(storeKey));
			} else if (tempPriceKeyMap.get(zoneKey) != null) {
				priceDTO.copy(tempPriceKeyMap.get(zoneKey));
			} else if (tempPriceKeyMap.get(chainKey) != null && (!isFuture || useChainLevelFutureData)) {
				priceDTO.copy(tempPriceKeyMap.get(chainKey));
			} else {
				priceDTO = null;
			}
		}

		return priceDTO;
	}
	
	private void updateItemWithLatestCost(HashMap<Integer, HashMap<String, RetailCostDTO>> itemCostHistory,
			HashMap<ProductKey, PRItemDTO> authorizedItems, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfCostHistory);

		// Each authorized Item
		for (PRItemDTO itemDTO : authorizedItems.values()) {
			HashMap<String, RetailCostDTO> itemWeekWiseCost = itemCostHistory.get(itemDTO.getItemCode());
			// only lig members and non lig
			if (!itemDTO.isLir() && itemWeekWiseCost != null) {
				// go from latest week
				for (RetailCalendarDTO curCalDTO : retailCalendarList) {
					RetailCostDTO retailCostDTO = itemWeekWiseCost.get(curCalDTO.getStartDate());
					
					//Recommendation week deal cost
					if(startWeekCalDTO.getStartDate().equals(curCalDTO.getStartDate()) && retailCostDTO != null) {
						itemDTO.setRecWeekDealCost((double) retailCostDTO.getDealCost());
					}
					
					if (retailCostDTO != null) {
						itemDTO.setListCost((double) retailCostDTO.getListCost());
						itemDTO.setDealCost((double) retailCostDTO.getDealCost());
						break;
					}
				}
			}

		}
	}
	
	private void updateItemWithLatestPrice(HashMap<Integer, HashMap<String, RetailPriceDTO>> itemPriceHistory,
			HashMap<ProductKey, PRItemDTO> authorizedItems, RetailCalendarDTO startWeekCalDTO, int noOfPriceHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfPriceHistory);

		// Each authorized Item
		for (PRItemDTO itemDTO : authorizedItems.values()) {
			HashMap<String, RetailPriceDTO> itemWeekWisePrice = itemPriceHistory.get(itemDTO.getItemCode());
			// only lig members and non lig
			if (!itemDTO.isLir() && itemWeekWisePrice != null) {
				// go from latest week
				for (RetailCalendarDTO curCalDTO : retailCalendarList) {
					RetailPriceDTO retailPriceDTO = itemWeekWisePrice.get(curCalDTO.getStartDate());
					if (retailPriceDTO != null) {
						itemDTO.setRecommendedRegPrice(retailPriceDTO.getRegularPrice());
//						logger.debug("itemDTO.getRecommendedRegPrice()-" + itemDTO.getItemCode() + ":" + itemDTO.getRecommendedRegPrice());
						break;
					}
				}
			}

		}
	}


	public AdDetail getActualAdDetails(Connection conn, int locationLevelId, int locationId, int productLevelId, Set<Integer> productIdSet,
			String weekStartDate, int noOfPreviousWeeks, int pageNumber) throws GeneralException {
		List<PromoItemDTO> promoAdDetailsList = new ArrayList<PromoItemDTO>();
//		List<PromoItemDTO> adItemsNoDuplicate = new ArrayList<PromoItemDTO>();
		PromotionEngineDAO promoDAO = new PromotionEngineDAO();
//		HashSet<ProductKey> tempKeySet = new HashSet<ProductKey>();
		AdDetail adDetail = new AdDetail();
		PageDetail pageDetail = null;
		BlockDetail blockDetail = null;

		promoAdDetailsList = promoDAO.getAdDetails(conn, locationLevelId, locationId, productLevelId, productIdSet, weekStartDate, noOfPreviousWeeks,
				pageNumber, true);

		

//		for (PromoItemDTO promoItemDTO : promoAdDetailsList) {
//			if (!tempKeySet.contains(promoItemDTO.getProductKey())) {
//				tempKeySet.add(promoItemDTO.getProductKey());
//				adItemsNoDuplicate.add(promoItemDTO);
//			}
//		}

		// Convert to AdDetail
		for (PromoItemDTO promoItemDTO : promoAdDetailsList) {
			int pageNo = promoItemDTO.getAdInfo().getAdPageNo();
			int blockNo = promoItemDTO.getAdInfo().getAdBlockNo();

			pageDetail = new PageDetail();
			blockDetail = new BlockDetail();

			if (adDetail.getPageMap().get(pageNo) != null) {
				pageDetail = adDetail.getPageMap().get(pageNo);
			}

			if (pageDetail.getBlockMap().get(blockNo) != null) {
				blockDetail = pageDetail.getBlockMap().get(blockNo);
			}

			blockDetail.getDepartments().put(promoItemDTO.getDeptId(), promoItemDTO.getDeptName());
			blockDetail.setPageBlockNoKey(new PageBlockNoKey(pageNo, blockNo));
			blockDetail.getItems().add(promoItemDTO);

			pageDetail.setPageNo(pageNo);
			pageDetail.getBlockMap().put(blockNo, blockDetail);
			adDetail.getPageMap().put(pageNo, pageDetail);
		}
		
		// Handling Same items repeated with different reg and sale price for different location (Other solution)
		//Group items by LIG and reg price
		//Pick the least price of the LIG
		//Keep only those items
		// HashMap<ProductKey,HashMap<RegPrice, List<PromoItemDTO>>
		
		// Handling Same items repeated with different reg and sale price for different location
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		// If an item appears more than once in a block
		// Pick most common sale price and pick items with those price only
		for (PageDetail pages : adDetail.getPageMap().values()) {
			for (BlockDetail blocks : pages.getBlockMap().values()) {
				boolean isItemsRepeated = false;
				List<MultiplePrice> salePrices = new ArrayList<MultiplePrice>();
				List<Integer> promoTypes = new ArrayList<Integer>();
				Set<ProductKey> itemSet = new HashSet<ProductKey>();

				logger.debug("1.blocks.getItems():" + blocks.getItems().toString());
				for (PromoItemDTO promoItemDTO : blocks.getItems()) {
					if (itemSet.contains(promoItemDTO.getProductKey())) {
						isItemsRepeated = true;
					} else {
						itemSet.add(promoItemDTO.getProductKey());
					}

					salePrices.add(promoItemDTO.getSaleInfo().getSalePrice());
					promoTypes.add(promoItemDTO.getSaleInfo().getPromoTypeId());
				}

				if (isItemsRepeated) {
					List<PromoItemDTO> distinctItems = new ArrayList<PromoItemDTO>();
					Set<ProductKey> distinctProductKey = new HashSet<ProductKey>();
					// Get most common sale price
					MultiplePrice commonSalePrice = (MultiplePrice) mostOccurrenceData.getMaxOccurance(salePrices);
					Integer commonPromoTypeId = (Integer) mostOccurrenceData.getMaxOccurance(promoTypes);

					// Handling when items in BOGO where sale prices varies between the items
					// If common promo type is BOGO
					if (commonPromoTypeId == PromoTypeLookup.BOGO.getPromoTypeId()) {
						// Pick only BOGO promotions
						for (PromoItemDTO promoItemDTO : blocks.getItems()) {
							if (promoItemDTO.getSaleInfo().getPromoTypeId() == commonPromoTypeId) {
								if (!distinctProductKey.contains(promoItemDTO.getProductKey())) {
									distinctItems.add(promoItemDTO);
									distinctProductKey.add(promoItemDTO.getProductKey());
								}
							}
						}
					} else {
						// Find items with the most common price
						for (PromoItemDTO promoItemDTO : blocks.getItems()) {
							if (promoItemDTO.getSaleInfo().getSalePrice().equals(commonSalePrice)) {
								distinctItems.add(promoItemDTO);
							}
						}
					}

					blocks.setItems(new ArrayList<PromoItemDTO>());
					// Replace the items in the block
					blocks.setItems(distinctItems);

					logger.debug("2.blocks.getItems():" + blocks.getItems().toString());
				}
			}
		}

		return adDetail;
	}

	public Set<Integer> getNonPerishableDepts(AdDetail adDetail) {
		HashSet<Integer> nonPerishableDepts = new HashSet<Integer>();
		List<String> perishablesDeptIds = Arrays.asList(PropertyManager.getProperty("PERISHABLES_DEPARTMENT_IDS").split("\\s*,\\s*"));

		logger.debug("perishablesDeptIds : " + perishablesDeptIds);

		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				for (PromoItemDTO promoItemDTO : blockDetail.getItems()) {
					if (!(perishablesDeptIds.contains(String.valueOf(promoItemDTO.getDeptId())))) {
						nonPerishableDepts.add(promoItemDTO.getDeptId());
					}
				}
			}
		}

		Set<Integer> nonPerishableDeptsList = new HashSet<Integer>(nonPerishableDepts);
		return nonPerishableDeptsList;
	}

	// HashMap<DeptId , HashMap <Block, List<PromoAdDetailDTO>>>
	public HashMap<PageBlockNoKey, List<PromoItemDTO>> findNPItemsCurrentlyOnPromo(AdDetail adDetail) {

		HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsInBlocks = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();

		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				actualAdItemsInBlocks.put(blockDetail.getPageBlockNoKey(), blockDetail.getItems());
			}
		}

		return actualAdItemsInBlocks;
	}
	
	public HashMap<PageBlockNoKey, List<PromoItemDTO>> convertToPageBlock(AdDetail adDetail) {

		HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsInBlocks = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();

		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				actualAdItemsInBlocks.put(blockDetail.getPageBlockNoKey(), blockDetail.getItems());
			}
		}

		return actualAdItemsInBlocks;
	}

	public HashMap<ItemKey, PRItemDTO> populateItemDataMap(HashMap<ProductKey, PRItemDTO> authroizedItemMap) {

		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		Set<Integer> itemSet = new HashSet<Integer>();

		for (Map.Entry<ProductKey, PRItemDTO> item : authroizedItemMap.entrySet()) {
			if (item.getKey().getProductLevelId() == Constants.ITEMLEVELID) {
				itemSet.add(item.getValue().getItemCode());
				itemDataMap.put(new ItemKey(item.getValue().getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR), item.getValue());
			}
		}
		return itemDataMap;
	}

	public HashMap<ProductKey, List<PromoItemDTO>> getPredictionOfActualAd(int locationLevelId, int locationId,
			List<RetailCalendarDTO> retailCalendarCache, HashMap<Integer, List<PRItemDTO>> ligAndItsMembers, String recWeekStartDate,
			AdDetail adDetail) throws GeneralException, OfferManagementException {
		HashMap<ProductKey, List<PromoItemDTO>> adItems = new HashMap<ProductKey, List<PromoItemDTO>>();

		// Convert to hashmap
		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				for (PromoItemDTO promoItemDTO : blockDetail.getItems()) {
					List<PromoItemDTO> tempList = new ArrayList<PromoItemDTO>();

					if (adItems.get(promoItemDTO.getProductKey()) != null) {
						tempList = adItems.get(promoItemDTO.getProductKey());
					}
					tempList.add(promoItemDTO);
					adItems.put(promoItemDTO.getProductKey(), tempList);
				}
			}
		}

		// group by category
		HashMap<ProductKey, List<PromoItemDTO>> itemByCategory = groupItemsByCategory(retailCalendarCache, recWeekStartDate, ligAndItsMembers,
				adItems);

		// update prediction
		HashMap<ProductKey, List<PromoItemDTO>> predResults = callPredictionEngine(retailCalendarCache, locationLevelId, locationId, recWeekStartDate,
				itemByCategory, true);
		
		return predResults;

	}

	public void updateAttributesOfActualAdItems(AdDetail adDetail, HashMap<ProductKey, PRItemDTO> authroizedItemMap,
			HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap,
			HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> displayDetailMap, String recWeekSaleDate) {

		//Handle items with different prices across location
		//Pick items with 
		
		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				for (PromoItemDTO itemDTO : blockDetail.getItems()) {
					// if it is lig member, update with lig info
					ProductKey productKey = null;
					if (itemDTO.getRetLirId() > 0) {
						productKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, itemDTO.getRetLirId());
					} else {
						productKey = new ProductKey(Constants.ITEMLEVELID, itemDTO.getProductKey().getProductId());
					}

					// Update reg price, list and deal cost
					if (authroizedItemMap.get(productKey) != null) {
//						itemDTO.setRegPrice(authroizedItemMap.get(productKey).getRecommendedRegPrice());
						itemDTO.setListCost(authroizedItemMap.get(productKey).getListCost());
						itemDTO.setDealCost(authroizedItemMap.get(productKey).getRecWeekDealCost());
//						itemDTO.setMinDealCost(authroizedItemMap.get(productKey).getMinDealCost());
						itemDTO.setFinalCost(findFinalCostOfActualAdItem(itemDTO));
						itemDTO.setItemName(authroizedItemMap.get(itemDTO.getProductKey()).getItemName());
						itemDTO.setRetLirName(authroizedItemMap.get(itemDTO.getProductKey()).getRetLirName());
						itemDTO.setNoOfHHRecommendedTo(authroizedItemMap.get(itemDTO.getProductKey()).getUniqueHHCount());
						itemDTO.setBrandId(authroizedItemMap.get(itemDTO.getProductKey()).getBrandId());
						
						logger.debug("1.1 " + authroizedItemMap.get(productKey).getRecommendedRegPrice() + ","
								+ authroizedItemMap.get(productKey).getListCost() + "," + authroizedItemMap.get(productKey).getRecWeekDealCost());
					}

					// Update sale price
//					if (saleDetailMap.get(productKey) != null) {
//						HashMap<String, PRItemSaleInfoDTO> weekSaleDetail = saleDetailMap.get(productKey);
//						if (weekSaleDetail.get(recWeekSaleDate) != null) {
//							itemDTO.setSaleInfo(weekSaleDetail.get(recWeekSaleDate));
//							logger.debug("1.2 " + weekSaleDetail.get(recWeekSaleDate));
//						}
//					}

					// Update display details
					if (displayDetailMap.get(productKey) != null) {
						HashMap<String, PRItemDisplayInfoDTO> weekDisplayDetail = displayDetailMap.get(productKey);
						if (weekDisplayDetail.get(recWeekSaleDate) != null) {
							itemDTO.setDisplayInfo(weekDisplayDetail.get(recWeekSaleDate));
						}
					}
				}
			}
		}
	}

	public HashMap<Integer, List<PageBlockNoKey>> findDeptBlockMapforActualAd(Set<Integer> departmentsNP, AdDetail adDetail) {
		HashMap<Integer, List<PageBlockNoKey>> deptBlockMap = new HashMap<Integer, List<PageBlockNoKey>>();
		List<PageBlockNoKey> pageBlockKeys = null;

		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				for (Integer deptId : blockDetail.getDepartments().keySet()) {
					if (departmentsNP.contains(deptId)) {
						pageBlockKeys = new ArrayList<PageBlockNoKey>();

						if (deptBlockMap.get(deptId) != null) {
							pageBlockKeys = deptBlockMap.get(deptId);
						}
						pageBlockKeys.add(blockDetail.getPageBlockNoKey());

						deptBlockMap.put(deptId, pageBlockKeys);
					}
				}
			}
		}

		return deptBlockMap;
	}

	public HashMap<PageBlockNoKey, List<PromoItemDTO>> groupActualAdItemsByPageBlock(
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsInBlocks,
			HashMap<ProductKey, ItemClassificationDTO> itemClassificationFullData) {

		logger.info("groupActualAdInfoByPageByBlock() : Start ....");

		HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsInBlockLIGNonLigSummary = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();
		// Aggregate at LIG level

		for (Map.Entry<PageBlockNoKey, List<PromoItemDTO>> blockAdItems : actualAdItemsInBlocks.entrySet()) {
			PageBlockNoKey pageBlockNoKey = blockAdItems.getKey();

			HashMap<ProductKey, PromoItemDTO> finalActualAdItemsLIGNonLIG = new HashMap<ProductKey, PromoItemDTO>();

			for (PromoItemDTO blockItem : blockAdItems.getValue()) {
				logger.debug("Duplicate Ad item : Block no " + pageBlockNoKey.toString() + " Block no inside items : "
						+ (blockItem.getAdInfo() != null ? blockItem.getAdInfo().getAdBlockNo() : "AdInfoNotPresent"));
				// only aggregate with lig member level items
				if (blockItem.getProductKey().getProductLevelId() == Constants.ITEMLEVELID && blockItem.getRetLirId() > 0) {
					ProductKey ligKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, blockItem.getRetLirId());
					PromoItemDTO lig = new PromoItemDTO();

					if (finalActualAdItemsLIGNonLIG.get(ligKey) != null) {
						// Get LIG item and add margin to it
						lig = finalActualAdItemsLIGNonLIG.get(ligKey);
						lig.setPredMar(lig.getPredMar() + blockItem.getPredMar());
						lig.setPredRev(lig.getPredRev() + blockItem.getPredRev());
						
						if (lig.getPredMov() == null) {
							lig.setPredMov(new Double(0.0));
						}
						if (blockItem.getPredMov() == null) {
							blockItem.setPredMov(new Double(0.0));
						}
						lig.setPredMov(lig.getPredMov() + blockItem.getPredMov());
						
						
						lig.setPredMarReg(lig.getPredMarReg() + blockItem.getPredMarReg());
						lig.setPredRevReg(lig.getPredRevReg() + blockItem.getPredRevReg());
						lig.setPredMovReg(lig.getPredMovReg() + blockItem.getPredMovReg());
						
						if (itemClassificationFullData.get(ligKey) != null) {
							lig.setNoOfHHRecommendedTo(itemClassificationFullData.get(ligKey).getUniqueHouseHoldCount());
						}
						lig.setAdditionalDetailForLog("Final Actual Ad Items");
					} else {
						copyPromoItem(ligKey.getProductLevelId(), ligKey.getProductId(), blockItem.getUpc(),
								(blockItem.getRetLirName() != null ? blockItem.getRetLirName() : blockItem.getItemName()), blockItem, lig);
						lig.setPredMar(blockItem.getPredMar());
						lig.setPredRev(blockItem.getPredRev());
						lig.setPredMov(blockItem.getPredMov());
						
						lig.setPredMarReg(lig.getPredMarReg() + blockItem.getPredMarReg());
						lig.setPredRevReg(lig.getPredRevReg() + blockItem.getPredRevReg());
						lig.setPredMovReg(blockItem.getPredMovReg());
						
						if (itemClassificationFullData.get(ligKey) != null) {
							lig.setNoOfHHRecommendedTo(itemClassificationFullData.get(ligKey).getUniqueHouseHoldCount());
						}
						lig.setAdditionalDetailForLog("Final Actual Ad Items");
						finalActualAdItemsLIGNonLIG.put(ligKey, lig);
					}
					// authroizedItemMapDeptWise
					// Non LIG item
				} else if (blockItem.getProductKey().getProductLevelId() == Constants.ITEMLEVELID && blockItem.getRetLirId() <= 0) {
					// Add existing NON LIG items without any change
					if (itemClassificationFullData.get(blockItem.getProductKey()) != null) {
						blockItem.setNoOfHHRecommendedTo(itemClassificationFullData.get(blockItem.getProductKey()).getUniqueHouseHoldCount());
					}
					blockItem.setAdditionalDetailForLog("Final Actual Ad Items");
					finalActualAdItemsLIGNonLIG.put(blockItem.getProductKey(), blockItem);
				}
			}

			List<PromoItemDTO> actualAdItemsforLog = new ArrayList<PromoItemDTO>();
			List<PromoItemDTO> itemsInBlock = null;
			for (Map.Entry<ProductKey, PromoItemDTO> pItem : finalActualAdItemsLIGNonLIG.entrySet()) {
				PromoItemDTO promoItemDTO = pItem.getValue();

				PageBlockNoKey tempPageBlockKey = new PageBlockNoKey(promoItemDTO.getAdInfo().getAdPageNo(), promoItemDTO.getAdInfo().getAdBlockNo());
				itemsInBlock = new ArrayList<PromoItemDTO>();

				if (actualAdItemsInBlockLIGNonLigSummary.get(tempPageBlockKey) != null) {
					itemsInBlock = actualAdItemsInBlockLIGNonLigSummary.get(tempPageBlockKey);
				}

				itemsInBlock.add(promoItemDTO);

				actualAdItemsInBlockLIGNonLigSummary.put(tempPageBlockKey, itemsInBlock);
				actualAdItemsforLog.add(promoItemDTO);
			}

			itemLogService.writeToCSVFile(itemAnalysisLogPath, actualAdItemsforLog);

		}

		logger.info("groupActualAdInfoByPageByBlock() : Completed ....");

		return actualAdItemsInBlockLIGNonLigSummary;
	}
	

	public void updateHHCountInAuthorizedItemMap(HashMap<ProductKey, ItemClassificationDTO> itemClassificationFullData,
			HashMap<ProductKey, PRItemDTO> authroizedItemMap) {
		List<ProductKey> productKeyNotPresentInItemClassification = new ArrayList<ProductKey>();

		for (Map.Entry<ProductKey, PRItemDTO> entry : authroizedItemMap.entrySet()) {
			if (itemClassificationFullData.get(entry.getKey()) != null
					&& itemClassificationFullData.get(entry.getKey()).getUniqueHouseHoldCount() != null) {
				entry.getValue().setUniqueHHCount(itemClassificationFullData.get(entry.getKey()).getUniqueHouseHoldCount());
			} else {
				productKeyNotPresentInItemClassification.add(entry.getKey());

			}
		}
	}

	public void updatePredictionsOfActualAd(int locationLevelId, int locationId, HashMap<ProductKey, PRItemDTO> authroizedItemMap, AdDetail adDetail,
			HashMap<ProductKey, List<PromoItemDTO>> actualAdPredResults, HashMap<Integer, List<PRItemDTO>> ligAndItsMembers) {
		List<PromoItemDTO> itemsForDebug = new ArrayList<PromoItemDTO>();

		for (PageDetail pageDetail : adDetail.getPageMap().values()) {
			// Each page
			for (BlockDetail blockDetail : pageDetail.getBlockMap().values()) {
				// Each block
				for (PromoItemDTO itemDTO : blockDetail.getItems()) {
					itemDTO = findLigOrNonLigLevelMetrics(locationLevelId, locationId, ligAndItsMembers, actualAdPredResults, itemDTO);
					itemDTO.setAdditionalDetailForLog("Actual Ad Prediction with other metrics");
					itemsForDebug.add(itemDTO);
				}
			}
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, itemsForDebug);
		
		
	}

	public HashMap<Integer, ArrayList<PromoAdSaleInfo>> getLastXWeeksAdDetail(Connection conn, RetailCalendarDTO recommendedWeek, int previousXWeeks,
			int chainId, int locationLevelId, int locationId, int productLevelId, String productIdList, List<Integer> allStores,
			HashMap<ProductKey, PRItemDTO> authroizedItemMap, boolean isDebugMode) throws GeneralException {

		if (isDebugMode) {
			productLevelId = Constants.ITEMLEVELID;
			productIdList = authorizedMapToString(authroizedItemMap);
		}

		HashMap<Integer, ArrayList<PromoAdSaleInfo>> saleAdDetailsTemp = new PromotionEngineDAO().getPastAdDetails(conn, productLevelId,
				productIdList, chainId, locationLevelId, locationId, recommendedWeek.getStartDate(), previousXWeeks, allStores);

		return saleAdDetailsTemp;
	}

	// TODO:: NU:: this function requires refactoring
	public void getMinXPricePoints(HashMap<Integer, ArrayList<PromoAdSaleInfo>> saleAdDetails, HashMap<Integer, List<Double>> minXSalePointsAllItems,
			Set<Integer> itemsOnBOGOinLastXWeeks, HashMap<Integer, List<MultiplePrice>> minXMultiplePrices) throws GeneralException {

		// HashMap<ItemCode, List<Min X SalePrice points>>
		HashMap<Integer, List<Double>> tempMinXSalePointsAllItems = new HashMap<Integer, List<Double>>();

		HashMap<Integer, List<MultiplePrice>> tempMinXMultiplePrices = new HashMap<Integer, List<MultiplePrice>>();

		try {
			logger.debug("getMinXPricePoints() : Start (For Last X Weeks picking Sale price points)...");

			// For each item
			for (Map.Entry<Integer, ArrayList<PromoAdSaleInfo>> saleAdDetailEntry : saleAdDetails.entrySet()) {

				// 1. get adpage 1 , start date as key and sale prices
				int itemCode = saleAdDetailEntry.getKey();
				// HashMap<adStartDate, Multipleprice> //Only for Page 1
				HashMap<String, List<Double>> salePricesOnePageOneItem = new HashMap<String, List<Double>>();
				HashMap<String, List<MultiplePrice>> multipleSalePricesOnePageOneItem = new HashMap<String, List<MultiplePrice>>();

				
				// Find all previous multiple price, sale price and BOGO
				// Group by each ad and sale percentage
				for (PromoAdSaleInfo promoAdSaleInfo : saleAdDetailEntry.getValue()) {
					if (promoAdSaleInfo != null && promoAdSaleInfo.getAdPage() == 1 && promoAdSaleInfo.getAdStartDate() != null
							&& promoAdSaleInfo.getSalePricePoint() != null) {
						List<Double> salePricePoints = new ArrayList<Double>();
						List<MultiplePrice> multipleSalePricePoints = new ArrayList<MultiplePrice>();

						if (promoAdSaleInfo.getPromoTypeId() == PromoTypeLookup.STANDARD.getPromoTypeId()) {

							if (promoAdSaleInfo.getSalePricePoint().multiple > 1) {

								if (multipleSalePricesOnePageOneItem.get(promoAdSaleInfo.getAdStartDate()) != null) {
									multipleSalePricePoints = multipleSalePricesOnePageOneItem.get(promoAdSaleInfo.getAdStartDate());
								}
								multipleSalePricePoints.add(promoAdSaleInfo.getSalePricePoint());

								multipleSalePricesOnePageOneItem.put(promoAdSaleInfo.getAdStartDate(), multipleSalePricePoints);

							} else {
								if (salePricesOnePageOneItem.get(promoAdSaleInfo.getAdStartDate()) != null) {
									salePricePoints = salePricesOnePageOneItem.get(promoAdSaleInfo.getAdStartDate());
								}
								salePricePoints.add(PRCommonUtil.getSaleDiscountPCT(promoAdSaleInfo.getRegPricePoint(),
										promoAdSaleInfo.getSalePricePoint(), true));

								salePricesOnePageOneItem.put(promoAdSaleInfo.getAdStartDate(), salePricePoints);
							}

						} else if (promoAdSaleInfo.getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()) {
							itemsOnBOGOinLastXWeeks.add(saleAdDetailEntry.getKey());
						}
					}
				}

				logger.debug("itemcode:" + itemCode + "salePricesOnePageOneItem:" + salePricesOnePageOneItem);

				// 2. get the max sale discount percentage in each week
				// HashMap<adStartDate, Multipleprice> //Only for Page 1
				Set<Double> tempMostCommonSaleDiscountPCTOnePageOneItem = new HashSet<Double>();

				for (Map.Entry<String, List<Double>> salePricePointsEntry : salePricesOnePageOneItem.entrySet()) {
					tempMostCommonSaleDiscountPCTOnePageOneItem
							.add(salePricePointsEntry.getValue().stream().mapToDouble((x) -> x).max().getAsDouble());
				}

				// 3. get the min X sale price points
				List<Double> mostCommonSaleDiscountPCTOnePageOneItem = new ArrayList<Double>(tempMostCommonSaleDiscountPCTOnePageOneItem);
				Collections.sort(mostCommonSaleDiscountPCTOnePageOneItem);

				logger.debug("itemcode:" + itemCode + "mostCommonSaleDiscountPCTOnePageOneItem:" + mostCommonSaleDiscountPCTOnePageOneItem);

				List<Double> minXSalePricePoints = new ArrayList<Double>();
				if (mostCommonSaleDiscountPCTOnePageOneItem.size() >= maxPromoCombination) {
					minXSalePricePoints.addAll(mostCommonSaleDiscountPCTOnePageOneItem.subList(0, maxPromoCombination - 1));
				} else {
					minXSalePricePoints.addAll(mostCommonSaleDiscountPCTOnePageOneItem);
				}

				tempMinXSalePointsAllItems.put(itemCode, minXSalePricePoints);

				HashMap<String, MultiplePrice> mostCommonMultipleSalePricesOnePageOneItem = new HashMap<String, MultiplePrice>();

				for (Map.Entry<String, List<MultiplePrice>> salePricePointsEntry : multipleSalePricesOnePageOneItem.entrySet()) {

					mostCommonMultipleSalePricesOnePageOneItem.put(salePricePointsEntry.getKey(),
							getMostOccurringMultiplePrice(salePricePointsEntry.getValue()));
				}

				// 3. get the min X sale price points
				List<MultiplePrice> minXMutlipleSalePricePoints = getXMinSalePricePoints(
						new ArrayList<MultiplePrice>(mostCommonMultipleSalePricesOnePageOneItem.values()));
				tempMinXMultiplePrices.put(itemCode, minXMutlipleSalePricePoints);

				logger.debug("itemcode:" + itemCode + "minXSalePricePoints:" + minXSalePricePoints.toString());
			}
			logger.debug("getMinXPricePoints() : End...");
		} catch (Exception e) {
			logger.error("getMinXPricePoints() : Exception occurred : " + e.getMessage());
			throw new GeneralException(e.getMessage());
		}
		minXSalePointsAllItems.putAll(tempMinXSalePointsAllItems);
		minXMultiplePrices.putAll(tempMinXMultiplePrices);
	}
	
	public HashMap<Integer, Set<SalePriceKey>> getPastSalePrices(HashMap<Integer, ArrayList<PromoAdSaleInfo>> saleAdDetails) {
		HashMap<Integer, Set<SalePriceKey>> pastSalePrices = new HashMap<Integer, Set<SalePriceKey>>();

		for (Map.Entry<Integer, ArrayList<PromoAdSaleInfo>> saleAdDetailEntry : saleAdDetails.entrySet()) {
			int itemCode = saleAdDetailEntry.getKey();
			for (PromoAdSaleInfo promoAdSaleInfo : saleAdDetailEntry.getValue()) {
				if (promoAdSaleInfo != null && promoAdSaleInfo.getAdPage() == 1 && promoAdSaleInfo.getAdStartDate() != null
						&& promoAdSaleInfo.getSalePricePoint() != null) {

					Set<SalePriceKey> salePriceSet = new HashSet<SalePriceKey>();
					if (pastSalePrices.get(itemCode) != null) {
						salePriceSet = pastSalePrices.get(itemCode);
					}

					SalePriceKey salePriceKey = new SalePriceKey();
					salePriceKey.setRegPrice(promoAdSaleInfo.getRegPricePoint());
					salePriceKey.setSalePrice(promoAdSaleInfo.getSalePricePoint());
					salePriceKey.setPromoTypeId(promoAdSaleInfo.getPromoTypeId());

					salePriceSet.add(salePriceKey);
					pastSalePrices.put(itemCode, salePriceSet);
				}
			}

			logger.debug("Past Distinct sale prices of itemcode:" + itemCode + pastSalePrices.get(itemCode));
		}

		return pastSalePrices;
	}

	private List<MultiplePrice> getXMinSalePricePoints(List<MultiplePrice> inputSalePricesList) {

		List<MultiplePrice> minXSalePricePoints = new ArrayList<MultiplePrice>();

		if (inputSalePricesList != null && inputSalePricesList.size() > 0) {
			HashSet<MultiplePrice> uniqueSalePrices = new HashSet<MultiplePrice>();
			for (MultiplePrice salePrice : inputSalePricesList) {
				uniqueSalePrices.add(salePrice);
			}

			List<MultiplePrice> uniqueSalePricesList = new ArrayList<MultiplePrice>(uniqueSalePrices);

			Collections.sort(uniqueSalePricesList, new Comparator<MultiplePrice>() {

				@Override
				public int compare(MultiplePrice multiplePrice1, MultiplePrice multiplePrice2) {
					// TODO Auto-generated method stub
					if (multiplePrice1.price < multiplePrice2.price) {
						return -1;
					} else if (multiplePrice1.price > multiplePrice2.price) {
						return 1;
					}
					return 0;
				}
			});

			minXSalePricePoints = uniqueSalePricesList;
			if (uniqueSalePricesList.size() > maxPromoCombination) {
				minXSalePricePoints = uniqueSalePricesList.subList(0, maxPromoCombination);
			}
		}

		return minXSalePricePoints;
	}

	//TODO:: Refactor this function
	public void getPastSaleMarginStats(HashMap<Integer, ArrayList<PromoAdSaleInfo>> adHistory, HashMap<ProductKey, PRItemDTO> authroizedItemMap,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfItemOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfCategoryOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> minDealCostOfItemOnAd,
			HashMap<Integer, List<PRItemDTO>> ligAndItsMember) {
		
		HashMap<ProductKey, HashMap<PromoTypeLookup, List<Double>>> tempCatLevelMarginPCT = new HashMap<ProductKey, HashMap<PromoTypeLookup, List<Double>>>();
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		
		// Item history
		for (Map.Entry<Integer, ArrayList<PromoAdSaleInfo>> adHistoryEntry : adHistory.entrySet()) {
			
			ProductKey itemKey = new ProductKey(Constants.ITEMLEVELID, adHistoryEntry.getKey());
			HashMap<PromoTypeLookup, Double> minDealCostMap = new HashMap<PromoTypeLookup, Double>();
			HashMap<PromoTypeLookup, Double> avgMarPCTMap = new HashMap<PromoTypeLookup, Double>();
			
			ProductKey categoryKey = null;
			if (authroizedItemMap.get(itemKey) != null) {
				PRItemDTO itemDTO = authroizedItemMap.get(itemKey);
				categoryKey = new ProductKey(Constants.CATEGORYLEVELID, itemDTO.getCategoryProductId());

				// Group by promo type
				HashMap<Integer, List<PromoAdSaleInfo>> groupByPromo = (HashMap<Integer, List<PromoAdSaleInfo>>) adHistoryEntry.getValue().stream()
						.collect(Collectors.groupingBy(PromoAdSaleInfo::getPromoTypeId));

				// Find stats for each promo type
				for (Map.Entry<Integer, List<PromoAdSaleInfo>> groupByPromoEntry : groupByPromo.entrySet()) {
					PromoTypeLookup promoType = PromoTypeLookup.get(groupByPromoEntry.getKey());

					double avgMarPct = 0, minDealCost = 0;
					// Find average margin percentage on first page for each promotion type
					OptionalDouble tempAvgMarPct = groupByPromoEntry.getValue().stream()
							.filter(p -> p.getAdPage() == 1 && p.getUnitSalePrice() > 0 && p.getDealCost() > 0).mapToDouble(p -> p.getSaleMarPCT())
							.average();

					avgMarPct = (tempAvgMarPct.isPresent() ? tempAvgMarPct.getAsDouble() : 0);
					avgMarPCTMap.put(promoType, avgMarPct);

					// Find minimum deal cost on ad for each promotion type
					OptionalDouble tempMinDealCost = groupByPromoEntry.getValue().stream().filter(p -> p.getDealCost() > 0)
							.mapToDouble(p -> p.getDealCost()).min();

					minDealCost = (tempMinDealCost.isPresent() ? tempMinDealCost.getAsDouble() : 0);
					minDealCostMap.put(promoType, minDealCost);

					// Keep data by category
					if (categoryKey != null && avgMarPct > 0) {
						HashMap<PromoTypeLookup, List<Double>> catMarginsForEachPromo = new HashMap<PromoTypeLookup, List<Double>>();
						List<Double> catMargins = new ArrayList<Double>();

						if (tempCatLevelMarginPCT.get(categoryKey) != null) {
							catMarginsForEachPromo = tempCatLevelMarginPCT.get(categoryKey);
						}

						if (catMarginsForEachPromo.get(promoType) != null) {
							catMargins = catMarginsForEachPromo.get(promoType);
						}

						catMargins.add(avgMarPct);
						catMarginsForEachPromo.put(promoType, catMargins);
						tempCatLevelMarginPCT.put(categoryKey, catMarginsForEachPromo);
					}

					logger.debug("ItemCode:" + itemKey + ",categoryKey:" + categoryKey + ",promoType:" + promoType + ",avgMarPct:" + avgMarPct);
					logger.debug("ItemCode:" + itemKey + ",categoryKey:" + categoryKey + ",promoType:" + promoType + ",minDealCost:" + minDealCost);
				}
			}
			
			minDealCostOfItemOnAd.put(itemKey, minDealCostMap);
			avgSaleMarPCTOfItemOnFirstPage.put(itemKey, avgMarPCTMap);
		}			
		
		
		// Find lig level data
		for (Map.Entry<ProductKey, PRItemDTO> authorizedItem : authroizedItemMap.entrySet()) {
			if (authorizedItem.getKey().getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
//				PRItemDTO ligItem = authorizedItem.getValue();
				ProductKey ligKey = authorizedItem.getKey();
				
				// get lig members
				List<PRItemDTO> ligMembers = ligAndItsMember.get(authorizedItem.getKey().getProductId());
				
				HashMap<PromoTypeLookup, List<Double>> avgSaleMarPCTOfLigMemOnFirstPage = new HashMap<PromoTypeLookup, List<Double>>();
				HashMap<PromoTypeLookup, List<Double>> minDealCostOfLigMem = new HashMap<PromoTypeLookup, List<Double>>();
				
				//Get all items. Convert to promotion type and its average margin percentage of all item
				for(PRItemDTO ligMember : ligMembers) {
					ProductKey ligMemberKey = new ProductKey(Constants.ITEMLEVELID, ligMember.getItemCode());
					if(avgSaleMarPCTOfItemOnFirstPage.get(ligMemberKey) != null) {
						HashMap<PromoTypeLookup, Double> ligMemberAvgMarPCT =  avgSaleMarPCTOfItemOnFirstPage.get(ligMemberKey);
						
						 for(Map.Entry<PromoTypeLookup, Double> avgMarPCTEntry : ligMemberAvgMarPCT.entrySet()) {
							 
							 List<Double> tempAvgMarPCT = new ArrayList<Double>();
							 if(avgSaleMarPCTOfLigMemOnFirstPage.get(avgMarPCTEntry.getKey()) != null) {
								 tempAvgMarPCT = avgSaleMarPCTOfLigMemOnFirstPage.get(avgMarPCTEntry.getKey());
							 }
							 
							 tempAvgMarPCT.add(avgMarPCTEntry.getValue());
							 
							 avgSaleMarPCTOfLigMemOnFirstPage.put(avgMarPCTEntry.getKey(), tempAvgMarPCT);
						 }
					}
					
					if(minDealCostOfItemOnAd.get(ligMemberKey) != null) {
						HashMap<PromoTypeLookup, Double> ligMemberMinDealCost =  minDealCostOfItemOnAd.get(ligMemberKey);
						
						 for(Map.Entry<PromoTypeLookup, Double> minDealCostEntry : ligMemberMinDealCost.entrySet()) {
							 
							 List<Double> tempMinDealCost = new ArrayList<Double>();
							 if(minDealCostOfLigMem.get(minDealCostEntry.getKey()) != null) {
								 tempMinDealCost = minDealCostOfLigMem.get(minDealCostEntry.getKey());
							 }
							 
							 tempMinDealCost.add(minDealCostEntry.getValue());
							 
							 minDealCostOfLigMem.put(minDealCostEntry.getKey(), tempMinDealCost);
						 }
					}
				}
				
				//Promotion type and all its average margin percentage of lig members
				
				 for(Map.Entry<PromoTypeLookup, List<Double>> avgMarPCTEntry : avgSaleMarPCTOfLigMemOnFirstPage.entrySet()) {
					 
					 HashMap<PromoTypeLookup, Double> tempMap = new HashMap<PromoTypeLookup, Double>();
					 
					 if(avgSaleMarPCTOfItemOnFirstPage.get(ligKey) != null) {
						 tempMap = avgSaleMarPCTOfItemOnFirstPage.get(ligKey);
					 }
					 
					 Double mostCommonData = (Double) mostOccurrenceData.getMaxOccurance(avgMarPCTEntry.getValue());
					 
					 tempMap.put(avgMarPCTEntry.getKey(), mostCommonData);
					 
					 avgSaleMarPCTOfItemOnFirstPage.put(ligKey, tempMap);
					 
					 logger.debug("LIG:" + ligKey + ",promoType:" + avgMarPCTEntry.getKey() + ",avgMarPCT:" + mostCommonData);
				 }
				 
				 
				for (Map.Entry<PromoTypeLookup, List<Double>> minDealCostEntry : minDealCostOfLigMem.entrySet()) {

					HashMap<PromoTypeLookup, Double> tempMap = new HashMap<PromoTypeLookup, Double>();

					if (minDealCostOfItemOnAd.get(ligKey) != null) {
						tempMap = minDealCostOfItemOnAd.get(ligKey);
					}

					Double mostCommonData = (Double) mostOccurrenceData.getMaxOccurance(minDealCostEntry.getValue());

					tempMap.put(minDealCostEntry.getKey(), mostCommonData);

					minDealCostOfItemOnAd.put(ligKey, tempMap);
					
					logger.debug("LIG:" + ligKey + ",promoType:" + minDealCostEntry.getKey() + ",minDealCost:" + mostCommonData);
				}
				
			}
		}
		
		// Find category level average margin percentage on first page
		for (Map.Entry<ProductKey, HashMap<PromoTypeLookup, List<Double>>> catLevelMarginPCTEntry : tempCatLevelMarginPCT.entrySet()) {

			HashMap<PromoTypeLookup, Double> catMarginsForEachPromo = new HashMap<PromoTypeLookup, Double>();
			for (Map.Entry<PromoTypeLookup, List<Double>> catLevelPromoTypePCTEntry : catLevelMarginPCTEntry.getValue().entrySet()) {
				double avgMarPct = catLevelPromoTypePCTEntry.getValue().stream().mapToDouble((x) -> x).average().getAsDouble();
				catMarginsForEachPromo.put(catLevelPromoTypePCTEntry.getKey(), avgMarPct);
				logger.debug("Category Key:" + catLevelMarginPCTEntry.getKey() + ",promo:" + catLevelPromoTypePCTEntry.getKey() + ", avg mar pct:" + avgMarPct);
			}

			avgSaleMarPCTOfCategoryOnFirstPage.put(catLevelMarginPCTEntry.getKey(), catMarginsForEachPromo);
		}
		
	}

	public HashMap<Long, PromoProductGroup> getPPGBaseData(Connection conn, String deparmentIdList) throws GeneralException {
		HashMap<Long, PromoProductGroup> ppgGroupDetails = new PromotionEngineDAO().getPPGGroupDetails(conn, deparmentIdList);

		List<PromoItemDTO> ppgListForLog = new ArrayList<PromoItemDTO>();

		// for debugging log only
		for (Map.Entry<Long, PromoProductGroup> ppgGroup : ppgGroupDetails.entrySet()) {
			for (PromoItemDTO ppgItem : ppgGroup.getValue().getItems().values()) {
				ppgItem.setAdditionalDetailForLog("PPG Items");
				ppgListForLog.add(ppgItem);
			}
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, ppgListForLog);

		return ppgGroupDetails;
	}

	public HashMap<Long, List<PromoProductGroup>> getPPGPromoCombinations(int pageNo, HashMap<Long, PromoProductGroup> ppgGroupDetails,
			HashMap<Integer, List<PageBlockNoKey>> deptBlockMap, HashMap<Integer, List<PRItemDTO>> ligAndItsMember,
			HashMap<ProductKey, PRItemDTO> authroizedItemMap, HashMap<ProductKey, ProductDTO> deptDetails,
			HashMap<Integer, Set<SalePriceKey>>  pastSalePrices,  
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfItemOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfCategoryOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> minDealCostOfItemOnAd,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary) {

		// PPGGROUP and its different price combinations
		HashMap<Long, List<PromoProductGroup>> ppgItemsPromoCombinationsFull = new HashMap<Long, List<PromoProductGroup>>();

		HashMap<ProductKey, List<PromoItemDTO>> ppgLeadPromoItems = new HashMap<ProductKey, List<PromoItemDTO>>();

		// Get all lead items
		for (Map.Entry<Long, PromoProductGroup> tempPPGGroup : ppgGroupDetails.entrySet()) {
			for (PromoItemDTO ppgItem : tempPPGGroup.getValue().getItems().values()) {
				// copy prices
				populatePromoItemDetails(authroizedItemMap, ppgItem, deptDetails);

				if (ppgItem.isPPGLeadItem()) {
					// distinct lead items
					if (ppgLeadPromoItems.get(ppgItem.getProductKey()) == null) {
						List<PromoItemDTO> tempList = new ArrayList<PromoItemDTO>();
						tempList.add(ppgItem);
						ppgLeadPromoItems.put(ppgItem.getProductKey(), tempList);
					}
				}
			}
		}

		// get the promo combinations for lead items
		HashMap<ProductKey, List<PromoItemDTO>> ppgLeadItemsPromoCombination = findAllPromotionCombination(ppgLeadPromoItems, deptBlockMap, pageNo,
				ligAndItsMember, true, pastSalePrices,authroizedItemMap, avgSaleMarPCTOfItemOnFirstPage, avgSaleMarPCTOfCategoryOnFirstPage, minDealCostOfItemOnAd,
				actualAdItemsLIGNonLigSummary);

		// impose the promo combination of lead items to the PPG group member items
		for (Map.Entry<Long, PromoProductGroup> ppgGroup : ppgGroupDetails.entrySet()) {
			Long ppgGroupId = ppgGroup.getKey();
			ProductKey leadItem = null;
			PromoProductGroup promoProductGroup = ppgGroup.getValue();

			for (PromoItemDTO ppgItem : ppgGroup.getValue().getItems().values()) {
				if (ppgItem.isPPGLeadItem()) {
					leadItem = ppgItem.getProductKey();
				}
			}

			// if lead items is found then only process further
			// Also note that this lead item can belong to many groups
			if (leadItem != null && ppgLeadItemsPromoCombination.get(leadItem) != null) {
				List<PromoItemDTO> leadPPGItemPromoCombos = ppgLeadItemsPromoCombination.get(leadItem);

				List<PromoProductGroup> promoProductGroups = new ArrayList<PromoProductGroup>();

				// Loop each lead item promo combinations
				for (PromoItemDTO leadPromoItemCombo : leadPPGItemPromoCombos) {
					PromoProductGroup tempPromoProductGroup = new PromoProductGroup();
					HashMap<ProductKey, PromoItemDTO> ppgItems = new HashMap<ProductKey, PromoItemDTO>();

					// Copy attributes
					copyPromoProductGroup(promoProductGroup, tempPromoProductGroup);

					// Loop all items in the group
					for (PromoItemDTO ppgItem : ppgGroup.getValue().getItems().values()) {
						PromoItemDTO tempPromoItemDTO = new PromoItemDTO();

						PRItemAdInfoDTO adInfoDTO = new PRItemAdInfoDTO();
						PRItemDisplayInfoDTO displayInfoDTO = new PRItemDisplayInfoDTO();
						PRItemSaleInfoDTO saleInfoDTO = new PRItemSaleInfoDTO();

						copyPromoItem(ppgItem.getProductKey().getProductLevelId(), ppgItem.getProductKey().getProductId(), ppgItem.getUpc(),
								ppgItem.getProductKey().getProductLevelId() != 1 ? ppgItem.getItemName() : ppgItem.getRetLirName(), ppgItem,
								tempPromoItemDTO);

						saleInfoDTO.setPromoTypeId(leadPromoItemCombo.getSaleInfo().getPromoTypeId());
						// setting up the sale price point
						if(leadPromoItemCombo.getSaleInfo().getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()) {
							saleInfoDTO.setSalePrice(new MultiplePrice(2, ppgItem.getRegPrice().price));
						} else {
							saleInfoDTO.setSalePrice(leadPromoItemCombo.getSaleInfo().getSalePrice());
						}
						
						tempPromoItemDTO.setSaleInfo(saleInfoDTO);

						// no displays
						displayInfoDTO.setDisplayTypeLookup(DisplayTypeLookup.NONE);
						tempPromoItemDTO.setDisplayInfo(displayInfoDTO);

						// Updating Ad info
						adInfoDTO.setAdBlockNo(leadPromoItemCombo.getAdInfo().getAdBlockNo());
						adInfoDTO.setAdPageNo(leadPromoItemCombo.getAdInfo().getAdPageNo());
						tempPromoItemDTO.setAdInfo(adInfoDTO);

						boolean ignoreItem = false;
						// if no cost (only list cost as deal cost is derived later) and standard promotion ignore it, as we
						// can't come with sale price
						if (tempPromoItemDTO.getListCost() == null
								&& saleInfoDTO.getPromoTypeId() == PromoTypeLookup.STANDARD.getPromoTypeId()) {
							ignoreItem = true;
						} else if (saleInfoDTO.getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()
								&& tempPromoItemDTO.getRegPrice() != null && tempPromoItemDTO.getRegPrice().multiple > 1) {
							// if it is bogo, but current price is in multiples
							// already
							ignoreItem = true;
						}

						// Set derived deal cost
//						tempPromoItemDTO.setDerivedDealCost(findDerivedDealCost(tempPromoItemDTO, catLevelMarginPCT));

						tempPromoItemDTO.setFinalCost(findFinalCostOfItem(tempPromoItemDTO, avgSaleMarPCTOfItemOnFirstPage,
								avgSaleMarPCTOfCategoryOnFirstPage, minDealCostOfItemOnAd, actualAdItemsLIGNonLigSummary));

						if (!ignoreItem) {
							ppgItems.put(tempPromoItemDTO.getProductKey(), tempPromoItemDTO);
						}
					}

					tempPromoProductGroup.setItems(ppgItems);

					if (ppgItemsPromoCombinationsFull.get(ppgGroupId) != null) {
						promoProductGroups = ppgItemsPromoCombinationsFull.get(ppgGroupId);
					}

					promoProductGroups.add(tempPromoProductGroup);
					ppgItemsPromoCombinationsFull.put(ppgGroupId, promoProductGroups);
				}

			}
		}

		// for logging purpose
		List<PromoItemDTO> logPPGPromoCombinations = new ArrayList<PromoItemDTO>();
		for (Map.Entry<Long, List<PromoProductGroup>> ppgPromo : ppgItemsPromoCombinationsFull.entrySet()) {
			for (PromoProductGroup ppg : ppgPromo.getValue()) {
				for (PromoItemDTO tempItem : ppg.getItems().values()) {
					tempItem.setAdditionalDetailForLog("PPG Promo Combination");
					logPPGPromoCombinations.add(tempItem);
				}
			}
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, logPPGPromoCombinations);

		return ppgItemsPromoCombinationsFull;
	}

	public HashMap<ProductKey, List<PromoItemDTO>> mergePPGItemsWithCandidateItems(
			HashMap<ProductKey, List<PromoItemDTO>> candidateItemMapWithPromoCombinations,
			HashMap<Long, List<PromoProductGroup>> ppgItemsPromoCombinationsFull) {

		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMapWithPromoCombinationsWithPPG = new HashMap<ProductKey, List<PromoItemDTO>>();

		for (Map.Entry<ProductKey, List<PromoItemDTO>> indItemPromoCombination : candidateItemMapWithPromoCombinations.entrySet()) {
			List<PromoItemDTO> itemList = new ArrayList<PromoItemDTO>();
			itemList.addAll(indItemPromoCombination.getValue());
			candidateItemMapWithPromoCombinationsWithPPG.put(indItemPromoCombination.getKey(), itemList);

		}

		for (List<PromoProductGroup> promoProductGroups : ppgItemsPromoCombinationsFull.values()) {

			for (PromoProductGroup promoProductGroup : promoProductGroups) {
				for (PromoItemDTO ppgItem : promoProductGroup.getItems().values()) {
					List<PromoItemDTO> candidateItemList = new ArrayList<PromoItemDTO>();
					if (candidateItemMapWithPromoCombinationsWithPPG.get(ppgItem.getProductKey()) != null) {
						candidateItemList = candidateItemMapWithPromoCombinationsWithPPG.get(ppgItem.getProductKey());
					}

					candidateItemList.add(ppgItem);
					candidateItemMapWithPromoCombinationsWithPPG.put(ppgItem.getProductKey(), candidateItemList);
				}
			}
		}

		return candidateItemMapWithPromoCombinationsWithPPG;
	}

	private String authorizedMapToString(HashMap<ProductKey, PRItemDTO> authroizedItemMap) {
		String itemCodes = "";
		List<Integer> items = new ArrayList<Integer>();
		for (ProductKey productKey : authroizedItemMap.keySet()) {
			if (productKey.getProductLevelId() == Constants.ITEMLEVELID) {
				items.add(productKey.getProductId());
			}
		}
		itemCodes = PRCommonUtil.getCommaSeperatedStringFromIntArray(items);
		return itemCodes;
	}

	private void copyPromoProductGroup(PromoProductGroup source, PromoProductGroup dest) {
		dest.setGroupId(source.getGroupId());
		dest.setMinRegUnitPrice(source.getMinRegUnitPrice());
		dest.setMaxRegUnitPrice(source.getMaxRegUnitPrice());
		dest.setSupportedPromoType(source.getSupportedPromoType());
		dest.setLeadItem(source.getLeadItem());
	}

	public void writeToCSVLog(HashMap<ProductKey, List<PromoItemDTO>> itemMap, String additionalDetailLog) {
		List<PromoItemDTO> itemsForLogs = new ArrayList<PromoItemDTO>();
		for (List<PromoItemDTO> promoItems : itemMap.values()) {
			for (PromoItemDTO promoItemDTO : promoItems) {
				if (additionalDetailLog != "") {
					promoItemDTO.setAdditionalDetailForLog(additionalDetailLog);
				}
				itemsForLogs.add(promoItemDTO);
			}
		}

		itemLogService.writeToCSVFile(itemAnalysisLogPath, itemsForLogs);
	}

	public void writeToCSVLog(List<PromoItemDTO> itemList, String additionalDetailLog) {
		List<PromoItemDTO> itemsForLogs = new ArrayList<PromoItemDTO>();
		for (PromoItemDTO promoItemDTO : itemList) {
			if (additionalDetailLog != "") {
				promoItemDTO.setAdditionalDetailForLog(additionalDetailLog);
			}
			itemsForLogs.add(promoItemDTO);
		}
		itemLogService.writeToCSVFile(itemAnalysisLogPath, itemsForLogs);
	}

	/**
	 * Finds PPG level and individual item level data New objects are created
	 * 
	 * @param locationLevelId
	 * @param locationId
	 * @param ppgItemsPromoCombinations
	 * @param itemsWithPrediction
	 * @param ligAndItsMembers
	 * @return
	 */
	public List<PromoItemDTO> ppgLevelAndItsBreakDownSummary(int locationLevelId, int locationId,
			HashMap<Long, List<PromoProductGroup>> ppgItemsPromoCombinations, HashMap<ProductKey, List<PromoItemDTO>> itemsWithPrediction,
			HashMap<Integer, List<PRItemDTO>> ligAndItsMembers) {

		List<PromoItemDTO> resultAtPPGAndItemLevel = new ArrayList<PromoItemDTO>();
		List<PromoItemDTO> resultAtLigAndNonLigLevel = new ArrayList<PromoItemDTO>();
		List<PromoItemDTO> resultAtPPGLevel = new ArrayList<PromoItemDTO>();
		int ppgPromoCombinationId = 1;

		logger.info("ppgLevelAndItsBreakDownSummary is Started...");

		// Each promo group
		for (List<PromoProductGroup> promoProductGroups : ppgItemsPromoCombinations.values()) {
			ppgPromoCombinationId = 1;
			// each combination of promo group
			for (PromoProductGroup ppgCombination : promoProductGroups) {
				PromoItemDTO ppgLevelData = new PromoItemDTO();
				int householdCntOfPPG = 0;
				double predictedMovementOfPPG = 0, predictedRevenueOfPPG = 0, predictedMarginOfPPG = 0, predTotalCostOnSalePriceOfPPG = 0;
				double predRegPriceMovOfPPG = 0, predRegPriceRevOfPPG = 0, predRegPriceMarOfPPG = 0;

				// items inside each combination (PPG items)
				for (PromoItemDTO ppgItem : ppgCombination.getItems().values()) {
					ppgItem = findLigOrNonLigLevelMetrics(locationLevelId, locationId, ligAndItsMembers, itemsWithPrediction, ppgItem);
					ppgItem.setAdditionalDetailForLog("PPG Lig and Non Lig Summary");
					resultAtLigAndNonLigLevel.add(ppgItem);

					householdCntOfPPG = householdCntOfPPG + ppgItem.getNoOfHHRecommendedTo();
					predictedMovementOfPPG = predictedMovementOfPPG + (ppgItem.getPredMov() != null ? ppgItem.getPredMov() : 0);
					predictedRevenueOfPPG = predictedRevenueOfPPG + ppgItem.getPredRev();
					predictedMarginOfPPG = predictedMarginOfPPG + ppgItem.getPredMar();
					predTotalCostOnSalePriceOfPPG = predTotalCostOnSalePriceOfPPG +  ppgItem.getPredTotalCostOnSalePrice();
					
					predRegPriceMovOfPPG = predRegPriceMovOfPPG + ppgItem.getPredMovReg();
					predRegPriceRevOfPPG = predRegPriceRevOfPPG + ppgItem.getPredRevReg();
					predRegPriceMarOfPPG = predRegPriceMarOfPPG + ppgItem.getPredMarReg();

					ppgItem.setPpgPromoCombinationId(ppgPromoCombinationId);
				}

				// Copy lead item data
				PromoItemDTO leadItem = getLeadItem(ppgCombination);
				ppgLevelData.setRegPrice(leadItem.getRegPrice());
				ppgLevelData.setSaleInfo(leadItem.getSaleInfo());
				ppgLevelData.setAdInfo(leadItem.getAdInfo());
				ppgLevelData.setDisplayInfo(leadItem.getDisplayInfo());

				ppgLevelData.setDeptId(leadItem.getDeptId());
				ppgLevelData.setDeptName(leadItem.getDeptName());
				ppgLevelData.setProductKey(new ProductKey(0, 0));
				ppgLevelData.setPpgGroupId(ppgCombination.getGroupId());
				ppgLevelData.setNoOfHHRecommendedTo(householdCntOfPPG);
				ppgLevelData.setPredMov(predictedMovementOfPPG);
				ppgLevelData.setPredRev(predictedRevenueOfPPG);
				ppgLevelData.setPredMar(predictedMarginOfPPG);

				ppgLevelData.setPredMovReg(predRegPriceMovOfPPG);
				ppgLevelData.setPredRevReg(predRegPriceRevOfPPG);
				ppgLevelData.setPredMarReg(predRegPriceMarOfPPG);
				ppgLevelData.setPredTotalCostOnSalePrice(predTotalCostOnSalePriceOfPPG);
				
				ppgLevelData.setPPGLevelSummary(true);
				ppgLevelData.setPpgPromoCombinationId(ppgPromoCombinationId);

				ppgLevelData.setAdditionalDetailForLog("PPG Level Summary");
				resultAtPPGLevel.add(ppgLevelData);
				ppgPromoCombinationId = ppgPromoCombinationId + 1;
			}
		}

		resultAtPPGAndItemLevel.addAll(resultAtPPGLevel);
		resultAtPPGAndItemLevel.addAll(resultAtLigAndNonLigLevel);

		logger.info("ppgLevelAndItsBreakDownSummary is Completed...");

		return resultAtPPGAndItemLevel;
	}

	/**
	 * New objects are created
	 * 
	 * @param locationLevelId
	 * @param locationId
	 * @param individualItemsPromoCombinations
	 * @param itemsWithPrediction
	 * @param ligAndItsMembers
	 * @return
	 */
	public List<PromoItemDTO> individualItemLevelSummary(int locationLevelId, int locationId,
			HashMap<ProductKey, List<PromoItemDTO>> individualItemsPromoCombinations, HashMap<ProductKey, List<PromoItemDTO>> itemsWithPrediction,
			HashMap<Integer, List<PRItemDTO>> ligAndItsMembers) {
		List<PromoItemDTO> resultAtLigAndNonLigLevel = new ArrayList<PromoItemDTO>();
		logger.info("individualItemLevelSummary is Started...");

		for (List<PromoItemDTO> ligOrNonLigItems : individualItemsPromoCombinations.values()) {
			for (PromoItemDTO ligOrNonLigItem : ligOrNonLigItems) {
				ligOrNonLigItem = findLigOrNonLigLevelMetrics(locationLevelId, locationId, ligAndItsMembers, itemsWithPrediction, ligOrNonLigItem);
				ligOrNonLigItem.setAdditionalDetailForLog("Individual Item Lig and Non Lig Summary");
				resultAtLigAndNonLigLevel.add(ligOrNonLigItem);
			}
		}

		logger.info("individualItemLevelSummary is Completed...");

		return resultAtLigAndNonLigLevel;
	}

	private HashMap<ProductKey, List<PromoItemDTO>> getLigMembersAndNonLig(HashMap<Integer, List<PRItemDTO>> ligAndItsMembers,
			HashMap<ProductKey, List<PromoItemDTO>> ligAndNonLigItemList) {
		HashMap<ProductKey, List<PromoItemDTO>> ligMembersAndNonLig = new HashMap<ProductKey, List<PromoItemDTO>>();

		for (Map.Entry<ProductKey, List<PromoItemDTO>> candidateItem : ligAndNonLigItemList.entrySet()) {

			if (candidateItem.getKey().getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {

				// Add all members
				if (ligAndItsMembers.get(candidateItem.getKey().getProductId()) != null) {
					List<PRItemDTO> ligMembers = ligAndItsMembers.get(candidateItem.getKey().getProductId());

					logger.debug("LIG:" + candidateItem.getKey() + "ligMemberPromoItems:" + ligMembers);

					// For each lig, assign the same promo combination for all its members
					// Loop all lig members
					for (PRItemDTO itemDTO : ligMembers) {
						List<PromoItemDTO> ligMemberPromoItems = new ArrayList<PromoItemDTO>();
						ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode());

						for (PromoItemDTO ligItem : candidateItem.getValue()) {
							PromoItemDTO ligMemberPromoItem = new PromoItemDTO();
							copyPromoItem(Constants.ITEMLEVELID, itemDTO.getItemCode(), itemDTO.getUpc(), itemDTO.getItemName(), ligItem,
									ligMemberPromoItem);
							ligMemberPromoItems.add(ligMemberPromoItem);
						}

						ligMembersAndNonLig.put(productKey, ligMemberPromoItems);
					}

				} else {
					logger.error("getLigMembersAndNonLig() :: INFO: LIG " + candidateItem.getKey() + " not found in ligmap : ligAndItsMembers");
				}
			} else {
				ProductKey productKey = new ProductKey(candidateItem.getKey().getProductLevelId(), candidateItem.getKey().getProductId());

				List<PromoItemDTO> nonLigItems = new ArrayList<PromoItemDTO>();
				for (PromoItemDTO promoItemDTO : candidateItem.getValue()) {
					PromoItemDTO ligMemberPromoItem = new PromoItemDTO();
					copyPromoItem(Constants.ITEMLEVELID, promoItemDTO.getProductKey().getProductId(), promoItemDTO.getUpc(),
							promoItemDTO.getItemName(), promoItemDTO, ligMemberPromoItem);
					nonLigItems.add(ligMemberPromoItem);
				}

				ligMembersAndNonLig.put(productKey, nonLigItems);
			}
		}

		return ligMembersAndNonLig;
	}

	private HashMap<ProductKey, List<PromoItemDTO>> getLigMembersAndNonLig1(HashMap<Integer, List<PRItemDTO>> ligAndItsMembers,
			HashMap<ProductKey, PromoItemDTO> ligAndNonLigItemList) {
		HashMap<ProductKey, List<PromoItemDTO>> modifiedLIGAndNonLigItemList = new HashMap<ProductKey, List<PromoItemDTO>>();

		for (Map.Entry<ProductKey, PromoItemDTO> item : ligAndNonLigItemList.entrySet()) {
			List<PromoItemDTO> items = new ArrayList<PromoItemDTO>();
			items.add(item.getValue());

			modifiedLIGAndNonLigItemList.put(item.getKey(), items);
		}

		return getLigMembersAndNonLig(ligAndItsMembers, modifiedLIGAndNonLigItemList);
	}

	private void updateRegAndSalePrediction(int locationLevelId, int locationId, PromoItemDTO promoItemDTO,
			HashMap<ProductKey, List<PromoItemDTO>> itemsWithPrediction) {

		List<PromoItemDTO> items = itemsWithPrediction.get(promoItemDTO.getProductKey());

		if (items != null) {
			for (PromoItemDTO predItem : items) {

				// predicted item key
				PredictionDetailKey predSaleItemPDK = generatePredictionDetailKey(locationLevelId, locationId, predItem);

				PredictionDetailKey predRegItemPDK = generatePredictionDetailKeyOfRegPrice(locationLevelId, locationId, predItem);

				// actual item key
				PredictionDetailKey actualSaleItemPDK = generatePredictionDetailKey(locationLevelId, locationId, promoItemDTO);

				PredictionDetailKey actualRegItemPDK = generatePredictionDetailKeyOfRegPrice(locationLevelId, locationId, promoItemDTO);

				if (actualSaleItemPDK.equals(predSaleItemPDK)) {
					promoItemDTO.setPredMov(predItem.getPredMov());
					promoItemDTO.setPredStatus(predItem.getPredStatus());
				}

				if (actualRegItemPDK.equals(predRegItemPDK)) {
					promoItemDTO.setPredMovReg(predItem.getPredMovReg());
					promoItemDTO.setPredStatusReg(predItem.getPredStatusReg());
				}
			}
		}

	}

	private PredictionDetailKey generatePredictionDetailKey(int locationLevelId, int locationId, PromoItemDTO itemDTO) {

		int itemCode = itemDTO.getProductKey().getProductId();
		MultiplePrice curRegPrice = itemDTO.getRegPrice();

		PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemCode, (curRegPrice != null ? curRegPrice.multiple : 0),
				(curRegPrice != null ? curRegPrice.price : 0),
				(itemDTO.getSaleInfo() != null && itemDTO.getSaleInfo().getSalePrice() != null ? itemDTO.getSaleInfo().getSalePrice().multiple : 0),
				(itemDTO.getSaleInfo() != null && itemDTO.getSaleInfo().getSalePrice() != null ? itemDTO.getSaleInfo().getSalePrice().price : 0),
				(itemDTO.getAdInfo() != null ? itemDTO.getAdInfo().getAdPageNo() : 0),
				(itemDTO.getAdInfo() != null ? itemDTO.getAdInfo().getAdBlockNo() : 0),
				(itemDTO.getSaleInfo() != null && itemDTO.getSaleInfo() != null
						? itemDTO.getSaleInfo().getPromoTypeId()
						: 0),
				(itemDTO.getDisplayInfo() != null && itemDTO.getDisplayInfo().getDisplayTypeLookup() != null
						? itemDTO.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId()
						: 0));

		return pdk;
	}

	private PredictionDetailKey generatePredictionDetailKeyOfRegPrice(int locationLevelId, int locationId, PromoItemDTO itemDTO) {

		int itemCode = itemDTO.getProductKey().getProductId();
		MultiplePrice curRegPrice = itemDTO.getRegPrice();

		PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemCode, (curRegPrice != null ? curRegPrice.multiple : 0),
				(curRegPrice != null ? curRegPrice.price : 0), 0, 0d, 0, 0, 0, 0);

		return pdk;
	}

	private double findSaleRevenueOfItem(PromoItemDTO itemDTO) {

		double movement = (itemDTO.getPredStatus() == PredictionStatus.SUCCESS ? itemDTO.getPredMov() : 0);

		double totalSales = (movement > 0 && itemDTO.getSaleInfo() != null)
				? (PRCommonUtil.getSalesDollar(itemDTO.getSaleInfo().getSalePrice(), movement))
				: 0;

		return totalSales;
	}

	private double findSaleMarginOfItem(PromoItemDTO itemDTO) {
		double totalMargin = 0;

		double movement = (itemDTO.getPredStatus() == PredictionStatus.SUCCESS ? itemDTO.getPredMov() : 0);

		Double cost = itemDTO.getFinalCost();

		if (movement > 0 && cost != null && cost > 0 && itemDTO.getSaleInfo() != null) {
			totalMargin = PRCommonUtil.getMarginDollar(itemDTO.getSaleInfo().getSalePrice(), cost, movement);
		}

		return totalMargin;
	}
	
	private double findTotalCostOfItem(PromoItemDTO itemDTO) {
		double totalCost = 0;

		double movement = (itemDTO.getPredStatus() == PredictionStatus.SUCCESS ? itemDTO.getPredMov() : 0);

		Double cost = itemDTO.getFinalCost();

		if (movement > 0 && cost != null && cost > 0 && itemDTO.getSaleInfo() != null) {
			totalCost = movement * cost;
		}

		return totalCost;
	}

	private double findRegRevenueOfItem(PromoItemDTO itemDTO) {

		double movement = (itemDTO.getPredStatusReg() == PredictionStatus.SUCCESS ? itemDTO.getPredMovReg() : 0);

		double totalSales = (movement > 0 && itemDTO.getRegPrice() != null) ? (PRCommonUtil.getSalesDollar(itemDTO.getRegPrice(), movement)) : 0;

		return totalSales;
	}

	private double findRegMarginOfItem(PromoItemDTO itemDTO) {
		double totalMargin = 0;

		double movement = (itemDTO.getPredStatusReg() == PredictionStatus.SUCCESS ? itemDTO.getPredMovReg() : 0);

		Double cost = itemDTO.getListCost();

		if (movement > 0 && cost != null && cost > 0 && itemDTO.getRegPrice() != null) {
			totalMargin = PRCommonUtil.getMarginDollar(itemDTO.getRegPrice(), cost, movement);
		}

		return totalMargin;
	}

	@SuppressWarnings("unused")
	private Double findDealCostBasedOnCategoryMarginPCT(PromoItemDTO itemDTO, HashMap<ProductKey, Double> catLevelMarginPCT) {

		MultiplePrice salePrice = (itemDTO.getSaleInfo() != null ? itemDTO.getSaleInfo().getSalePrice() : null);
		Double cost = (itemDTO.getListCost() != null ? itemDTO.getListCost()
				: (itemDTO.getRegPrice() != null ? (itemDTO.getRegPrice().price / itemDTO.getRegPrice().multiple) : null));
		ProductKey categoryKey = new ProductKey(Constants.CATEGORYLEVELID, itemDTO.getCategoryId());

		Double derivedCost = 0d;
		if (salePrice != null && catLevelMarginPCT.get(categoryKey) != null) {
			derivedCost = getDerivedCost(salePrice, catLevelMarginPCT.get(categoryKey));
		} else {
			derivedCost = cost;
		}

		return derivedCost;
	}

	private double getDerivedCost(MultiplePrice salePrice, Double discountPCT) {
		double derivedCost = 0.0d;

		double unitSalePrice = PRCommonUtil.getUnitPrice(salePrice, true);

		derivedCost = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit((unitSalePrice - (unitSalePrice * discountPCT))));

		return derivedCost;
	}

	// private double getDerivedCost(MultiplePrice regPrice, MultiplePrice salePrice, double ListCost) {
	//
	// double derivedCost = 0.0d;
	//
	// if (salePrice != null && regPrice != null) {
	// if (salePrice.price != null && salePrice.multiple != null && salePrice.multiple > 0 && regPrice.price != null &&
	// regPrice.multiple != null
	// && regPrice.price > 0.0d && regPrice.multiple > 0) {
	// derivedCost = Double.valueOf(PRFormatHelper
	// .roundToTwoDecimalDigit((ListCost * (salePrice.price / salePrice.multiple)) / (regPrice.price / regPrice.multiple)));
	// }
	// }
	// return derivedCost;
	// }

	private Double findFinalCostOfItem(PromoItemDTO itemDTO,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfItemOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfCategoryOnFirstPage,
			HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> minDealCostOfItemOnAd, 
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary) {
		Double finalCost = 0d;
		boolean isPresentInActualAd = false;
		ProductKey itemKey = itemDTO.getProductKey();
		int promoType = itemDTO.getSaleInfo().getPromoTypeId();
		ProductKey categoryKey = new ProductKey(Constants.CATEGORYLEVELID, itemDTO.getCategoryId());
		MultiplePrice salePrice = (itemDTO.getSaleInfo() != null ? itemDTO.getSaleInfo().getSalePrice() : null);
		
		logger.debug("findFinalCostOfItem->ItemCode:" + itemDTO.getProductKey());
		//If item is present in actual ad 
		for (List<PromoItemDTO> actualAdItems : actualAdItemsLIGNonLigSummary.values()) {
			for (PromoItemDTO actualAdItem : actualAdItems) {
				if (itemDTO.getProductKey().equals(actualAdItem.getProductKey())) {
					finalCost = findFinalCostOfActualAdItem(actualAdItem);
					isPresentInActualAd = true;
					logger.debug("ItemCode:" + itemDTO.getProductKey() + ",1.finalCost:" + finalCost);
					break;
				}
			}
		}
		
		
		if (!isPresentInActualAd) {
			// If the item has average sale margin pct - find cost based on sale price, don't go below min deal cost
			if (avgSaleMarPCTOfItemOnFirstPage.get(itemKey) != null && avgSaleMarPCTOfItemOnFirstPage.get(itemKey).get(promoType) != null
					&& avgSaleMarPCTOfItemOnFirstPage.get(itemKey).get(promoType) > 0) {
				double minDealCost = itemDTO.getListCost();

				if (minDealCostOfItemOnAd.get(itemKey) != null && minDealCostOfItemOnAd.get(itemKey).get(promoType) != null
						&& minDealCostOfItemOnAd.get(itemKey).get(promoType) > 0) {
					minDealCost = minDealCostOfItemOnAd.get(itemKey).get(promoType);
				}

				finalCost = getDerivedCost(salePrice, avgSaleMarPCTOfItemOnFirstPage.get(itemKey).get(promoType));
				
				if(finalCost < minDealCost) {
					finalCost = minDealCost;
				}
				
				logger.debug("ItemCode:" + itemDTO.getProductKey() + ",salePrice:" + salePrice + "avgMarPCT:"
						+ avgSaleMarPCTOfItemOnFirstPage.get(itemKey).get(promoType) + ",2.finalCost:" + finalCost + ",listCost:"
						+ itemDTO.getListCost() + ",minDealCost:" + minDealCost);
			} else {
				// If the item doesn't have avg sale margin pct - find cost based on sale price with category level margin pct
				if (avgSaleMarPCTOfCategoryOnFirstPage.get(categoryKey) != null
						&& avgSaleMarPCTOfCategoryOnFirstPage.get(categoryKey).get(promoType) != null
						&& avgSaleMarPCTOfCategoryOnFirstPage.get(categoryKey).get(promoType) > 0) {
					finalCost = getDerivedCost(salePrice, avgSaleMarPCTOfCategoryOnFirstPage.get(categoryKey).get(promoType));
					
					// don't go above list cost
					if(itemDTO.getListCost() != null && finalCost > itemDTO.getListCost()) {
						finalCost = itemDTO.getListCost();
					}
					logger.debug("ItemCode:" + itemDTO.getProductKey() + ",salePrice:" + salePrice + "avgMarPCT:"
							+ avgSaleMarPCTOfCategoryOnFirstPage.get(categoryKey).get(promoType) + ",3.finalCost:" + finalCost + ",listCost:"
							+ itemDTO.getListCost());
					
				} else {
					finalCost = itemDTO.getListCost();
					
					logger.debug("ItemCode:" + itemDTO.getProductKey() + ",salePrice:" + salePrice + "avgMarPCT:" + 0 + ",4.finalCost:" + finalCost
							+ ",listCost:" + itemDTO.getListCost());
				}
				
			}
		}
		
		
		
		
		// Min deal cost will be the max discount can be given for an item
//		if (itemDTO.getMinDealCost() != null && itemDTO.getDerivedDealCost() != null && itemDTO.getMinDealCost() > itemDTO.getDerivedDealCost()) {
//			finalCost = itemDTO.getMinDealCost();
//		} else {
//			finalCost = itemDTO.getDerivedDealCost();
//		}

		// If there is deal cost, give preference to that
		// if (itemDTO.getMinDealCost() != null && itemDTO.getMinDealCost() > 0) {
		// finalCost = itemDTO.getMinDealCost();
		// } else {
		// finalCost = itemDTO.getDerivedDealCost();
		// }

		return finalCost;
	}

	private Double findFinalCostOfActualAdItem(PromoItemDTO itemDTO) {
		return itemDTO.getDealCost() != null && itemDTO.getDealCost() > 0 ? itemDTO.getDealCost() : itemDTO.getListCost();
	}

	public HashMap<ProductKey, List<PromoItemDTO>> findRevenueAndMargin(HashMap<ProductKey, List<PromoItemDTO>> itemsWithPrediction) {
		logger.info("findRevenueAndMargin is Started...");
		for (List<PromoItemDTO> promoItems : itemsWithPrediction.values()) {
			for (PromoItemDTO promoItemDTO : promoItems) {
				promoItemDTO.setPredRev(findSaleRevenueOfItem(promoItemDTO));
				promoItemDTO.setPredMar(findSaleMarginOfItem(promoItemDTO));
				promoItemDTO.setPredTotalCostOnSalePrice(findTotalCostOfItem(promoItemDTO));
				
				promoItemDTO.setPredRevReg(findRegRevenueOfItem(promoItemDTO));
				promoItemDTO.setPredMarReg(findRegMarginOfItem(promoItemDTO));
			}
		}
		logger.info("findRevenueAndMargin is Completed...");
		return itemsWithPrediction;
	}

	private PromoItemDTO findLigOrNonLigLevelMetrics(int locationLevelId, int locationId, HashMap<Integer, List<PRItemDTO>> ligAndItsMembers,
			HashMap<ProductKey, List<PromoItemDTO>> itemsWithPrediction, PromoItemDTO ligOrNonLigItem) {

		double predSalePriceMovOfLigOrNonLig = 0, predSalePriceRevOfLigOrNonLig = 0, predSalePriceMarOfLigOrNonLig = 0, predTotalCostOnSalePrice = 0;

		double predRegPriceMovOfLigOrNonLig = 0, predRegPriceRevOfLigOrNonLig = 0, predRegPriceMarOfLigOrNonLig = 0;

		HashMap<ProductKey, PromoItemDTO> tempMap = new HashMap<ProductKey, PromoItemDTO>();
		tempMap.put(ligOrNonLigItem.getProductKey(), ligOrNonLigItem);

		HashMap<ProductKey, List<PromoItemDTO>> ligMembersAndNonLigMap = getLigMembersAndNonLig1(ligAndItsMembers, tempMap);

		// LIG level or non lig level data
		for (Map.Entry<ProductKey, List<PromoItemDTO>> ligMemberAndNonLigItemEntry : ligMembersAndNonLigMap.entrySet()) {
			for (PromoItemDTO ligMemberOrNonLigItem : ligMemberAndNonLigItemEntry.getValue()) {
				// Update prediction
				updateRegAndSalePrediction(locationLevelId, locationId, ligMemberOrNonLigItem, itemsWithPrediction);

				// Find sales, margin, household
				Double predMov = ligMemberOrNonLigItem.getPredMov();
				predSalePriceMovOfLigOrNonLig = predSalePriceMovOfLigOrNonLig
						+ (predMov != null && predMov > 0 ? ligMemberOrNonLigItem.getPredMov() : 0);
				predSalePriceRevOfLigOrNonLig = predSalePriceRevOfLigOrNonLig + findSaleRevenueOfItem(ligMemberOrNonLigItem);
				predSalePriceMarOfLigOrNonLig = predSalePriceMarOfLigOrNonLig + findSaleMarginOfItem(ligMemberOrNonLigItem);
				predTotalCostOnSalePrice = predTotalCostOnSalePrice + findTotalCostOfItem(ligMemberOrNonLigItem);

				Double predRegPriceMov = ligMemberOrNonLigItem.getPredMovReg();
				predRegPriceMovOfLigOrNonLig = predRegPriceMovOfLigOrNonLig
						+ (predRegPriceMov != null && predRegPriceMov > 0 ? ligMemberOrNonLigItem.getPredMovReg() : 0);
				predRegPriceRevOfLigOrNonLig = predRegPriceRevOfLigOrNonLig + findRegRevenueOfItem(ligMemberOrNonLigItem);
				predRegPriceMarOfLigOrNonLig = predRegPriceMarOfLigOrNonLig + findRegMarginOfItem(ligMemberOrNonLigItem);
			}
		}

		ligOrNonLigItem.setPredMov(predSalePriceMovOfLigOrNonLig);
		ligOrNonLigItem.setPredRev(predSalePriceRevOfLigOrNonLig);
		ligOrNonLigItem.setPredMar(predSalePriceMarOfLigOrNonLig);
		ligOrNonLigItem.setPredTotalCostOnSalePrice(predTotalCostOnSalePrice);

		ligOrNonLigItem.setPredMovReg(predRegPriceMovOfLigOrNonLig);
		ligOrNonLigItem.setPredRevReg(predRegPriceRevOfLigOrNonLig);
		ligOrNonLigItem.setPredMarReg(predRegPriceMarOfLigOrNonLig);

		return ligOrNonLigItem;
	}

	private PromoItemDTO getLeadItem(PromoProductGroup promoProductGroup) {
		PromoItemDTO leadItem = null;
		for (PromoItemDTO ppgItem : promoProductGroup.getItems().values()) {
			if (ppgItem.isPPGLeadItem()) {
				leadItem = ppgItem;
			}
		}
		return leadItem;
	}

	public HashMap<PageBlockNoKey, BlockDetail> getBlockLevelSummary(HashMap<PageBlockNoKey, List<PromoItemDTO>> itemsInBlock) {
		HashMap<PageBlockNoKey, BlockDetail> actualAdBlockLevelSummary = new HashMap<PageBlockNoKey, BlockDetail>();
		logger.info("getBlockLevelSummary() : Start ....");

		for (Map.Entry<PageBlockNoKey, List<PromoItemDTO>> ligMemberAndNonLigItemEntry : itemsInBlock.entrySet()) {

			BlockDetail blockDetail = getBlockSummary(ligMemberAndNonLigItemEntry.getValue());
			blockDetail.setPageBlockNoKey(ligMemberAndNonLigItemEntry.getKey());

			actualAdBlockLevelSummary.put(ligMemberAndNonLigItemEntry.getKey(), blockDetail);
		}

		logger.info("getBlockLevelSummary() : Completed ....");

		return actualAdBlockLevelSummary;
	}

	public HashMap<PageBlockNoKey, BlockDetail> getRecBlockLevelSummary(HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> recAdItemsLIGNonLigSummary) {
		HashMap<PageBlockNoKey, BlockDetail> actualAdBlockLevelSummary = new HashMap<PageBlockNoKey, BlockDetail>();

		for (Map.Entry<PageBlockNoKey, List<PromoItemDTO>> ligMemberAndNonLigItemEntry : recAdItemsLIGNonLigSummary.entrySet()) {

			BlockDetail blockDetail = null;

			// If there is no system recommendation & there is one active item in actual ad, then copy actual items
			if (ligMemberAndNonLigItemEntry.getValue().size() == 0
					&& actualAdItemsLIGNonLigSummary.get(ligMemberAndNonLigItemEntry.getKey()).size() > 0) {
				logger.debug("There is no system recommendation");
				blockDetail = getBlockSummary(actualAdItemsLIGNonLigSummary.get(ligMemberAndNonLigItemEntry.getKey()));
			} else {
				blockDetail = getBlockSummary(ligMemberAndNonLigItemEntry.getValue());
			}

			blockDetail.setPageBlockNoKey(ligMemberAndNonLigItemEntry.getKey());

			actualAdBlockLevelSummary.put(ligMemberAndNonLigItemEntry.getKey(), blockDetail);
		}

		return actualAdBlockLevelSummary;
	}

	private BlockDetail getBlockSummary(List<PromoItemDTO> ligMemberAndNonLigItems) {
		BlockDetail blockDetail = new BlockDetail();

		blockDetail.setTotalHHCnt(ligMemberAndNonLigItems.stream().mapToLong(p -> p.getNoOfHHRecommendedTo()).sum());
		blockDetail.getSalePricePredictionMetrics().setPredMov(ligMemberAndNonLigItems.stream().mapToDouble(p -> p.getPredMov()).sum());
		blockDetail.getSalePricePredictionMetrics().setPredRev(ligMemberAndNonLigItems.stream().mapToDouble(p -> p.getPredRev()).sum());
		blockDetail.getSalePricePredictionMetrics().setPredMar(ligMemberAndNonLigItems.stream().mapToDouble(p -> p.getPredMar()).sum());

		blockDetail.getRegPricePredictionMetrics().setPredMov(ligMemberAndNonLigItems.stream().mapToDouble(p -> p.getPredMovReg()).sum());
		blockDetail.getRegPricePredictionMetrics().setPredRev(ligMemberAndNonLigItems.stream().mapToDouble(p -> p.getPredRevReg()).sum());
		blockDetail.getRegPricePredictionMetrics().setPredMar(ligMemberAndNonLigItems.stream().mapToDouble(p -> p.getPredMarReg()).sum());

		blockDetail.setDepartments((HashMap<Integer, String>) ligMemberAndNonLigItems.stream()
				.collect(Collectors.toMap(PromoItemDTO::getDeptId, PromoItemDTO::getDeptName, (p1, p2) -> p1)));

		return blockDetail;
	}

	public AdDetail getAdLevelSummary(HashMap<PageBlockNoKey, List<PromoItemDTO>> adItemsLIGNonLigSummary,
			HashMap<PageBlockNoKey, BlockDetail> adBlockLevelSummary) {
		AdDetail adDetail = new AdDetail();
		PageDetail pageDetail = null;
		BlockDetail blockDetail = null;

		adDetail.getPredictionMetrics()
				.setPredMov(adBlockLevelSummary.values().stream().mapToDouble(p -> p.getSalePricePredictionMetrics().getPredMov()).sum());

		adDetail.getPredictionMetrics()
				.setPredRev(adBlockLevelSummary.values().stream().mapToDouble(p -> p.getSalePricePredictionMetrics().getPredRev()).sum());

		adDetail.getPredictionMetrics()
				.setPredMar(adBlockLevelSummary.values().stream().mapToDouble(p -> p.getSalePricePredictionMetrics().getPredMar()).sum());

		adDetail.setTotalHHReachCnt(adBlockLevelSummary.values().stream().mapToLong(p -> p.getTotalHHCnt()).sum());

		for (Map.Entry<PageBlockNoKey, BlockDetail> blocks : adBlockLevelSummary.entrySet()) {
			int pageNo = blocks.getKey().getPageNumber();
			int blockNo = blocks.getKey().getBlockNumber();

			pageDetail = new PageDetail();
			blockDetail = blocks.getValue();

			if (adDetail.getPageMap().get(pageNo) != null) {
				pageDetail = adDetail.getPageMap().get(pageNo);
			}

			if (pageDetail.getBlockMap().get(blockNo) != null) {
				blockDetail = pageDetail.getBlockMap().get(blockNo);
			}

			if (adItemsLIGNonLigSummary.get(blocks.getKey()) != null) {
				blockDetail.setItems(adItemsLIGNonLigSummary.get(blocks.getKey()));
			}

			pageDetail.setPageNo(pageNo);
			pageDetail.getBlockMap().put(blockNo, blockDetail);
			adDetail.getPageMap().put(pageNo, pageDetail);
		}

		return adDetail;
	}

	public void generateFinalReport(String adWeekStartDate, AdDetail finalActualAdDetail, AdDetail finalRecAdDetail) throws GeneralException {

		new AdRecReportService().generateAdRecExcelReport(PropertyManager.getProperty("AD_REC_ITEM_ANALYSIS_LOG_PATH"), adWeekStartDate,
				finalActualAdDetail, finalRecAdDetail, departmentList);

	}

	public int getLigMemNonLIGCount(Set<ProductKey> keySet, HashMap<ProductKey, PRItemDTO> authorizedMap,
			HashMap<Integer, List<PRItemDTO>> ligAndItsMember) {

		int totalCount = 0;
		HashSet<ProductKey> productKeySet = new HashSet<ProductKey>();
		for (ProductKey productKey : keySet) {

			if (productKey.getProductLevelId() == Constants.ITEMLEVELID) {
				if (authorizedMap.get(productKey) != null) {
					productKeySet.add(new ProductKey(Constants.ITEMLEVELID, authorizedMap.get(productKey).getItemCode()));
				}
			} else {
				if (ligAndItsMember.get(productKey.getProductId()) != null) {
					for (PRItemDTO itemDTO : ligAndItsMember.get(productKey.getProductId())) {
						productKeySet.add(new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode()));
					}
				}
			}
		}

		totalCount = productKeySet.size();
		return totalCount;
	}
}
