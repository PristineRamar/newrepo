package com.pristine.fileformatter.riteaid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.supercsv.io.ICsvBeanWriter;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dataload.AdAndPromoSetup;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.riteaid.RAPromoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParserV2;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

@SuppressWarnings("unused")
public class PromoFileFormatter {

	private static Logger logger = Logger.getLogger("PromoFileFormatter");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String STORE_OUTPUT_FOLDER = "STORE_OUTPUT_FOLDER=";
	private final static String MODE = "MODE=";
	private final static String CURRENTDATE = "CURRENTDATE=";
	private String rootPath, startDate = null;
	private static String relativeInputPath, processingFile, relativeOutputpath, storeOutputpath;
	private List<PromoDataStandardDTO> stdFormat = null;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns1 = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns2 = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns3 = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns4 = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns5 = new TreeMap<Integer, String>();
	private static String week;
	Date weekdate, weekEndDate = null;
	int statusFlag = 0;
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	private Connection conn = null;
	RetailCalendarDAO calDAO = new RetailCalendarDAO();
	@SuppressWarnings("rawtypes")
	private ExcelFileParserV2 parser = null;
	ICsvBeanWriter beanWriter;
	private FileWriter fw = null;
	private PrintWriter pw = null;

	private FileWriter fw1 = null;
	private PrintWriter pw1 = null;

	List<RAPromoDTO> eventfile = null;
	List<RAPromoDTO> itemFile = null;
	List<RAPromoDTO> storeFile = null;
	List<RAPromoDTO> promoFile = null;
	List<RAPromoDTO> marketingFile = null;
	List<RAPromoDTO> maapdFile = null;
	HashMap<String, List<PromoDataStandardDTO>> mergedMap = null;
	HashMap<String, List<String>> zoneAndItsStores = new HashMap<>();
	int locationNo = 0;
	// String zoneNum = "";
	String DivisionNum = "";
	List<PromoDataStandardDTO> tempList = null;
	PriceDataLoad objPriceLoad;
	List<ItemDTO> activeItems = null;
	HashMap<ItemDetailKey, Integer> itemDetailKeyItems = null;
	Set<String> itemCodeNotFound = null;
	Set<String> priceNotFoundList = null;
	Set<String> ignoredItem = null;
	Set<String> futureDate = null;

	Set<String> noPromo = null;
	private static boolean isDeltaMode = false;
	private List<RAPromoDTO> locationInfo = new ArrayList<>();
	private List<RAPromoDTO> itemTemp = null;
	private List<RAPromoDTO> eventTemp = null;
	private List<RAPromoDTO> promotemp = null;
	private List<RAPromoDTO> marketingtemp = null;
	int counter = 0;
	Map<String, List<RAPromoDTO>> tempMap = null;
	HashMap<String, List<RAPromoDTO>> getItems = new HashMap<>();
	HashMap<String, List<RAPromoDTO>> buyitems = new HashMap<>();

	HashMap<String, List<ItemDTO>> retailerItemCodeAndItsItem = null;
	HashMap<String, List<ItemDTO>> upcAndItsItem = null;
	HashSet<String> activeEventIds = new HashSet<String>();
	static String currentWkStartDt = "";
	HashSet<String> chainLevelEvents = new HashSet<String>();
	HashMap<String, Integer> zoneAndItsStoreCount = new HashMap<>();
	HashMap<String, String> storeToZoneMap = new HashMap<>();
	int curweekcalId = 0;
	String endDate = "";
	int activeZonesThreshold = 0;
	int chainID = 0;
	double thresholdValue = 0;

	public PromoFileFormatter() {

		PropertyManager.initialize("analysis.properties");

		try {
			conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}

	}

	public static void main(String[] args) throws GeneralException {

		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}

			if (arg.startsWith(CURRENTDATE)) {
				currentWkStartDt = arg.substring(CURRENTDATE.length());
			}

			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputpath = arg.substring(OUTPUT_FOLDER.length());
			}
			if (arg.startsWith(STORE_OUTPUT_FOLDER)) {
				storeOutputpath = arg.substring(STORE_OUTPUT_FOLDER.length());
			}

		}

		if (currentWkStartDt.isEmpty()) {
			currentWkStartDt = DateUtil.getWeekStartDate(0);
		}

		logger.info("CurrentWkStartDt: - " + currentWkStartDt);

		try {
			PromoFileFormatter fileformatter = new PromoFileFormatter();
			fileformatter.processFile();
		} catch (Exception e) {

			logger.error("Error occured in RiteAidPromoLoader() main method", e);
		}

	}

	private void processFile() throws GeneralException, Exception {

		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillEPVETfile();
		fillEPITMfile();
		fillEPSTRfile();
		fillEPVITfile();
		fillMAAPDfile();
		fillMAEGXfile();
		initializeData();
		parseFile();
		getActiveEvents();

		mergeRewardPromotions();

	}

	private void getActiveEvents() throws IOException {
		logger.debug("getActiveEvents()-start");

		itemTemp = new ArrayList<>();
		eventTemp = new ArrayList<>();
		promotemp = new ArrayList<>();
		marketingtemp = new ArrayList<>();

		for (RAPromoDTO event : eventfile) {

			if (!event.getStatus().equals(Constants.CANCELLED) && !event.getStoreBeginDate().equals("")
					&& !event.getStoreEndDate().equals("") && !event.getThemeCode().equals("0028")) {
				RAPromoDTO raPromoDTO = new RAPromoDTO();
				activeEventIds.add(event.getEventId());
				raPromoDTO = event;
				eventTemp.add(raPromoDTO);
			} else if (!event.getStatus().equals(Constants.CANCELLED) && !event.getStoreBeginDate().equals("")
					&& !event.getStoreEndDate().equals("") && event.getRegionNo() == 99999
					&& event.getThemeCode().equals("0028")) {
				chainLevelEvents.add(event.getEventId());
				RAPromoDTO raPromoDTO = new RAPromoDTO();
				raPromoDTO = event;
				eventTemp.add(raPromoDTO);

			} else {
				if (event.getStoreBeginDate().equals("") || event.getStoreEndDate().equals("")) {
					ignoredItem.add(
							"Ignored from EventFile : " + event.getEventId() + "as date is blank" + " storebeginDate:"
									+ event.getStoreBeginDate() + "store enddate :" + event.getStoreEndDate());
				} else {
					ignoredItem
							.add("Ignored from EventFile : " + event.getEventId() + "as status is" + event.getStatus());
				}
			}
		}
		logger.info("otherEvents: " + activeEventIds.size());
		logger.info("chainLevelEvents: " + chainLevelEvents.size());
		logger.info("# items in eventTemp:" + eventTemp.size());

		setPromoLocations();

		HashSet<String> activeLocations = new HashSet<String>();

		for (RAPromoDTO stores : locationInfo) {
			activeLocations.add(stores.getEventId());
		}

		for (RAPromoDTO item : itemFile) {

			if (item.getStatus().equals(Constants.ACTIVE) && activeLocations.contains(item.getEventId())) {
				RAPromoDTO raPromoDTO = new RAPromoDTO();
				raPromoDTO = item;
				itemTemp.add(raPromoDTO);
			} else {

				if (!activeEventIds.contains(item.getEventId()) || chainLevelEvents.contains(item.getEventId())) {

				} else {
					ignoredItem.add("Ignored from ItemFile : " + item.getEventId() + "as eventId not matched ");
				}

			}
		}
		logger.info("# items in itemTemp:" + itemTemp.size());
		for (RAPromoDTO promo : promoFile) {

			if (promo.getStatus().equals(Constants.ACTIVE) && !promo.getEventGroupId().contains("WAG")
					&& activeLocations.contains(promo.getEventId())) {
				RAPromoDTO raPromoDTO = new RAPromoDTO();
				raPromoDTO = promo;
				promotemp.add(raPromoDTO);
			} else {

				if (!activeEventIds.contains(promo.getEventId())) {
					ignoredItem.add("Ignored from promoFile : " + promo.getEventId() + "as eventId not matched ");

				} else if (promo.getEventGroupId().contains("WAG")) {
					ignoredItem.add("Ignored from promoFile : " + promo.getEventId() + "as its walgreen promotion ");
				}

				else {
					ignoredItem
							.add("Ignored from promoFile : " + promo.getEventId() + "as status is" + promo.getStatus());
				}
			}
		}
		logger.info("# items in promoTemp:" + promotemp.size());
		for (RAPromoDTO marketingData : marketingFile) {

			if (!marketingData.getEventGroupId().contains("WAG")
					&& activeLocations.contains(marketingData.getEventId())) {
				RAPromoDTO raPromoDTO = new RAPromoDTO();
				raPromoDTO = marketingData;
				marketingtemp.add(raPromoDTO);
			} else {

				if (!activeEventIds.contains(marketingData.getEventId())) {
					ignoredItem
							.add("Ignored from marketing : " + marketingData.getEventId() + "as eventId not matched ");

				} else if (marketingData.getEventGroupId().contains("WAG")) {
					ignoredItem.add(
							"Ignored from marketing : " + marketingData.getEventId() + "as its walgreen promotion ");
				}

				else {
					ignoredItem.add("Ignored from marketing : " + marketingData.getEventId() + "as status is"
							+ marketingData.getStatus());
				}

			}

		}
		logger.info("# items in marketingtemp:" + marketingtemp.size());
		logger.debug("getActiveEvents()-end");
	}

	private void setPromoLocations() throws IOException {
		logger.debug("setLocation()- Setting Locations");
		HashMap<String, List<String>> storeandGNSL = new HashMap<>();

		HashMap<String, HashMap<String, List<RAPromoDTO>>> zoneStrMap = new HashMap<String, HashMap<String, List<RAPromoDTO>>>();
		HashMap<String, List<String>> zoneList = null;
		for (RAPromoDTO store : storeFile) {

			if (store.getStatus().equals(Constants.ACTIVE) && activeEventIds.contains(store.getEventId())) {
				if (storeToZoneMap.containsKey(store.getLocationNo())) {
					String zoneNum = storeToZoneMap.get(store.getLocationNo());
					store.setZoneNo(zoneNum);
					HashMap<String, List<RAPromoDTO>> temp = new HashMap<String, List<RAPromoDTO>>();
					if (zoneStrMap.containsKey(zoneNum)) {
						temp = zoneStrMap.get(zoneNum);
						List<RAPromoDTO> tempList = new ArrayList<RAPromoDTO>();
						if (temp.containsKey(store.getEventId())) {
							tempList = temp.get(store.getEventId());
							tempList.add(store);
							temp.put(store.getEventId(), tempList);
						} else {
							tempList.add(store);
							temp.put(store.getEventId(), tempList);
						}

					} else {
						List<RAPromoDTO> tempList = new ArrayList<RAPromoDTO>();
						tempList.add(store);
						temp.put(store.getEventId(), tempList);
					}
					zoneStrMap.put(zoneNum, temp);

				}
			}
		}
		zoneList = new HashMap<String, List<String>>();
		for (Map.Entry<String, HashMap<String, List<RAPromoDTO>>> entry : zoneStrMap.entrySet()) {

			// logger.info("Looping zone: " + entry.getKey());

			int totalStoresInzone = zoneAndItsStoreCount.get(entry.getKey());

			int strPer = Integer.parseInt(String.valueOf(Math.round(thresholdValue * totalStoresInzone)));

			if (zoneAndItsStoreCount.containsKey(entry.getKey())) {

				HashMap<String, List<RAPromoDTO>> eventStoreList = entry.getValue();

				for (Map.Entry<String, List<RAPromoDTO>> eventMap : eventStoreList.entrySet()) {

					int totalStores = eventMap.getValue().size();

					logger.debug("Event: - " + eventMap.getKey() + "Total Stores:" + totalStores);

					logger.debug("strPer :" + strPer);

					// rollup to zone level if promotions given for all stores or 90% of stores from
					// the zone

					if (zoneAndItsStoreCount.get(entry.getKey()) == totalStores || totalStores >= strPer) {

						List<String> tempMap = new ArrayList<String>();

						if (zoneList.containsKey(eventMap.getKey())) {
							tempMap = zoneList.get(eventMap.getKey());
						}
						tempMap.add(entry.getKey());
						zoneList.put(eventMap.getKey(), tempMap);

					} else {
						eventMap.getValue().forEach(store -> {
							RAPromoDTO sampleObj = new RAPromoDTO();
							sampleObj.setLocationNo(store.getLocationNo());
							sampleObj.setLocationLevel(Constants.STORELEVEL);
							sampleObj.setEventId(store.getEventId());
							sampleObj.setStatus(Constants.ACTIVE);
							locationInfo.add(sampleObj);
							// logger.debug("storepromo:" + eventMap.getKey());
						});

					}

				}

			}

		}

		if (zoneList != null) {

			zoneList.forEach((EventID, zones) -> {

				if (zones.size() >= activeZonesThreshold) {
					chainLevelEvents.add(EventID);
				} else {
					for (String zone : zones) {
						RAPromoDTO sampleObj = new RAPromoDTO();
						sampleObj.setLocationNo(zone);
						sampleObj.setLocationLevel(Constants.ZONE);
						sampleObj.setEventId(EventID);
						sampleObj.setStatus(Constants.ACTIVE);
						locationInfo.add(sampleObj);
					}

				}

			});

		}

		// ADD CHAINLEVEL PROMOTIONS
		chainLevelEvents.forEach(event -> {

			RAPromoDTO sampleObj = new RAPromoDTO();
			sampleObj.setLocationNo(String.valueOf(chainID));
			sampleObj.setLocationLevel(Constants.CHAIN);
			sampleObj.setEventId(event);
			sampleObj.setStatus(Constants.ACTIVE);
			locationInfo.add(sampleObj);

		});

		logger.info("setPromoLocations()-# items in storeToZoneMap :" + locationInfo.size());

	}

	private List<PromoDataStandardDTO> populateItemCode(List<PromoDataStandardDTO> stdformatList)
			throws Exception, GeneralException {

		List<PromoDataStandardDTO> finalItemList = new ArrayList<>();

		for (PromoDataStandardDTO item : stdformatList) {

			if (retailerItemCodeAndItsItem.containsKey(item.getItemCode())) {
				List<ItemDTO> itemcodeList = retailerItemCodeAndItsItem.get(item.getItemCode());

				for (ItemDTO itemCode : itemcodeList) {
					List<String> itemDetail = new ArrayList<String>();
					/*
					 * PromoDataStandardDTO promoDataStandardDTO = (PromoDataStandardDTO)
					 * item.clone(); promoDataStandardDTO.setUpc(itemCode.getUpc());
					 * promoDataStandardDTO.setItemName(itemCode.getItemName());
					 * promoDataStandardDTO.setPrestoItemCode(String.valueOf(itemCode.getItemCode())
					 * );
					 */
					String value = String.valueOf(itemCode.getItemCode()) + "-" + itemCode.getUpc() + "-"
							+ itemCode.getItemName();

					item.setPrestoItemCodeList(String.valueOf(itemCode.getItemCode()));

					if (item.getItemAndItsDetail().containsKey(item.getItemCode())) {
						itemDetail = item.getItemAndItsDetail().get(item.getItemCode());
					}
					itemDetail.add(value);

					item.setItemAndItsDetail(item.getItemCode(), itemDetail);
				}
				finalItemList.add(item);

			} else {
				itemCodeNotFound.add(item.getItemCode() + "," + item.getPromoID());
				logger.debug("Presto Itemcode not found for" + item.getPromoID() + ";" + item.getItemCode());
			}

		}

		logger.info("populateItemCode()-#items in List - " + finalItemList.size());

		return finalItemList;
	}

	private List<PromoDataStandardDTO> ConvertToStandardFormat(Map<String, List<RAPromoDTO>> finalMap) {
		logger.debug("ConvertToStandardFormat()-start");
		stdFormat = new ArrayList<>();
		finalMap.forEach((key, value) -> {

			for (RAPromoDTO finalRecords : value) {

				if (finalRecords.getEventGroupId() == null) {
					ignoredItem.add("Ignored from finalList as null " + finalRecords.getEventId() + ""
							+ finalRecords.getEventGroupId());
				} else {
					PromoDataStandardDTO promoFormat = new PromoDataStandardDTO();
					promoFormat.setPromoID(finalRecords.getEventId());
					promoFormat.setPromoStartDate(finalRecords.getStoreBeginDate());
					promoFormat.setPromoEndDate(finalRecords.getStoreEndDate());
					promoFormat.setPromoDescription(finalRecords.getEventDesc().replaceAll(",", ""));
					promoFormat.setLocationNo(finalRecords.getLocationNo());
					promoFormat.setItemCode(finalRecords.getCurrentitemNo());
					promoFormat.setStatus(finalRecords.getStatus());
					promoFormat.setDollarOff(finalRecords.getDollarOff());
					promoFormat.setPctOff(finalRecords.getPctOff());
					promoFormat.setBlockNumber(finalRecords.getBlockNo());
					promoFormat.setPageNumber(finalRecords.getAdPageNo());
					promoFormat.setSalePrice(finalRecords.getSalePrice());
					promoFormat.setSaleQty(finalRecords.getSaleQty());
					promoFormat.setBuyQty(finalRecords.getBuyQty());
					promoFormat.setGetQty(finalRecords.getGetQty());
					promoFormat.setCouponAmt(finalRecords.getCouponAmt());
					promoFormat.setCouponType(finalRecords.getAdType());
					promoFormat.setMustbuyPrice(finalRecords.getMustbuyPrice());
					promoFormat.setMustBuyQty(finalRecords.getMustBuyQty());
					promoFormat.setPromoGroup(finalRecords.getEventGroupId());
					promoFormat.setTypeCode(finalRecords.getTypeCode());
					promoFormat.setLocationLevel(finalRecords.getLocationLevel());
					promoFormat.setAnotherItem(finalRecords.getGetItemCode());

					stdFormat.add(promoFormat);
				}

			}

		});
		logger.info("#Items in stdFormat list :" + stdFormat.size());
		logger.debug("**ConvertToStandardFormat()-complete****");
		return stdFormat;
	}

	private void mergeRewardPromotions() throws Exception, GeneralException {
		logger.info("mergeRewardPromotions() - start:");
		Map<String, List<RAPromoDTO>> promoDetailMap = new HashMap<>();
		Map<String, List<RAPromoDTO>> itemDetailMap = new HashMap<>();
		Map<String, List<RAPromoDTO>> eventMap = new HashMap<>();
		Map<String, List<RAPromoDTO>> storedetailMap = new HashMap<>();
		Map<String, List<RAPromoDTO>> marketingMap = new HashMap<>();
		Map<String, List<RAPromoDTO>> maapdMap = new HashMap<>();

		Map<String, List<RAPromoDTO>> finalMap = new HashMap<>();

		Map<String, List<RAPromoDTO>> processMap = new HashMap<>();

		promoDetailMap = promotemp.stream().collect(Collectors.groupingBy(RAPromoDTO::getEventId));
		itemDetailMap = itemTemp.stream().collect(Collectors.groupingBy(RAPromoDTO::getEventId));
		eventMap = eventTemp.stream().collect(Collectors.groupingBy(RAPromoDTO::getEventId));
		storedetailMap = locationInfo.stream().collect(Collectors.groupingBy(RAPromoDTO::getEventId));
		marketingMap = marketingtemp.stream().collect(Collectors.groupingBy(RAPromoDTO::getEventId));

		logger.debug("#Items in initial promoDetailMap list :" + promoDetailMap.size());
		logger.debug("#Items in initial itemDetailMap list :" + itemDetailMap.size());
		logger.debug("#Items in initial storedetailMap list :" + storedetailMap.size());
		logger.debug("#Items in initial marketingMap list :" + marketingMap.size());

		fillinEvents(eventMap, finalMap, "EVENTFILE");

		logger.info("# items in FinalMap " + finalMap.size());

		int loopNo = 0;
		do {
			tempMap = new HashMap<>();
			counter = 0;
			finalMap.forEach((key, value) -> {
				List<RAPromoDTO> temp = finalMap.get(key);
				if (counter <= 50) {
					tempMap.put(key, temp);
					counter++;
				}
			});

			tempMap.forEach((key, value) -> {

				String keyToBeRemoved = key;

				Iterator<Entry<String, List<RAPromoDTO>>> iterator = finalMap.entrySet().iterator();

				while (iterator.hasNext()) {

					// Get the entry at this iteration
					Entry<String, List<RAPromoDTO>> entry = iterator.next();

					if (keyToBeRemoved == entry.getKey()) {

						iterator.remove();
					}
				}

			});
			loopNo++;
			logger.info("# items in FinalMap after loop : " + loopNo + " " + finalMap.size());

			ProcessInBatch(tempMap, promoDetailMap, itemDetailMap, storedetailMap, marketingMap, loopNo);

		} while (finalMap.size() > 0);

	}

	private void ProcessInBatch(Map<String, List<RAPromoDTO>> tempMap, Map<String, List<RAPromoDTO>> promoDetailMap,
			Map<String, List<RAPromoDTO>> itemDetailMap, Map<String, List<RAPromoDTO>> storeDetailMap,
			Map<String, List<RAPromoDTO>> marketingMap, int loopNo) throws Exception, GeneralException {

		Map<String, List<RAPromoDTO>> promoDetailMaptemp = new HashMap<>();
		Map<String, List<RAPromoDTO>> itemDetailMaptemp = new HashMap<>();
		Map<String, List<RAPromoDTO>> eventMaptemp = new HashMap<>();
		Map<String, List<RAPromoDTO>> storedetailMaptemp = new HashMap<>();
		Map<String, List<RAPromoDTO>> marketingMaptemp = new HashMap<>();

		tempMap.forEach((key, value) -> {

			String eventId = key;

			if (promoDetailMap.containsKey(key)) {
				promoDetailMaptemp.put(key, promoDetailMap.get(key));

			}
			if (storeDetailMap.containsKey(key)) {
				storedetailMaptemp.put(key, storeDetailMap.get(key));

			}

			if (itemDetailMap.containsKey(key)) {
				itemDetailMaptemp.put(key, itemDetailMap.get(key));

			}

			if (marketingMap.containsKey(key)) {
				marketingMaptemp.put(key, marketingMap.get(key));

			}

		});

		logger.debug("Processing STOREFILE ");
		applyPromoDetails(storedetailMaptemp, tempMap, "STOREFILE");

		logger.debug("Processing ITEMFILE");
		applyPromoDetails(itemDetailMaptemp, tempMap, "ITEMFILE");

		logger.debug("Processing PROMOFILE");
		applyPromoDetails(promoDetailMaptemp, tempMap, "PROMOFILE");

		if (buyitems.size() > 0 && getItems.size() > 0) {
			logger.debug("Processing ");
			fillDetails(buyitems, getItems, tempMap);

		}
		logger.debug("Processing MARKETINGFILE");
		applyPromoDetails(marketingMaptemp, tempMap, "MARKETINGFILE");

		logger.info("#items in TempMap:" + tempMap.size());

		List<PromoDataStandardDTO> stdformatList = ConvertToStandardFormat(tempMap);

		List<PromoDataStandardDTO> promoList = populateItemCode(stdformatList);

		if (promoList.size() > 0) {
			HashMap<String, List<PromoDataStandardDTO>> finalPromoMap = populateRegularPrice(promoList);

			logger.info("finalPromoList to write :" + finalPromoMap.size());
			writeToCsv(finalPromoMap, loopNo);

		}

		else {
			logger.warn("No itemcodes found to process  :" + promoList.size());
		}
		logNotProcessed(loopNo);

	}

	private void fillDetails(HashMap<String, List<RAPromoDTO>> buyitems, HashMap<String, List<RAPromoDTO>> getItems,
			Map<String, List<RAPromoDTO>> tempMap) {

		buyitems.forEach((key, value) -> {
			if (tempMap.containsKey(key) && getItems.containsKey(key)) {
				List<RAPromoDTO> existingRecords = tempMap.get(key);

				List<RAPromoDTO> getitems = getItems.get(key);

				for (RAPromoDTO buyRecord : value) {

					// System.out.println(buyRecord.getCurrentitemNo() + " ; " +
					// buyRecord.getLocationNo());
					for (RAPromoDTO getItem : getitems) {

						if (buyRecord.getEventGroupId().equals(getItem.getEventGroupId())
								&& buyRecord.getLocationNo().equals(getItem.getLocationNo())) {
							// System.out.println(getItem.getCurrentitemNo() + " ; " +
							// getItem.getLocationNo());
							try {
								RAPromoDTO promoDetail = (RAPromoDTO) buyRecord.clone();
								promoDetail.setGetItemCode(String.valueOf(getItem.getCurrentitemNo()));
								promoDetail.setGetQty(getItem.getGetQty());
								List<RAPromoDTO> newList = new ArrayList<>();
								if (tempMap.containsKey(key)) {
									newList.addAll(tempMap.get(key));
								}
								newList.add(promoDetail);
								tempMap.put(key, newList);

							} catch (CloneNotSupportedException e) {

								e.printStackTrace();
							}

						}
					}
				}
			}
			// }
		});
	}

	private void fillinEvents(Map<String, List<RAPromoDTO>> Map, Map<String, List<RAPromoDTO>> finalMap,
			String fileName) {

		if (fileName.equals("EVENTFILE")) {
			Map.forEach((key, value) -> {

				for (RAPromoDTO mapRecord : value) {
					finalMap.put(key, value);
				}

			});

		}

	}

	private void applyPromoDetails(Map<String, List<RAPromoDTO>> Map, Map<String, List<RAPromoDTO>> tempMap,
			String fileName) {

		if (fileName.equals("STOREFILE"))

		{
			Map.forEach((key, value) -> {
				if (tempMap.containsKey(key)) {

					List<RAPromoDTO> existingRecords = tempMap.get(key);
					List<RAPromoDTO> newList = new ArrayList<>();

					for (RAPromoDTO record : existingRecords) {
						for (RAPromoDTO mapRecord : value) {
							{
								if (mapRecord.getEventId().equals(key)
										&& mapRecord.getStatus().equals(Constants.ACTIVE)) {

									try {
										RAPromoDTO promoDetail = (RAPromoDTO) record.clone();
										promoDetail.setLocationNo(mapRecord.getLocationNo());
										promoDetail.setLocationLevel(mapRecord.getLocationLevel());
										newList.add(promoDetail);
									} catch (CloneNotSupportedException e) {
										logger.debug("applyPromoDetails()-Exception " + e);

									}
								} else if (!mapRecord.getStatus().equals(Constants.ACTIVE)) {
									ignoredItem.add("Ignored from store : " + mapRecord.getEventId() + ","
											+ mapRecord.getStatus() + "," + mapRecord.getCurrentitemNo());

								}
							}
						}

						if (newList.size() > 0) {
							tempMap.put(key, newList);
						}

					}

				}
			});
		}

		if (fileName.equals("ITEMFILE"))

		{
			Map.forEach((key, value) -> {
				if (tempMap.containsKey(key)) {

					List<RAPromoDTO> existingRecords = tempMap.get(key);
					List<RAPromoDTO> newList = new ArrayList<>();

					for (RAPromoDTO record : existingRecords) {
						for (RAPromoDTO mapRecord : value) {
							{
								if (mapRecord.getEventId().equals(key)
										&& mapRecord.getStatus().equals(Constants.ACTIVE)) {
									try {
										RAPromoDTO promoDetail = (RAPromoDTO) record.clone();
										promoDetail.setCurrentitemNo(mapRecord.getCurrentitemNo());
										promoDetail.setEventGroupId(mapRecord.getEventGroupId());
										newList.add(promoDetail);
									} catch (CloneNotSupportedException e) {
										logger.debug("applyPromoDetails()-Exception " + e);

									}
								} else if (!mapRecord.getStatus().equals(Constants.ACTIVE)) {
									ignoredItem.add("Ignored from itemfile : " + mapRecord.getEventId() + ","
											+ mapRecord.getStatus() + "," + mapRecord.getCurrentitemNo());

								}
							}
						}
						if (newList.size() > 0) {
							tempMap.put(key, newList);
						}

					}

				}
			});
		}

		if (fileName.equals("PROMOFILE"))

		{
			Map.forEach((key, value) -> {
				if (tempMap.containsKey(key)) {
					List<RAPromoDTO> existingRecords = tempMap.get(key);
					List<RAPromoDTO> newList = new ArrayList<>();
					List<RAPromoDTO> tempList = new ArrayList<>();

					for (RAPromoDTO record : existingRecords) {
						for (RAPromoDTO mapRecord : value) {
							{
								if (mapRecord.getEventId().equals(key)
										&& mapRecord.getCurrentitemNo().equals(record.getCurrentitemNo())
										&& mapRecord.getStatus().equals(Constants.ACTIVE)) {
									try {
										RAPromoDTO promoDetail = (RAPromoDTO) record.clone();
										promoDetail.setEventGroupId(mapRecord.getEventGroupId());
										setUpPromoFields(mapRecord, promoDetail);

										if (promoDetail.getStatusFlag() == 0) {
											newList.add(promoDetail);
										}
									} catch (CloneNotSupportedException e) {
										logger.info(e);
									}
								} else if (!mapRecord.getStatus().equals(Constants.ACTIVE)) {
									ignoredItem.add("Ignored from promoFile : " + mapRecord.getEventId() + ","
											+ mapRecord.getStatus() + "," + mapRecord.getCurrentitemNo());
								}
							}
						}
						if (newList.size() > 0) {
							tempMap.put(key, newList);
						}

					}

				}

			});
		}

		if (fileName.equals("MARKETINGFILE"))

		{
			Map.forEach((key, value) -> {
				if (tempMap.containsKey(key)) {

					List<RAPromoDTO> existingRecords = tempMap.get(key);
					List<RAPromoDTO> newList = new ArrayList<>();

					for (RAPromoDTO record : existingRecords) {
						for (RAPromoDTO mapRecord : value) {
							{
								if (mapRecord.getEventId().equals(key)
										&& mapRecord.getEventGroupId().equals(record.getEventGroupId()))
									try {
										RAPromoDTO promoDetail = (RAPromoDTO) record.clone();
										if (!mapRecord.getBlockNo().equals("null")
												&& !mapRecord.getAdPageNo().equals("null")) {
											promoDetail.setBlockNo(mapRecord.getBlockNo());
											promoDetail.setAdPageNo(mapRecord.getAdPageNo());
										}
										if (mapRecord.getCouponAmt() != 0) {
											promoDetail.setCouponAmt(mapRecord.getCouponAmt());
											if (!mapRecord.getAdType().equals("null")) {
												promoDetail.setAdType(mapRecord.getAdType());
											} else {
												logger.info("AdType null for: " + mapRecord.getEventId()
														+ " eventGroup Type" + mapRecord.getEventGroupId());
											}
											record.setStatusFlag(0);

										}

										if (record.getStatusFlag() == 1) {

											noPromo.add(mapRecord.getEventId() + "," + mapRecord.getCurrentitemNo()
													+ "," + mapRecord.getEventGroupId());
										} else {
											newList.add(promoDetail);
										}

									} catch (CloneNotSupportedException e) {
										logger.info(e);

									}
							}
						}
						if (newList.size() > 0) {

							tempMap.put(key, newList);
						}

					}

				}

			});
		}
	}

	private void setUpPromoFields(RAPromoDTO mapRecord, RAPromoDTO promoDetail) throws CloneNotSupportedException {

		promoDetail.setStatusFlag(0);

		if (mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_STANDARD) && mapRecord.getPrice() > 0
				&& mapRecord.getPrice2() == 0 && mapRecord.getBuyQty() == 0 && mapRecord.getGetQty() == 0) {

			promoDetail.setSalePrice(mapRecord.getPrice());
			promoDetail.setSaleQty(mapRecord.getPriceMultiple());

		}
		// dollarOff
		else if (mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_DOLLAR) && mapRecord.getPrice() > 0
				&& mapRecord.getPrice2() == 0 && mapRecord.getBuyQty() == 0 && mapRecord.getGetQty() == 0) {

			promoDetail.setDollarOff(mapRecord.getPrice());
		}
		// percentageOff
		else if ((mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_PERCENTAGE) && mapRecord.getPrice() > 0
				&& mapRecord.getPrice2() == 0 && mapRecord.getBuyQty() == 0 && mapRecord.getGetQty() == 0)
				|| (mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_PERCENTAGE) && mapRecord.getPrice() > 0
						&& mapRecord.getPriceMultiple() == 1 && mapRecord.getBuyQty() == 0 && mapRecord.getGetQty() == 0
						&& mapRecord.getPrice2() > 0 && mapRecord.getPriceMult2() == 1)

		) {

			promoDetail.setPctOff(mapRecord.getPrice());

		} // BOGO
		else if (mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_STANDARD) && mapRecord.getBuyQty() == 1
				&& mapRecord.getGetQty() == 1) {
			promoDetail.setBuyQty(mapRecord.getBuyQty());
			promoDetail.setGetQty(mapRecord.getGetQty());
		} // BOGO%
		else if (mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_PERCENTAGE) && mapRecord.getBuyQty() == 1
				&& mapRecord.getGetQty() == 1 && mapRecord.getPrice() > 0) {
			promoDetail.setBuyQty(mapRecord.getBuyQty());
			promoDetail.setGetQty(mapRecord.getGetQty());
			promoDetail.setPctOff(mapRecord.getPrice());

		} // BXGY
		else if ((mapRecord.getBuyQty() >= 1 && mapRecord.getGetQty() > 1)
				|| (mapRecord.getBuyQty() > 1 && mapRecord.getGetQty() >= 1)) {
			promoDetail.setBuyQty(mapRecord.getBuyQty());
			promoDetail.setGetQty(mapRecord.getGetQty());

		} // MustBuy
		else if (mapRecord.getPrice() > 0 && mapRecord.getPriceMultiple() > 1 && mapRecord.getPrice2() > 0
				&& mapRecord.getPriceMult2() == 1) {

			promoDetail.setSalePrice(mapRecord.getPrice2());
			promoDetail.setSaleQty(mapRecord.getPriceMult2());
			promoDetail.setMustBuyQty(mapRecord.getPriceMultiple());
			promoDetail.setMustbuyPrice(mapRecord.getPrice());
		}

		// BAGB
		// storebuyItemInfo
		else if (mapRecord.getBuyQty() > 0 && mapRecord.getMustBuyInd().equals("Y") && mapRecord.getGetQty() == 0) {

			RAPromoDTO promoInfo = (RAPromoDTO) promoDetail.clone();
			if (mapRecord.getPrice() > 0 && mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_STANDARD)) {
				promoInfo.setSalePrice(mapRecord.getPrice());
				promoInfo.setSaleQty(mapRecord.getPriceMultiple());
			} else if (mapRecord.getPrice() > 0
					&& mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_PERCENTAGE)) {
				promoInfo.setPctOff(mapRecord.getPrice());
			}
			promoInfo.setEventGroupId(mapRecord.getEventGroupId());
			promoInfo.setBuyQty(mapRecord.getBuyQty());

			List<RAPromoDTO> tempList = new ArrayList<RAPromoDTO>();
			if (buyitems.containsKey(mapRecord.getEventId())) {
				tempList.addAll(buyitems.get(mapRecord.getEventId()));
			}

			tempList.add(promoInfo);
			buyitems.put(mapRecord.getEventId(), tempList);

			promoDetail.setStatusFlag(2);
		}
		// coupon
		else if (mapRecord.getPriceCode().equals(Constants.OFFER_UNIT_TYPE_DOLLAR) && mapRecord.getPrice() == 0)

		{
			promoDetail.setStatusFlag(0);
		}
		// storegetItemInfo
		else if (mapRecord.getBuyQty() == 0 && mapRecord.getMustBuyInd().equals("N") && mapRecord.getGetQty() > 0) {

			RAPromoDTO promoInfo = (RAPromoDTO) promoDetail.clone();
			promoInfo.setEventGroupId(mapRecord.getEventGroupId());
			promoInfo.setGetQty(mapRecord.getGetQty());

			List<RAPromoDTO> tempList = new ArrayList<RAPromoDTO>();
			if (getItems.containsKey(mapRecord.getEventId())) {
				tempList.addAll(getItems.get(mapRecord.getEventId()));
			}

			tempList.add(promoInfo);
			getItems.put(mapRecord.getEventId(), tempList);
			promoDetail.setStatusFlag(2);
		} else {

			promoDetail.setStatusFlag(1);
		}

	}

	public void logNotProcessed(int loopNo) throws IOException {
		logger.info("logNotProcessed()-start");
		String fileName = getRootPath() + "/" + relativeInputPath + "/" + "NotProcessedRecords_ " + loopNo + ".csv";
		FileWriter writer = new FileWriter(fileName);
		try {

			writer.append("***priceNotFoundList #records:***" + priceNotFoundList.size());
			writer.append('\n');
			writer.append("EVENT ID ; ITEMCODE");
			writer.append('\n');
			for (String itemNotFound : priceNotFoundList) {
				writer.append(itemNotFound);
				writer.append('\n');

			}
			writer.append("***InValidPromo #records: ***" + noPromo.size());
			writer.append('\n');
			writer.append("EVENT ID ; ITEMCODE ,GROUPID");
			writer.append('\n');
			for (String itemNotFound : noPromo) {
				writer.append(itemNotFound);
				writer.append('\n');
			}
			writer.append("***ItemCodeNotfound #records: ***" + itemCodeNotFound.size());
			writer.append('\n');
			writer.append("EVENT ID ; ITEMCODE");
			writer.append('\n');
			for (String itemcodeNf : itemCodeNotFound) {
				writer.append(itemcodeNf);
				writer.append('\n');
			}
			writer.append("***IgnoredItem #records: ***" + ignoredItem.size());
			writer.append('\n');
			writer.append("FILENAME; EVENT ID ");
			writer.append('\n');
			for (String igItem : ignoredItem) {
				writer.append(igItem);
				writer.append('\n');
			}
			writer.append("***FutureDate  #records: ***" + futureDate.size());
			writer.append('\n');
			writer.append(" EVENT ID; DATE ");
			writer.append('\n');
			for (String fItem : futureDate) {
				writer.append(fItem);
				writer.append('\n');
			}

			logger.info("logNotProcessed()-Log Write Complete");

		} catch (IOException e) {

			logger.info("logNotProcessed()-Exception in fileWriter" + e);
		} finally {
			writer.flush();
			writer.close();
		}

	}

	private HashMap<String, List<PromoDataStandardDTO>> populateRegularPrice(List<PromoDataStandardDTO> mergedList)
			throws Exception, GeneralException {

		logger.debug("populateRegularPrice() - Inside populate RegPrice " + mergedList.size());
		/*
		 * LocalDate minDate =
		 * mergedList.stream().map(PromoDataStandardDTO::getPromoStartDateRAAsLocalDate)
		 * .min(LocalDate::compareTo).get(); // logger.info("minDate is " + minDate);
		 * week = PRCommonUtil.getDateFormatter().format(minDate);
		 */
		// logger.info("populateRegularPrice() - week:" + week);
		weekdate = new SimpleDateFormat(Constants.APP_DATE_FORMAT).parse(currentWkStartDt);
		weekEndDate = new SimpleDateFormat(Constants.APP_DATE_FORMAT).parse(endDate);

		logger.info("populateRegularPrice() - weekdate is : " + weekdate);
		HashMap<String, List<PromoDataStandardDTO>> finalMap = new HashMap<String, List<PromoDataStandardDTO>>();

		HashMap<String, Map<String, List<PromoDataStandardDTO>>> promoByWeek = new HashMap<String, Map<String, List<PromoDataStandardDTO>>>();

		List<PromoDataStandardDTO> promotempList = new ArrayList<>();

		for (PromoDataStandardDTO pr : mergedList) {

			DateFormat originalFormat = new SimpleDateFormat("yyyy-MM-dd");
			DateFormat targetFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			Date date = null;
			Date date2 = null;

			try {
				date = originalFormat.parse(pr.getPromoStartDate());
				date2 = originalFormat.parse(pr.getPromoEndDate());

			} catch (ParseException e) {

				e.printStackTrace();
			}
			pr.setPromoStartDate(targetFormat.format(date));

			pr.setPromoEndDate(targetFormat.format(date2));

			List<PromoDataStandardDTO> weeklypromo = splitPromoByWeek(pr, weekdate, weekEndDate);

			promotempList.addAll(weeklypromo);
		}

		for (PromoDataStandardDTO promo : promotempList) {

			for (String startDate : promo.getStarDateList()) {

				Map<String, List<PromoDataStandardDTO>> LocKeytempMap = new HashMap<>();
				List<PromoDataStandardDTO> tempList = new ArrayList<>();

				String startDt = startDate.split("-")[0];
				String endDt = startDate.split("-")[1];

				if (!promo.getLocationNo().isEmpty()) {
					tempList.add(promo);
					LocKeytempMap = tempList.stream()
							.collect(Collectors.groupingBy(PromoDataStandardDTO::getLocationNo));
				} else {
					logger.debug("Location not found for :" + promo.getPromoID() + " ;" + promo.getPromoGroup() + ";"
							+ promo.getItemCode());
				}
				if (promoByWeek.containsKey(startDate)) {
					LocKeytempMap = promoByWeek.get(startDate);
					if (promo.getLocationNo() != null && !promo.getLocationNo().isEmpty()) {
						if (LocKeytempMap.containsKey(promo.getLocationNo())) {
							tempList.addAll(LocKeytempMap.get(promo.getLocationNo()));

						}
					}
					LocKeytempMap.put(promo.getLocationNo(), tempList);
				}
				if (LocKeytempMap.size() > 0) {
					promoByWeek.put(startDate, LocKeytempMap);
				}

			}

		}
		List<PromoDataStandardDTO> finalListmap = new ArrayList<PromoDataStandardDTO>();
		String currentWeekStartDate = DateUtil.getWeekStartDate(0);
		LocalDate currentDate = LocalDate.parse(currentWeekStartDate, PRCommonUtil.getDateFormatter());
		int currentStartCalendarId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, currentWeekStartDate,
				Constants.CALENDAR_DAY)).getCalendarId();
		logger.info(
				"currentWeekStartDate: " + currentWeekStartDate + "currentStartCalendarId :" + currentStartCalendarId);
		promoByWeek.forEach((key, value) -> {
			for (Entry<String, List<PromoDataStandardDTO>> entry : value.entrySet()) {

				locationNo = 0;
				String startDt = key.split("-")[0];
				String endDate = key.split("-")[1];
				PromoDataStandardDTO sampleObj = entry.getValue().get(0);
				// int endCalendarId = sampleObj.getEndcalendarID();
				// if(endCalendarId!=0)
				// {
				sampleObj.setPromoStartDate(startDt);
				sampleObj.setPromoEndDate(endDate);

				int calIdfound = 0;
				try {
					calIdfound = setCalendarId(sampleObj);
				} catch (GeneralException e2) {
					logger.error("exception " + e2);
				}

				if (calIdfound == 0) {
					List<PromoDataStandardDTO> prestoItem = entry.getValue();

					Set<String> itemCodes = new HashSet<String>();
					int endCalendarId = sampleObj.getEndcalendarID();
					String gsnl = sampleObj.getPromoID();
					// Set<String> itemCodes =
					// entry.getValue().stream().map(PromoDataStandardDTO::getPrestoItemCode)
					// .collect(Collectors.toSet());

					for (PromoDataStandardDTO itemCode : prestoItem) {
						itemCodes.addAll(itemCode.getPrestoItemCodeList());
					}
					List<String> itemCodeList = new ArrayList<>(itemCodes);

					logger.debug("populateRegularPrice() - # of items: " + itemCodeList.size());

					HashMap<String, RetailPriceDTO> currentWeekPrice = new HashMap<>();

					DateFormat df = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
					Date foramtteDate = null;
					try {
						foramtteDate = new SimpleDateFormat(Constants.APP_DATE_FORMAT).parse(startDt);
					} catch (ParseException e1) {
						logger.error("populateRegularPrice()-Exception in " + e1);

					}
					LocalDate weekStartDate = LocalDate.parse(df.format(foramtteDate), PRCommonUtil.getDateFormatter());

					if (sampleObj.getLocationLevel() == Constants.STORELEVEL) {
						locationNo = Constants.STORE_LEVEL_TYPE_ID;
					}
					if (sampleObj.getLocationLevel() == Constants.ZONE
							|| sampleObj.getLocationLevel() == Constants.DIVISION) {
						locationNo = Constants.ZONE_LEVEL_TYPE_ID;
					}

					if (sampleObj.getLocationLevel() == Constants.CHAIN) {
						locationNo = Constants.CHAIN_LEVEL_TYPE_ID;
					}

					boolean isFuturePromo = false;

					logger.debug("eventi id is " + gsnl + ";" + " start calId id is " + sampleObj.getStartcalendarID()
							+ "end calId id is " + endCalendarId + "itemcode " + sampleObj.getItemCode() + "location  "
							+ sampleObj.getLocationNo());

					if (weekStartDate.isAfter(currentDate)) {

						try {
							currentWeekPrice = objPriceLoad.getRetailPrice(conn, sampleObj.getLocationNo(),
									itemCodeList, currentStartCalendarId, null, locationNo);
						} catch (GeneralException e) {
							logger.error("populateRegularPrice()-Exception in populating Price" + e);

						}
						isFuturePromo = true;
					}

					HashMap<String, RetailPriceDTO> priceDataMap = null;
					try {
						priceDataMap = objPriceLoad.getRetailPrice(conn, sampleObj.getLocationNo(), itemCodeList,
								endCalendarId, null, locationNo);
					} catch (GeneralException e) {
						logger.error("populateRegularPrice()-Exception in populating Price" + e);
					}

					for (PromoDataStandardDTO promoStandardDTO : entry.getValue()) {

						boolean isPricePopulated = false;
						for (String prestoItmCode : promoStandardDTO.getPrestoItemCodeList()) {
							// Check future price or corresponding price data is present is for this item
							if (priceDataMap != null) {
								if (priceDataMap.containsKey(prestoItmCode)) {

									RetailPriceDTO retailPriceDTO = priceDataMap.get(prestoItmCode);

									setPrice(promoStandardDTO, retailPriceDTO, startDt, prestoItmCode);

									isPricePopulated = true;

								} else if (isFuturePromo && currentWeekPrice.containsKey(prestoItmCode)) {

									// Check current week price data for this item if it is future and there is no
									// future price data

									RetailPriceDTO retailPriceDTO = currentWeekPrice.get(prestoItmCode);

									setPrice(promoStandardDTO, retailPriceDTO, startDt, prestoItmCode);

									isPricePopulated = true;
								}
							}
							if (!isPricePopulated) {
								priceNotFoundList
										.add(promoStandardDTO.getPromoID() + " ;" + promoStandardDTO.getItemCode());
							}
						}
						// Add items only with price
						if (promoStandardDTO.getPriceMap().size() > 0) {
							List<PromoDataStandardDTO> finalList = new ArrayList<>();
							if (finalMap.containsKey(key)) {
								finalList = finalMap.get(key);
							}
							finalList.add(promoStandardDTO);
							finalMap.put(key, finalList);
						}

					}
				}
			}
		});

		logger.info("populateRegularPrice()-#items in FinalList is : " + finalMap.size());
		return finalMap;

	}

	private void setPrice(PromoDataStandardDTO promoStandardDTO, RetailPriceDTO retailPriceDTO, String date,
			String prestoItmCode) {

		String regprice = retailPriceDTO.getRegMPrice() > 0 ? String.valueOf(retailPriceDTO.getRegMPrice())
				: String.valueOf(retailPriceDTO.getRegPrice());
		String regQnty = retailPriceDTO.getRegQty() == 0 ? "1" : String.valueOf(retailPriceDTO.getRegQty());

		promoStandardDTO.addRegPrice(date + "-" + prestoItmCode + "-" + promoStandardDTO.getLocationNo(),
				regprice + "-" + regQnty);
	}

	private List<PromoDataStandardDTO> splitPromoByWeek(PromoDataStandardDTO pr, Date processingWeek, Date weekEndDate)
			throws Exception, GeneralException {
		// logger.info("splitPromoByWeek - inside" );
		List<PromoDataStandardDTO> promoList = new ArrayList<>();

		Date promoStartDate = getFirstDateOfWeek(sdf.parse(pr.getPromoStartDate()));
		Date promoEndDate = getLastDateOfWeek(sdf.parse(pr.getPromoEndDate()));

		pr.setpStartDate(promoStartDate);
		pr.setpEndDate(promoEndDate);
		pr.getStarDateList().clear();
		long diff = promoEndDate.getTime() - promoStartDate.getTime();

		long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

		if (diffDates > 7) {

			float dif = (float) diffDates / 7;
			int noOfWeeks = (int) Math.ceil(dif);

			for (int i = 0; i < noOfWeeks; i++) {
				// PromoDataStandardDTO promoNew = (PromoDataStandardDTO) pr.clone();
				pr.setpStartDate(promoStartDate);
				promoEndDate = DateUtil.incrementDate(promoStartDate, 6);
				promoStartDate = DateUtil.incrementDate(promoStartDate, 7);
				pr.setpEndDate(promoEndDate);
				if (pr.getpStartDate().compareTo(processingWeek) >= 0
						&& pr.getpStartDate().compareTo(weekEndDate) <= 0) {
					String startDate = sdf.format(pr.getpStartDate());
					String endDate = sdf.format(pr.getpEndDate());
					pr.addStarDateList(startDate + "-" + endDate);
					pr.addEndDateList(endDate);

				}
			}
		} else {
			if (promoStartDate.compareTo(processingWeek) >= 0 && pr.getpStartDate().compareTo(weekEndDate) <= 0) {
				String startDate = sdf.format(promoStartDate);
				String endDate = sdf.format(promoEndDate);
				pr.addStarDateList(startDate + "-" + endDate);
				pr.addEndDateList(endDate);

			}
		}

		if (pr.getStarDateList().size() > 0) {
			promoList.add(pr);
		}

		return promoList;

	}

	/**
	 * 
	 * @param inputDate
	 * @return week start date for a given date
	 * @throws ParseException
	 */
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}

	/**
	 * 
	 * @param inputDate
	 * @return week end date for a given date
	 * @throws ParseException
	 */
	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date endDate = appDateFormatter.parse(strDate);
		Date outputDate = DateUtil.incrementDate(endDate, 6);
		return outputDate;
	}

	private int setCalendarId(PromoDataStandardDTO promoDTO) throws GeneralException {
		int calIDfound = 0;
		String promoStartDate = (promoDTO.getPromoStartDate());
		String promoEndDate = (promoDTO.getPromoEndDate());

		// Get and set start calendar id
		promoDTO.setStartcalendarID(getCalendarId(promoStartDate));
		if (promoDTO.getStartcalendarID() == 0) {
			logger.debug("setCalendarId()-Promo Start date: " + promoStartDate);
			futureDate.add("future start Date  for :" + promoDTO.getPromoID() + " Promo End date" + promoStartDate);
			calIDfound = 1;
		}
		// Get and set end calendar id
		promoDTO.setEndcalendarID(getCalendarId(promoEndDate));

		if (promoDTO.getEndcalendarID() == 0) {
			logger.debug("setCalendarId()-Promo endDate : " + promoEndDate);
			futureDate.add("future end Date  for :" + promoDTO.getPromoID() + " Promo End date" + promoEndDate);
			calIDfound = 1;
		}
		return calIDfound;
	}

	/**
	 * 
	 * @param inputDate
	 * @return day calendar id for given input
	 * @throws GeneralException
	 */
	private int getCalendarId(String inputDate) throws GeneralException {
		if (calendarMap.get(inputDate) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_DAY))
					.getCalendarId();
			calendarMap.put(inputDate, startCalId);
		}

		return calendarMap.get(inputDate);
	}

	private void initializeData() throws GeneralException {

		itemCodeNotFound = new HashSet<>();
		priceNotFoundList = new HashSet<>();
		ignoredItem = new HashSet<>();
		noPromo = new HashSet<>();
		futureDate = new HashSet<>();
		objPriceLoad = new PriceDataLoad();
		try {

			int noOfWeeks = Integer.parseInt(PropertyManager.getProperty("WEEKS.TOLOAD", "13"));
			thresholdValue = Double.parseDouble(PropertyManager.getProperty("STORE.PERECNT", "0.9"));
			curweekcalId = calDAO.getWeekCalendarFromDate(conn, currentWkStartDt).getCalendarId();
			endDate = calDAO.getXweekCalId(conn, noOfWeeks, curweekcalId);
			logger.info("End date: -" + endDate);
			ItemDAO itemDAO = new ItemDAO();
			StoreDAO storeDAO = new StoreDAO();
			CompStoreDAO compStoreDAO = new CompStoreDAO();
			chainID = compStoreDAO.getCompChainId(conn);
			logger.info("initializeData() - Getting all stores in zone started...");
			storeToZoneMap = storeDAO.getZoneandStoreInfo(conn);
			logger.info("# items in zoneAndItsStores: " + zoneAndItsStores.size());

			logger.info("initializeData() - Getting stores in zone count...");
			zoneAndItsStoreCount = storeDAO.getZoneStrcount(conn);

			int activeZonesCount = storeDAO.getActiveZonesCount(conn);

			String v = String.valueOf(thresholdValue * activeZonesCount);
			int index = v.indexOf(".");

			activeZonesThreshold = Integer.parseInt(v.substring(0, index));

			logger.info("initializeData() - Getting all stores in zone completed...");

			logger.info("initializeData() - Getting all items started...");
			activeItems = itemDAO.getAllActiveItems(conn);
			logger.info("initializeData() -	Active items" + activeItems.size());

			logger.info("initializeData() - Getting all items complete.....");

			logger.info("initializeData() - groupItemByRetailerItemCode started...");
			groupItemByRetailerItemCode();
			logger.info("initializeData() - groupItemByRetailerItemCodes end...");

		} catch (GeneralException | Exception e) {
			throw new GeneralException("initializeData() - Error while initializing cache", e);
		}
	}

	private void groupItemByRetailerItemCode() {
		retailerItemCodeAndItsItem = (HashMap<String, List<ItemDTO>>) activeItems.stream()
				.filter(p -> p.getRetailerItemCode() != null && !p.getRetailerItemCode().equals("null"))
				.collect(Collectors.groupingBy(ItemDTO::getRetailerItemCode));
	}

	private void fillMAEGXfile() {

		allColumns5.put(0, "eventId");
		allColumns5.put(1, "versId");
		allColumns5.put(2, "eventGroupId");
		allColumns5.put(3, "prjRegn");
		allColumns5.put(4, "projYearwKSrt");
		allColumns5.put(5, "projYearwKEnd");
		allColumns5.put(6, "evtGrpType");
		allColumns5.put(7, "currentitemNo");
		allColumns5.put(8, "adPageNo");
		allColumns5.put(9, "blockNo");
		allColumns5.put(10, "creator");
		allColumns5.put(11, "lastmainTime");
		allColumns5.put(12, "lastmainOpt");
		allColumns5.put(13, "lastmainTran");
		allColumns5.put(14, "blockPrty");
		allColumns5.put(15, "aprvlInd");
		allColumns5.put(16, "versPrcCode");
		allColumns5.put(17, "price");
		allColumns5.put(18, "pricMultiple");
		allColumns5.put(19, "scanBkAlw");
		allColumns5.put(20, "scanBkPct");
		allColumns5.put(21, "avgStrSales");
		allColumns5.put(22, "evtInclInd");
		allColumns5.put(23, "createdTime");
		allColumns5.put(24, "prjSalUnit");
		allColumns5.put(25, "prjSalDlr");
		allColumns5.put(26, "prjDmDlr");
		allColumns5.put(27, "prjMkdnDlr");
		allColumns5.put(28, "prjExtCost");
		allColumns5.put(29, "prjItmCnt");
		allColumns5.put(30, "dstnMeth");
		allColumns5.put(31, "oiAlwAmt");
		allColumns5.put(32, "bbAlwAmt");
		allColumns5.put(33, "adPictInd");
		allColumns5.put(34, "cntrcId");
		allColumns5.put(35, "coopDlrAmt");
		allColumns5.put(36, "mixMtchCode");
		allColumns5.put(37, "rebateAmt");
		allColumns5.put(38, "couponAmt");
		allColumns5.put(39, "adType");
		allColumns5.put(40, "promoNo");
		allColumns5.put(41, "crdReqInd");
		allColumns5.put(42, "grpTypeInd");
		allColumns5.put(43, "crdReqInd2");

	}

	private void fillMAAPDfile() {

		allColumns4.put(0, "eventId");
		allColumns4.put(1, "versId");
		allColumns4.put(2, "adPageNo");
		allColumns4.put(3, "blockNo");
		allColumns4.put(4, "blockDesc");
		allColumns4.put(5, "xRatio");
		allColumns4.put(6, "yRatio");
		allColumns4.put(7, "hghtRatio");
		allColumns4.put(8, "lenRatio");
		allColumns4.put(9, "crtoprId");
		allColumns4.put(10, "crtTsmp");
		allColumns4.put(11, "lstmntTsmp");
		allColumns4.put(12, "lstmnTrn");
		allColumns4.put(13, "lstoprId");
		allColumns4.put(14, "prmryInd");
		allColumns4.put(15, "blkPrmoType");
		allColumns4.put(16, "blockPrty");
		allColumns4.put(17, "ovrdInd");
	}

	private void fillEPSTRfile() {

		allColumns2.put(0, "eventId");
		allColumns2.put(1, "locationNo");
		allColumns2.put(2, "versId");
		allColumns2.put(3, "status");
		allColumns2.put(4, "modelStrNo");
		allColumns2.put(5, "projStrInd");
		allColumns2.put(6, "excludeRepInd");
		/*
		 * allColumns2.put(7, "extracol1"); allColumns2.put(8, "extracol2");
		 * allColumns2.put(9, "extracol3");
		 */
		allColumns2.put(10, "strBeginDate");
		allColumns2.put(11, "strEndDate");
	}

	private void fillEPVETfile() {
		allColumns.put(0, "eventId");
		allColumns.put(1, "groupId");
		allColumns.put(2, "typeCode");
		allColumns.put(3, "subTypeCode");
		allColumns.put(4, "status");
		allColumns.put(5, "activityStageId");
		allColumns.put(6, "actBeginDate");
		allColumns.put(7, "actEndDate");
		allColumns.put(8, "storeBeginDate");
		allColumns.put(9, "storeEndDate");
		allColumns.put(10, "eventDesc");
		allColumns.put(14, "themeCode");

		allColumns.put(15, "reportDate");
		allColumns.put(16, "adLinkCd");
		allColumns.put(17, "leadVer");
		allColumns.put(18, "regionNo");
		allColumns.put(19, "acqInd");

	}

	private void fillEPITMfile() {
		allColumns1.put(0, "eventId");
		allColumns1.put(1, "itemNum");
		allColumns1.put(2, "vendSbsy");
		allColumns1.put(3, "status");
		allColumns1.put(4, "modelItemNum");
		allColumns1.put(5, "mdlSubcls");
		allColumns1.put(6, "currentitemNo");
		// allColumns1.put(7, "extraColumn");
		allColumns1.put(8, "invUnits");
		allColumns1.put(9, "dspnInd");
		allColumns1.put(10, "totSalesDlr");
		allColumns1.put(11, "totSalesUnit");
		allColumns1.put(12, "couponAmt");
		allColumns1.put(13, "rebateAmt");
		allColumns1.put(14, "newImgInd");
		/*
		 * allColumns1.put(15, "extracolumns"); allColumns1.put(16, "extraColumn1");
		 * allColumns1.put(17, "extraColumn2");
		 */
		allColumns1.put(18, "wkAvgSale");
		allColumns1.put(19, "mdAprIind");
	}

	private void fillEPVITfile() {

		allColumns3.put(0, "eventId");
		allColumns3.put(1, "versId");
		// allColumns3.put(2, "extracol1");
		allColumns3.put(3, "itemNo");
		allColumns3.put(4, "currentitemNo");
		allColumns3.put(5, "status");
		allColumns3.put(6, "price");
		allColumns3.put(7, "verPrcUom");
		allColumns3.put(8, "priceMultiple");
		allColumns3.put(9, "priceCode");
		allColumns3.put(10, "priceChgInd");
		allColumns3.put(11, "avgStrSale");
		// allColumns3.put(12, "extracol2");
		allColumns3.put(13, "dstnMeth");
		allColumns3.put(14, "poMeth");
		allColumns3.put(15, "scanBkAlw");
		allColumns3.put(16, "itmRefNo");
		allColumns3.put(17, "adPageId");
		allColumns3.put(18, "adPctrInd");
		// allColumns3.put(19, "extracol3");
		// allColumns3.put(20, "extracol4");
		// allColumns3.put(21, "extracol5");
		allColumns3.put(22, "wgtngFctr");
		allColumns3.put(23, "InvoiceAlwAmt");
		allColumns3.put(24, "bbAlwAmt");
		allColumns3.put(25, "totProjSales");
		allColumns3.put(26, "mixMtchCode");
		allColumns3.put(27, "cntrId");
		allColumns3.put(28, "coopDlrAmt");
		allColumns3.put(29, "rvwCode");
		allColumns3.put(30, "projMeth");
		allColumns3.put(31, "itmGrpCode");
		// allColumns3.put(32, "extracol6");
		// allColumns3.put(33, "extracol7");
		allColumns3.put(34, "actualSalesDlr");
		allColumns3.put(35, "actualSalesUnits");
		allColumns3.put(36, "prjItemSalesDlr");
		allColumns3.put(37, "actItemmdDlr");
		allColumns3.put(38, "prjItemmdDlr");
		allColumns3.put(39, "prjItemGmDlr");
		allColumns3.put(40, "actItemGmDlr");
		allColumns3.put(41, "pvtItlbInd");
		allColumns3.put(42, "dsdInd");
		allColumns3.put(43, "actItemwtAmt");
		allColumns3.put(44, "onhandQty");
		allColumns3.put(45, "itemAvgCost");
		allColumns3.put(46, "bltcrtInd");
		allColumns3.put(47, "eventGroupId");
		allColumns3.put(48, "mustBuyInd");
		allColumns3.put(49, "avgPromoUnit");
		allColumns3.put(50, "avgPromoRtl");
		allColumns3.put(51, "buyQty");
		allColumns3.put(52, "getQty");
		allColumns3.put(53, "price2");
		allColumns3.put(54, "priceMult2");
		// allColumns3.put(55, "priceCode2");

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void parseFile() {
		try {

			logger.info("parseFile()-**Read Start*****");
			// getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);

			// Start with -1 so that if any regular files are present, they are processed
			// first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			parser = new ExcelFileParserV2();
			parser.setFirstRowToProcess(0);

			String zipFilePath = getRootPath() + "/" + relativeInputPath;

			eventfile = new ArrayList<RAPromoDTO>();
			itemFile = new ArrayList<RAPromoDTO>();
			storeFile = new ArrayList<RAPromoDTO>();
			promoFile = new ArrayList<RAPromoDTO>();
			marketingFile = new ArrayList<RAPromoDTO>();
			maapdFile = new ArrayList<RAPromoDTO>();

			List<RAPromoDTO> temp = new ArrayList<RAPromoDTO>();
			List<RAPromoDTO> tempItem = new ArrayList<RAPromoDTO>();
			List<RAPromoDTO> tempStore = new ArrayList<RAPromoDTO>();
			List<RAPromoDTO> tempPromo = new ArrayList<RAPromoDTO>();
			List<RAPromoDTO> tempMarketing = new ArrayList<RAPromoDTO>();
			List<RAPromoDTO> tempMaapd = new ArrayList<RAPromoDTO>();

			do {
				ArrayList<String> fileList = null;

				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(relativeInputPath);

					for (int i = 0; i < fileList.size(); i++) {

						String files = fileList.get(i);

						String outputFileName[] = files.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);

						String fileName = outputFileName[outputFileName.length - 1].replace(".txt", "").toUpperCase();

						if (fileName.contains("EPEVTAD") || fileName.contains("EPEVTCM") || fileName.contains("EPEVTOT")
								|| fileName.contains("EPEVTTP")) {
							processingFile = "EVENTFILE";
							String fieldNames[] = new String[allColumns.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}

							temp = parser.parseCSVFile(RAPromoDTO.class, files, allColumns, ',');
							eventfile.addAll(temp);

							logger.info("#records Read from " + fileName + ":" + temp.size());
							logger.info("#records in List:  " + eventfile.size());

						}
						if (fileName.contains("EPITMAD") || fileName.contains("EPITMCM") || fileName.contains("EPITMOT")
								|| fileName.contains("EPITMTP")) {
							processingFile = "ITEMFILE";
							String fieldNames[] = new String[allColumns1.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns1.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}

							tempItem = parser.parseCSVFile(RAPromoDTO.class, files, allColumns1, ',');
							itemFile.addAll(tempItem);
							logger.info("#records Read from " + fileName + ":" + tempItem.size());
							logger.info("#records in List:  " + itemFile.size());
						}

						if (fileName.contains("EPSTRAD") || fileName.contains("EPSTRCM") || fileName.contains("EPSTROT")
								|| fileName.contains("EPSTRTP")) {
							processingFile = "STOREFILE";
							String fieldNames[] = new String[allColumns2.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns2.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}

							tempStore = parser.parseCSVFile(RAPromoDTO.class, files, allColumns2, ',');
							storeFile.addAll(tempStore);
							logger.info("#records Read from " + fileName + ":" + tempStore.size());
							logger.info("#records in List : " + storeFile.size());
						}

						if (fileName.contains("EPVITAD") || fileName.contains("EPVITCM") || fileName.contains("EPVITOT")
								|| fileName.contains("EPVITTP")) {
							processingFile = "PROMOFILE";
							String fieldNames[] = new String[allColumns3.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns3.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}

							tempPromo = parser.parseCSVFile(RAPromoDTO.class, files, allColumns3, ',');
							promoFile.addAll(tempPromo);
							logger.info("#records Read from " + fileName + ":" + tempPromo.size());
							logger.info("#records in List : " + promoFile.size());
						}

						if (fileName.contains("MAAPDAD") || fileName.contains("MAAPDCM") || fileName.contains("MAAPDOT")
								|| fileName.contains("MAAPDTP")) {
							processingFile = "MAAPDFILE";
							String fieldNames[] = new String[allColumns4.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns4.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}

							tempMaapd = parser.parseCSVFile(RAPromoDTO.class, files, allColumns4, ',');
							maapdFile.addAll(tempMaapd);
							logger.info("#records Read from " + fileName + ":" + tempMaapd.size());
							logger.info("#records in List:  " + maapdFile.size());
						}

						if (fileName.contains("MAEGXAD") || fileName.contains("MAEGXCM") || fileName.contains("MAEGXOT")
								|| fileName.contains("MAEGXTP")) {
							processingFile = "MARKETINGFILE";
							String fieldNames[] = new String[allColumns5.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns5.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}

							tempMarketing = parser.parseCSVFile(RAPromoDTO.class, files, allColumns5, ',');
							marketingFile.addAll(tempMarketing);
							logger.info("#records Read from " + fileName + ":" + tempMarketing.size());
							logger.info("#records in List:  " + marketingFile.size());
						}

					}
				} catch (GeneralException | Exception ex) {
					logger.error("parseFile()-GeneralException", ex);

				}
				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}

				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());
			logger.info("parseFile()- ******Reading Files Complete*****");
		} catch (Exception ex) {
			logger.error("parseFile()- Outer Exception - JavaException", ex);

		}

	}

	public ArrayList<String> getFiles(String specificPath) throws GeneralException {
		String fullPath = rootPath;
		if (specificPath != null && specificPath.trim().length() > 0) {
			fullPath = fullPath + "/" + specificPath;
		}

		ArrayList<String> fileList = new ArrayList<String>();

		File dir = new File(fullPath);

		File[] files = dir.listFiles();

		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				String filename = files[i].getName();

				if (filename.toLowerCase().contains(".txt") || filename.toLowerCase().contains(".csv")
						|| filename.toLowerCase().contains(".xls")) {
					fileList.add(fullPath + "/" + filename);
					logger.info("getFiles() - Valid data file found. name=" + filename + ", Bytes=" + files[i].length()
							+ ", canRead=" + files[i].canRead() + ", lastModified="
							+ new Date(files[i].lastModified()));
				} else if (filename.toLowerCase().contains(".zip")) {
					logger.info("getFiles() - Zip file found.        name=" + filename + ", Bytes=" + files[i].length()
							+ ", canRead=" + files[i].canRead() + ", lastModified="
							+ new Date(files[i].lastModified()));
				} else {
					logger.warn("getFiles() - INVALID file found.    name=" + filename + ", Bytes=" + files[i].length()
							+ ", canRead=" + files[i].canRead() + ", lastModified="
							+ new Date(files[i].lastModified()));
				}
			}
		}

		return fileList;
	}

	public String getRootPath() {
		return rootPath;
	}

	private ArrayList<String> getZipFiles(String specificPath) {
		String fullPath = rootPath;
		if (specificPath != null && specificPath.trim().length() > 0) {
			fullPath = fullPath + "/" + specificPath;
		}
		ArrayList<String> fileList = new ArrayList<String>();

		File dir = new File(fullPath);
		String[] children = dir.list();
		if (children != null) {
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				String filename = children[i];

				// logger.debug(filename);
				if (filename.contains(".zip")) {
					fileList.add(fullPath + "/" + filename);
					logger.info("getZipFiles() - filename=" + filename);
				}
			}
		}
		return fileList;
	}

	private void writeToCsv(HashMap<String, List<PromoDataStandardDTO>> stdformatList, int loopNo)
			throws GeneralException, IOException {

		String csvOutputPath = getRootPath() + "/" + relativeOutputpath + "/" + "PromoFormatted_" + loopNo + "_"
				+ new Date().getTime() + ".csv";

		fw = new FileWriter(csvOutputPath);
		pw = new PrintWriter(fw);

		String csvStrOutputPath2 = getRootPath() + "/" + storeOutputpath + "/" + "StoreFeed_" + loopNo + "_"
				+ new Date().getTime() + ".csv";
		fw1 = new FileWriter(csvStrOutputPath2);
		pw1 = new PrintWriter(fw1);

		writeHeader();
		String separtor = ",";
		for (Entry<String, List<PromoDataStandardDTO>> promoMap : stdformatList.entrySet()) {
			List<PromoDataStandardDTO> promoFinlList = promoMap.getValue();
			for (PromoDataStandardDTO promo : promoFinlList) {
				if (promoMap.getKey() != null) {
					String startDate = promoMap.getKey().split("-")[0];
					String endDate = promoMap.getKey().split("-")[1];
					HashMap<String, List<String>> itemDetailMap = promo.getItemAndItsDetail();
					for (Entry<String, List<String>> itemDetail : itemDetailMap.entrySet()) {
						for (String detail : itemDetail.getValue()) {
							String regPriceNQnty = null;
							String prestoItmCode = detail.split("-")[0];
							String upc = detail.split("-")[1];
							String itemName = detail.split("-")[2];

							regPriceNQnty = promo.getPriceMap()
									.get(startDate + "-" + prestoItmCode + "-" + promo.getLocationNo());
							if (regPriceNQnty != null && !regPriceNQnty.isEmpty()) {
								if (!promo.getLocationLevel().equalsIgnoreCase("STORE")) {
									promo.setEverydayPrice(regPriceNQnty.split("-")[0]);
									promo.setEverdayQty(regPriceNQnty.split("-")[1]);
									setSaleprice(promo);

									pw.print(promo.getCategory() != null ? promo.getCategory() : "");
									pw.print(separtor);
									pw.print(startDate);
									pw.print(separtor);
									pw.print(endDate);
									pw.print(separtor);
									pw.print(promo.getPromoID());
									pw.print(separtor);
									pw.print(promo.getPromoDescription());
									pw.print(separtor);
									pw.print(itemDetail.getKey());
									pw.print(separtor);
									pw.print(upc);
									pw.print(separtor);
									pw.print(itemName);
									pw.print(separtor);
									pw.print(promo.getLirName() != null ? promo.getLirName() : "");
									pw.print(separtor);
									pw.print(promo.getPromoGroup());
									pw.print(separtor);
									pw.print(promo.getEverdayQty());
									pw.print(separtor);
									pw.print(promo.getEverydayPrice());
									pw.print(separtor);
									pw.print(promo.getSaleQty());
									pw.print(separtor);
									pw.print(promo.getSalePrice());
									pw.print(separtor);
									pw.print(promo.getMustBuyQty() > 0 ? promo.getMustBuyQty() : 0);
									pw.print(separtor);
									pw.print(promo.getMustbuyPrice() > 0 ? promo.getMustbuyPrice() : 0);
									pw.print(separtor);
									pw.print(promo.getDollarOff() > 0 ? promo.getDollarOff() : 0);
									pw.print(separtor);
									pw.print(promo.getPctOff() > 0 ? promo.getPctOff() : 0);
									pw.print(separtor);
									pw.print(promo.getBuyQty() > 0 ? promo.getBuyQty() : 0);
									pw.print(separtor);
									pw.print(promo.getGetQty() > 0 ? promo.getGetQty() : 0);
									pw.print(separtor);
									pw.print(promo.getMinimumQty() > 0 ? promo.getMinimumQty() : 0);
									pw.print(separtor);
									pw.print(promo.getMinimumAmt() > 0 ? promo.getMinimumAmt() : 0);
									pw.print(separtor);
									pw.print(promo.getBmsmDollaroffperunits() > 0 ? promo.getBmsmDollaroffperunits()
											: 0);
									pw.print(separtor);
									pw.print(promo.getBmsmPctoffperunit() > 0 ? promo.getBmsmPctoffperunit() : 0);
									pw.print(separtor);
									pw.print(promo.getBmsmsaleQty() > 0 ? promo.getBmsmsaleQty() : 0);
									pw.print(separtor);
									pw.print(promo.getBmsmsalePrice() > 0 ? promo.getBmsmsalePrice() : 0);
									pw.print(separtor);
									pw.print(promo.getStatus());
									pw.print(separtor);
									pw.print(promo.getLocationLevel());
									pw.print(separtor);
									pw.print(promo.getLocationNo());
									pw.print(separtor);
									pw.print(promo.getPageNumber() != null ? promo.getPageNumber() : "");
									pw.print(separtor);
									pw.print(promo.getBlockNumber() != null ? promo.getBlockNumber() : "");
									pw.print(separtor);
									pw.print(promo.getDisplayOffer() != null ? promo.getDisplayOffer() : "");
									pw.print(separtor);
									pw.print(promo.getDescription() != null ? promo.getDescription() : 0);
									pw.print(separtor);
									pw.print(promo.getCouponType() != null ? promo.getCouponType() : "");
									pw.print(separtor);
									pw.print(promo.getCouponAmt() > 0 ? promo.getCouponAmt() : 0);
									pw.print(separtor);
									pw.print(promo.getTypeCode());
									pw.print(separtor);
									pw.print(promo.getAnotherItem() != null ? promo.getAnotherItem() : "");
									pw.println();
								} else {
									pw1.print(promo.getCategory() != null ? promo.getCategory() : "");
									pw1.print(separtor);
									pw1.print(startDate);
									pw1.print(separtor);
									pw1.print(endDate);
									pw1.print(separtor);
									pw1.print(promo.getPromoID());
									pw1.print(separtor);
									pw1.print(promo.getPromoDescription());
									pw1.print(separtor);
									pw1.print(itemDetail.getKey());
									pw1.print(separtor);
									pw1.print(upc);
									pw1.print(separtor);
									pw1.print(itemName);
									pw1.print(separtor);
									pw1.print(promo.getLirName() != null ? promo.getLirName() : "");
									pw1.print(separtor);
									pw1.print(promo.getPromoGroup());
									pw1.print(separtor);
									pw1.print(promo.getEverdayQty());
									pw1.print(separtor);
									pw1.print(promo.getEverydayPrice());
									pw1.print(separtor);
									pw1.print(promo.getSaleQty());
									pw1.print(separtor);
									pw1.print(promo.getSalePrice());
									pw1.print(separtor);
									pw1.print(promo.getMustBuyQty() > 0 ? promo.getMustBuyQty() : 0);
									pw1.print(separtor);
									pw1.print(promo.getMustbuyPrice() > 0 ? promo.getMustbuyPrice() : 0);
									pw1.print(separtor);
									pw1.print(promo.getDollarOff() > 0 ? promo.getDollarOff() : 0);
									pw1.print(separtor);
									pw1.print(promo.getPctOff() > 0 ? promo.getPctOff() : 0);
									pw1.print(separtor);
									pw1.print(promo.getBuyQty() > 0 ? promo.getBuyQty() : 0);
									pw1.print(separtor);
									pw1.print(promo.getGetQty() > 0 ? promo.getGetQty() : 0);
									pw1.print(separtor);
									pw1.print(promo.getMinimumQty() > 0 ? promo.getMinimumQty() : 0);
									pw1.print(separtor);
									pw1.print(promo.getMinimumAmt() > 0 ? promo.getMinimumAmt() : 0);
									pw1.print(separtor);
									pw1.print(promo.getBmsmDollaroffperunits() > 0 ? promo.getBmsmDollaroffperunits()
											: 0);
									pw1.print(separtor);
									pw1.print(promo.getBmsmPctoffperunit() > 0 ? promo.getBmsmPctoffperunit() : 0);
									pw1.print(separtor);
									pw1.print(promo.getBmsmsaleQty() > 0 ? promo.getBmsmsaleQty() : 0);
									pw1.print(separtor);
									pw1.print(promo.getBmsmsalePrice() > 0 ? promo.getBmsmsalePrice() : 0);
									pw1.print(separtor);
									pw1.print(promo.getStatus());
									pw1.print(separtor);
									pw1.print(promo.getLocationLevel());
									pw1.print(separtor);
									pw1.print(promo.getLocationNo());
									pw1.print(separtor);
									pw1.print(promo.getPageNumber() != null ? promo.getPageNumber() : "");
									pw1.print(separtor);
									pw1.print(promo.getBlockNumber() != null ? promo.getBlockNumber() : "");
									pw1.print(separtor);
									pw1.print(promo.getDisplayOffer() != null ? promo.getDisplayOffer() : "");
									pw1.print(separtor);
									pw1.print(promo.getDescription() != null ? promo.getDescription() : 0);
									pw1.print(separtor);
									pw1.print(promo.getCouponType() != null ? promo.getCouponType() : "");
									pw1.print(separtor);
									pw1.print(promo.getCouponAmt() > 0 ? promo.getCouponAmt() : 0);
									pw1.print(separtor);
									pw1.print(promo.getTypeCode());
									pw1.print(separtor);
									pw1.print(promo.getAnotherItem() != null ? promo.getAnotherItem() : "");
									pw1.println();
								}
							}

						}
					}
				}
			}
		}

		pw.flush();
		fw.flush();
		pw1.flush();
		fw1.flush();

	}

	/***
	 * calculate salePrice for BOGO with % off and mustBuy promo where the
	 * salePr==Regprice
	 * 
	 * @param promo
	 */

	private void setSaleprice(PromoDataStandardDTO promo) {

		if (promo.getBuyQty() == 1 && promo.getGetQty() == 1 && promo.getPctOff() != 0) {
			double discount = Double.parseDouble(promo.getEverydayPrice()) * promo.getPctOff() / 100;
			double secondItemprice = (Double.parseDouble(promo.getEverydayPrice()) - discount);
			double SalePr = Double.parseDouble(promo.getEverydayPrice()) + secondItemprice;
			promo.setSalePrice(Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(SalePr)));
			promo.setSaleQty(2);
		}

		if (promo.getMustBuyQty() > 0 && promo.getMustbuyPrice() > 0
				&& (promo.getSalePrice() == Double.parseDouble(promo.getEverydayPrice()))) {
			promo.setSalePrice(0);
			promo.setSaleQty(0);
		}
	}

	private void writeHeader() {
		String separtor = ",";
		pw.print("CATEGORY");
		pw.print(separtor);
		pw.print("PROMO START DATE");
		pw.print(separtor);
		pw.print("PROMO END DATE");
		pw.print(separtor);
		pw.print("PROMO ID");
		pw.print(separtor);
		pw.print("PROMO DESCRIPTION");
		pw.print(separtor);
		pw.print("ITEMCODE");
		pw.print(separtor);
		pw.print("UPC");
		pw.print(separtor);
		pw.print("ITEM NAME");
		pw.print(separtor);
		pw.print("LIR NAME");
		pw.print(separtor);
		pw.print("PROMO GROUP");
		pw.print(separtor);
		pw.print("EVERYDAY QTY");
		pw.print(separtor);
		pw.print("EVERYDAY PRICE");
		pw.print(separtor);
		pw.print("SALE QTY");
		pw.print(separtor);
		pw.print("SALE PRICE");
		pw.print(separtor);
		pw.print("MUSTBUY QTY");
		pw.print(separtor);
		pw.print("MUSTBUY PRICE");
		pw.print(separtor);
		pw.print("DOLLAR OFF");
		pw.print(separtor);
		pw.print("PCT OFF");
		pw.print(separtor);
		pw.print("Buy Qty");
		pw.print(separtor);
		pw.print("Get Qty");
		pw.print(separtor);
		pw.print("Minimum Qty");
		pw.print(separtor);
		pw.print("Minimum Amt");
		pw.print(separtor);
		pw.print("BMSM Dollar Off Per Units");
		pw.print(separtor);
		pw.print("BMSM Pct Off per Unit");
		pw.print(separtor);
		pw.print("BMSM Sale Qty");
		pw.print(separtor);
		pw.print("BMSM Sale Price");
		pw.print(separtor);
		pw.print("STATUS");
		pw.print(separtor);
		pw.print("LOCATION LEVEL");
		pw.print(separtor);
		pw.print("LOCATION NUMBER");
		pw.print(separtor);
		pw.print("Page Number");
		pw.print(separtor);
		pw.print("Block Number");
		pw.print(separtor);
		pw.print("Display");
		pw.print(separtor);
		pw.print("Offer Description");
		pw.print(separtor);
		pw.print("CouponType");
		pw.print(separtor);
		pw.print("Coupon Amount");
		pw.print(separtor);
		pw.print("Event Type Code");
		pw.print(separtor);
		pw.print("Offer Item");
		pw.println();

		pw1.print("CATEGORY");
		pw1.print(separtor);
		pw1.print("PROMO START DATE");
		pw1.print(separtor);
		pw1.print("PROMO END DATE");
		pw1.print(separtor);
		pw1.print("PROMO ID");
		pw1.print(separtor);
		pw1.print("PROMO DESCRIPTION");
		pw1.print(separtor);
		pw1.print("ITEMCODE");
		pw1.print(separtor);
		pw1.print("UPC");
		pw1.print(separtor);
		pw1.print("ITEM NAME");
		pw1.print(separtor);
		pw1.print("LIR NAME");
		pw1.print(separtor);
		pw1.print("PROMO GROUP");
		pw1.print(separtor);
		pw1.print("EVERYDAY QTY");
		pw1.print(separtor);
		pw1.print("EVERYDAY PRICE");
		pw1.print(separtor);
		pw1.print("SALE QTY");
		pw1.print(separtor);
		pw1.print("SALE PRICE");
		pw1.print(separtor);
		pw1.print("MUSTBUY QTY");
		pw1.print(separtor);
		pw1.print("MUSTBUY PRICE");
		pw1.print(separtor);
		pw1.print("DOLLAR OFF");
		pw1.print(separtor);
		pw1.print("PCT OFF");
		pw1.print(separtor);
		pw1.print("Buy Qty");
		pw1.print(separtor);
		pw1.print("Get Qty");
		pw1.print(separtor);
		pw1.print("Minimum Qty");
		pw1.print(separtor);
		pw1.print("Minimum Amt");
		pw1.print(separtor);
		pw1.print("BMSM Dollar Off Per Units");
		pw1.print(separtor);
		pw1.print("BMSM Pct Off per Unit");
		pw1.print(separtor);
		pw1.print("BMSM Sale Qty");
		pw1.print(separtor);
		pw1.print("BMSM Sale Price");
		pw1.print(separtor);
		pw1.print("STATUS");
		pw1.print(separtor);
		pw1.print("LOCATION LEVEL");
		pw1.print(separtor);
		pw1.print("LOCATION NUMBER");
		pw1.print(separtor);
		pw1.print("Page Number");
		pw1.print(separtor);
		pw1.print("Block Number");
		pw1.print(separtor);
		pw1.print("Display");
		pw1.print(separtor);
		pw1.print("Offer Description");
		pw1.print(separtor);
		pw1.print("CouponType");
		pw1.print(separtor);
		pw1.print("Coupon Amount");
		pw1.print(separtor);
		pw1.print("Event Type Code");
		pw1.print(separtor);
		pw1.print("Offer Item");
		pw1.println();

	}

}
