package com.pristine.dataload.autozone;

import java.io.FileReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.service.StoreItemMapService;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

@SuppressWarnings({ "unused", "rawtypes", "unchecked" })
public class PriceDataLoad extends PristineFileParser {

	private static Logger logger = Logger.getLogger("PriceDataLoad");

	private static LinkedHashMap PRICE_FIELD = null;

	HashMap<ItemDetailKey, List<RetailPriceDTO>> priceDataMap = new HashMap<ItemDetailKey, List<RetailPriceDTO>>();
	HashMap<String, String> storeInfoMap = null;
	HashMap<String, List<String>> zoneStoreMap = null;
	HashMap<ItemDetailKey, String> itemCodeMap = null;

	HashMap<String, List<RetailPriceDTO>> retailPriceCache = new HashMap<String, List<RetailPriceDTO>>();
	private static String ignoreStoresNotInDB = "";
	static List<RetailPriceDTO> currentProcPriceList = new ArrayList<RetailPriceDTO>();
	private Set<String> storesWithNoZone = new HashSet<String>();
	// Changes for populating store id in Store item map table.
	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
	List<RetailPriceDTO> ignoredRecords = new ArrayList<RetailPriceDTO>();
	static int calendarId = -1;
	int prevCalendarId = -1;

	static boolean isCopyEnabled = true;
	String startDate = null;
	static String prevWkStartDate = null;
	private static String ARG_IGN_STR_NOT_IN_DB = "IGN_STR_NOT_IN_DB=";
	private RetailPriceDAO retailPriceDAO = null;
	private CostDAO costDAO = null;
	Set<String> levelIdNotFound = new HashSet<String>();

	String chainId = null;
	Connection conn = null;
	public static final String DATA_LOAD_MODE = "MODE=";
	public static final String WEEK_TYPE = "WEEK_TYPE=";
	public static final String DATE = "DATE=";
	public static final String FILE_PATH = "FILE_PATH=";

	public static char separator;

	static HashSet<String> regPricenotfound = new HashSet<String>();
	HashSet<String> storeNotFound = new HashSet<String>();
	static HashSet<String> itemCodeNotFound = new HashSet<>();
	HashMap<String, List<RetailPriceDTO>> retailerItemCodeAndItsItem = null;
	List<RetailPriceDTO> allItemsList = null;
	Set<String> itemCodeSet = new HashSet<String>();
	Set<String> uniqueItems = new HashSet<String>();

	public static boolean loadstores = false;

	HashMap<String, List<RetailPriceDTO>> priceMap = new HashMap<String, List<RetailPriceDTO>>();

	public static void main(String[] args) throws Exception {

		PriceDataLoad dataload = new PriceDataLoad();
		PropertyConfigurator.configure("log4j-price-retail.properties");

		String weekType = null;
		String mode = null;
		String date = null;
		String filepath = null;

		for (String arg : args) {

			if (arg.startsWith("DATA_LOAD_MODE=")) {
				mode = arg.substring("DATA_LOAD_MODE=".length());
			}

			if (arg.startsWith("WEEK_TYPE=")) {
				weekType = arg.substring("WEEK_TYPE=".length());
			}
			if (arg.startsWith("DATE=")) {
				date = arg.substring("DATE=".length());
			}
			if (arg.startsWith("FILE_PATH=")) {
				filepath = arg.substring("FILE_PATH=".length());
			}
		}

		String dateStr = null;
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(weekType)) {
			dateStr = DateUtil.getWeekStartDate(0);
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(weekType)) {
			dateStr = DateUtil.getDateFromCurrentDate(7);
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(weekType)) {
			dateStr = DateUtil.getWeekStartDate(1);
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType)) {
			try {
				dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(date), 0);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}

		ignoreStoresNotInDB = PropertyManager.getProperty("IGN_STR_NOT_IN_DB_TRUE", "");
		separator = (PropertyManager.getProperty("SEPARATOR", ",")).charAt(0);
		loadstores = Boolean.parseBoolean((PropertyManager.getProperty("LOADSTORES", "false")));

		dataload.setupObjects(dateStr, mode);
		dataload.processPriceFile(filepath);

		try {

			if (currentProcPriceList.size() > 0) {
				dataload.processRecords();

			} else {
				logger.info("No file passed hence data copied from :  " + prevWkStartDate + " to Cal id" + calendarId);
			}
			logger.info("**** Delta Loading Complete**** ");

		} catch (GeneralException e) {
			logger.error("Exception in processRecords" + e);

		}

		logger.info("# Items for which 	itemCodeNotFound not found " + itemCodeNotFound.size());
		for (String storeNo : itemCodeNotFound) {
			logger.info(storeNo);
		}
		itemCodeNotFound.clear();

		logger.info("# Items for which regpice not found " + regPricenotfound.size());
		for (String storeNo : regPricenotfound) {
			logger.info(storeNo);
		}
		regPricenotfound.clear();

	}

	/**
	 * Sets up class level objects for the given date
	 * 
	 * @param dateStr
	 */
	public void setupObjects(String dateStr, String mode) {
		try {

			logger.info("******* Setting up required objects *******");

			populateCalendarId(dateStr);

			costDAO = new CostDAO();
			retailPriceDAO = new RetailPriceDAO();

			// Retrieve Subscriber Chain Id
			chainId = retailPriceDAO.getChainId(conn);

			logger.info("setupObjects() - Subscriber Chain Id - " + chainId);

			// Retrieve all stores and its corresponding zone#

			logger.info("setupObjects() - Get zone Store Info- ");
			storeInfoMap = costDAO.getStoreZoneInfo(conn, chainId);
			logger.info("setupObjects() - No of store available - " + storeInfoMap.size());

			long startTime = System.currentTimeMillis();

			ItemDAO itemDAO = new ItemDAO();
			logger.info("setupObjects() -Get all items");
			allItemsList = itemDAO.getAllItemsFromLookup(conn);
			retailerItemCodeAndItsItem = new HashMap<String, List<RetailPriceDTO>>();
			groupItemByRetailerItemCode();

			long endTime = System.currentTimeMillis();
			logger.info("setupObjects() -#items cached: " + allItemsList.size());
			logger.info(
					"setupObjects() - Time taken to cache all items - " + ((endTime - startTime) / 1000) + " seconds");

			// Changes for populating store id in Store item map table.

			startTime = System.currentTimeMillis();
			storeIdsMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
			endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to retreive store id mapping - " + (endTime - startTime));

			startTime = System.currentTimeMillis();
			retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
			endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to retreive zone id mapping - " + (endTime - startTime));

			// Populate a map with zone# as key and list of corresponding stores as value

			zoneStoreMap = getZoneMapping(storeInfoMap);

			logger.info("setupObjects() - No of zones available - " + zoneStoreMap.size());

			if (mode != null && isCopyEnabled)
				logger.info("setupObjects()- delete Existing data for: " + calendarId);

			retailPriceDAO.deleteRetailPriceData(conn, calendarId);

			logger.info("setupObjects()- delete data for: " + calendarId + "complete");

			if (mode != null && mode.equals(Constants.DATA_LOAD_DELTA) && isCopyEnabled) {
				logger.info("setupObjects()- insert data for : " + calendarId + "from" + prevCalendarId);
				startTime = System.currentTimeMillis();
				retailPriceDAO.insertRetailPriceData(conn, calendarId, prevCalendarId);
				PristineDBUtil.commitTransaction(conn, "Retail Price Data Setup");
				endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to insert data from  - " + (endTime - startTime));

			}

			logger.info("******* Objects setup is done *******");

		} catch (GeneralException ge) {
			logger.error("Error in setting up objects", ge);
			return;
		}
	}

	/**
	 * Returns a Map containing zone numbers and list of stores under each zone
	 * 
	 * @param storeNumberMap HashMap containing store no and its corresponding zone
	 *                       no
	 * @return HashMap
	 */

	public HashMap<String, List<String>> getZoneMapping(HashMap<String, String> storeNumberMap) {
		HashMap<String, List<String>> zoneStoreMap = new HashMap<String, List<String>>();
		String storeNo = null, zoneNo = null;
		for (Map.Entry<String, String> entry : storeNumberMap.entrySet()) {
			storeNo = entry.getKey();
			zoneNo = entry.getValue();
			if (zoneStoreMap.get(zoneNo) != null) {
				List<String> storeNoList = zoneStoreMap.get(zoneNo);
				storeNoList.add(storeNo);
				zoneStoreMap.put(zoneNo, storeNoList);
			} else {
				List<String> storeNoList = new ArrayList<String>();
				storeNoList.add(storeNo);
				zoneStoreMap.put(zoneNo, storeNoList);
			}
		}
		return zoneStoreMap;
	}

	private void groupItemByRetailerItemCode() {
		retailerItemCodeAndItsItem = (HashMap<String, List<RetailPriceDTO>>) allItemsList.stream()
				.filter(p -> p.getRetailerItemCode() != null && !p.getRetailerItemCode().equals("null"))
				.collect(Collectors.groupingBy(RetailPriceDTO::getRetailerItemCode));
	}

	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate Input Date
	 * @throws GeneralException
	 */
	public void populateCalendarId(String weekStartDate) throws GeneralException {
		conn = getOracleConnection();

		String noOfweeks = PropertyManager.getProperty("NO_OF_WEEKS", "-1");

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);

		calendarId = calendarDTO.getCalendarId();
		logger.info("Processing data Calendar Id - " + calendarDTO.getCalendarId());
		startDate = calendarDTO.getStartDate();
		logger.info("Processing week - " + calendarDTO.getStartDate());

		String prevWeekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate),
				Integer.parseInt(noOfweeks));
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate, Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();

		logger.info("Previous/Next Week Calendar Id: - " + calendarDTO.getCalendarId());

		prevCalendarId = calendarDTO.getCalendarId();
	}

	/**
	 * Processes records in price file
	 * 
	 * @param relativePath Relative path of price file
	 */

	private void processPriceFile(String relativePath) {
		ArrayList<String> fileList = null;

		// get zip files
		ArrayList<String> zipFileList = null;

		logger.info("Setting up Retail Price Information ");
		String zipFilePath = getRootPath() + "/" + relativePath;

		try {
			zipFileList = getZipFiles(relativePath);
		} catch (GeneralException ge) {
			logger.error("Error in setting up objects", ge);
			return;
		}

		int curZipFileCount = -1;
		boolean processZipFile = false;
		boolean commit = true;
		do {
			try {

				if (processZipFile)
					PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

				fileList = getFiles(relativePath);
				for (int i = 0; i < fileList.size(); i++) {
					String file = fileList.get(i);

					readDeltaFile(file);

				}
			} catch (GeneralException ex) {
				logger.error("GeneralException", ex);
				commit = false;
			} catch (Exception ex) {
				logger.error("JavaException", ex);
				commit = false;
			}

			if (processZipFile) {
				PrestoUtil.deleteFiles(fileList);
				fileList.clear();
				fileList.add(zipFileList.get(curZipFileCount));
			}

			/*
			 * String archivePath = getRootPath() + "/" + relativePath + "/"; if (commit) {
			 * PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER); }
			 * else { PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER); }
			 */

			curZipFileCount++;
			processZipFile = true;
		} while (curZipFileCount < zipFileList.size());
		if (!levelIdNotFound.isEmpty()) {
			List<String> skippedStores = new ArrayList<String>(levelIdNotFound);
			logger.error("Level id not found for list of Stores: "
					+ PRCommonUtil.getCommaSeperatedStringFromStrArray(skippedStores));
		}
		logger.info("Retail Price Data Load successfully completed");

		return;

	}

	public void processRecords() throws GeneralException, Exception {

		processPriceRecords(currentProcPriceList);
		currentProcPriceList.clear();

	}

	private void processPriceRecords(List<RetailPriceDTO> priceList) throws Exception {

		logger.info("***populateItemCode()-start****");
		populateItemCode(priceList);
		logger.info("***populateItemCode()-complete****");

		try {
			loadPriceData();
			PristineDBUtil.commitTransaction(conn, "Price Data Setup");
		} catch (GeneralException exception) {
			logger.error("Exception in processPriceRecords of PriceDataLoad - " + exception);
		}
	}

	private void populateItemCode(List<RetailPriceDTO> items) throws Exception {

		List<RetailPriceDTO> finalItemList = new ArrayList<>();

		for (RetailPriceDTO item : items) {

			if (!item.getRetailerItemCode().equals(Constants.EMPTY) && item.getRetailerItemCode() != null) {

				if (retailerItemCodeAndItsItem.containsKey(item.getRetailerItemCode())) {
					List<RetailPriceDTO> itemcodeList = retailerItemCodeAndItsItem
							.get(item.getRetailerItemCode().toString());

					for (RetailPriceDTO itemCode : itemcodeList) {
						RetailPriceDTO retailPriceDTo = (RetailPriceDTO) item.clone();
						retailPriceDTo.setUpc(itemCode.getUpc());
						retailPriceDTo.setItemcode(String.valueOf(itemCode.getItemcode()));

						List<RetailPriceDTO> temp = null;

						itemCodeSet.add(String.valueOf(itemCode.getItemcode()));

						if (priceMap.containsKey(retailPriceDTo.getItemcode())) {
							temp = priceMap.get(retailPriceDTo.getItemcode());
							temp.add(retailPriceDTo);
						} else {
							temp = new ArrayList<RetailPriceDTO>();
							temp.add(retailPriceDTo);

						}

						priceMap.put(retailPriceDTo.getItemcode(), temp);
					}

				} else {
					itemCodeNotFound.add(item.getRetailerItemCode() + ";" + item.getLevelId());
					logger.debug("Presto Itemcode not found for" + item.getRetailerItemCode());
				}
			} else {
				itemCodeNotFound.add(item.getRetailerItemCode());
				logger.debug("Presto Itemcode not found for" + item.getRetailerItemCode());
			}

		}

		logger.info("populateItemCode()-#items in List " + priceMap.size());

	}

	private void loadPriceData() throws GeneralException, CloneNotSupportedException {

		HashMap<String, List<RetailPriceDTO>> retailPriceDataMap = new HashMap<String, List<RetailPriceDTO>>();
		HashMap<String, List<RetailPriceDTO>> storesFilteredPriceMap = new HashMap<>();

		String filterStores = PropertyManager.getProperty("FILTER_STORES", "TRUE");

		long startTime = System.currentTimeMillis();

		logger.info("Getting PriceData from table for items for calendarId:" + calendarId);

		HashMap<String, List<RetailPriceDTO>> priceRolledUpMapForItems = retailPriceDAO.getRetailPriceInfoAZ(conn,
				itemCodeSet, calendarId, false);

		logger.info("itemsfetched:" + priceRolledUpMapForItems.size());

		long endTime = System.currentTimeMillis();

		logger.info("Time taken to retrieve items from retail_price_info - " + (endTime - startTime));

		if (priceRolledUpMapForItems != null && priceRolledUpMapForItems.size() > 0) {

			RetailPriceDTO storeLevelDTO = null;

			// unroll the prices for the items in db
			HashMap<String, HashMap<String, RetailPriceDTO>> unrolledPriceMap = unrollRetailPriceInfo(
					priceRolledUpMapForItems, zoneStoreMap, retailPriceDAO);

			for (Map.Entry<String, List<RetailPriceDTO>> prices : priceMap.entrySet()) {

				if (unrolledPriceMap.containsKey(prices.getKey())) {

					List<RetailPriceDTO> retailPriceDTOList = prices.getValue();
					HashMap<String, RetailPriceDTO> unrolledMapValues = unrolledPriceMap.get(prices.getKey());

					for (RetailPriceDTO rDto : retailPriceDTOList) {

						// update the price for records in DB with the prices from the file

						if (unrolledMapValues.containsKey(rDto.getLevelId() + ";" + rDto.getLevelTypeId())) {
							unrolledMapValues.put(rDto.getLevelId() + ";" + rDto.getLevelTypeId(), rDto);

						} else {
							unrolledMapValues.put(rDto.getLevelId() + ";" + rDto.getLevelTypeId(), rDto);
						}

					}

					List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
					unrolledMapValues.forEach((k, v) -> {

						tempList.add(v);

					});

					retailPriceDataMap.put(prices.getKey(), tempList);
				}
				// if the item is present in file but not in db add it in the list
				else {

					List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();

					prices.getValue().forEach(val -> {
						tempList.add(val);
					});

					retailPriceDataMap.put(prices.getKey(), tempList);
				}

			}

		}

		startTime = System.currentTimeMillis();

		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = priceRollUp(retailPriceDataMap, storeInfoMap, chainId);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for priceRollUp - " + (endTime - startTime));

		startTime = System.currentTimeMillis();
		if (filterStores.equalsIgnoreCase(Constants.IGN_STR)) {

			storesFilteredPriceMap = filterStores(priceRolledUpMap);
		}
		endTime = System.currentTimeMillis();
		logger.info("Time taken for Filtering stores  - " + (endTime - startTime));

		startTime = System.currentTimeMillis();
		List<String> itemCodeList = new ArrayList<String>(itemCodeSet);
		try {
			logger.info("#items sent to delete:" + itemCodeList.size() + " for CalendarId : " + calendarId);

			retailPriceDAO.deleteRetailPriceData(conn, calendarId, itemCodeList);
			PristineDBUtil.commitTransaction(conn, "Price Data Setup");
		} catch (Exception e) {
			PristineDBUtil.rollbackTransaction(conn, "Price Data Setup");
			logger.error("Error in delete :" + e);
		}
		endTime = System.currentTimeMillis();
		logger.info("Time taken for deleting data from retail_price_info  for calId - " +  calendarId +":"+ (endTime - startTime));

		Set<DuplicateKey> duplicateSet = new HashSet<DuplicateKey>();
		List<RetailPriceDTO> insertList = new ArrayList<RetailPriceDTO>();
		for (List<RetailPriceDTO> priceDTOList : storesFilteredPriceMap.values()) {
			for (RetailPriceDTO retailPriceDTO : priceDTOList) {

				DuplicateKey duplicateKey = new DuplicateKey(retailPriceDTO.getItemcode(), retailPriceDTO.getLevelId(),
						retailPriceDTO.getLevelTypeId(), retailPriceDTO.getCalendarId());
				if (!duplicateSet.contains(duplicateKey)) {
					duplicateSet.add(duplicateKey);
					insertList.add(retailPriceDTO);
				}
			}
		}

		logger.info("#inserList size:" + insertList.size());
		updateLocationId(insertList);

		startTime = System.currentTimeMillis();
		retailPriceDAO.insertRetailPriceData(conn, insertList);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for inserting data into retail_price_info - " + (endTime - startTime));
	}

	float chainlevelPrice = 0;
	HashMap<String, Float> zonePriceMap = null;
	boolean storesPresent = false;

	private HashMap<String, List<RetailPriceDTO>> filterStores(HashMap<String, List<RetailPriceDTO>> priceRolledUpMap) {

		HashMap<String, List<RetailPriceDTO>> finalMap = new HashMap<>();

		priceRolledUpMap.forEach((itemcode, priceList) -> {

			chainlevelPrice = 0;
			zonePriceMap = new HashMap<>();
			storesPresent = false;
			List<RetailPriceDTO> strList = new ArrayList<>();

			priceList.forEach((price) -> {

				if (price.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID) {
					chainlevelPrice = price.getRegPrice();
					List<RetailPriceDTO> tempList = null;
					if (finalMap.containsKey(itemcode)) {
						tempList = finalMap.get(itemcode);
						tempList.add(price);
					} else {
						tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(price);
					}

					finalMap.put(itemcode, tempList);
				} else if (price.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {

					List<RetailPriceDTO> tempList = null;
					if (finalMap.containsKey(itemcode)) {
						tempList = finalMap.get(itemcode);
						tempList.add(price);
					} else {
						tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(price);
					}

					finalMap.put(itemcode, tempList);

					zonePriceMap.put(price.getLevelId(), price.getRegPrice());

				} else if (price.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
					strList.add(price);
					storesPresent = true;
				}

			});

			if (storesPresent) {

				for (RetailPriceDTO str : strList)

				{

					String zoneNo = str.getZoneNbr();

					if (zonePriceMap.get(zoneNo) != null) {
						float val = zonePriceMap.get(zoneNo);

						if (str.getRegPrice() != val) {

							List<RetailPriceDTO> tempList = null;
							if (finalMap.containsKey(itemcode)) {
								tempList = finalMap.get(itemcode);
								tempList.add(str);
							} else {
								tempList = new ArrayList<RetailPriceDTO>();
								tempList.add(str);
							}

							finalMap.put(itemcode, tempList);

						}
					} else if (chainlevelPrice != str.getRegPrice()) {
						List<RetailPriceDTO> tempList = null;
						if (finalMap.containsKey(itemcode)) {
							tempList = finalMap.get(itemcode);
							tempList.add(str);
						} else {
							tempList = new ArrayList<RetailPriceDTO>();
							tempList.add(str);
						}

						finalMap.put(itemcode, tempList);
					}

				}
			}

		});

		return finalMap;
	}

	RetailPriceDTO chaindata = null;
	HashMap<String, RetailPriceDTO> unrolledstoreZoneMap = null;
	Set<String> zonesPresent = null;

	private HashMap<String, HashMap<String, RetailPriceDTO>> unrollRetailPriceInfo(
			HashMap<String, List<RetailPriceDTO>> unrolledPriceMap, HashMap<String, List<String>> zoneStoreMap,
			RetailPriceDAO retailPriceDAO) throws GeneralException {

		RetailPriceDTO chainLevelData = null;

		boolean isChainLevelPresent = false;

		RetailPriceDTO zoneLevelData = null;

		HashMap<String, RetailPriceDTO> storeList = null;

		HashMap<String, HashMap<String, RetailPriceDTO>> unrolledMap = new HashMap<String, HashMap<String, RetailPriceDTO>>();

		for (Map.Entry<String, List<RetailPriceDTO>> entry : unrolledPriceMap.entrySet()) {
			boolean isStoreLevelPresent = false;

			isChainLevelPresent = false;
			RetailPriceDTO chainLevelDTO = null;
			RetailPriceDTO zoneLevelDTO = null;
			boolean isZoneLevelPresent = false;
			storeList = new HashMap<String, RetailPriceDTO>();
			chaindata = new RetailPriceDTO();
			zonesPresent = new HashSet<String>();
			unrolledstoreZoneMap = new HashMap<String, RetailPriceDTO>();
			// logger.info("item unrollimg" + entry.getKey());
			for (RetailPriceDTO retailPriceDTO : entry.getValue()) {

				if (Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()) {
					isChainLevelPresent = true;
					chainLevelData = retailPriceDTO;
					chaindata = retailPriceDTO;
				} else if (Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()) {

					zoneLevelData = retailPriceDTO;

					isZoneLevelPresent = true;
					// adding the zones whose prices are different from chain Level
					unrolledstoreZoneMap.put(retailPriceDTO.getLevelId() + ";" + retailPriceDTO.getLevelTypeId(),
							retailPriceDTO);

					// this added to further unroll chain level price to remaining zones which were
					// not present in DB
					zonesPresent.add(retailPriceDTO.getLevelId());

				} else if (Constants.STORE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()) {
					isStoreLevelPresent = true;
					storeList.put(retailPriceDTO.getLevelId() + ";" + retailPriceDTO.getLevelTypeId(), retailPriceDTO);
				}
			}
			// if only chainLevel data is present for an item then unroll price to all zones
			if (isChainLevelPresent && !isStoreLevelPresent && !isZoneLevelPresent)

			{
				retailPriceZone.forEach((zone, id) -> {

					{
						if (!(null == zoneStoreMap.get(zone))) {

							if (!zone.equals("1000")) {
								RetailPriceDTO zoneData = new RetailPriceDTO();
								zoneData.copy(chaindata);
								zoneData.setLevelId(zone);
								zoneData.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);

								unrolledstoreZoneMap.put(zone + ";" + Constants.ZONE_LEVEL_TYPE_ID, zoneData);
							}

						}
					}

				});

			} else {

				// add the storeLevel prices which differ from Chain level prices in the Map
				if (isStoreLevelPresent && loadstores) {
					storeList.forEach((key, value) -> {
						{
							unrolledstoreZoneMap.put(key, value);
						}

					});

				}

				if (isZoneLevelPresent) {
					retailPriceZone.forEach((zone, id) -> {

						// unroll the chain level prices to remaining zones whose value was already
						// rolledup

						if (!zonesPresent.contains(zone)) {
							if (!zone.equals("1000")) {
								RetailPriceDTO zoneData = new RetailPriceDTO();
								zoneData.copy(chaindata);
								zoneData.setLevelId(zone);
								zoneData.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);

								unrolledstoreZoneMap.put(zone + ";" + Constants.ZONE_LEVEL_TYPE_ID, zoneData);

							}
						}

					});
				}
			}

			unrolledMap.put(entry.getKey(), unrolledstoreZoneMap);
		}

		return unrolledMap;
	}

	/**
	 * This method is used to rollUp the prices to Chainlevel
	 * 
	 * @param retailPriceDataMap
	 * @param storeInfoMap
	 * @param chainId
	 * @throws CloneNotSupportedException
	 */

	public HashMap<String, List<RetailPriceDTO>> priceRollUp(HashMap<String, List<RetailPriceDTO>> retailPriceDataMap,
			HashMap<String, String> storeInfoMap, String chainId) throws CloneNotSupportedException {

		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = new HashMap<String, List<RetailPriceDTO>>();
		List<RetailPriceDTO> priceRolledUpList;
		HashMap<String, List<RetailPriceDTO>> storeZoneMap = null;

		for (List<RetailPriceDTO> retailPriceDTOList : retailPriceDataMap.values()) {

			HashMap<String, List<RetailPriceDTO>> chainLevelMap = new HashMap<String, List<RetailPriceDTO>>();
			HashMap<String, List<RetailPriceDTO>> finalMap = new HashMap<String, List<RetailPriceDTO>>();
			HashMap<String, HashMap<String, List<RetailPriceDTO>>> zoneLevelMap = new HashMap<String, HashMap<String, List<RetailPriceDTO>>>();
			HashMap<String, List<RetailPriceDTO>> storeMap = new HashMap<>();
			priceRolledUpList = new ArrayList<RetailPriceDTO>();
			String itemCode = null;
			String upc = null;
			boolean isPricedAtZoneLevel = false;
			boolean isPricedAtStoreLevel = false;

			for (RetailPriceDTO retailPriceDTO : retailPriceDTOList) {

				String priceStr = Constants.EMPTY;

				retailPriceDTO.setPromotionFlag("N");
				if (retailPriceDTO.getRegPrice() > 0) {
					priceStr = priceStr + retailPriceDTO.getRegPrice();
				} else if (retailPriceDTO.getRegMPrice() > 0) {
					priceStr = priceStr + retailPriceDTO.getRegMPrice() + Constants.INDEX_DELIMITER
							+ retailPriceDTO.getRegQty();
				}

				if (retailPriceDTO.getSalePrice() > 0) {
					priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSalePrice();
					retailPriceDTO.setPromotionFlag("Y");
				} else if (retailPriceDTO.getSaleMPrice() > 0) {
					priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSaleMPrice()
							+ Constants.INDEX_DELIMITER + retailPriceDTO.getSaleQty();
					retailPriceDTO.setPromotionFlag("Y");
				}

				priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getRegEffectiveDate();

				// Set Item Code
				if (retailPriceDTO.getItemcode() != null) {
					itemCode = retailPriceDTO.getItemcode();
				}

				// Set Zone# and Store#
				if (retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
					retailPriceDTO.setStoreNbr(retailPriceDTO.getLevelId());

					retailPriceDTO.setZoneNbr(String.valueOf(storeInfoMap.get(retailPriceDTO.getLevelId())));
					// Change to handle stores with no zone
					if (storeInfoMap.get(retailPriceDTO.getStoreNbr()) == null)
						storesWithNoZone.add(retailPriceDTO.getStoreNbr());
					// Change to handle stores with no zone - Ends
				} else if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
					
					retailPriceDTO.setZoneNbr(retailPriceDTO.getLevelId());
				}

				// If item is priced at zone level populate a hashmap with price as key and list
				// of corresponding retailPriceDTO as its value
				if (Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()) {

					isPricedAtZoneLevel = true;
					List<RetailPriceDTO> tempList = null;
					if (chainLevelMap.get(priceStr) != null) {
						tempList = chainLevelMap.get(priceStr);
						tempList.add(retailPriceDTO);
					} else {
						tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(retailPriceDTO);
					}

					chainLevelMap.put(priceStr, tempList);
				} else {
					/*
					 * If item is priced at store level populate a hashmap with zone number as key
					 * and hashmap containing price and its list of retail price info as value
					 */
					isPricedAtStoreLevel = true;
					HashMap<String, List<RetailPriceDTO>> zonePriceMap = null;
					if (zoneLevelMap.get(retailPriceDTO.getZoneNbr()) != null) {
						zonePriceMap = zoneLevelMap.get(retailPriceDTO.getZoneNbr());
						List<RetailPriceDTO> tempList = null;
						if (zonePriceMap.get(priceStr) != null) {
							tempList = zonePriceMap.get(priceStr);
							tempList.add(retailPriceDTO);
						} else {
							tempList = new ArrayList<RetailPriceDTO>();
							tempList.add(retailPriceDTO);
						}
						zonePriceMap.put(priceStr, tempList);
						storeMap.put(priceStr, tempList);
					} else {
						zonePriceMap = new HashMap<String, List<RetailPriceDTO>>();
						List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(retailPriceDTO);
						zonePriceMap.put(priceStr, tempList);
						storeMap.put(priceStr, tempList);
					}
					zoneLevelMap.put(retailPriceDTO.getZoneNbr(), zonePriceMap);

				}
			}

			// If item is priced at store level perform an additional step of rolling it up
			// to zone level.
			if (isPricedAtStoreLevel) {

				for (Map.Entry<String, HashMap<String, List<RetailPriceDTO>>> entry : zoneLevelMap.entrySet()) {

					if (entry.getKey() != null) {

						for (Map.Entry<String, List<RetailPriceDTO>> inEntry : entry.getValue().entrySet()) {

							{
								List<RetailPriceDTO> tempList = null;
								if (chainLevelMap.get(inEntry.getKey()) != null) {

									tempList = chainLevelMap.get(inEntry.getKey());
									tempList.addAll(inEntry.getValue());
								} else {
									tempList = new ArrayList<RetailPriceDTO>();
									tempList.addAll(inEntry.getValue());
								}
								chainLevelMap.put(inEntry.getKey(), tempList);
							}

						}
					} else {
						// To handle stores with null zone ids
						if (!ignoreStoresNotInDB.equalsIgnoreCase(Constants.IGN_STR_NOT_IN_DB_TRUE)) {
							for (Map.Entry<String, List<RetailPriceDTO>> inEntry : entry.getValue().entrySet()) {
								for (RetailPriceDTO retailPriceDTO : inEntry.getValue()) {
									String priceStr = Constants.EMPTY;

									priceStr = priceStr + retailPriceDTO.getRegPrice() + Constants.INDEX_DELIMITER
											+ retailPriceDTO.getRegEffectiveDate();

									List<RetailPriceDTO> tempList = null;
									if (chainLevelMap.get(priceStr) != null) {
										tempList = chainLevelMap.get(priceStr);
										retailPriceDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailPriceDTO.setLevelId(retailPriceDTO.getStoreNbr());
										tempList.add(retailPriceDTO);
									} else {
										tempList = new ArrayList<RetailPriceDTO>();
										retailPriceDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailPriceDTO.setLevelId(retailPriceDTO.getStoreNbr());
										tempList.add(retailPriceDTO);
									}
									chainLevelMap.put(inEntry.getKey(), tempList);
								}
							}
						}
					}
				}
			}
			// Determine most prevalent price for this UPC
			String mostPrevalentPrice = null;
			int mostPrevalentCnt = 0;
			int tempCnt = 0;
			boolean isZonePresent = false;
			HashMap<String, Integer> valuemap = new HashMap<>();

			for (Map.Entry<String, List<RetailPriceDTO>> entry : chainLevelMap.entrySet()) {

				List<RetailPriceDTO> retailPriceDTOLst = entry.getValue();
				tempCnt = 0;
				for (RetailPriceDTO retailPriceDTO : retailPriceDTOLst) {

					if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {

						zoneLevelrollup(entry, retailPriceDTO, valuemap, finalMap, isZonePresent, tempCnt);
						isZonePresent = true;
					} else {

						List<RetailPriceDTO> tempList = null;

						if (finalMap.containsKey(entry.getKey())) {
							tempList = finalMap.get(entry.getKey());
							tempList.add(retailPriceDTO);
						} else {
							tempList = new ArrayList<RetailPriceDTO>();
							tempList.add(retailPriceDTO);
						}

						finalMap.put(entry.getKey(), tempList);

					}

				}
			}

			if (!isZonePresent) {

				storeZoneMap = new HashMap<>();
				finalMap = new HashMap<>();
				HashMap<String, RetailPriceDTO> finalZMap = new HashMap<>();
				for (Map.Entry<String, List<RetailPriceDTO>> entry : chainLevelMap.entrySet()) {

					List<RetailPriceDTO> retailPriceDTOLst = entry.getValue();

					for (RetailPriceDTO retailPriceDTO : retailPriceDTOLst) {

						String key = retailPriceDTO.getZoneNbr() + Constants.INDEX_DELIMITER
								+ retailPriceDTO.getRegEffectiveDate() + Constants.INDEX_DELIMITER
								+ retailPriceDTO.getRegPrice();

						List<RetailPriceDTO> tempList = null;
						if (storeZoneMap.containsKey(key)) {
							tempList = storeZoneMap.get(key);
							tempList.add(retailPriceDTO);
						} else {
							tempList = new ArrayList<RetailPriceDTO>();
							tempList.add(retailPriceDTO);
						}

						storeZoneMap.put(key, tempList);
					}

					for (Map.Entry<String, List<RetailPriceDTO>> strEntry : storeZoneMap.entrySet()) {

						String key = strEntry.getKey().split(Constants.INDEX_DELIMITER)[0];

						RetailPriceDTO retailDto = (RetailPriceDTO) (strEntry.getValue().get(0).clone());
						retailDto.setLevelId(key);
						retailDto.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
						finalZMap.put(key, retailDto);

					}

					for (Map.Entry<String, RetailPriceDTO> en : finalZMap.entrySet()) {

						zoneLevelrollup(entry, en.getValue(), valuemap, finalMap, isZonePresent, tempCnt);

					}

				}

			}

			int max = 0;
			for (Map.Entry<String, Integer> entry : valuemap.entrySet()) {

				if (entry.getValue() > max) {
					max = max + entry.getValue();
					mostPrevalentPrice = entry.getKey();

				} else if (entry.getValue() == max) {
					max = max + entry.getValue();
					String[] values = entry.getKey().split("-");
					String[] mostPrevalentPriceVal = mostPrevalentPrice.split("-");

					if (Float.parseFloat(values[0]) > Float.parseFloat((mostPrevalentPriceVal[0]))) {
						mostPrevalentPrice = entry.getKey();
					}
				}

			}
			// logger.info("MCP:" + mostPrevalentPrice);

			// Add Items to the final list to be compared against the database
			for (Map.Entry<String, List<RetailPriceDTO>> entry : finalMap.entrySet()) {
				if (entry.getKey().equals(mostPrevalentPrice)) {
					RetailPriceDTO retailPriceDTO = entry.getValue().get(0);
					RetailPriceDTO chainLevelDTO = new RetailPriceDTO();
					chainLevelDTO.copy(retailPriceDTO);

					chainLevelDTO.setLevelId(chainId);
					chainLevelDTO.setCalendarId(calendarId);
					chainLevelDTO.setLevelTypeId(Constants.CHAIN_LEVEL_TYPE_ID);
					chainLevelDTO.setWhseZoneRolledUpRecord(false);
					priceRolledUpList.add(chainLevelDTO);
					for (RetailPriceDTO retailPrice : entry.getValue()) {
						if (retailPrice.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
							priceRolledUpList.add(retailPrice);
						}
					}
				} else {

					for (RetailPriceDTO retailPriceDTO : entry.getValue()) {
						priceRolledUpList.add(retailPriceDTO);
					}
				}
			}
			if (itemCode != null)
				priceRolledUpMap.put(itemCode, priceRolledUpList);

		}
		return priceRolledUpMap;

	}

	private void zoneLevelrollup(Entry<String, List<RetailPriceDTO>> entry, RetailPriceDTO retailPriceDTO,
			HashMap<String, Integer> valuemap, HashMap<String, List<RetailPriceDTO>> finalMap, boolean isZonePresent,
			int tempCnt) {
		if (zoneStoreMap.get(retailPriceDTO.getLevelId()) != null) {
			isZonePresent = true;
			List<String> stores = zoneStoreMap.get(retailPriceDTO.getLevelId());

			tempCnt = tempCnt + stores.size();

			List<RetailPriceDTO> tempList = null;

			if (finalMap.containsKey(entry.getKey())) {
				tempList = finalMap.get(entry.getKey());
				tempList.add(retailPriceDTO);
			} else {
				tempList = new ArrayList<RetailPriceDTO>();
				tempList.add(retailPriceDTO);
			}
			int val = 0;
			if (valuemap.containsKey(entry.getKey())) {
				val = valuemap.get(entry.getKey());
				val = val + tempCnt;

			} else {
				val = tempCnt;
			}

			valuemap.put(entry.getKey(), val);
			finalMap.put(entry.getKey(), tempList);

		}

		else if (retailPriceDTO.getLevelId().equals("30")) {
			isZonePresent = true;
			tempCnt = tempCnt + 1;

			List<RetailPriceDTO> tempList = null;

			if (finalMap.containsKey(entry.getKey())) {
				tempList = finalMap.get(entry.getKey());
				tempList.add(retailPriceDTO);
			} else {
				tempList = new ArrayList<RetailPriceDTO>();
				tempList.add(retailPriceDTO);
			}

			int val = 0;
			if (valuemap.containsKey(entry.getKey())) {
				val = valuemap.get(entry.getKey());
				val = val + tempCnt;

			} else {
				val = tempCnt;
			}

			valuemap.put(entry.getKey(), val);
			finalMap.put(entry.getKey(), tempList);
		} else {
			logger.debug("zone not found:" + retailPriceDTO.getLevelId());
		}

	}

	private void updateLocationId(List<RetailPriceDTO> insertList) {
		for (RetailPriceDTO retailPriceDTO : insertList) {

			retailPriceDTO.setCalendarId(calendarId);
			// Update chain id for zone level records
			if (retailPriceDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID) {

				retailPriceDTO.setLocationId(Integer.parseInt(retailPriceDTO.getLevelId()));

			}
			// Update price zone id from the cache when there is a zone level record
			else if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
				// Set location id only if it is available else put it as null

				if (retailPriceZone.get(String.valueOf(Integer.parseInt(retailPriceDTO.getLevelId()))) != null)
					retailPriceDTO.setLocationId(

							retailPriceZone.get(String.valueOf(Integer.parseInt(retailPriceDTO.getLevelId()))));

			}
			// Update comp_str_id from the cache when there is a store level record
			else if (retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
				// Set location id only if it is available else put it as null
				if (storeIdsMap.get(retailPriceDTO.getLevelId()) != null)
					retailPriceDTO.setLocationId(storeIdsMap.get(retailPriceDTO.getLevelId()));

			}
		}
	}

	class DuplicateKey {
		String itemCode;
		String levelId;
		int levelTypeId;
		int calendarId;

		public DuplicateKey(String itemCode, String levelId, int levelTypeId, int calendarId) {
			this.itemCode = itemCode;
			this.levelId = levelId;
			this.levelTypeId = levelTypeId;
			this.calendarId = calendarId;
		}

		public String getItemCode() {
			return itemCode;
		}

		public void setItemCode(String itemCode) {
			this.itemCode = itemCode;
		}

		public String getLevelId() {
			return levelId;
		}

		public void setLevelId(String levelId) {
			this.levelId = levelId;
		}

		public int getLevelTypeId() {
			return levelTypeId;
		}

		public void setLevelTypeId(int levelTypeId) {
			this.levelTypeId = levelTypeId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + calendarId;
			result = prime * result + ((itemCode == null) ? 0 : itemCode.hashCode());
			result = prime * result + ((levelId == null) ? 0 : levelId.hashCode());
			result = prime * result + levelTypeId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DuplicateKey other = (DuplicateKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (calendarId != other.calendarId)
				return false;
			if (itemCode == null) {
				if (other.itemCode != null)
					return false;
			} else if (!itemCode.equals(other.itemCode))
				return false;
			if (levelId == null) {
				if (other.levelId != null)
					return false;
			} else if (!levelId.equals(other.levelId))
				return false;
			if (levelTypeId != other.levelTypeId)
				return false;
			return true;
		}

		private PriceDataLoad getOuterType() {
			return PriceDataLoad.this;
		}

	}

	public void readDeltaFile(String file) throws Exception, GeneralException {

		CsvReader csvReader = readFilecheck(file, separator);
		String line[];
		int counter = 0;

		while (csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();
				try {

					if (line[0] != "") {
						RetailPriceDTO priceDataDTO = new RetailPriceDTO();

						priceDataDTO.setUpc(line[0]);

						priceDataDTO.setRetailerItemCode(line[1]);

						priceDataDTO.setLevelTypeId(Integer.parseInt(line[2]));

						priceDataDTO.setLevelId(line[3]);
						priceDataDTO.setRegEffectiveDate(line[4]);

						if (!line[5].equals(Constants.EMPTY)) {
							priceDataDTO.setRegPrice(Float.parseFloat(line[5]));
							priceDataDTO.setRegQty(Integer.parseInt(line[6]));
							priceDataDTO.setSaleStartDate(line[7]);
							priceDataDTO.setSaleEndDate(line[8]);
							if (!line[9].equals(Constants.EMPTY)) {
								priceDataDTO.setSalePrice(Float.parseFloat(line[9]));
								priceDataDTO.setSaleQty(Integer.parseInt(line[10]));
							}

							priceDataDTO.setCoreRetailValue((line[11]));
							priceDataDTO.setVdprRetail((line[12]));

							if (!uniqueItems.contains(line[1] + ";" + line[2] + ";" + line[3] + ";" + line[4])) {
								// logger.info(line[1] + ";" +line[2] + ";" + line[3] + ";" + line[4]);
								uniqueItems.add(line[1] + ";" + line[2] + ";" + line[3] + ";" + line[4]);
								currentProcPriceList.add(priceDataDTO);
							}

						} else {
							regPricenotfound.add(line[1]);
						}

					}
				}

				catch (Exception ex) {
					logger.error("Ignored record" + line[0] + ex);
					continue;
				}
			}

			counter++;
		}

		logger.info("#records read from inputFile " + currentProcPriceList.size());

	}

	private CsvReader readFilecheck(String fileName, char delimiter) throws Exception {

		CsvReader reader = null;
		try {
			reader = new CsvReader(new FileReader(fileName));
			if (delimiter != '0') {
				reader.setDelimiter(delimiter);
			}
		} catch (Exception e) {
			throw new Exception("File read error ", e);
		}
		return reader;

	}

	@Override
	public void processRecords(List listobj) throws GeneralException {

	}

}
