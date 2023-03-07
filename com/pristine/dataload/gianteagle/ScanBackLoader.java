package com.pristine.dataload.gianteagle;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.prestoload.RetailCostSetup;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.ScanBackDTO;
import com.pristine.exception.GeneralException;
import com.pristine.fileformatter.gianteagle.CostFileFormatter;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

@SuppressWarnings("rawtypes")
public class ScanBackLoader extends PristineFileParser{
	static Logger logger = Logger.getLogger("ScanBack setup");
	Connection conn = null;
	RetailCostDAO retailCostDAO;
	List<ScanBackDTO> scanBackInputList;
	List<String> scanBackKeyList;
	HashSet<String> skippedRetailerItemCode;
	HashSet<String> listCostIsZero;
	List<ItemDTO> activeItems;
	private String chainId = null;
	int stopCount = -1;
	int calendarId = -1;
	int prevCalendarId = -1;
	int commitRecCount = 1000;
	static String dateStr = null;
	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
	CostFileFormatter costFileFormatter = new CostFileFormatter();
	String startDate = null;
	String prevWkStartDate = null;
	private final static String LOAD_FUTURE_WEEKS = "LOAD_FUTURE_WEEKS=";
	private static boolean loadFutureWeekScanback = true;
	
	public ScanBackLoader() {
		super("analysis.properties");
		try {
			conn = DBManager.getConnection();

		} catch (GeneralException ex) {

		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-scanBackSetup.properties");
		PropertyManager.initialize("analysis.properties");
		String subFolder = null;
//		String weekType = null;
//		String mode = null;
//		String copyExisting = null;

		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("INPUT_FOLDER")) {
				subFolder = arg.substring("INPUT_FOLDER=".length());
			}else if (arg.startsWith(LOAD_FUTURE_WEEKS)) {
				loadFutureWeekScanback = Boolean.parseBoolean(arg.substring(LOAD_FUTURE_WEEKS.length()));
			}
		}
		// Default week type to current week if it is not specified

		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null){
			dayIndex = Integer.parseInt(PropertyManager.
					getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if(dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex+1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[1])) {
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[1])) {
			try {
				dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}

		ScanBackLoader scanBackLoader = new ScanBackLoader();
		try {
			scanBackLoader.initializeCache();
			scanBackLoader.processScanBackSetupFile(subFolder);
		} catch (GeneralException e) {
			logger.error("Error -- processScanBackFile() ", e);
			e.printStackTrace();

		}

	}

	private void initializeCache() throws GeneralException {
		scanBackKeyList = new ArrayList<String>();
		scanBackInputList = new ArrayList<ScanBackDTO>();
		skippedRetailerItemCode = new HashSet<String>();
		listCostIsZero = new HashSet<String>();
		populateCalendarId(dateStr);
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		retailCostDAO = new RetailCostDAO();
		// Retrieve Subscriber Chain Id
		chainId = retailPriceDAO.getChainId(conn);
		retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
		
	}

	@SuppressWarnings("unchecked")
	private void processScanBackSetupFile(String subFolder) {
		try {
			ArrayList<String> zipFileList = getZipFiles(subFolder);
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + subFolder;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				try {
					if (processZipFile) {
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);
					}
					fileList = getFiles(subFolder);
					// To get list of loyalty card number from
					// Customer_loyalty_info table...
					String fieldNames[] = new String[9];
					setHeaderPresent(true);
					fieldNames[0] = "retailerItemCode";
					fieldNames[1] = "zoneNumber";
					fieldNames[2] = "splrNo";
					fieldNames[3] = "bnrCD";
					fieldNames[4] = "scanBackAmt";
					fieldNames[5] = "scanBackStartDate";
					fieldNames[6] = "scanBackEndDate";
					fieldNames[7] = "scanBackNo";
					fieldNames[8] = "dealId";
					logger.info("Scan Back Setup starts");

					for (int j = 0; j < fileList.size(); j++) {
						// clearVariables();
						logger.info("processing - " + fileList.get(j));
						parseDelimitedFile(ScanBackDTO.class, fileList.get(j), '|', fieldNames, stopCount);
					}
					processScanBackDetails();
					
					if(Boolean.parseBoolean(PropertyManager.
							getProperty("LOAD_ALLOWANCE_AMT_AND_LONG_TRM_FLG", "FALSE"))){
						logger.debug("Calendar id: "+calendarId);
						retailCostDAO.updateListCost2Final(conn, calendarId);
					}
					PristineDBUtil.commitTransaction(conn, "batch record update");
					logger.info("Scan Back Setup Completed");
				} catch (GeneralException ge) {
					ge.printStackTrace();
					logger.error(ge.toString(), ge);
					logger.error("Error while processing Scan Back Setup File - ",ge);
					PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
					commit = false;
				} catch (Exception e) {
					logger.error("Error while processing Scan Back Setup File - ",e);
					commit = false;
				}
				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + subFolder + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());
		} catch (GeneralException ex) {
			logger.error("Outer Exception -  GeneralException", ex);
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}

	public void processScanBackDetails() throws GeneralException, CloneNotSupportedException {
		// Get item code values with respect to Retailer item code and Upc as key
		HashMap<String, List<ItemDTO>> retailerItemCodeMap = getRetailerItemCodeMap();
		ItemDAO itemDAO = new ItemDAO();
		
		logger.info("setupObjects() - ***ROLL_UP_DSD_TO_WARHOUSE_ZONE is enabled***");
		logger.info("setupObjects() - Getting DSD and Warehouse zone mapping...");
		HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = retailCostDAO.getDSDAndWHSEZoneMap(conn, null);
		logger.info("setupObjects() - Getting DSD and Warehouse zone mapping is completed.");
		
		logger.info("setupObjects() - Getting Item and category mapping...");
		HashMap<Integer, Integer> itemCodeCategoryMap = itemDAO.getCategoryAndItemCodeMap(conn, null);
		logger.info("setupObjects() - Getting Item and category mapping is completed.");
		
		HashMap<ItemDetailKey, String> itemLookupMap = new HashMap<>();
		activeItems.forEach(item->{
			ItemDetailKey key = new ItemDetailKey(item.getUpc(), item.getRetailerItemCode());
			itemLookupMap.put(key, String.valueOf(item.getItemCode()));
		});
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = findEachItemScanbackAmtInDiffWeek(startDate, scanBackInputList);

		// Set item codes for each retailer item code and group based on item code
		HashMap<String, List<ScanBackDTO>> scanBackDetailsMap = setItemCodeBasedOnRetailerItemCode(retailerItemCodeMap, scanBackInputList);
		
		// Dinesh::23-Mar-2018, Code changes done to get only authorized items. Other wise while unrolling cost records
		// unauthorized zones were included which is throwing unique constraint error. Duplicate records are getting be created
		// for DSD zones. Due to Authorized flag, some unauthorized items getting skipped in scan process.
		HashMap<String, List<String>> priceZoneFromStoreItemMap = getPriceZoneFromStoreItemMap(scanBackDetailsMap.keySet());
		
		for (Map.Entry<String, HashMap<String, ScanBackDTO>> multiWeekScanbacks : multiWeekScanbackDetails.entrySet()) {
			
			// To filter processing week and future weeks to load
			if (multiWeekScanbacks.getKey().equals(startDate) || (!multiWeekScanbacks.getKey().equals(startDate) && loadFutureWeekScanback)) {
				
				int processingWeekCalId = costFileFormatter.getWeekStartDateCalDTO(multiWeekScanbacks.getKey()).getCalendarId();
				logger.info("Scan back details processing for Week Start Date: " + multiWeekScanbacks.getKey() + " and It is Calendar Id: "
						+ processingWeekCalId);
				
				retailCostDAO = new RetailCostDAO();
				HashMap<String, ItemDTO> itemCodeMap = new HashMap<String, ItemDTO>();
				HashMap<ItemDetailKey, List<RetailCostDTO>> retailCostDataMap = new HashMap<ItemDetailKey, List<RetailCostDTO>>();
				List<ScanBackDTO> scanBackList = new ArrayList<>();
				
				multiWeekScanbacks.getValue().forEach((key, value) -> {
					scanBackList.add(value);
				});
				// Set item codes for each retailer item code and group based on item code
				HashMap<String, List<ScanBackDTO>> scanBackDetails = setItemCodeBasedOnRetailerItemCode(retailerItemCodeMap, scanBackList);
				// Rolled up values for each item code..
				// logger.info("Actual feed Item count" + scanBackDetails.keySet().size());
				HashMap<String, List<RetailCostDTO>> costRolledUpMap = retailCostDAO.getRetailCostInfo(conn, scanBackDetails.keySet(),
						processingWeekCalId, false);
				
				
				// Unroll the cost details and set values for all the zone...
				HashMap<String, List<RetailCostDTO>> unRolledMap = getZoneLevelCostEntries(costRolledUpMap, priceZoneFromStoreItemMap);
				
				// To process the scan back data and set list cost values for each zone
				subtractScanBackCost(unRolledMap, scanBackDetails);
				
				// Get values based on item code as key in hash map
				for (ItemDTO itemDTO : activeItems) {
					itemCodeMap.put(String.valueOf(itemDTO.getItemCode()), itemDTO);
				}
				// To set key using UPC and Retailer Item Code based on Item code
				for (Map.Entry<String, List<RetailCostDTO>> retailCostDTOMap : unRolledMap.entrySet()) {
					ItemDTO itemDTO = itemCodeMap.get(retailCostDTOMap.getKey());
					ItemDetailKey itemDetailKey = new ItemDetailKey(itemDTO.getUpc(), itemDTO.getRetailerItemCode());
					
					// Set Price group code for all the items
					retailCostDTOMap.getValue().forEach(item->{
						if(itemDTO.getPrcGrpCd()!=null){
							item.setPrcGrpCode(itemDTO.getPrcGrpCd());
						}else{
							item.setPrcGrpCode("");
						}
					});
					retailCostDataMap.put(itemDetailKey, retailCostDTOMap.getValue());
				}
				
				RetailCostSetup retailCosSetup = new RetailCostSetup();
				
				logger.info("Rolling up DSD zones...");
				retailCosSetup.rollupDSDZonesToWhseZone(retailCostDataMap, itemLookupMap, itemCodeCategoryMap, dsdAndWhseZoneMap);
				
				Set<String> noItemCodeSet = new HashSet<String>();
				logger.info("Processing Cost roll up ");
				HashMap<String, List<RetailCostDTO>> scanBackrolledUpMap = retailCosSetup.costRollUpV2(retailCostDataMap, null, null, noItemCodeSet,
						chainId);
				
				List<RetailCostDTO> insertList = new ArrayList<RetailCostDTO>();
				for (List<RetailCostDTO> costDTOList : scanBackrolledUpMap.values()) {
					for (RetailCostDTO retailCostDTO : costDTOList) {
						if (retailCostDTO.getLevelId() != null) {
							insertList.add(retailCostDTO);
						}
					}
				}
				logger.info("Processing roll up completed");
				updateLocationId(insertList);
				logger.info("location id updated ");
				// List<String> scanBackItemCodes = new ArrayList<String>(scanBackrolledUpMap.keySet());
				logger.info("Deleting Retail cost for the scan back Items for Week calendar Id: "+processingWeekCalId);
				toDeleteCostBasedOnScanBackItems(scanBackrolledUpMap.keySet(), processingWeekCalId);

				long startTime = System.currentTimeMillis();
				retailCostDAO.saveRetailCostData(conn, insertList);
				long endTime = System.currentTimeMillis();
				logger.debug("Time taken for inserting data into retail_cost_info - " + (endTime - startTime));
			}
		}
		
		
		if (!skippedRetailerItemCode.isEmpty()) {
			List<String> skippedRetailerItems = new ArrayList<String>(skippedRetailerItemCode);
			logger.error("Retailer item code not matching: " + PRCommonUtil.getCommaSeperatedStringFromStrArray(skippedRetailerItems));
		}
		if (!listCostIsZero.isEmpty()) {
			List<String> listCostIsZeros = new ArrayList<String>(listCostIsZero);
			logger.error("List Cost is Zero for list of Items: " + PRCommonUtil.getCommaSeperatedStringFromStrArray(listCostIsZeros));
		}
	}
	
	/**
	 * To delete Cost records based on Scan back Items
	 * @param scanBackItemCodes
	 * @throws GeneralException
	 */
	private void toDeleteCostBasedOnScanBackItems(Set<String> scanBackItemCodes, int processingCalId) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
		int itemNoInBatch = 0;
		for(String itemCode : scanBackItemCodes){
			
        	itemCodeList.add(itemCode);
        	itemNoInBatch++;
        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
        		String itemCodeValue = PRCommonUtil.getCommaSeperatedStringFromStrArray(itemCodeList);
				retailCostDAO.deleteCostInfoUsingScanBackItems(conn, processingCalId, itemCodeValue);
				itemCodeList.clear();
        		itemNoInBatch = 0;
        	}
		}if(itemNoInBatch > 0){
//			logger.info("Size of item batch:"+itemNoInBatch);
			String itemCodeValue = PRCommonUtil.getCommaSeperatedStringFromStrArray(itemCodeList);
			retailCostDAO.deleteCostInfoUsingScanBackItems(conn, processingCalId, itemCodeValue);
		}
	}
	/**
	 * To update location based on level type id
	 * 
	 * @param insertList
	 */
	public void updateLocationId(List<RetailCostDTO> insertList) {
		for (RetailCostDTO retailCostDTO : insertList) {
			// Update chain id for zone level records
			if (retailCostDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID)
				retailCostDTO.setLocationId(Integer.parseInt(retailCostDTO.getLevelId()));
			// Update price zone id from the cache when there is a zone level
			// record
			else if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
				// Set location id only if it is available else put it as null
				if (retailPriceZone.get(retailCostDTO.getLevelId()) != null)
					retailCostDTO.setLocationId(retailPriceZone.get(retailCostDTO.getLevelId()));
			}
			// Update comp_str_id from the cache when there is a store level
			// record
			else if (retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
				// Set location id only if it is available else put it as null
				if (storeIdsMap.get(retailCostDTO.getLevelId()) != null)
					retailCostDTO.setLocationId(storeIdsMap.get(retailCostDTO.getLevelId()));
			}
		}
	}

	/**
	 * To get values from item lookup table and group them based on retailer
	 * item code
	 * 
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, List<ItemDTO>> getRetailerItemCodeMap() throws GeneralException {
		ItemDAO itemDAO = new ItemDAO();
		HashMap<String, List<ItemDTO>> retailerItemCodeMap = new HashMap<String, List<ItemDTO>>();
		activeItems = itemDAO.getAllActiveItems(conn);
		for (ItemDTO itemDTO : activeItems) {
			if (retailerItemCodeMap.get(itemDTO.getRetailerItemCode()) == null) {
				List<ItemDTO> items = new ArrayList<>();
				items.add(itemDTO);
				retailerItemCodeMap.put(itemDTO.getRetailerItemCode(), items);
			} else {
				List<ItemDTO> items = retailerItemCodeMap.get(itemDTO.getRetailerItemCode());
				items.add(itemDTO);
				retailerItemCodeMap.put(itemDTO.getRetailerItemCode(), items);
			}
		}
		return retailerItemCodeMap;
	}

	/**
	 * Set item code value based on the retailer item code and form level id and
	 * group by using item code as a key..
	 * 
	 * @param retailerItemCodeMap
	 * @return
	 */
	public HashMap<String, List<ScanBackDTO>> setItemCodeBasedOnRetailerItemCode(
			HashMap<String, List<ItemDTO>> retailerItemCodeMap, List<ScanBackDTO> scanBackList) {
		HashMap<String, List<ScanBackDTO>> itemcodeAndValuesMap = new HashMap<String, List<ScanBackDTO>>();
		// Iterate scanBack Input List to get Upc values
		for (ScanBackDTO scanBackDTO : scanBackList) {
			if (retailerItemCodeMap.containsKey(scanBackDTO.getRetailerItemCode())) {
				List<ItemDTO> retailerItemCodeValues = retailerItemCodeMap.get(scanBackDTO.getRetailerItemCode());
				// Set item code values by looping list of
				// retailerItemCodeValues
				for (ItemDTO itemDTO : retailerItemCodeValues) {
					// Set item code
					scanBackDTO.setItemCode(String.valueOf(itemDTO.getItemCode()));
					// Set level Id usign Bnr code, Zone number, Price group
					// code and Splr number
					String levelId = scanBackDTO.getBnrCD() + "-" + scanBackDTO.getZoneNumber() + "-"
							+ itemDTO.getPrcGrpCd();
					if ((!Constants.EMPTY.equals(scanBackDTO.getSplrNo().trim()) && scanBackDTO.getSplrNo() != null)) {
						levelId = levelId + "-" + scanBackDTO.getSplrNo();
					}
					scanBackDTO.setLevelId(levelId);
					
					//Set prc group code
					scanBackDTO.setPrcGrpCode(itemDTO.getPrcGrpCd());
					
					// Group by based on item code...
					List<ScanBackDTO> scanBackDTOs = new ArrayList<ScanBackDTO>();
					if (itemcodeAndValuesMap.containsKey(scanBackDTO.getItemCode())) {
						scanBackDTOs = itemcodeAndValuesMap.get(scanBackDTO.getItemCode());
					}
					scanBackDTOs.add(scanBackDTO);
					itemcodeAndValuesMap.put(scanBackDTO.getItemCode(), scanBackDTOs);
				}
			} else {
				skippedRetailerItemCode.add(scanBackDTO.getRetailerItemCode());
			}
		}
		return itemcodeAndValuesMap;
	}

	/**
	 * To get zone from store item map based on item code...
	 * 
	 * @param itemCodes
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, List<String>> getPriceZoneFromStoreItemMap(Set<String> itemCodes) throws GeneralException {
		HashMap<String, List<String>> StoreItemMap = new HashMap<String, List<String>>();
		HashMap<String, List<String>> StoreItemMapTemp = new HashMap<String, List<String>>();
		int limitcount = 0;
		List<String> itemCodeList = new ArrayList<String>();
	
		for (String itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % 1000 == 0)) {
				StoreItemMapTemp = retailCostDAO.getPrizeZoneFromStoreItemMap(conn, itemCodeList);
				StoreItemMap.putAll(StoreItemMapTemp);
				itemCodeList.clear();
			}
		}
		if (itemCodeList.size() > 0) {
			StoreItemMapTemp = retailCostDAO.getPrizeZoneFromStoreItemMap(conn, itemCodeList);
			StoreItemMap.putAll(StoreItemMapTemp);
			itemCodeList.clear();
		}

		return StoreItemMap;
	}

	@SuppressWarnings({ "unchecked" })
	public void processRecords(List listobj) throws GeneralException {
		scanBackInputList.addAll((List<ScanBackDTO>) listobj);
	}
	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate
	 *            Input Date
	 * @throws GeneralException
	 */
	private void populateCalendarId(String weekStartDate) throws GeneralException {
		conn = getOracleConnection();

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		calendarId = calendarDTO.getCalendarId();
		startDate = calendarDTO.getStartDate();

		String prevWeekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 1);
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate, Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();
		logger.info("Previous Week Calendar Id - " + calendarDTO.getCalendarId());
		prevCalendarId = calendarDTO.getCalendarId();
	}

	/**
	 * Unroll to zone level cost entries for each itemcode..
	 * 
	 * @param costRolledUpMap
	 * @param priceZoneFromStoreItemMap
	 * @return
	 * @throws CloneNotSupportedException
	 */
	private HashMap<String, List<RetailCostDTO>> getZoneLevelCostEntries(
			HashMap<String, List<RetailCostDTO>> costRolledUpMap,
			HashMap<String, List<String>> priceZoneFromStoreItemMap)
			throws CloneNotSupportedException, GeneralException {
		HashMap<String, List<RetailCostDTO>> zoneLevelCostList = new HashMap<String, List<RetailCostDTO>>();

		// Iterating cos rolled up map based on item code to set zone level
		// cost...
		for (Map.Entry<String, List<RetailCostDTO>> retailCostEntry : costRolledUpMap.entrySet()) {
			HashMap<String, RetailCostDTO> costData = new HashMap<>();
			List<RetailCostDTO> retailCostDTOs = retailCostEntry.getValue();
			List<String> zoneList = new ArrayList<String>();
			// Get list of zone available for the particular item code and set
			// values for those zones
			if (priceZoneFromStoreItemMap.get(retailCostEntry.getKey()) != null) {
				zoneList = priceZoneFromStoreItemMap.get(retailCostEntry.getKey());
				for (RetailCostDTO retailCostDTO : retailCostDTOs) {
					if (Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()) {
						for (String zone : zoneList) {
							RetailCostDTO costDTONew = (RetailCostDTO) retailCostDTO.clone();
							costDTONew.setLevelId(zone);
							costDTONew.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
							String zoneKey = costDTONew.getLevelTypeId() + "-" + costDTONew.getLevelId();
							costData.put(zoneKey, costDTONew);
						}
					}
				}
				for (RetailCostDTO retailCostDTO : retailCostDTOs) {
					if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()) {
						RetailCostDTO costDTONew = (RetailCostDTO) retailCostDTO.clone();
						String zoneKey = costDTONew.getLevelTypeId() + "-" + costDTONew.getLevelId();
						costData.put(zoneKey, costDTONew);
					}
				}
				// Iterate the cost data to set the values based on Item code
				List<RetailCostDTO> retailCostList = new ArrayList<RetailCostDTO>();
				for (RetailCostDTO retailCostDTO : costData.values()) {
					if(!retailCostDTO.isWhseZoneRolledUpRecord())
						retailCostList.add(retailCostDTO);
				}
				zoneLevelCostList.put(retailCostEntry.getKey(), retailCostList);
			} else {
				logger.warn("Item is  Unauthorised: " + retailCostEntry.getKey());
			}
		}

		return zoneLevelCostList;

	}

	/**
	 * Handling Scan back values from list cost and adding them in the list..
	 * 
	 * @param zoneLevelCostList
	 * @param scanBackDetails
	 */
	public void subtractScanBackCost(HashMap<String, List<RetailCostDTO>> zoneLevelCostList,
			HashMap<String, List<ScanBackDTO>> scanBackDetails) {
		logger.info("Processing Scan back values");
		List<ScanBackDTO> scanBackDTOs = new ArrayList<ScanBackDTO>();
		// Iterate each zone to get scan back values..
		for (Map.Entry<String, List<RetailCostDTO>> entry : zoneLevelCostList.entrySet()) {
			scanBackDTOs = scanBackDetails.get(entry.getKey());
			for (RetailCostDTO retailCostDTO : entry.getValue()) {
				// Get Scan back list to based on item code from Hash map
				// logger.debug("Cost Item code: "+retailCostDTO.getItemcode());

				// Iterate each scan back list for a particular zone number to get scan back cost...
				for (ScanBackDTO scanBackDTO : scanBackDTOs) {
					// If zone are matching then extract the scan back amount
					// for
					// particular zone and update in zone level cost list
					if (scanBackDTO.getLevelId().equals(retailCostDTO.getLevelId()) && retailCostDTO.getLevelId()!= null) {
						// logger.debug("Inside Scan Back Level Id: "+
						// retailCostDTO.getLevelId());
						if (scanBackDTO.getScanBackAmt1() != null && !scanBackDTO.getScanBackAmt1().equals("")) {
							retailCostDTO.setScanBackAmt1(Float.parseFloat(scanBackDTO.getScanBackAmt1()));
						}
						if (scanBackDTO.getScanBackAmt2() != null && !scanBackDTO.getScanBackAmt2().equals("")) {
							retailCostDTO.setScanBackAmt2(Float.parseFloat(scanBackDTO.getScanBackAmt2()));
						}
						if (scanBackDTO.getScanBackAmt3() != null && !scanBackDTO.getScanBackAmt3().equals("")) {
							retailCostDTO.setScanBackAmt3(Float.parseFloat(scanBackDTO.getScanBackAmt3()));
						}
						// Handle Actual cost by subtracting the deal cost and
						// to find Allowance cost
						// logger.debug("DealCost:
						// "+retailCostDTO.getDealCost());
						/*if (retailCostDTO.getDealCost() != 0) {
							retailCostDTO.setAllowanceAmount(retailCostDTO.getListCost() - retailCostDTO.getDealCost());
						}*/
						
						retailCostDTO.setScanBackStartDate1(scanBackDTO.getScanBackStartDate1());
						retailCostDTO.setScanBackStartDate2(scanBackDTO.getScanBackStartDate2());
						retailCostDTO.setScanBackStartDate3(scanBackDTO.getScanBackStartDate3());
						retailCostDTO.setScanBackEndDate1(scanBackDTO.getScanBackEndDate1());
						retailCostDTO.setScanBackEndDate2(scanBackDTO.getScanBackEndDate2());
						retailCostDTO.setScanBackEndDate3(scanBackDTO.getScanBackEndDate3());
						
						// To calculate total deductions using allowance value and scan back value
						if(retailCostDTO.getListCost() != 0){
							float totalDeductions = retailCostDTO.getAllowanceAmount() + scanBackDTO.getScanBackTotalAmt();
							float dealCost = (retailCostDTO.getListCost() - totalDeductions);
							retailCostDTO.setDealCost(dealCost);

							calculateFinalListCost(retailCostDTO);
							
						}else{
							listCostIsZero.add(entry.getKey());
						}

						/*retailCostDTO.setAllowStartDate(retailCostDTO.getDealStartDate());
						retailCostDTO.setAllowEndDate(retailCostDTO.getDealEndDate());*/
						if (scanBackDTO.getScanBackStartDate3() != null && scanBackDTO.getScanBackStartDate3() != "") {
							retailCostDTO.setDealStartDate(scanBackDTO.getScanBackStartDate3());
							if (scanBackDTO.getScanBackEndDate3() != null && scanBackDTO.getScanBackEndDate3() != "") {
								retailCostDTO.setDealEndDate(scanBackDTO.getScanBackEndDate3());
							}
						} else if (scanBackDTO.getScanBackStartDate2() != null
								&& scanBackDTO.getScanBackStartDate2() != "") {
							retailCostDTO.setDealStartDate(scanBackDTO.getScanBackStartDate2());
							if (scanBackDTO.getScanBackEndDate2() != null && scanBackDTO.getScanBackEndDate2() != "") {
								retailCostDTO.setDealEndDate(scanBackDTO.getScanBackEndDate2());
							}
						} else if (scanBackDTO.getScanBackStartDate1() != null
								&& scanBackDTO.getScanBackStartDate1() != "") {
							retailCostDTO.setDealStartDate(scanBackDTO.getScanBackStartDate1());

						}
						// To set deal cost end date using scan back amount 1 end date.
						if (scanBackDTO.getScanBackEndDate1() != null && scanBackDTO.getScanBackEndDate1() != "") {
							retailCostDTO.setDealEndDate(scanBackDTO.getScanBackEndDate1());
						}
						
//						//Set price grp code...
//						if(scanBackDTO.getPrcGrpCode()!=null){
//							retailCostDTO.setPrcGrpCode(scanBackDTO.getPrcGrpCode());
//						}else{
//							retailCostDTO.setPrcGrpCode("");
//						}
					}
				}
			}
		}
		logger.info("Scan back values processed");
	}
	
	
	/**
	 * 
	 * @param retailCostDTO
	 * @param scanBackDTO
	 * @return new list cost
	 */
	private void calculateFinalListCost(RetailCostDTO retailCostDTO){
		// Get long term allownace
		float allowanceAmt = 0;
		if(String.valueOf(Constants.YES).equals(retailCostDTO.getLongTermFlag())){
			allowanceAmt = retailCostDTO.getAllowanceAmount();
		}
		
		// Find scan backs with long-term deal
		float longTermScanBackAmtTotal = getLongTermScanBackAmt(startDate, retailCostDTO);		
		
		if(longTermScanBackAmtTotal > 0 || allowanceAmt > 0){
			float finalListCost = retailCostDTO.getListCost() - (longTermScanBackAmtTotal + allowanceAmt);
			
			retailCostDTO.setFinalListCost(finalListCost);	
		}
	}
	
	/**
	 * 
	 * @param retailCostDTO
	 * @return total long term scan back
	 */
	private float getLongTermScanBackAmt(String startDate, RetailCostDTO retailCostDTO){
		float longTermScanBackAmt = 0;
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		
		int longTermScanBackDuration = Integer.parseInt(PropertyManager.getProperty("LONG_TERM_SCAN_BACK_DURATION","6"));
		
		LocalDate weekStartDate = LocalDate.parse(startDate, formatter);
		
		// Check scan back 1
		if(retailCostDTO.getScanBackAmt1() > 0){
		
			LocalDate scanBack1StartDate = LocalDate.parse(retailCostDTO.getScanBackStartDate1(), formatter);
			LocalDate scanBack1EndDate = LocalDate.parse(retailCostDTO.getScanBackEndDate1(), formatter);
			
			long weeks = ChronoUnit.WEEKS.between(scanBack1StartDate, scanBack1EndDate);
			
			if(weeks > longTermScanBackDuration){
				if((weekStartDate.isEqual(scanBack1StartDate) || weekStartDate.isAfter(scanBack1StartDate))
						&& weekStartDate.isBefore(scanBack1EndDate)){
					longTermScanBackAmt += retailCostDTO.getScanBackAmt1();
					retailCostDTO.setLongTermScan1(String.valueOf(Constants.YES));
				}
			}
			
			
		}
		
		// Check scan back 2
		if(retailCostDTO.getScanBackAmt2() > 0){
			
			LocalDate scanBack2StartDate = LocalDate.parse(retailCostDTO.getScanBackStartDate2(), formatter);
			LocalDate scanBack2EndDate = LocalDate.parse(retailCostDTO.getScanBackEndDate2(), formatter);
			
			long weeks = ChronoUnit.WEEKS.between(scanBack2StartDate, scanBack2EndDate);
			
			if(weeks > longTermScanBackDuration){
				if((weekStartDate.isEqual(scanBack2StartDate) || weekStartDate.isAfter(scanBack2StartDate))
						&& weekStartDate.isBefore(scanBack2EndDate)){
					longTermScanBackAmt += retailCostDTO.getScanBackAmt2();
					retailCostDTO.setLongTermScan2(String.valueOf(Constants.YES));
				}
			}
		}
		
		// Check scan back 3
		if(retailCostDTO.getScanBackAmt3() > 0){
		
			LocalDate scanBack3StartDate = LocalDate.parse(retailCostDTO.getScanBackStartDate3(), formatter);
			LocalDate scanBack3EndDate = LocalDate.parse(retailCostDTO.getScanBackEndDate3(), formatter);
			
			long weeks = ChronoUnit.WEEKS.between(scanBack3StartDate, scanBack3EndDate);
			
			if(weeks > longTermScanBackDuration){
				if((weekStartDate.isEqual(scanBack3StartDate) || weekStartDate.isAfter(scanBack3StartDate))
						&& weekStartDate.isBefore(scanBack3EndDate)){
					longTermScanBackAmt += retailCostDTO.getScanBackAmt3();
					retailCostDTO.setLongTermScan3(String.valueOf(Constants.YES));
				}
			}
		}
		
		return longTermScanBackAmt;
	}
	
	public HashMap<String, HashMap<String, ScanBackDTO>> findEachItemScanbackAmtInDiffWeek(String currentWeekStartDate, 
			List<ScanBackDTO> scanBackInputList){
		
		int noOfFutureWeeksToLoad = Integer.parseInt(PropertyManager.getProperty("NO_OF_FUTURE_WEEKS_TO_LOAD_IN_COST","13"));
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = new HashMap<>();
		
		String futureWeekStartDateAfterXWeeks = formatter.format(LocalDate.parse(currentWeekStartDate, formatter).plus(noOfFutureWeeksToLoad, ChronoUnit.WEEKS));
		
		// Loop Each scan back items and apply total scan back values
		scanBackInputList.forEach(scanBackItem -> {
			String key = scanBackItem.getRetailerItemCode() + "-" + scanBackItem.getBnrCD() + "-" + scanBackItem.getZoneNumber();
			if (scanBackItem.getSplrNo() != null && !scanBackItem.getSplrNo().isEmpty()) {
				key = key + "-" + scanBackItem.getSplrNo();
			}

			String sbWeekStartDate = costFileFormatter.getWeekStartDateCalDTO(scanBackItem.getScanBackStartDate()).getStartDate();
			String sbWeekEndDate = costFileFormatter.getWeekStartDateCalDTO(scanBackItem.getScanBackEndDate()).getEndDate();

			long diffBtwAllowStartAndCurWeek = ChronoUnit.DAYS.between(LocalDate.parse(currentWeekStartDate, formatter),
					LocalDate.parse(scanBackItem.getScanBackStartDate(), formatter));

			// Get no of weeks between each allowance (By default one week is given)
			List<String> futureWeeks = new ArrayList<>();
			String processFromWeekStart = null;

			if (diffBtwAllowStartAndCurWeek < 0) {
				processFromWeekStart = currentWeekStartDate;
			} else {
				processFromWeekStart = sbWeekStartDate;
			}
			long promoDuration = ChronoUnit.WEEKS.between(LocalDate.parse(processFromWeekStart, formatter),
					LocalDate.parse(sbWeekEndDate, formatter));
			futureWeeks = costFileFormatter.getFutureWeeks(processFromWeekStart, (int) promoDuration);

			// to consider current week
			futureWeeks.add(processFromWeekStart);

			for (String weekStartDate : futureWeeks) {
				
				// Process Week records only if it is within given limited weeks
				if (ChronoUnit.WEEKS.between(LocalDate.parse(futureWeekStartDateAfterXWeeks, formatter),
						LocalDate.parse(weekStartDate, formatter)) <= 0) {
					HashMap<String, ScanBackDTO> scanBackBasedOnWeek = new HashMap<>();
					
					if (multiWeekScanbackDetails.get(weekStartDate) != null) {
						scanBackBasedOnWeek = multiWeekScanbackDetails.get(weekStartDate);
					}
					ScanBackDTO scanBackDTO = new ScanBackDTO();
					if (scanBackBasedOnWeek.get(key) != null) {
						scanBackDTO = scanBackBasedOnWeek.get(key);

						float sbTotalAmt = scanBackDTO.getScanBackTotalAmt();
						if (!scanBackItem.getScanBackAmt().trim().equals("") && scanBackItem.getScanBackAmt() != null) {
							sbTotalAmt += Float.parseFloat(scanBackItem.getScanBackAmt());
						}

						scanBackDTO.setScanBackTotalAmt(sbTotalAmt);
						if (scanBackDTO.getScanBackAmt2() == null || scanBackDTO.getScanBackAmt2().isEmpty()) {
							scanBackDTO.setScanBackAmt2(scanBackItem.getScanBackAmt());
							scanBackDTO.setScanBackStartDate2(scanBackItem.getScanBackStartDate());
							scanBackDTO.setScanBackEndDate2(scanBackItem.getScanBackEndDate());
						} else {
							if (scanBackDTO.getScanBackAmt3() == null || scanBackDTO.getScanBackAmt3().isEmpty()){
								scanBackDTO.setScanBackAmt3(scanBackItem.getScanBackAmt());
								scanBackDTO.setScanBackStartDate3(scanBackItem.getScanBackStartDate());
								scanBackDTO.setScanBackEndDate3(scanBackItem.getScanBackEndDate());
							}
							// If scan back has more than 3 records for an item, then add scan back amount in Scan back 3.
							else if (scanBackDTO.getScanBackAmt3() != null && !scanBackDTO.getScanBackAmt3().isEmpty() && 
									!scanBackItem.getScanBackAmt().trim().equals("") && scanBackItem.getScanBackAmt() != null) {
								float tempScanBack = Float.parseFloat(scanBackDTO.getScanBackAmt3()) + Float.parseFloat(scanBackItem.getScanBackAmt());
								scanBackDTO.setScanBackAmt3(String.valueOf(tempScanBack));
							}
						}
					} else {
						try {
							scanBackDTO = (ScanBackDTO) scanBackItem.clone();
							scanBackDTO.setScanBackTotalAmt(Float.parseFloat(scanBackItem.getScanBackAmt()));
							scanBackDTO.setScanBackAmt1(scanBackItem.getScanBackAmt());
							scanBackDTO.setScanBackStartDate1(scanBackItem.getScanBackStartDate());
							scanBackDTO.setScanBackEndDate1(scanBackItem.getScanBackEndDate());
						} catch (Exception e) {
							e.printStackTrace();
							logger.error("Error while cloning ScanBackDTO in findEachItemScanbackAmtInDiffWeek()....", e);
						}
					}

					scanBackBasedOnWeek.put(key, scanBackDTO);
					multiWeekScanbackDetails.put(weekStartDate, scanBackBasedOnWeek);
				}
			}
		});
		return multiWeekScanbackDetails;
	}
}
