/*
 * Title: Cost Setup
 *
 **********************************************************************************
 * Modification History
 *---------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *---------------------------------------------------------------------------------
 * Version 0.1	08/13/2012	Janani			Initial Version
 **********************************************************************************
 */
package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.prestoload.MovementWeeklyDataLoadV2;
import com.pristine.dataload.prestoload.RetailCostSetup;
import com.pristine.dataload.service.PriceAndCostService;
import com.pristine.dataload.service.StoreItemMapService;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.NumberUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

/**
 * 
 * @author Janani
 */
public class CostDataLoad extends PristineFileParser {

	private static Logger logger = Logger.getLogger("CostDataLoad");

	private static LinkedHashMap COST_FIELD = null;

	HashMap<ItemDetailKey, List<PriceAndCostDTO>> costDataMap = new HashMap<ItemDetailKey, List<PriceAndCostDTO>>();
	HashMap<String, String> storeInfoMap = null;
	HashMap<String, List<String>> zoneStoreMap = null;
	HashMap<ItemDetailKey, String> itemCodeMap = null;
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	HashMap<String, Double> unrolledCostDataMap = new HashMap<String, Double>();
	// HashMap<String, RetailCostDTO> unrolledRCostDataMap = new HashMap<String,
	// RetailCostDTO>();
	HashMap<String, List<RetailCostDTO>> retailCostCache = new HashMap<String, List<RetailCostDTO>>();

	List<PriceAndCostDTO> currentProcCostList = new ArrayList<PriceAndCostDTO>();

	int costRecordCount = 0;
	int calendarId = -1;
	int prevCalendarId = -1;
	int commitRecCount = 1000;

	String startDate = null;
	String prevWkStartDate = null;
	static boolean isCopyEnabled = true;
	private CostDAO costDAO = null;
	private RetailPriceDAO retailPriceDAO = null;
	private RetailCostDAO retailCostDAO = null;

	String chainId = null;
	Connection conn = null;
  	//Changes for populating store id in Store item map table.
  	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
  	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
  	private static String ignoreStoresNotInDB = "";
  	private Set<String> storesWithNoZone = new HashSet<String>();
  	List<PriceAndCostDTO> ignoredRecords = new ArrayList<PriceAndCostDTO>();
  	StoreItemMapService storeItemMapService = null;
	Set<String> levelIdNotFound= new HashSet<String>(); 
  	public CostDataLoad(){
  		
  	}
  	
  	/**
	 * Arguments args[0] Relative path of Cost File args[1] weektype -
	 * lastweek/currentweek/nextweek/specificweek args[2] Date if weektype is
	 * specific week args[3] FULL or DELTA file
	 * 
	 * @param args
	 */
  	
  	
  	
	public static void main(String[] args) {

		CostDataLoad dataload = new CostDataLoad();
		PropertyConfigurator.configure("log4j-cost-retail.properties");

		String weekType = null;
		String mode = null;
		String copyExisting = null;
		if (args[args.length - 1].startsWith(Constants.DATA_LOAD_MODE)) {
			mode = args[args.length - 1].substring(Constants.DATA_LOAD_MODE
					.length());
		}

		// Default week type to currentweek if it is not specified
		if ((mode == null && args.length == 1)
				|| (mode != null && args.length == 2)) {
			weekType = Constants.CURRENT_WEEK;
		} else {
			weekType = args[1];
		}

		if (args[2].startsWith(Constants.IGN_COPY_EXISTING_DATA)
				|| args[3].startsWith(Constants.IGN_COPY_EXISTING_DATA)) {
			if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType))
				copyExisting = args[3]
						.substring(Constants.IGN_COPY_EXISTING_DATA.length());
			else
				copyExisting = args[2]
						.substring(Constants.IGN_COPY_EXISTING_DATA.length());
		}

		if (!(Constants.NEXT_WEEK.equalsIgnoreCase(weekType)
				|| Constants.CURRENT_WEEK.equalsIgnoreCase(weekType)
				|| Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) || Constants.LAST_WEEK
					.equalsIgnoreCase(weekType))) {
			logger.info("Invalid Argument, args[1] should be lastweek/currentweek/nextweek/specificweek");
			System.exit(-1);
		}

		if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType)
				&& ((mode == null && copyExisting == null && args.length != 3)
						|| (mode != null && copyExisting != null && args.length != 5)
						|| (mode == null && copyExisting != null && args.length != 4) || (mode != null
						&& copyExisting == null && args.length != 4))) {
			logger.info("Invalid Arguments, args[0] should be relative path of cost file, "
					+ " args[1] lastweek/currentweek/nextweek/specificweek, args[2] data for a specific week");
			System.exit(-1);
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
				dateStr = DateUtil
						.getWeekStartDate(DateUtil.toDate(args[2]), 0);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - "
						+ exception.toString());
				System.exit(-1);
			}

		}
		if (copyExisting.equals(Constants.VALUE_TRUE)) {
			isCopyEnabled = false;
		}
		dataload.setupObjects(dateStr, mode);
		dataload.processInputFiles(args[0]);
	}

	/**
	 * Sets up class level objects for the given date
	 * 
	 * @param dateStr
	 */
	private void setupObjects(String dateStr, String mode) {
		try {

			String tempCommitCount = PropertyManager.getProperty(
					"DATALOAD.COMMITRECOUNT", "1000");
			commitRecCount = Integer.parseInt(tempCommitCount);

			populateCalendarId(dateStr);

			costDAO = new CostDAO();
			retailPriceDAO = new RetailPriceDAO();
			retailCostDAO = new RetailCostDAO();

			// Retrieve Subscriber Chain Id
			chainId = retailPriceDAO.getChainId(conn);
			logger.info("Subscriber Chain Id - " + chainId);

			// Retrieve all stores and its corresponding zone#
			storeInfoMap = costDAO.getStoreZoneInfo(conn, chainId);
			castZoneNumbers(storeInfoMap);
			logger.info("No of store available - " + storeInfoMap.size());

			// Populate a map with zone# as key and list of corresponding stores
			// as value
			
			//Retrieve all items
			//Changes by Pradeep on 06/24/2014.
			long startTime  = System.currentTimeMillis();
			ItemDAO itemDAO = new ItemDAO();
			itemCodeMap = itemDAO.getAllItemsFromItemLookupV2(conn);
			long endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to cache all items - " + ((endTime - startTime)/1000) + " seconds");
		 	//Changes for populating store id in Store item map table.
			//Added by Pradeep 04-27-2015
			startTime  = System.currentTimeMillis();
			storeIdsMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
			endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to retreive store id mapping - " + (endTime - startTime));
			startTime = System.currentTimeMillis();
			retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
			endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to retreive zone id mapping - " + (endTime - startTime));
			//Changes ends.
			
			zoneStoreMap = getZoneMapping(storeInfoMap);
			logger.info("No of zones available - " + zoneStoreMap.size());

			// Delete from retail_cost_info for the week being processed
			if (mode != null && isCopyEnabled)
				retailCostDAO.deleteRetailCostData(conn, calendarId);

			if (mode != null && mode.equals(Constants.DATA_LOAD_DELTA)
					&& isCopyEnabled) {
				// Insert previous week retail cost value
				retailCostDAO.setupRetailCostData(conn, calendarId,
						prevCalendarId);
				PristineDBUtil.commitTransaction(conn, "Cost Data Setup");
			}
			
			storeItemMapService = new StoreItemMapService(storeIdsMap, retailPriceZone, storeInfoMap);
		} catch (GeneralException ge) {
			logger.error("Error in setting up objects", ge);
			return;
		}
	}

	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate
	 *            Input Date
	 * @throws GeneralException
	 */
	private void populateCalendarId(String weekStartDate)
			throws GeneralException {
		conn = getOracleConnection();

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn,
				weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		calendarId = calendarDTO.getCalendarId();
		startDate = calendarDTO.getStartDate();

		String prevWeekStartDate = DateUtil.getWeekStartDate(
				DateUtil.toDate(weekStartDate), 1);
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate,
				Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();
		logger.info("Previous Week Calendar Id - "
				+ calendarDTO.getCalendarId());

		prevCalendarId = calendarDTO.getCalendarId();
	}

	/**
	 * Processes records in both cost and billback data files
	 * 
	 * @param relativePath
	 *            Relative path of cost file
	 * @param billbackRelativePath
	 *            Relative path of billback file
	 * @param fileType
	 *            COST/BILLBACK file
	 */
	private void processInputFiles(String relativePath) {
		ArrayList<String> fileList = null;

		// get zip files
		ArrayList<String> zipFileList = null;

		logger.info("Setting up Cost Information ");
		String zipFilePath = getRootPath() + "/" + relativePath;

		try {
			zipFileList = getZipFiles(relativePath);
		} catch (GeneralException ge) {
			logger.error("Error in setting up objects", ge);
			return;
		}

		// Start with -1 so that if any regular files are present, they are
		// processed first
		int curZipFileCount = -1;
		boolean processZipFile = false;
		boolean commit = true;
		do {
			try {

				if (processZipFile)
					PrestoUtil.unzip(zipFileList.get(curZipFileCount),
							zipFilePath);

				fileList = getFiles(relativePath);
				for (int i = 0; i < fileList.size(); i++) {
					String files = fileList.get(i);
					int stopCount = Integer.parseInt(PropertyManager
							.getProperty("DATALOAD.STOP_AFTER", "-1"));

					parseTextFile(PriceAndCostDTO.class, files,
							getCost_Field(), stopCount);
					logger.info("No of cost data records = " + costRecordCount);

					if(ignoredRecords.size() > 0){
						//Save all ignored records
						logger.error("Some records were missed while processing cost file. Please check IGNORED_COST_RECORDS table for calendar_id - " + calendarId);
						retailCostDAO.saveIgnoredData(conn, ignoredRecords);
						//Reuse list for next file.
						ignoredRecords.clear();
					}
					
					PristineDBUtil.commitTransaction(conn, "Cost Data Setup");
					PristineDBUtil.close(conn);
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

			String archivePath = getRootPath() + "/" + relativePath + "/";
			if (commit) {
				PrestoUtil.moveFiles(fileList, archivePath
						+ Constants.COMPLETED_FOLDER);
			} else {
				PrestoUtil.moveFiles(fileList, archivePath
						+ Constants.BAD_FOLDER);
			}

			curZipFileCount++;
			processZipFile = true;
		} while (curZipFileCount < zipFileList.size());
		if(!levelIdNotFound.isEmpty()){
	    	List<String> skippedStores = new ArrayList<String>(levelIdNotFound);
			logger.error("Level id not found for list of Stores: "
					+ PRCommonUtil.getCommaSeperatedStringFromStrArray(skippedStores));
	    }
		logger.info("Cost/Billback Data Load successfully completed");

		return;

	}

	public void processRecords(List listObject) throws GeneralException {
		List<PriceAndCostDTO> costList = (List<PriceAndCostDTO>) listObject;

		if (costList != null && costList.size() > 0) {
			PriceAndCostDTO costDTO = costList.get(0);

			if (costList.size() >= commitRecCount) {
				if (currentProcCostList.size() >= Constants.RECS_TOBE_PROCESSED) {
					String lastUPC = currentProcCostList.get(
							currentProcCostList.size() - 1).getUpc();
					boolean eofFlag = true;
					for (PriceAndCostDTO priceandcostDTO : costList) {
						if (priceandcostDTO.getUpc().equals(lastUPC)) {
							currentProcCostList.add(priceandcostDTO);
						} else {
							if (currentProcCostList.size() >= Constants.RECS_TOBE_PROCESSED) {
								processCostRecords(currentProcCostList);
								currentProcCostList.clear();
								eofFlag = false;
							}
							currentProcCostList.add(priceandcostDTO);
						}
					}
					if (eofFlag) {
						processCostRecords(currentProcCostList);
						currentProcCostList.clear();
					}
				} else {
					currentProcCostList.addAll(costList);
				}
			} else {
				currentProcCostList.addAll(costList);
				processCostRecords(currentProcCostList);
				currentProcCostList.clear();
			}
		}
	}

	/**
	 * Processes Cost records (Popuates costDataMap with UPC as key and
	 * corresponding list of cost records as value)
	 * 
	 * @param costList
	 *            List of Cost records to be processed
	 */
	private void processCostRecords(List<PriceAndCostDTO> costList) {
		costDataMap = new HashMap<ItemDetailKey, List<PriceAndCostDTO>>();
		for (PriceAndCostDTO costDTO : costList) {
			String upc = PrestoUtil.castUPC(costDTO.getUpc(), false);
			costDTO.setUpc(upc);
			//Changes by Pradep on 06/24/2015.
			String sourceVendor = costDTO.getVendorNo();
			String itemNo = costDTO.getItemNo();
			costDTO.setCalendarId(calendarId); 
			String ret_item_code = sourceVendor + itemNo;
			ItemDetailKey itemDetailKey = new ItemDetailKey(upc, ret_item_code);
			//Changes Ends.
			
			// Store level records
			boolean canBeAdded = true;
			if (costDataMap.get(itemDetailKey) != null) {
				List<PriceAndCostDTO> tempList = costDataMap.get(itemDetailKey);

				// Logic to handle duplicates in input file
				for (PriceAndCostDTO tempDTO : tempList) {
					if (tempDTO.getSourceCode().equals(costDTO.getSourceCode())
							&& tempDTO.getZone().equals(costDTO.getZone())) {
						if (tempDTO.getRecordType().equals(
								costDTO.getRecordType())) {
							canBeAdded = false;
						}
						// Code changes to give priority to record type A than
						// record type C
						else if (Constants.RECORD_TYPE_ADDED.equals(tempDTO
								.getRecordType())
								&& Constants.RECORD_TYPE_UPDATED.equals(costDTO
										.getRecordType())) {
							canBeAdded = false;
						} else if (Constants.RECORD_TYPE_UPDATED.equals(tempDTO
								.getRecordType())
								&& Constants.RECORD_TYPE_ADDED.equals(costDTO
										.getRecordType())) {
							tempDTO.setRecordType(costDTO.getRecordType());
							tempDTO.setCostEffDate(costDTO.getCostEffDate());
							tempDTO.setStrCurrentCost(costDTO
									.getStrCurrentCost());
							tempDTO.setPromoCostEffDate(costDTO
									.getPromoCostEffDate());
							tempDTO.setPromoCostEndDate(costDTO
									.getPromoCostEndDate());
							tempDTO.setStrPromoCost(costDTO.getStrPromoCost());
							tempDTO.setCompanyPack(costDTO.getCompanyPack());
							canBeAdded = false;
						}
					}
				}
				if (canBeAdded) {
					tempList.add(costDTO);
					costDataMap.put(itemDetailKey, tempList);
				}
			} else {
				List<PriceAndCostDTO> tempList = new ArrayList<PriceAndCostDTO>();
				tempList.add(costDTO);
				costDataMap.put(itemDetailKey, tempList);
			}

			costRecordCount++;
			if (costRecordCount % Constants.RECS_TOBE_PROCESSED == 0)
				logger.info("No of cost records Processed " + costRecordCount);
		}

		try {
			loadCostData();

			PristineDBUtil.commitTransaction(conn, "Cost Data Setup");
		} catch (GeneralException exception) {
			logger.error("Exception in processCostRecords of CostDataLoad - "
					+ exception);
		}
	}

	private void loadCostData() throws GeneralException {
		// Retrieve Item Code for UPCs in the costDataMap populated from input
		// file
		//itemCodeMap = retailPriceDAO.getItemCode(conn, costDataMap.keySet());
		/*logger.info("No of cost data records with Item Code - "
				+ itemCodeMap.size());*/

		// Insert/Update/Delete item mapping in store_item_map table
		boolean costIndicator = true;
		long startTime = System.currentTimeMillis();
		HashMap<String, Long> vendorInfo = storeItemMapService.setupVendorInfo(conn, costDataMap);
		storeItemMapService.mergeIntoStoreItemMap(conn, costDataMap, vendorInfo, itemCodeMap, costIndicator,levelIdNotFound);
		long endTime = System.currentTimeMillis();
		logger.info("loadCostData() - Time taken to merge data into store_item_map - " + (endTime - startTime) + "ms");
		
		
		// Process every 1000 UPCs in costDataMap
		Set<ItemDetailKey> itemSet = costDataMap.keySet();

		if (itemSet.size() > 0)
			try {
				processCostData(storeInfoMap, zoneStoreMap, itemSet);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("loadCostData() - Error while processing data - " + e.toString());
				e.printStackTrace();
			}
	}
	
	/**
	 * Sets up RetailCostDTO for PriceAndCostDTO passed as input
	 * 
	 * @param priceandCostDTO
	 *            PriceAndCostDTO from which RetailCostDTO needs to be setup
	 * @param retailCostDTO
	 *            RetailCostDTO to be setup
	 */
	private void setupRetailCostDTO(PriceAndCostDTO priceandCostDTO,
			RetailCostDTO retailCostDTO) {
		try {
			/*
			 * Per item cost = ((Smaller of (CURR-COST, PROMO-COST) - (BB-AMT +
			 * BA-AMT)) / CasePack) - BS-AMT
			 */

			float smallerCost = priceandCostDTO.getCurrentCost();
			if (priceandCostDTO.getPromoCost() > 0.0
					&& smallerCost > priceandCostDTO.getPromoCost())
				smallerCost = priceandCostDTO.getPromoCost();
			retailCostDTO.setListCost(priceandCostDTO.getCurrentCost()
					/ priceandCostDTO.getCasePack());
			retailCostDTO.setLevel2Cost(priceandCostDTO.getPromoCost()
					/ priceandCostDTO.getCasePack());
			// Commenting deal cost related field population since they will be cleared as first step in processing billback file
			/*retailCostDTO.setDealCost(priceandCostDTO.getPromoCost()
					/ priceandCostDTO.getCasePack());*/
			retailCostDTO.setEffListCostDate(formatDate(priceandCostDTO
					.getCostEffDate()));
			/*retailCostDTO.setDealStartDate(formatDate(priceandCostDTO
					.getPromoCostEffDate()));
			retailCostDTO.setDealEndDate(formatDate(priceandCostDTO
					.getPromoCostEndDate()));*/
			// Changes to store Off Invoice start and end date for TOPS
			retailCostDTO.setLevel2StartDate(formatDate(priceandCostDTO.getPromoCostEffDate()));
			retailCostDTO.setLevel2EndDate(formatDate(priceandCostDTO.getPromoCostEndDate()));
			// Changes to store Off Invoice start and end date for TOPS - Ends
		} catch (Exception e) {
			logger.error("Building Retail Cost DTO - JavaException"
					+ ", UPC = + " + priceandCostDTO.getUpc() + ", STORE = + "
					+ priceandCostDTO.getZone(), e);
			retailCostDTO = null;
		}
	}

	/**
	 * Formats given date
	 * 
	 * @param inputDate
	 * @return
	 */
	private String formatDate(String inputDate) {
		String formatDate = "";
		if (inputDate.equals("000000"))
			formatDate = "";
		else {
			formatDate = inputDate.substring(0, 2) + "/"
					+ inputDate.substring(2, 4) + "/" + "20"
					+ inputDate.substring(4);
		}
		return formatDate;

	}

	/**
	 * Populates price and cost information in PriceAndCostDTO
	 * 
	 * @param priceandCostDTO
	 * @return
	 */
	private boolean populatePrices(PriceAndCostDTO priceandCostDTO) {
		boolean retVal = true;

		try {
			float cost = Float.parseFloat(priceandCostDTO.getStrCurrentCost()
					.trim());
			priceandCostDTO.setCurrentCost(NumberUtil.RoundFloat(cost / 10000,
					4)); // Round it to 2

			float Promo = Float.parseFloat(priceandCostDTO.getStrPromoCost()
					.trim());
			priceandCostDTO.setPromoCost(NumberUtil
					.RoundFloat(Promo / 10000, 4));

			if (priceandCostDTO.getStrSizeUnits() != null) {
				float sizeUnit = Float.parseFloat(priceandCostDTO
						.getStrSizeUnits().trim());
				priceandCostDTO.setSizeUnits(sizeUnit);
			}

			if (priceandCostDTO.getCompanyPack() != null) {
				int casePack = Integer.parseInt(priceandCostDTO
						.getCompanyPack().trim());
				priceandCostDTO.setCasePack(casePack);
			}
		} catch (Exception e) {
			logger.error("Populate Prices - JavaException" + ", UPC = + "
					+ priceandCostDTO.getUpc() + ", Store = + "
					+ priceandCostDTO.getZone(), e);
			retVal = false;
		}
		return retVal;
	}

	/**
	 * This method process cost and billback data to be stored in
	 * retail_cost_info
	 * 
	 * @param storeZoneInfo
	 * @param zoneStoreMap
	 * @throws Exception 
	 */
	private void processCostData(HashMap<String, String> storeZoneInfo,
			HashMap<String, List<String>> zoneStoreMap, Set<ItemDetailKey> itemSet)
			throws GeneralException, Exception {
		// Item code List to be processed
		Set<String> itemCodeSet = new HashSet<String>();
		for (ItemDetailKey itemDetailKey : itemSet) {
			String itemCode = itemCodeMap.get(itemDetailKey);
			if (itemCode != null) {
				itemCodeSet.add(itemCode);
			}
		}

		// Retrieve item store mapping from store_item_map table for items in
		// itemCodeList
		long startTime = System.currentTimeMillis();
		HashMap<String, List<String>> itemStoreMapping = costDAO
				.getStoreItemMap(conn, prevWkStartDate, itemCodeSet,"COST", false);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve items from store_item_map - "
				+ (endTime - startTime) + "ms");
		
		startTime = System.currentTimeMillis();
		HashMap<String, List<String>> authorizedItemsMap = costDAO
				.getStoreItemMap(conn, prevWkStartDate, itemCodeSet,"COST", true);
		endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve authorized Items from store_item_map - "
				+ (endTime - startTime) + "ms");


		// Retrieve from Retail_Cost_Info for items in itemCodeList
		startTime = System.currentTimeMillis();
		HashMap<String, List<RetailCostDTO>> costRolledUpMapForItems = retailCostDAO
				.getRetailCostInfo(conn, itemCodeSet, calendarId, false);
		endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve items from retail_cost_info - "
				+ (endTime - startTime));

		// Unroll previous week's cost data for items in itemCodeList
		HashMap<String, List<RetailCostDTO>> unrolledCostMapForItems = new HashMap<String, List<RetailCostDTO>>();
		startTime = System.currentTimeMillis();
		if (costRolledUpMapForItems != null
				&& costRolledUpMapForItems.size() > 0) {
			/*MovementWeeklyDataLoadV2 movementDataLoad = new MovementWeeklyDataLoadV2(
					conn);*/
			HashMap<String, List<RetailCostDTO>> unrolledCostMap = unrollRetailCostInfo(costRolledUpMapForItems,
							storeZoneInfo.keySet(), zoneStoreMap,
							retailPriceDAO, itemStoreMapping, storeZoneInfo);

			for (List<RetailCostDTO> retailCostDTOList : unrolledCostMap
					.values()) {
				for (RetailCostDTO retailCostDTO : retailCostDTOList) {
					if (unrolledCostMapForItems
							.get(retailCostDTO.getItemcode()) != null) {
						List<RetailCostDTO> tempList = unrolledCostMapForItems
								.get(retailCostDTO.getItemcode());
						tempList.add(retailCostDTO);
						unrolledCostMapForItems.put(
								retailCostDTO.getItemcode(), tempList);
					} else {
						List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
						tempList.add(retailCostDTO);
						unrolledCostMapForItems.put(
								retailCostDTO.getItemcode(), tempList);
					}
				}
			}
		}
		endTime = System.currentTimeMillis();
		logger.info("Time taken to unroll cost data - " + (endTime - startTime));

		// Compare items from input files with unrolled cost data from
		// retail_cost_info
		HashMap<String, List<RetailCostDTO>> retailCostDTOMap = new HashMap<String, List<RetailCostDTO>>();
		startTime = System.currentTimeMillis();
		for (ItemDetailKey itemDetailKey : itemSet) {
			List<String> addedStores = new ArrayList<String>(); // Changes to
																// handle zone
																// level items
			List<RetailCostDTO> finalRetailCostList = new ArrayList<RetailCostDTO>();
			String itemCode = itemCodeMap.get(itemDetailKey);
			List<PriceAndCostDTO> costDataList = costDataMap.get(itemDetailKey);
			if (itemCode != null) {
				List<RetailCostDTO> retailCostList = unrolledCostMapForItems
						.get(itemCode);

				// Changes to handle items for which we received price at both
				// store and zone level
				boolean isBothStoreAndZonePresent = false;
				boolean store = false;
				boolean zone = false;
				for (PriceAndCostDTO priceAndCostDTO : costDataList) {
					if (Integer.parseInt(priceAndCostDTO.getSourceCode()) == Constants.ZONE_LEVEL_TYPE_ID) {
						zone = true;
					}
					if (Integer.parseInt(priceAndCostDTO.getSourceCode()) == Constants.STORE_LEVEL_TYPE_ID) {
						store = true;
					}
				}
				if (store && zone)
					isBothStoreAndZonePresent = true;
				// Changes to handle items for which we received price at both
				// store and zone level - Ends

				// Process cost of an item in a store that is present in both
				// input file and retail_cost_info
				if (retailCostList != null && retailCostList.size() > 0) {
					for (PriceAndCostDTO priceAndCostDTO : costDataList) {
						for (RetailCostDTO retailCostDTO : retailCostList) {
							String storeZone = priceAndCostDTO.getZone();
							if ((!isBothStoreAndZonePresent && ((priceAndCostDTO
									.getSourceCode()
									.equals(String
											.valueOf(Constants.STORE_LEVEL_TYPE_ID)) && storeZone
									.equals(retailCostDTO.getLevelId())) || (priceAndCostDTO
									.getSourceCode()
									.equals(String
											.valueOf(Constants.ZONE_LEVEL_TYPE_ID))
									&& zoneStoreMap.get(storeZone) != null && zoneStoreMap
									.get(storeZone).contains(
											retailCostDTO.getLevelId()))))
									|| (isBothStoreAndZonePresent
											&& priceAndCostDTO
													.getSourceCode()
													.equals(String
															.valueOf(Constants.STORE_LEVEL_TYPE_ID)) && storeZone
												.equals(retailCostDTO
														.getLevelId()))) {
								if (Constants.RECORD_TYPE_ADDED
										.equals(priceAndCostDTO.getRecordType())
										|| Constants.RECORD_TYPE_UPDATED
												.equals(priceAndCostDTO
														.getRecordType())) {
									populatePrices(priceAndCostDTO);
									setupRetailCostDTO(priceAndCostDTO,
											retailCostDTO);
									retailCostDTO.setCalendarId(calendarId);
									retailCostDTO.setRetailerItemCode(itemDetailKey.getRetailerItemCode());
									finalRetailCostList.add(retailCostDTO);
									retailCostDTO.setUpc(itemDetailKey.getUpc());
									retailCostDTO.setProcessedFlag(true);
									priceAndCostDTO.setProcessedFlag(true);
								} else {
									String key = "";
									if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
										if (retailPriceZone.get(
												String.valueOf(Integer.parseInt(retailCostDTO.getLevelId()))) != null) {
											int zoneId = retailPriceZone
													.get(String.valueOf(Integer.parseInt(retailCostDTO.getLevelId())));
											key = retailCostDTO.getLevelTypeId() + Constants.INDEX_DELIMITER + zoneId;
										}
									}else if(retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
										if (storeIdsMap.get(retailCostDTO.getLevelId()) != null) {
											int storeId = storeIdsMap.get(retailCostDTO.getLevelId());
											key = retailCostDTO.getLevelTypeId() + Constants.INDEX_DELIMITER + storeId;
										}
									}
									if(authorizedItemsMap.containsKey(key) && key != ""){
										List<String> authorizedItems = authorizedItemsMap.get(key);
										if(authorizedItems.contains(retailCostDTO.getItemcode())){
											retailCostDTO.setCalendarId(calendarId);
											retailCostDTO.setRetailerItemCode(itemDetailKey.getRetailerItemCode());
											retailCostDTO.setUpc(itemDetailKey.getUpc());
											finalRetailCostList.add(retailCostDTO);
										}
									}
									retailCostDTO.setProcessedFlag(true);
									priceAndCostDTO.setProcessedFlag(true);
								}
							}
						}
					}
				}

				// Process cost of an item in a store that is present only in
				// input file and not in retail_cost_info
				for (PriceAndCostDTO priceAndCostDTO : costDataList) {
					if (!priceAndCostDTO.isProcessedFlag()) {
						// Changes to handle items for which we received price
						// at both store and zone level
						if (isBothStoreAndZonePresent
								&& (Constants.RECORD_TYPE_ADDED
										.equals(priceAndCostDTO.getRecordType()) || Constants.RECORD_TYPE_UPDATED
										.equals(priceAndCostDTO.getRecordType()))
								&& Constants.ZONE_LEVEL_TYPE_ID == Integer
										.parseInt(priceAndCostDTO
												.getSourceCode())) {
							String zoneNo = priceAndCostDTO.getZone();
							if (zoneNo != null
									&& zoneStoreMap.get(zoneNo) != null) {
								for (String storeNo : zoneStoreMap.get(zoneNo)) {
									if (!addedStores.contains(storeNo)) {
										RetailCostDTO retailCostDTO = new RetailCostDTO();
										retailCostDTO.setCalendarId(calendarId);
										retailCostDTO.setRetailerItemCode(itemDetailKey.getRetailerItemCode());
										retailCostDTO.setUpc(priceAndCostDTO
												.getUpc());
										retailCostDTO.setItemcode(itemCodeMap
												.get(itemDetailKey));
										retailCostDTO
												.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailCostDTO.setLevelId(storeNo);
										retailCostDTO.setUpc(itemDetailKey.getUpc());
										populatePrices(priceAndCostDTO);
										setupRetailCostDTO(priceAndCostDTO,
												retailCostDTO);
										finalRetailCostList.add(retailCostDTO);
									}
								}
							}
						}
						// Changes to handle items for which we received price
						// at both store and zone level - Ends
						else {
							if (Constants.RECORD_TYPE_ADDED
									.equals(priceAndCostDTO.getRecordType())) {
								RetailCostDTO retailCostDTO = new RetailCostDTO();
								retailCostDTO.setCalendarId(calendarId);
								retailCostDTO.setUpc(priceAndCostDTO.getUpc());
								retailCostDTO.setItemcode(itemCodeMap
										.get(itemDetailKey));
								retailCostDTO.setLevelTypeId(Integer
										.parseInt(priceAndCostDTO
												.getSourceCode()));
								retailCostDTO.setLevelId(priceAndCostDTO
										.getZone());
								retailCostDTO.setUpc(itemDetailKey.getUpc());
								retailCostDTO.setRetailerItemCode(itemDetailKey.getRetailerItemCode());
								populatePrices(priceAndCostDTO);
								setupRetailCostDTO(priceAndCostDTO,
										retailCostDTO);
								finalRetailCostList.add(retailCostDTO);
							}
						}
					}
				}
			}
			else{
				ignoredRecords.addAll(costDataList);
			}

			if(retailCostDTOMap.get(itemCode) != null){
				List<RetailCostDTO> tempList = retailCostDTOMap.get(itemCode); 
				tempList.addAll(finalRetailCostList);
				retailCostDTOMap.put(itemCode, tempList);
			}
			else
				retailCostDTOMap.put(itemCode, finalRetailCostList);
		}

		// Process cost of an item in a store that is present only in
		// retail_cost_info and not in input file
		for (String itemCode : costRolledUpMapForItems.keySet()) {
			List<RetailCostDTO> finalRetailCostList = new ArrayList<RetailCostDTO>();
			List<RetailCostDTO> retailCostList = unrolledCostMapForItems
					.get(itemCode);
			if (retailCostList != null && retailCostList.size() > 0) {
				for (RetailCostDTO retailCostDTO : retailCostList) {
					retailCostDTO.setCalendarId(calendarId);
					if (!retailCostDTO.isProcessedFlag()) {
						finalRetailCostList.add(retailCostDTO);
					}
				}
			}
			if (retailCostDTOMap.get(itemCode) != null) {
				List<RetailCostDTO> tempList = retailCostDTOMap.get(itemCode);
				tempList.addAll(finalRetailCostList);
				retailCostDTOMap.put(itemCode, tempList);
			} else {
				retailCostDTOMap.put(itemCode, finalRetailCostList);
			}

		}
		endTime = System.currentTimeMillis();
		logger.info("Time taken for processing data - " + (endTime - startTime));

		costRolledUpMapForItems = null;
		unrolledCostMapForItems = null;

		// Rollup Cost Info
		RetailCostSetup retailCostSetup = new RetailCostSetup();
		startTime = System.currentTimeMillis();
		HashMap<String, List<RetailCostDTO>> costRolledUpMap = costRollUp(retailCostDTOMap, itemCodeMap, storeInfoMap,
						new HashSet<String>(), chainId);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for processing data - " + (endTime - startTime));

		// Delete existing data
		startTime = System.currentTimeMillis();
		List<String> itemCodeList = new ArrayList<String>(itemCodeSet);
		retailCostDAO.deleteRetailCostData(conn, calendarId, itemCodeList);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for deleting data from retail_cost_info - "
				+ (endTime - startTime));

		//For handling duplicates the following set is added.
				Set<DuplicateKey> duplicateSet = new HashSet<DuplicateKey>();
		    	List<RetailCostDTO> insertList = new ArrayList<RetailCostDTO>();
		    	for(List<RetailCostDTO> costDTOList : costRolledUpMap.values()){
		    		for(RetailCostDTO retailCostDTO :  costDTOList){
		    			DuplicateKey duplicateKey = new DuplicateKey(retailCostDTO.getItemcode(), retailCostDTO.getLevelId(), retailCostDTO.getLevelTypeId(), retailCostDTO.getCalendarId());
						if(!duplicateSet.contains(duplicateKey)){
							duplicateSet.add(duplicateKey);
							insertList.add(retailCostDTO);
						}
		    		}
		    	}
		    	
		 updateLocationId(insertList);   	
		 
		startTime = System.currentTimeMillis();
		retailCostDAO.saveRetailCostData(conn, insertList);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for inserting data into retail_cost_info - "
				+ (endTime - startTime));
	}


	
	/**
	 * Returns a Map containing zone numbers and list of stores under each zone
	 * 
	 * @param storeNumberMap
	 *            HashMap containing store no and its corresponding zone no
	 * @return HashMap
	 */
	public HashMap<String, List<String>> getZoneMapping(
			HashMap<String, String> storeNumberMap) {
		HashMap<String, List<String>> zoneStoreMap = new HashMap<String, List<String>>();
		String storeNo = null, zoneNo = null;
		for (Map.Entry<String, String> entry : storeNumberMap.entrySet()) {
			storeNo = entry.getKey();
			zoneNo = PrestoUtil.castZoneNumber(entry.getValue());
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

	/**
	 * Returns HashMap containing Cost data fields as key and its start and end
	 * position in the input file.
	 * 
	 * @return LinkedHashMap
	 */
	public static LinkedHashMap getCost_Field() {
		if (COST_FIELD == null) {
			COST_FIELD = new LinkedHashMap();
			COST_FIELD.put("recordType", "0-1");
			COST_FIELD.put("vendorNo", "1-7");
			COST_FIELD.put("itemNo", "7-13");
			COST_FIELD.put("upc", "13-27");
			COST_FIELD.put("sourceCode", "27-28");
			COST_FIELD.put("zone", "28-32");
			COST_FIELD.put("costEffDate", "32-38");
			COST_FIELD.put("strCurrentCost", "38-47");
			COST_FIELD.put("promoCostEffDate", "47-53");
			COST_FIELD.put("promoCostEndDate", "53-59");
			COST_FIELD.put("strPromoCost", "59-68");
			COST_FIELD.put("companyPack", "68-74");
		}

		return COST_FIELD;
	}

	/**
	 * Returns unit cost of an item for a week
	 * 
	 * @param conn
	 *            Database Connection
	 * @param inputDate
	 *            Input Date
	 * @param itemCode
	 *            Item Code
	 * @param compStrNo
	 *            Competitor Store Number
	 * @return
	 */
	public double getRetailCost(Connection conn, String inputDate,
			String itemCode, String compStrNo) throws GeneralException {

		if (chainId == null) {
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			chainId = retailPriceDAO.getChainId(conn);
		}

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		int calId = -1;

		if (calendarMap.get(inputDate) != null) {
			calId = calendarMap.get(inputDate);
		} else {
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(
					conn, inputDate, Constants.CALENDAR_WEEK);
			logger.debug("Calendar Id - " + calendarDTO.getCalendarId());
			calId = calendarDTO.getCalendarId();
			calendarMap.put(inputDate, calId);
		}
		double cost = 0.0;
		String key = calId + "_" + compStrNo + "_" + itemCode;
		if (unrolledCostDataMap.get(key) != null) {
			return unrolledCostDataMap.get(key);
		} else {
			CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(conn);
			StoreDTO storeInfo = compDataDAO.getStoreInfo(chainId, compStrNo);

			// Assuming this is a store with price_zone_id as null
			if (storeInfo == null) {
				storeInfo = new StoreDTO();
				storeInfo.strNum = compStrNo;
			} else {
				storeInfo.zoneNum = PrestoUtil
						.castZoneNumber(storeInfo.zoneNum);
			}

			Set<String> itemCodeSet = new HashSet<String>();
			RetailCostDAO retailCostDAO = new RetailCostDAO();

			itemCodeSet.add(itemCode);
			HashMap<String, List<RetailCostDTO>> costRolledUpMap = retailCostDAO
					.getRetailCostInfo(conn, itemCodeSet, calId, false);
			HashMap<String, List<RetailCostDTO>> unrolledCostMap = unrollRetailCostInfo(
					costRolledUpMap, storeInfo);
			if (unrolledCostMap.get(compStrNo) != null) {
				RetailCostDTO retailCostDTO = unrolledCostMap.get(compStrNo)
						.get(0);
				if (retailCostDTO.getDealCost() != 0
						&& retailCostDTO.getDealCost() < retailCostDTO
								.getListCost()) {
					double dealCost = retailCostDTO.getDealCost();
					unrolledCostDataMap.put(key, dealCost);
					cost = retailCostDTO.getDealCost();
				} else {
					double listCost = retailCostDTO.getListCost();
					unrolledCostDataMap.put(key, listCost);
					cost = retailCostDTO.getListCost();
				}
			}
		}
		return cost;
	}

	/**
	 * Returns unit cost of items for a week
	 * 
	 * @param connection
	 *            Database Connection
	 * @param compStrNo
	 *            Competitor Store Number
	 * @param itemCodeList
	 *            List of Item Code
	 * @param dayCalendarId
	 *            Calendar Id of a day in a week
	 * @return HashMap containing item code as key and cost as value
	 */
	public HashMap<String, Double> getRetailCost(Connection connection,
			String compStrNo, List<String> itemCodeList, int dayCalendarId)
			throws GeneralException {
		logger.debug("Inside getRetailCost() of CostDataLoad");

		HashMap<String, Double> costDataMap = new HashMap<String, Double>();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(connection);

		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarIdForWeek(
				connection, dayCalendarId);
		int calId = calendarDTO.getCalendarId();

		// Retrieve Store Information
		if (chainId == null) {
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			chainId = retailPriceDAO.getChainId(connection);
		}
		StoreDTO storeInfo = compDataDAO.getStoreInfo(chainId, compStrNo);

		// Assuming this is a store with price_zone_id as null
		if (storeInfo == null) {
			storeInfo = new StoreDTO();
			storeInfo.strNum = compStrNo;
		} else {
			storeInfo.zoneNum = PrestoUtil.castZoneNumber(storeInfo.zoneNum);
		}

		if (calId <= 0) {
			logger.error("Error computing calendar id for the week");
		}
		logger.debug("Calendar Id for the week - " + calId);
		String key = calId + "_" + compStrNo + "_";
		logger.debug("No of items in input - " + itemCodeList.size());

		List<String> itemCodeNotInCache = new ArrayList<String>();
		for (String itemCode : itemCodeList) {
			if (unrolledCostDataMap.get(key + itemCode) != null) {
				costDataMap.put(itemCode,
						unrolledCostDataMap.get(key + itemCode));
			} else {
				itemCodeNotInCache.add(itemCode);
			}
		}
		logger.debug("No of items not in cache - " + itemCodeNotInCache.size());

		if (itemCodeNotInCache.size() > 0) {
			if (chainId == null) {
				RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
				chainId = retailPriceDAO.getChainId(connection);
			}

			RetailCostDAO retailCostDAO = new RetailCostDAO();

			Set<String> storeNumbers = new HashSet<String>();
			Set<String> itemCodeSet = new HashSet<String>(itemCodeList);
			storeNumbers.add(compStrNo);

			HashMap<String, List<RetailCostDTO>> costRolledUpMap = retailCostDAO
					.getRetailCostInfo(connection, itemCodeSet, calId, false);
			HashMap<String, List<RetailCostDTO>> unrolledCostMap = null;
			
			boolean useStoreItemMap = Boolean.parseBoolean(PropertyManager.
					getProperty("USE_STORE_ITEM_MAP_FOR_UNROLLING", "FALSE"));
			if(useStoreItemMap){
				PriceAndCostService priceAndCostService = new PriceAndCostService();
				unrolledCostMap = priceAndCostService.unrollAndFindGivenStoreCost(connection, costRolledUpMap,
						storeInfo, itemCodeSet, null);
			}
			else{
				unrolledCostMap = unrollRetailCostInfo(
						costRolledUpMap, storeInfo);
			}
			if (unrolledCostMap != null && unrolledCostMap.size() > 0) {
				List<RetailCostDTO> unrolledCostList = unrolledCostMap
						.get(compStrNo);
				for (RetailCostDTO retailCostDTO : unrolledCostList) {
					if (retailCostDTO.getDealCost() != 0
							&& retailCostDTO.getDealCost() < retailCostDTO
									.getListCost()) {
						double dealCost = retailCostDTO.getDealCost();
						costDataMap.put(retailCostDTO.getItemcode(), dealCost);
						unrolledCostDataMap.put(
								key + retailCostDTO.getItemcode(), dealCost);
					} else {
						double listCost = retailCostDTO.getListCost();
						costDataMap.put(retailCostDTO.getItemcode(), listCost);
						unrolledCostDataMap.put(
								key + retailCostDTO.getItemcode(), listCost);
					}
				}
				logger.debug("No of items with cost - " + costDataMap.size());
				return costDataMap;
			} else {
				logger.warn("No cost data available for the store - "
						+ compStrNo + " for calendar id " + calId);
				return costDataMap;
			}
		} else {
			return costDataMap;
		}
	}

	/**
	 * Returns unit cost of items for a week
	 * 
	 * @param connection
	 *            Database Connection
	 * @param compStrNumOrZoneNum
	 *            Competitor Store Number
	 * @param itemCodeList
	 *            List of Item Code
	 * @param dayCalendarId
	 *            Calendar Id of a day in a week
	 * @return HashMap containing item code as key and RetailCostDTO as value
	 */
	public HashMap<String, RetailCostDTO> getRetailCostV2(
			Connection connection, String compStrNumOrZoneNum, List<String> itemCodeList,
			int dayCalendarId, HashMap<String, HashMap<String, List<String>>> itemStoreMapping, int levelOfCost) throws GeneralException {
		logger.debug("Inside getRetailCost() of CostDataLoad");

		HashMap<String, RetailCostDTO> costDataMap = new HashMap<String, RetailCostDTO>();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(connection);

		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarIdForWeek(
				connection, dayCalendarId);
		int calId = calendarDTO.getCalendarId();

		// Retrieve Store Information
		if (chainId == null) {
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			chainId = retailPriceDAO.getChainId(connection);
		}
		
		StoreDTO storeInfo = null;
		
		if(levelOfCost == Constants.STORE_LEVEL_TYPE_ID){
			storeInfo = compDataDAO.getStoreInfo(chainId, compStrNumOrZoneNum);
			// Assuming this is a store with price_zone_id as null
			if (storeInfo == null) {
				storeInfo = new StoreDTO();
				storeInfo.strNum = compStrNumOrZoneNum;
			} else {
				storeInfo.zoneNum = PrestoUtil.castZoneNumber(storeInfo.zoneNum);
			}
		}else if(levelOfCost == Constants.ZONE_LEVEL_TYPE_ID){
			storeInfo = new StoreDTO();
			storeInfo.zoneNum = PrestoUtil.castZoneNumber(compStrNumOrZoneNum);
		}
		
		if (calId <= 0) {
			logger.error("Error computing calendar id for the week");
		}
		logger.debug("Calendar Id for the week - " + calId);
		String key = calId + "_";
		logger.debug("No of items in input - " + itemCodeList.size());

		if (chainId == null) {
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			chainId = retailPriceDAO.getChainId(connection);
		}

		RetailCostDAO retailCostDAO = new RetailCostDAO();

		Set<String> storeNumbers = new HashSet<String>();
		storeNumbers.add(compStrNumOrZoneNum);

		HashMap<String, List<RetailCostDTO>> costRolledUpMap = new HashMap<String, List<RetailCostDTO>>();
		List<String> itemCodeNotInCache = new ArrayList<String>();
		for (String itemCode : itemCodeList) {
			if (retailCostCache.get(key + itemCode) != null) {
				costRolledUpMap.put(itemCode,
						retailCostCache.get(key + itemCode));
			} else {
				itemCodeNotInCache.add(itemCode);
			}
		}
		logger.debug("No of items not in cache - " + itemCodeNotInCache.size());

		if (itemCodeNotInCache.size() > 0) {
			Set<String> itemCodeSet = new HashSet<String>(itemCodeNotInCache);
			HashMap<String, List<RetailCostDTO>> costRolledUpMap1 = retailCostDAO
					.getRetailCostInfo(connection, itemCodeSet, calId, false);
			for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap1
					.entrySet()) {
				retailCostCache.put(key + entry.getKey(), entry.getValue());
			}
			costRolledUpMap.putAll(costRolledUpMap1);
		}

		HashMap<String, List<RetailCostDTO>> unrolledCostMap = null;
		boolean useStoreItemMap = Boolean.parseBoolean(PropertyManager.
				getProperty("USE_STORE_ITEM_MAP_FOR_UNROLLING", "FALSE"));
		if(useStoreItemMap && Constants.STORE_LEVEL_TYPE_ID == levelOfCost){
			PriceAndCostService priceAndCostService = new PriceAndCostService();
			unrolledCostMap = priceAndCostService.unrollAndFindGivenStoreCost(connection, costRolledUpMap,
					storeInfo, costRolledUpMap.keySet(), itemStoreMapping);
		}
		else{
			
			if(levelOfCost == Constants.ZONE_LEVEL_TYPE_ID){
				
				unrolledCostMap = unrollRetailCostInfoForZone(
						costRolledUpMap, storeInfo);
				
			}else{
				unrolledCostMap = unrollRetailCostInfo(
						costRolledUpMap, storeInfo);	
			}
			

		}
		
		
		if (unrolledCostMap != null && unrolledCostMap.size() > 0) {
			List<RetailCostDTO> unrolledCostList = unrolledCostMap
					.get(PrestoUtil.castZoneNumber(compStrNumOrZoneNum));
			for (RetailCostDTO retailCostDTO : unrolledCostList) {
				costDataMap.put(retailCostDTO.getItemcode(), retailCostDTO);
			}
			logger.debug("No of items with cost - " + costDataMap.size());
			return costDataMap;
		} else {
			logger.warn("No cost data available for the store - " + compStrNumOrZoneNum
					+ " for calendar id " + calId);
			return costDataMap;
		}
	}

	/**
	 * This method unrolls cost data from the rolled up cost information passed
	 * as input
	 * 
	 * @param costRolledUpMap
	 *            Map containing rolled up cost information
	 * @param storeInfo
	 *            Store Information
	 * @return Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailCostDTO>> unrollRetailCostInfo(
			HashMap<String, List<RetailCostDTO>> costRolledUpMap,
			StoreDTO storeInfo) throws GeneralException {
		logger.debug("Inside unrollRetailCostInfo() of CostDataLoad");
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<String, List<RetailCostDTO>>();
		MovementWeeklyDataLoadV2 weeklyDataload = new MovementWeeklyDataLoadV2();

		for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap
				.entrySet()) {
			RetailCostDTO chainLevelData = new RetailCostDTO();
			RetailCostDTO zoneLevelData = new RetailCostDTO();
			boolean isChainLevelPresent = false;
			boolean isPopulated = false;
			for (RetailCostDTO retailCostDTO : entry.getValue()) {

				if (Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					isChainLevelPresent = true;
					chainLevelData.copy(retailCostDTO);
				} else if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					if (!isPopulated) {
						zoneLevelData.copy(retailCostDTO);
						// Unroll cost for zone level data
						if (retailCostDTO.getLevelId()
								.equals(storeInfo.zoneNum)
								|| retailCostDTO.getLevelId().equals(
										storeInfo.dept1ZoneNum)
								|| retailCostDTO.getLevelId().equals(
										storeInfo.dept2ZoneNum)
								|| retailCostDTO.getLevelId().equals(
										storeInfo.dept3ZoneNum)) {
							zoneLevelData.setLevelId(storeInfo.strNum);
							weeklyDataload.populateMap(unrolledCostMap,
									zoneLevelData);
							isPopulated = true;
						}
					}
				} else {
					if (!isPopulated) {
						if (storeInfo.strNum.equals(retailCostDTO.getLevelId())) {
							weeklyDataload.populateMap(unrolledCostMap,
									retailCostDTO);
							isPopulated = true;
						}
					}
				}
			}

			// Unroll cost for chain level data
			if (isChainLevelPresent) {
				if (!isPopulated) {
					chainLevelData.setLevelId(storeInfo.strNum);
					weeklyDataload.populateMap(unrolledCostMap, chainLevelData);
				}
			}
		}

		return unrolledCostMap;
	}

	
	/**
	 * This method unrolls cost data from the rolled up cost information passed
	 * as input
	 * 
	 * @param costRolledUpMap
	 *            Map containing rolled up cost information
	 * @param storeInfo
	 *            Store Information
	 * @return Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailCostDTO>> unrollRetailCostInfoForZone(
			HashMap<String, List<RetailCostDTO>> costRolledUpMap,
			StoreDTO storeInfo) throws GeneralException {
		logger.debug("Inside unrollRetailCostInfo() of CostDataLoad");
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<String, List<RetailCostDTO>>();
		MovementWeeklyDataLoadV2 weeklyDataload = new MovementWeeklyDataLoadV2();

		for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap
				.entrySet()) {
			RetailCostDTO chainLevelData = new RetailCostDTO();
			RetailCostDTO zoneLevelData = new RetailCostDTO();
			boolean isChainLevelPresent = false;
			boolean isPopulated = false;
			for (RetailCostDTO retailCostDTO : entry.getValue()) {

				if (Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					isChainLevelPresent = true;
					chainLevelData.copy(retailCostDTO);
				} else if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					if (!isPopulated) {
						zoneLevelData.copy(retailCostDTO);
						// Unroll cost for zone level data
						if (retailCostDTO.getLevelId()
								.equals(storeInfo.zoneNum)) {
							zoneLevelData.setLevelId(storeInfo.zoneNum);
							weeklyDataload.populateMap(unrolledCostMap,
									zoneLevelData);
							isPopulated = true;
						}
					}
				} /*else {
					if (!isPopulated) {
						if (storeInfo.strNum.equals(retailCostDTO.getLevelId())) {
							weeklyDataload.populateMap(unrolledCostMap,
									retailCostDTO);
							isPopulated = true;
						}
					}
				}*/
			}

			// Unroll cost for chain level data
			if (isChainLevelPresent) {
				if (!isPopulated) {
					chainLevelData.setLevelId(storeInfo.zoneNum);
					weeklyDataload.populateMap(unrolledCostMap, chainLevelData);
				}
			}
		}

		return unrolledCostMap;
	}

	
	
	
	
	public void castZoneNumbers(HashMap<String, String> storeNumberMap) {
		for (Map.Entry<String, String> entry : storeNumberMap.entrySet()) {
			String zone = PrestoUtil.castZoneNumber(entry.getValue());
			storeNumberMap.put(entry.getKey(), zone);
		}
	}
	
  
    
    
    /**
	 * This method unrolls data from retail cost info to be loaded in movement weekly table
	 * @param costRolledUpMap		Map containing item code as key and list of rolled up cost from retail cost info as value
	 * @param storeNumbers			Set of stores available for Price Index
	 * @param zoneStoreMap			Map containing zone number as key and store number as value
	 * @return HashMap containing store number as key and list of unrolled retail cost info as value
	 */
	public HashMap<String, List<RetailCostDTO>> unrollRetailCostInfo(HashMap<String, List<RetailCostDTO>>costRolledUpMap, Set<String> storeNumbers, HashMap<String, List<String>> zoneStoreMap,
																	RetailPriceDAO retailPriceDAO, HashMap<String, List<String>> storeItemMap, HashMap<String, String> storeNumberMap) throws GeneralException{
		logger.debug("Inside unrollRetailCostInfo() of MovementWeeklyDataLoadV2");
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<String, List<RetailCostDTO>>();
		MovementWeeklyDataLoadV2 movementDataLoad = new MovementWeeklyDataLoadV2(
				conn);
		RetailCostDTO chainLevelData = null;
		boolean isChainLevelPresent = false;
		RetailCostDTO zoneLevelData = null;
		Set<String> unrolledStores = null;
		HashMap<String,List<String>> deptZoneMap = retailPriceDAO.getDeptZoneMap(conn, chainId);
		HashMap<String,List<String>> storeDeptZoneMap = retailPriceDAO.getStoreDeptZoneMap(conn, chainId);
		
		// By Dinesh:: 21-Nov-2017. To improve performance. List of items were more in number then 
		// using Contains logic will cause performance issue
		// to avoid that issue, code changes done to use HashSet 
		Set<ItemStoreKey> itemsBasedOnStoreOrZone = new HashSet<ItemStoreKey>();
		storeItemMap.forEach((key, value)->{
			value.forEach(itemCode->{
				ItemStoreKey itemKey = new ItemStoreKey(key, itemCode);
				itemsBasedOnStoreOrZone.add(itemKey);
			});
			
		});
		
		for(Map.Entry<String, List<RetailCostDTO>> entry:costRolledUpMap.entrySet()){
			unrolledStores = new HashSet<String>();
			isChainLevelPresent = false;
			RetailCostDTO chainLevelDTO = null;
			RetailCostDTO zoneLevelDTO = null;
			for(RetailCostDTO retailCostDTO:entry.getValue()){
				if(Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
					isChainLevelPresent = true;
					chainLevelData = retailCostDTO;
				}else if(Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
					zoneLevelData = retailCostDTO;
					// Unroll cost for zone level data
					if(!(null == zoneStoreMap.get(retailCostDTO.getLevelId()))){
						for(String storeNo:zoneStoreMap.get(retailCostDTO.getLevelId())){
							if(!unrolledStores.contains(storeNo)){
								zoneLevelDTO = new RetailCostDTO();
								zoneLevelDTO.copy(zoneLevelData);
								zoneLevelDTO.setLevelId(storeNo);
								zoneLevelDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
								String zoneNo = Integer.toString(Integer.parseInt(zoneLevelData.getLevelId()));
								String zoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + retailPriceZone.get(zoneNo).toString();
								String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeIdsMap.get(zoneLevelDTO.getLevelId()).toString();
								// Check if this item exists for this store
								boolean isPopulated = false;
								if(storeItemMap.containsKey(zoneKey)){
									ItemStoreKey key = new ItemStoreKey(zoneKey, zoneLevelDTO.getItemcode());
									if(itemsBasedOnStoreOrZone.contains(key)){
										movementDataLoad.populateMap(unrolledCostMap, zoneLevelDTO);
										isPopulated = true;
									}
								}

								if(!isPopulated)
									if(storeItemMap.containsKey(storeKey)){
										ItemStoreKey key = new ItemStoreKey(storeKey, zoneLevelDTO.getItemcode());
										if(itemsBasedOnStoreOrZone.contains(key)){
											movementDataLoad.populateMap(unrolledCostMap, zoneLevelDTO);
										}
									}
								
								unrolledStores.add(storeNo);
							}
						}
					}else{
						// To handle items priced at dept zone level
						List<String> stores = deptZoneMap.get(retailCostDTO.getLevelId());
						if(stores != null && stores.size() > 0){
							for(String store:stores){
								if(!unrolledStores.contains(store)){
									zoneLevelDTO = new RetailCostDTO();
									zoneLevelDTO.copy(zoneLevelData);
									zoneLevelDTO.setLevelId(store);
									zoneLevelDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
									String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeIdsMap.get(zoneLevelDTO.getLevelId()).toString();
									
									
									if(storeItemMap.containsKey(storeKey)){
										ItemStoreKey key = new ItemStoreKey(storeKey, zoneLevelDTO.getItemcode());
										if(itemsBasedOnStoreOrZone.contains(key)){
											movementDataLoad.populateMap(unrolledCostMap, zoneLevelDTO);
										}
									}
									
									unrolledStores.add(store);
								}
							}
						}
					}
				}else{
					if(storeNumbers.contains(retailCostDTO.getLevelId())){
						movementDataLoad.populateMap(unrolledCostMap, retailCostDTO);
						unrolledStores.add(retailCostDTO.getLevelId());
					}
				}
			}
			
			// Unroll cost for chain level data
			if(isChainLevelPresent)
				for(String storeNo:storeNumbers){
					if(!unrolledStores.contains(storeNo)){
						chainLevelDTO = new RetailCostDTO();
						chainLevelDTO.copy(chainLevelData); 
						chainLevelDTO.setLevelId(storeNo);
						chainLevelDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
						String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeIdsMap.get(chainLevelDTO.getLevelId()).toString();
						String zoneNo = Integer.toString(Integer.parseInt(storeNumberMap.get(chainLevelDTO.getLevelId())));
						String zoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + retailPriceZone.get(zoneNo).toString();
						// Check if this item exists for this store
						boolean isPopulated = false;
						if(storeItemMap.containsKey(storeKey)){
							ItemStoreKey key = new ItemStoreKey(storeKey, chainLevelDTO.getItemcode()); 
							if(itemsBasedOnStoreOrZone.contains(key)){	
								movementDataLoad.populateMap(unrolledCostMap, chainLevelDTO);
								isPopulated = true;
							}
						}
						
						if(!isPopulated)
							if(storeItemMap.containsKey(zoneKey)){
								ItemStoreKey key = new ItemStoreKey(zoneKey, chainLevelDTO.getItemcode());
								if(itemsBasedOnStoreOrZone.contains(key)){	
									movementDataLoad.populateMap(unrolledCostMap, chainLevelDTO);
									isPopulated = true;
								}
							}
						
						// To handle items priced at dept zone level
						if(!isPopulated){
							if(storeDeptZoneMap != null && storeDeptZoneMap.size() > 0){
								List<String> deptZones = storeDeptZoneMap.get(storeNo);
								if(deptZones != null && deptZones.size() > 0){
									for(String deptZone:deptZones){
										String deptZoneNo = Integer.toString(Integer.parseInt(deptZone)); 
										String deptZoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + retailPriceZone.get(deptZoneNo).toString();
										if(storeItemMap.containsKey(deptZoneKey)){
											ItemStoreKey key = new ItemStoreKey(deptZoneKey, chainLevelDTO.getItemcode());
											if(itemsBasedOnStoreOrZone.contains(key)){	
												movementDataLoad.populateMap(unrolledCostMap, chainLevelDTO);
											}
										}
									}
								}
							}
						}
					}
				}
		}
		
		return unrolledCostMap;
	}
	
	/**
	 * Performs cost roll up logic for Retail Cost Data
	 * 
	 * @return List Map Containing Item Code as key and list of RetailCostDTO as
	 *         value to be compared against existing database entries
	 */
	public HashMap<String, List<RetailCostDTO>> costRollUp(
			HashMap<String, List<RetailCostDTO>> retailCostDataMap,
			HashMap<ItemDetailKey, String> itemCodeMap,
			HashMap<String, String> storeInfoMap, Set<String> noItemCodeSet,
			String chainId) {
		logger.debug("Inside costRollUp of RetailCostSetup");

		HashMap<String, List<RetailCostDTO>> costRolledUpMap = new HashMap<String, List<RetailCostDTO>>();
		List<RetailCostDTO> costRolledUpList;

		for (List<RetailCostDTO> retailCostDTOList : retailCostDataMap.values()) {

			HashMap<String, List<RetailCostDTO>> chainLevelMap = new HashMap<String, List<RetailCostDTO>>();
			HashMap<String, HashMap<String, List<RetailCostDTO>>> zoneLevelMap = new HashMap<String, HashMap<String, List<RetailCostDTO>>>();

			costRolledUpList = new ArrayList<RetailCostDTO>();
			String itemCode = null;
			String upc = null;
			boolean isAtZoneLevel = false;
			boolean isAtStoreLevel = false;
			for (RetailCostDTO retailCostDTO : retailCostDTOList) {

				String costStr = Constants.EMPTY;

				retailCostDTO.setPromotionFlag("N");

				/*
				 * if(retailCostDTO.getListCost() >= 0){ costStr = costStr +
				 * retailCostDTO.getListCost(); }
				 */

				costStr = costStr + retailCostDTO.getListCost();

				if (retailCostDTO.getDealCost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER
							+ retailCostDTO.getDealCost();
				}

				// Changes for TOPS Cost/Billback dataload
				if (retailCostDTO.getLevel2Cost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER
							+ retailCostDTO.getLevel2Cost();
				}

				// Changes for Ahold VIP Cost
				if (retailCostDTO.getVipCost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER
							+ retailCostDTO.getVipCost();
				}

				costStr = costStr + Constants.INDEX_DELIMITER
						+ retailCostDTO.getEffListCostDate();

				if (retailCostDTO.getDealCost() != 0
						&& retailCostDTO.getDealCost() < retailCostDTO
								.getListCost()) {
					retailCostDTO.setPromotionFlag("Y");
				}

				// Set Item Code
				if (retailCostDTO.getItemcode() != null) {
					itemCode = retailCostDTO.getItemcode();
				}

				if(retailCostDTO.getUpc() != null){
					ItemDetailKey itemDetailKey = new ItemDetailKey(retailCostDTO.getUpc(), retailCostDTO.getRetailerItemCode());	
						if(itemCodeMap.get(itemDetailKey) != null)
							itemCode = String.valueOf(itemCodeMap.get(itemDetailKey));
						else{
							noItemCodeSet.add(retailCostDTO.getUpc());
							logger.warn("Item code not found for UPC - " + itemDetailKey.getUpc() + " and retailer item code - " + itemDetailKey.getRetailerItemCode());
							continue;
						}	
						retailCostDTO.setItemcode(itemCode);
					upc = retailCostDTO.getUpc();
				}
				// Set Zone# and Store#
				if (retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
					retailCostDTO.setStoreNbr(retailCostDTO.getLevelId());
					retailCostDTO.setZoneNbr(storeInfoMap.get(retailCostDTO
							.getStoreNbr()));
					// Change to handle stores with no zone
					if (storeInfoMap.get(retailCostDTO.getStoreNbr()) == null)
						storesWithNoZone.add(retailCostDTO.getStoreNbr());
					// Change to handle stores with no zone - Ends
				} else if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
					retailCostDTO.setZoneNbr(retailCostDTO.getLevelId());
				}

				// If item is priced at zone level populate a hashmap with cost
				// as key and list of corresponding retailCostDTO as its value
				if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					isAtZoneLevel = true;
					List<RetailCostDTO> tempList = null;
					if (chainLevelMap.get(costStr) != null) {
						tempList = chainLevelMap.get(costStr);
						tempList.add(retailCostDTO);
					} else {
						tempList = new ArrayList<RetailCostDTO>();
						tempList.add(retailCostDTO);
					}
					chainLevelMap.put(costStr, tempList);
				} else {
					/*
					 * If item is priced at store level populate a hashmap with
					 * zone number as key and hashmap containing cost and its
					 * list of retail cost info as value
					 */
					isAtStoreLevel = true;
					HashMap<String, List<RetailCostDTO>> zoneCostMap = null;
					if (zoneLevelMap.get(retailCostDTO.getZoneNbr()) != null) {
						zoneCostMap = zoneLevelMap.get(retailCostDTO
								.getZoneNbr());
						List<RetailCostDTO> tempList = null;
						if (zoneCostMap.get(costStr) != null) {
							tempList = zoneCostMap.get(costStr);
							tempList.add(retailCostDTO);
						} else {
							tempList = new ArrayList<RetailCostDTO>();
							tempList.add(retailCostDTO);
						}
						zoneCostMap.put(costStr, tempList);
					} else {
						zoneCostMap = new HashMap<String, List<RetailCostDTO>>();
						List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
						tempList.add(retailCostDTO);
						zoneCostMap.put(costStr, tempList);
					}
					zoneLevelMap.put(retailCostDTO.getZoneNbr(), zoneCostMap);
				}
			}

			// If item is priced at store level perform an additional step of
			// rolling it up to zone level.
			if (isAtStoreLevel) {

				for (Map.Entry<String, HashMap<String, List<RetailCostDTO>>> entry : zoneLevelMap
						.entrySet()) {
					String mostPrevalentCostAtZone = null;
					int mostPrevalentCntAtZone = 0;
					if (entry.getKey() != null) {
						for (Map.Entry<String, List<RetailCostDTO>> inEntry : entry
								.getValue().entrySet()) {
							if (inEntry.getValue().size() > mostPrevalentCntAtZone) {
								mostPrevalentCntAtZone = inEntry.getValue()
										.size();
								mostPrevalentCostAtZone = inEntry.getKey();
							}
						}

						for (Map.Entry<String, List<RetailCostDTO>> inEntry : entry
								.getValue().entrySet()) {
							if (inEntry.getKey()
									.equals(mostPrevalentCostAtZone)) {
								RetailCostDTO retailCostDTO = inEntry
										.getValue().get(0);
								retailCostDTO.setLevelId(entry.getKey());
								retailCostDTO
										.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);

								List<RetailCostDTO> tempList = null;
								if (chainLevelMap.get(mostPrevalentCostAtZone) != null) {
									tempList = chainLevelMap
											.get(mostPrevalentCostAtZone);
									tempList.add(retailCostDTO);
								} else {
									tempList = new ArrayList<RetailCostDTO>();
									tempList.add(retailCostDTO);
								}
								chainLevelMap.put(mostPrevalentCostAtZone,
										tempList);
							} else {
								List<RetailCostDTO> tempList = null;
								if (chainLevelMap.get(inEntry.getKey()) != null) {
									tempList = chainLevelMap.get(inEntry
											.getKey());
									tempList.addAll(inEntry.getValue());
								} else {
									tempList = new ArrayList<RetailCostDTO>();
									tempList.addAll(inEntry.getValue());
								}
								chainLevelMap.put(inEntry.getKey(), tempList);
							}
						}
					} else {
						// Changes for TOPS Cost/Billback dataload - To handle
						// stores with null zone ids
						if (!ignoreStoresNotInDB
								.equalsIgnoreCase(Constants.IGN_STR_NOT_IN_DB_TRUE)) {
							for (Map.Entry<String, List<RetailCostDTO>> inEntry : entry
									.getValue().entrySet()) {
								for (RetailCostDTO retailCostDTO : inEntry
										.getValue()) {
									String costStr = Constants.EMPTY;

									retailCostDTO.setPromotionFlag("N");
									if (retailCostDTO.getListCost() >= 0) {
										costStr = costStr
												+ retailCostDTO.getListCost();
									}
									if (retailCostDTO.getDealCost() > 0) {
										costStr = costStr
												+ Constants.INDEX_DELIMITER
												+ retailCostDTO.getDealCost();
										retailCostDTO.setPromotionFlag("Y");
									}

									if (retailCostDTO.getLevel2Cost() > 0) {
										costStr = costStr
												+ Constants.INDEX_DELIMITER
												+ retailCostDTO.getLevel2Cost();
									}
									setupVipCost(retailCostDTO);
									List<RetailCostDTO> tempList = null;
									if (chainLevelMap.get(costStr) != null) {
										tempList = chainLevelMap.get(costStr);
										retailCostDTO
												.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailCostDTO.setLevelId(retailCostDTO
												.getStoreNbr());
										tempList.add(retailCostDTO);
									} else {
										tempList = new ArrayList<RetailCostDTO>();
										retailCostDTO
												.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailCostDTO.setLevelId(retailCostDTO
												.getStoreNbr());
										tempList.add(retailCostDTO);
									}
									chainLevelMap.put(inEntry.getKey(),
											tempList);
								}
							}
						}
					}
				}
			}

			// Determine most prevalent cost for this UPC
			String mostPrevalentCost = null;
			int mostPrevalentCnt = 0;
			int tempCnt = 0;
			for (Map.Entry<String, List<RetailCostDTO>> entry : chainLevelMap
					.entrySet()) {
				/*
				 * if(entry.getValue().size() > mostPrevalentCnt){
				 * mostPrevalentCnt = entry.getValue().size();
				 * mostPrevalentPrice = entry.getKey(); }
				 */
				List<RetailCostDTO> retailCostDTOLst = entry.getValue();
				tempCnt = 0;
				for (RetailCostDTO retailCostDTO : retailCostDTOLst) {
					if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
						tempCnt = tempCnt + 1;
					}
				}

				if (tempCnt > mostPrevalentCnt) {
					mostPrevalentCnt = tempCnt;
					mostPrevalentCost = entry.getKey();
				}
			}

			// Add Items to the final list to be compared against the database
			for (Map.Entry<String, List<RetailCostDTO>> entry : chainLevelMap
					.entrySet()) {
				if (entry.getKey().equals(mostPrevalentCost)) {
					RetailCostDTO retailCostDTO = entry.getValue().get(0);
					RetailCostDTO chainLevelDTO = new RetailCostDTO();
					chainLevelDTO.copy(retailCostDTO);
					setupVipCost(chainLevelDTO);
					chainLevelDTO.setLevelId(chainId);
					chainLevelDTO.setLevelTypeId(Constants.CHAIN_LEVEL_TYPE_ID);
					costRolledUpList.add(chainLevelDTO);
					for (RetailCostDTO retailCost : entry.getValue()) {
						if (retailCost.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
							setupVipCost(retailCost);
							costRolledUpList.add(retailCost);
						}
					}
				} else {
					for (RetailCostDTO retailCostDTO : entry.getValue()) {
						setupVipCost(retailCostDTO);
						costRolledUpList.add(retailCostDTO);
					}
				}
			}

			if (itemCode != null)
				costRolledUpMap.put(itemCode, costRolledUpList);
			else {
				if (upc != null)
					noItemCodeSet.add(upc);
			}
		}
		return costRolledUpMap;
	}
	public void setupVipCost(RetailCostDTO retailCostDTO) {
		// Make Vip cost as zero when list cost and vip cost are same.
		if (retailCostDTO.getVipCost() == retailCostDTO.getListCost()) {
			retailCostDTO.setVipCost(0);
		}
	}
	
	
	private void updateLocationId(List<RetailCostDTO> insertList){
		for(RetailCostDTO retailCostDTO: insertList){
			//Update chain id for zone level records
			if(retailCostDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID)
				retailCostDTO.setLocationId(Integer.parseInt(retailCostDTO.getLevelId()));
			//Update price zone id from the cache when there is a zone level record 
			else if(retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null 
				if(retailPriceZone.get(String.valueOf(Integer.parseInt(retailCostDTO.getLevelId()))) != null)
					retailCostDTO.setLocationId(retailPriceZone.get(String.valueOf(Integer.parseInt(retailCostDTO.getLevelId()))));
			}
			//Update comp_str_id from the cache when there is a store level record
			else if(retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null
				if(storeIdsMap.get(retailCostDTO.getLevelId()) != null)
					retailCostDTO.setLocationId(storeIdsMap.get(retailCostDTO.getLevelId()));
			}
		}
	}

	
    
	class DuplicateKey{
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
			result = prime * result
					+ ((itemCode == null) ? 0 : itemCode.hashCode());
			result = prime * result
					+ ((levelId == null) ? 0 : levelId.hashCode());
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
		private CostDataLoad getOuterType() {
			return CostDataLoad.this;
		}
	
	
	}
	
    
}
