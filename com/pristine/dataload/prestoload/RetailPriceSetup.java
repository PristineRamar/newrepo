/*
 * Title: Retail Price Setup
 *
 **********************************************************************************
 * Modification History
 *---------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *---------------------------------------------------------------------------------
 * Version 0.1	04/03/2012	Janani			Initial Version
 * Version 0.2	04/26/2012	Janani			Create mapping between levels and item 
 **********************************************************************************
 */
package com.pristine.dataload.prestoload;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.lang.time.DateUtils;

import com.csvreader.CsvReader;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.ProductLocationMappingDAO;
import com.pristine.dataload.service.StoreItemMapService;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class RetailPriceSetup extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("RetailPriceSetup");

	private Connection conn = null;
	//private static int columnCount = 11;
	private Set<String> storeIdSet;
	private HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> retailPriceDataMap;
	private HashMap<String, String> itemCodeMap;
	private HashMap<String, String> storeInfoMap;
	private HashMap<String, Integer> storeIdMap;
	private HashMap<String, Integer> priceZoneIdMap;
	private HashMap<String, List<Integer>> deptZoneMap;
	private Set<String> noItemCodeSet = new HashSet<String>();
	private int recordCount=0;
	private String chainId = null;
	private int calendarId = -1; 
	private Date weekEndDate = null; // Changes to handle future price records
	private HashMap<String, Integer> calendarMap = new HashMap<String, Integer>(); // Changes to handle future price records
	// Change to handle stores with no zone
	private Set<String> storesWithNoZone = new HashSet<String>();
	private static String ARG_IGN_STR_NOT_IN_DB = "IGN_STR_NOT_IN_DB=";
	private static String IGNORE_PRICE_LOAD = "IGNORE_PRICE_LOAD=";
	private static String ignoreStoresNotInDB = "";
	
	// Change to make setup in store_item_map confirgurable
	private static String SETUP_STORE_ITEM_MAP = "SETUP_STORE_ITEM_MAP=";
	private static String setupStoreItemMap = "";
	
	// Changes to handle future price records
	private static String LOAD_FUTURE_PRICE = "LOAD_FUTURE_PRICE=";
	private static boolean loadFuturePrice = false;
	private static boolean ignorePriceLoading = true;
	private int futurePriceRecordsSkipped = 0;
	private int recordCountPast26Weeks = 0;
	// Changes to handle future price records Ends
	String startDate = null;
	// Changes to retrieve item_code using upc and retailer_item_code
	private boolean checkRetailerItemCode = false;
	private HashMap<String, Integer> allItemMap = null;
	// Changes to retrieve item_code using upc and retailer_item_code - Ends
	
  	//Changes for populating location_id in RETAIL_PRICE_INFO
  	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
  	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
	
	List<RetailPriceDTO> currentProcPriceList = new ArrayList<RetailPriceDTO>();
	int noOfRecsToBeProcessed = 0;
	int commitRecCount = 1000;
	
	private int loadFuturePriceXWeeks = 0;

	float minItemPrice=0; // Changes to restrict items that has price lesser than minItemPrice
	private RetailCalendarDAO retailCalendarDAO = null;
	
 	private RetailPriceDAO retailPriceDAO = null;
 	StoreItemMapService storeItemMapService = new StoreItemMapService();
 	boolean useVendorCodeFromZone = false;
 	boolean useZoneStoreMapping = false;
 	private static boolean loadVendorInfo = false;
 	private static String LOAD_VENDOR_INFO = "LOAD_VENDOR_INFO=";
 	HashMap<String, List<String>> zoneToStoreMapping = null;
 	HashMap<String, List<RetailPriceDTO>> cacheRetailPriceData;
 	HashMap<String, Integer> storeCountBasedOnUPC;
 	private int recordCounts=0;
 	private boolean processActualValues = false;
 	private HashMap<String, List<Integer>> zoneStoreMap = null;
 	private boolean rollupDSDToWhseEnabled = false;
 	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
 	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
 	HashMap<String, String> primaryMatchingZone = null;
	public RetailPriceSetup(){
		retailCalendarDAO = new RetailCalendarDAO();
		// Changes to retrieve item_code using upc and retailer_item_code
		checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		conn = getOracleConnection();
		// Changes to retrieve item_code using upc and retailer_item_code - Ends
	}
	
	/**
	 * Main method of Retail Price Setup Batch
	 * @param args[0]	Relative path of the input file
	 * @param args[1]	specificweek/currentweek/nextweek
	 * @param args[2]	Will be present only if args[1] is specificweek
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-retail-price-setup.properties");
		PropertyManager.initialize("analysis.properties");
		
		RetailPriceSetup retailPriceSetup = new RetailPriceSetup();
		
		// Change to handle stores with no zone
		if (args[1].indexOf(ARG_IGN_STR_NOT_IN_DB) >= 0) {
			ignoreStoresNotInDB = args[1].substring(ARG_IGN_STR_NOT_IN_DB.length());			 
		} else {
			logger.info("Invalid Argument, args[1] IGN_STR_NOT_IN_DB=[true/false/empty string]");
			System.exit(-1);
		}
		// Change to handle stores with no zone - Ends
		
		if (args[2].indexOf(SETUP_STORE_ITEM_MAP) >= 0) {
			setupStoreItemMap = args[2].substring(SETUP_STORE_ITEM_MAP.length());			 
		} else {
			logger.info("Invalid Argument, args[2] SETUP_STORE_ITEM_MAP=[true/false/empty string]");
			System.exit(-1);
		}
				
		if(!(Constants.NEXT_WEEK.equalsIgnoreCase(args[3]) || Constants.CURRENT_WEEK.equalsIgnoreCase(args[3]) || Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[3]) || Constants.LAST_WEEK.equalsIgnoreCase(args[3]))){
			logger.info("Invalid Argument, args[3] should be lastweek/currentweek/nextweek/specificweek");
        	System.exit(-1);
		}
		
		/*if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[3]) && (args.length != 5)){
			logger.info("Invalid Arguments, args[0] should be relative path, args[1] SETUP_STORE_ITEM_MAP=[true/false/empty string], args[2] IGN_STR_NOT_IN_DB=[true/false/empty string], args[3] lastweek/currentweek/nextweek/specificweek, args[4] data for a specific week");
        	System.exit(-1);
		}*/
		int dayIndex = 0;
		String dateStr = null;
		Calendar c = Calendar.getInstance();
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null){
			dayIndex = Integer.parseInt(PropertyManager.
					getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if(dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if(Constants.CURRENT_WEEK.equalsIgnoreCase(args[3])){
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.NEXT_WEEK.equalsIgnoreCase(args[3])){
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.LAST_WEEK.equalsIgnoreCase(args[3])){
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[3])){
			dateStr = args[4];
		}
		
		// Changes to handle future price
		for(String arg : args){
			if(arg.indexOf(LOAD_FUTURE_PRICE) != -1){
				loadFuturePrice = Boolean.valueOf(arg.substring(LOAD_FUTURE_PRICE.length()));
			}
			if(arg.indexOf(IGNORE_PRICE_LOAD) != -1){
				ignorePriceLoading = Boolean.valueOf(arg.substring(IGNORE_PRICE_LOAD.length()));
			}
			if (arg.indexOf(LOAD_VENDOR_INFO) != -1) {
				loadVendorInfo = Boolean.valueOf(arg.substring(LOAD_VENDOR_INFO
						.length()));
			}
		}
		// Changes to handle future price ends
		retailPriceSetup.processRetailPriceFile(args[0], dateStr);
	}
	
	/**
	 * This method parses input text file and populates data as a list of RetailPriceDTO
	 * @param relativePath		Relative path of the input file
	 * @param inputDate 	    Date of the week
	 */
	private void  processRetailPriceFile(String relativePath, String inputDate){
		logger.info("Retail Price Setup Starts");
		
		try
		{		
			ItemDAO itemdao = new ItemDAO();
			if(checkRetailerItemCode){
				allItemMap = itemdao.getAllItems(conn);
				itemCodeMap = new HashMap<String, String>();
				for(Map.Entry<String, Integer> entry : allItemMap.entrySet()){
					itemCodeMap.put(entry.getKey(), String.valueOf(entry.getValue()));
				}
			}
			
			RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_WEEK);
			logger.info("Calendar Id - " + calendarDTO.getCalendarId());
			calendarId = calendarDTO.getCalendarId();
			weekEndDate = DateUtil.toDate(calendarDTO.getEndDate()); // Changes to handle future price records
			startDate = calendarDTO.getStartDate();
			retailPriceDAO = new RetailPriceDAO();
			chainId = retailPriceDAO.getChainId(conn); 
			String tempCommitCount = PropertyManager.getProperty("DATALOAD.COMMITRECOUNT", "1000");
            commitRecCount=Integer.parseInt(tempCommitCount);
            
            String recStr = PropertyManager.getProperty("DATALOAD.LOADRECORDS", String.valueOf(Constants.RECS_TOBE_PROCESSED));
            noOfRecsToBeProcessed = Integer.parseInt(recStr);
            
            // Changes to restrict items that has price lesser than minItemPrice
            String minPrice = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.MIN_ITEM_PRICE", "0.10");
			minItemPrice = Float.parseFloat(minPrice);
			// Changes to restrict items that has price lesser than minItemPrice - Ends
            
			//Read config
			useVendorCodeFromZone = Boolean.parseBoolean
					(PropertyManager.getProperty("USE_VENDOR_CODE_FROM_ZONE", "FALSE"));
			useZoneStoreMapping = Boolean.parseBoolean
					(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
			
			rollupDSDToWhseEnabled = Boolean.parseBoolean(PropertyManager.
					getProperty("ROLL_UP_DSD_TO_WARHOUSE_ZONE", "FALSE"));
			if(useZoneStoreMapping){
				zoneToStoreMapping = retailPriceDAO.getZoneStoreMapping(conn);
			}
			
			if(rollupDSDToWhseEnabled){
				logger.info("setupObjects() - ***ROLL_UP_DSD_TO_WARHOUSE_ZONE is enabled***");
				logger.info("setupObjects() - Getting DSD and Warehouse zone mapping...");
				dsdAndWhseZoneMap = retailCostDAO.getDSDAndWHSEZoneMap(conn, null);
				logger.info("setupObjects() - Getting DSD and Warehouse zone mapping is completed.");
				
				logger.info("setupObjects() - Getting Item and category mapping...");
				itemCodeCategoryMap = itemdao.getCategoryAndItemCodeMap(conn, null);
				logger.info("setupObjects() - Getting Item and category mapping is completed.");
				
				ProductLocationMappingDAO productLocationMappingDAO = new ProductLocationMappingDAO();
				
				primaryMatchingZone = productLocationMappingDAO.getPrimaryMatchingZoneNum(conn);
			}
			
			//Added for getting store id and zone id to update proper location_id saving price records.
			//Added by Pradeep 08-21-2015
			retailPriceDAO = new RetailPriceDAO();
			long startTime  = System.currentTimeMillis();
			storeIdsMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
			long endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to retreive store id mapping - " + (endTime - startTime));
			startTime = System.currentTimeMillis();
			retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
			endTime = System.currentTimeMillis();
			
			zoneStoreMap = retailPriceZoneDAO.getZoneStoreMap(conn);
			
			
			logger.info("setupObjects() - Time taken to retreive zone id mapping - " + (endTime - startTime));
			//Changes ends.
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			
			//Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + relativePath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
			
		    	
				try {
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(relativePath);
	
					for (int i = 0; i < fileList.size(); i++) {
						
						long fileProcessStartTime = System.currentTimeMillis(); 
						storeCountBasedOnUPC = new HashMap<String, Integer>();
						cacheRetailPriceData =new HashMap<String, List<RetailPriceDTO>>();
						processActualValues = false;
						recordCounts =0;
						recordCount = 0;
						futurePriceRecordsSkipped = 0;
						retailPriceDataMap = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
						storeIdSet = new HashSet<String>();
						if(!checkRetailerItemCode)
							itemCodeMap = new HashMap<String, String>();
						storeInfoMap = new HashMap<String, String>();
						storeIdMap = new HashMap<String, Integer>();
						deptZoneMap = new HashMap<String, List<Integer>>();
						noItemCodeSet = new HashSet<String>();
				    	
					    String files = fileList.get(i);
					    logger.info("File Name - " + fileList.get(i));
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
					    
					    
					    int columnCount = getColumnCount(files, '|');
					    String fieldNames[] = new String[columnCount];
		
				    	fieldNames[0] = "upc";
				    	fieldNames[1] = "retailerItemCode";
				    	fieldNames[2] = "levelTypeId";
				    	fieldNames[3] = "levelId";
				    	fieldNames[4] = "regEffectiveDate";
				    	fieldNames[5] = "regPrice";
				    	fieldNames[6] = "regQty";
				    	fieldNames[7] = "saleStartDate";
				    	fieldNames[8] = "saleEndDate";
				    	fieldNames[9] = "salePrice";
				    	fieldNames[10] = "saleQty";
				    	
				    	//Check if there are additional columns
				    	if(columnCount > 11){
				    		fieldNames[11] = "prcGrpCode";
				    	}
				    	
				    	
				    	String []fileFields  = mapFileField(fieldNames, columnCount); 
				    	
				    	logger.info("Processing Retail Price Records ...");
				    	// To get total stores were available for each UPC
				    	super.parseDelimitedFile(RetailPriceDTO.class, files, '|',fileFields, stopCount);
				    	logger.info("Number of Overall Distinct UPC :"+storeCountBasedOnUPC.size());
				    	processActualValues = true;
				    	recordCounts =0;
				    	super.parseDelimitedFile(RetailPriceDTO.class, files, '|',fileFields, stopCount);
				    	if(cacheRetailPriceData.size()>0){
				    		logger.error("Cache Retail Price Data map is not null");
				    	}
				    	
						logger.info("No of records processed - " + recordCount);
						
						PristineDBUtil.commitTransaction(conn, "Retail Price Data Setup");
					    
					    logger.info("UPCs with no item code - " + noItemCodeSet.size());
					    
					    // Change to handle stores with no zone
					    
					    StringBuffer storesWithNoZoneStr = new StringBuffer();
					    for(String storeNo : storesWithNoZone){
					    	storesWithNoZoneStr.append(storeNo + "\t");
					    }
					    if(storesWithNoZoneStr.length() > 0)
					    	logger.error("Number of Stores with no Zones - " + storesWithNoZone.size());
					    	logger.error("Stores with no Zones - " + storesWithNoZoneStr.toString());
					    // Change to handle stores with no zone - Ends
					    
					    long fileProcessEndTime = System.currentTimeMillis();
					    logger.info("No of future price records skipped - " + futurePriceRecordsSkipped);
					    logger.info("No of future price records, where Effective Date not with in "+ loadFuturePriceXWeeks +" weeks - " + recordCountPast26Weeks);
					    logger.debug("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
					}
				}catch (GeneralException ex) {
				    logger.error("GeneralException", ex);
				    commit = false;
				} catch (Exception ex) {
				    logger.error("JavaException", ex);
				    commit = false;
				}
				
				if( processZipFile){
			    	PrestoUtil.deleteFiles(fileList);
			    	fileList.clear();
			    	fileList.add(zipFileList.get(curZipFileCount));
			    }
			    String archivePath = getRootPath() + "/" + relativePath + "/";
			    
			    if( commit ){
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				}
				else{
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());
		}catch (GeneralException ex) {
	        logger.error("Outer Exception -  GeneralException", ex);
	    }
	    catch (Exception ex) {
	        logger.error("Outer Exception - JavaException", ex);
	    }
		
		logger.info("Retail Price Setup Complete");
		
		super.performCloseOperation(conn, true);
		return;
	}
	
	private void loadPriceData() throws GeneralException, Exception{
		List<RetailPriceDTO> toBeInserted = new ArrayList<RetailPriceDTO>();
    	List<RetailPriceDTO> toBeUpdated = new ArrayList<RetailPriceDTO>();
    	List<RetailPriceDTO> toBeDeleted = new ArrayList<RetailPriceDTO>();
    	List<RetailPriceDTO> futureDeleteList = new ArrayList<>();
		logger.info("Number of stores - " + storeIdSet.size());
		
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		
		for(Map.Entry<Integer, HashMap<String, List<RetailPriceDTO>>> entry : retailPriceDataMap.entrySet()){
			logger.info("Processing calendar Id " + entry.getKey());
			logger.info("Number of distinct UPCs - " + entry.getValue().size());
			
			// Populate ITEM_CODE for every UPC
			if(!checkRetailerItemCode){
				long getItemCodeStartTime = System.currentTimeMillis();
		    	itemCodeMap = retailPriceDAO.getItemCode(conn, entry.getValue().keySet());
		    	long getItemCodeEndTime = System.currentTimeMillis();
		    	logger.debug("Time taken to retrieve item codes - " + (getItemCodeEndTime - getItemCodeStartTime) + "ms");
			}
	    	
	    	// Populate zone number for every store number
	    	long getStoreInfoStartTime = System.currentTimeMillis();
	    	storeInfoMap = retailPriceDAO.getStoreInfo(conn, storeIdSet);
	    	storeIdMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
	    	priceZoneIdMap = retailPriceDAO.getPriceZoneData(conn);
	    	deptZoneMap = retailPriceDAO.getDeptZoneInfo(conn, chainId);
	    	long getStoreInfoEndTime = System.currentTimeMillis();
	    	logger.debug("Time taken to retrieve store info - " + (getStoreInfoEndTime - getStoreInfoStartTime) + "ms");
	    	
	    	// populate vendor info into vendor lookup...
			HashMap<String, Long> vendorIdMap = new HashMap<>();
			StoreItemMapService storeItemMapService = new StoreItemMapService();
			if(loadVendorInfo){
				logger.info("Setting up vendor information...");
				long vendorStart = System.currentTimeMillis();
				vendorIdMap = storeItemMapService.setupVendorInfoForPrice(conn, entry
						.getValue().values());
				long vendorEnd = System.currentTimeMillis();
				logger.debug("Time taken to setup vendor information - "
						+ (vendorEnd - vendorStart) + "ms");
			}
	    	
	    	// Populate mapping between store/zone and items
	    	if(!setupStoreItemMap.equalsIgnoreCase("FALSE")){
		    	long mapStoreWithItemsStartTime = System.currentTimeMillis();
		    	storeItemMapService.mapItemsWithStoreForPrice(conn, entry.getValue().values(), itemCodeMap, storeIdMap,
						priceZoneIdMap, noItemCodeSet, deptZoneMap, vendorIdMap, zoneStoreMap);
		    	long mapStoreWithItemsEndTIme = System.currentTimeMillis();
		    	logger.debug("Time taken for mapping store/zone and items - " + (mapStoreWithItemsEndTIme - mapStoreWithItemsStartTime) + "ms");
		    	if(setupStoreItemMap.equalsIgnoreCase("TRUE") && ignorePriceLoading){
		    		retailPriceDataMap = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
		    		return;
		    	}
	    	}
	    	
	    	
	    	
	    	
	    	if(rollupDSDToWhseEnabled){
	    		long dsdRollupStartTime = System.currentTimeMillis();
	    		//Rollup DSD level prices to warehouse level. Add an artificial entry 
		    	rollupDSDZonesToWhseZone(entry.getValue(), itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);	
		    	long dsdRollupEndTime = System.currentTimeMillis();
		    	
		    	logger.info("Time taken for DSD rollup - "
						+ (dsdRollupEndTime - dsdRollupStartTime) + "ms");
	    	}
	    	// Price Roll Up
	    	long priceRollupStartTime = System.currentTimeMillis();
	    	HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = priceRollUp(entry.getValue(), itemCodeMap, storeInfoMap, noItemCodeSet, chainId);
	    	long priceRollupEndTime = System.currentTimeMillis();
	    	logger.debug("Time taken for price rollup - " + (priceRollupEndTime - priceRollupStartTime) + "ms");
	    	
	    	// Make the object available for GC
	    	retailPriceDataMap = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
	    	
	    	// Compare new records with existing records in the database
	    	long compareEntriesStartTime = System.currentTimeMillis();
			compareExistingEntries(conn, retailPriceDAO, priceRolledUpMap, toBeInserted, toBeUpdated, toBeDeleted,
					entry.getKey(), futureDeleteList);
	    	long compareEntriesEndTime = System.currentTimeMillis();
	    	logger.debug("Time taken to compare records - " + (compareEntriesEndTime - compareEntriesStartTime) + "ms");
		}
		
    	logger.info("No of records to be inserted - " + toBeInserted.size());
    	logger.info("No of records to be deleted - " + toBeDeleted.size());
    	logger.info("No of records to be updated - " + toBeUpdated.size());
    	logger.info("No of future records to be deleted - " + futureDeleteList.size());
    	
    	long deleteStartTime = System.currentTimeMillis();
    	retailPriceDAO.deleteFuturePriceData(conn, futureDeleteList, startDate);
    	long deleteEndTime = System.currentTimeMillis();
    	
    	logger.debug("Time taken for deleting future records - " + (deleteEndTime - deleteStartTime) + "ms");
    	
    	// Delete retail price data
    	deleteStartTime = System.currentTimeMillis();
    	retailPriceDAO.deleteRetailPriceData(conn, toBeDeleted);
    	deleteEndTime = System.currentTimeMillis();
    	logger.debug("Time taken for deleting records - " + (deleteEndTime - deleteStartTime) + "ms");
    	
    	//update proper Location id
    	updateLocationId(toBeUpdated);
    	
    	// Update retail price data
    	long updateStartTime = System.currentTimeMillis();
    	retailPriceDAO.updateRetailPriceData(conn, toBeUpdated);
    	long updateEndTime = System.currentTimeMillis();
    	logger.debug("Time taken for updating records - " + (updateEndTime - updateStartTime) + "ms");

    	//update proper Location id
    	updateLocationId(toBeInserted);
    	
    	// Insert retail price data
    	long insertStartTime = System.currentTimeMillis();
    	retailPriceDAO.saveRetailPriceData(conn, toBeInserted);
    	long insertEndTime = System.currentTimeMillis();
    	logger.info("Time taken for inserting records - " + (insertEndTime - insertStartTime) + "ms");
	}
	
	/**
	 * 
	 * @param priceInputMap
	 * @throws GeneralException 
	 * @throws CloneNotSupportedException 
	 */
	public void rollupDSDZonesToWhseZone(
			HashMap<String, List<RetailPriceDTO>> priceInputMap,
			HashMap<String, String> itemCodeMap,
			HashMap<Integer, Integer> itemCodeCategoryMap,
			HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap)
					throws GeneralException, CloneNotSupportedException {

		HashMap<String, List<RetailPriceDTO>> DSDPriceMap = new HashMap<>();
		// Loop through the input map and identify all DSD zone prices
		for (Map.Entry<String, List<RetailPriceDTO>> priceInputEntry : priceInputMap.entrySet()) {
			for (RetailPriceDTO retailPriceDTO : priceInputEntry.getValue()) {
				if (retailPriceDTO.getPrcGrpCode().equals(Constants.GE_PRC_GRP_CD_DSD)) {
					List<RetailPriceDTO> tempList = new ArrayList<>();
					if (DSDPriceMap.containsKey(priceInputEntry.getKey())) {
						tempList = DSDPriceMap.get(priceInputEntry.getKey());
					}
					tempList.add(retailPriceDTO);
					DSDPriceMap.put(priceInputEntry.getKey(), tempList);
				}
			}
		}

		// Loop all the DSD items and rollup to warehouse level
		for (Map.Entry<String, List<RetailPriceDTO>> DSDPriceEntry : DSDPriceMap.entrySet()) {
			List<RetailPriceDTO> whsePriceList = new ArrayList<>();
			// Get dsd vs whse zone map

			for (RetailPriceDTO retailPriceDTO : DSDPriceEntry.getValue()) {
				String upc = PrestoUtil.castUPC(retailPriceDTO.getUpc(), false);
				String itemKey = upc + "-" + retailPriceDTO.getRetailerItemCode();
				if (itemCodeMap.containsKey(itemKey)) {
					String itemCode = itemCodeMap.get(itemKey);
					if (itemCodeCategoryMap.containsKey(Integer.parseInt(itemCode))) {
						int productId = itemCodeCategoryMap.get(Integer.parseInt(itemCode));
						if (dsdAndWhseZoneMap.containsKey(productId)) {
							HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
							if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
								if (zoneMap.containsKey(retailPriceDTO.getLevelId())) {
									// Get warehouse zones
									String whseZone = zoneMap.get(retailPriceDTO.getLevelId());
									
									//Below logic is commented because this is handled in product location mapping itself
									//Commented on 03/14/2018 by Pradeep
									//As per suggestion by Anand
									/*if (zoneMap.containsKey(whseZone)) {
										whseZone = zoneMap.get(whseZone);
									}*/
									RetailPriceDTO retailPriceDTO2 = (RetailPriceDTO) retailPriceDTO.clone();
									retailPriceDTO2.setLevelId(whseZone);
									retailPriceDTO2.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
									whsePriceList.add(retailPriceDTO2);
								}
							}
						}
					}
				}
			}
			// Attach warehouse price entry for each item
			if (whsePriceList.size() > 0) {
				logger.debug("Rolling up DSD prices...");

				HashMap<String, List<RetailPriceDTO>> priceGrp = new HashMap<>();
				for (RetailPriceDTO retailPriceDTO : whsePriceList) {
					List<RetailPriceDTO> tempList = new ArrayList<>();
					if (priceGrp.containsKey(retailPriceDTO.getLevelId())) {
						tempList = priceGrp.get(retailPriceDTO.getLevelId());
					}
					tempList.add(retailPriceDTO);
					priceGrp.put(retailPriceDTO.getLevelId(), tempList);
				}

				for (Map.Entry<String, List<RetailPriceDTO>> priceEntry : priceGrp.entrySet()) {

					HashMap<String, List<RetailPriceDTO>> priceGroupMap = new HashMap<>();
					for (RetailPriceDTO retailPriceDTO : priceEntry.getValue()) {
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

						// Group store prices to find most common
						// price
						
						List<RetailPriceDTO> tempList = new ArrayList<>();
						if (priceGroupMap.containsKey(priceStr)) {
							tempList = priceGroupMap.get(priceStr);
						}	
						tempList.add(retailPriceDTO);
						priceGroupMap.put(priceStr, tempList);
					}

					// Determine most prevalent price for this UPC
					RetailPriceDTO mostCommonPrice = new RetailPriceDTO();
					String mostPrevalentPrice = null;
					int mostPrevalentCnt = 0;
					int tempCnt = 0;
					for (Map.Entry<String, List<RetailPriceDTO>> entry : priceGroupMap.entrySet()) {
						/*
						 * if(entry.getValue().size() > mostPrevalentCnt){
						 * mostPrevalentCnt = entry.getValue().size();
						 * mostPrevalentPrice = entry.getKey(); }
						 */
						List<RetailPriceDTO> retailPriceDTOLst = entry.getValue();
						tempCnt = 0;
						for (RetailPriceDTO retailPriceDTO : retailPriceDTOLst) {
							if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
								tempCnt = tempCnt + 1;
							}
						}

						if (tempCnt > mostPrevalentCnt) {
							mostPrevalentCnt = tempCnt;
							mostPrevalentPrice = entry.getKey();
						}
					}

					for (Map.Entry<String, List<RetailPriceDTO>> entry : priceGroupMap.entrySet()) {
						if (entry.getKey().equals(mostPrevalentPrice)) {
							mostCommonPrice = entry.getValue().get(0);
							mostCommonPrice.setWhseZoneRolledUpRecord(true);
						}
					}

					// Attach warehouse price
					List<RetailPriceDTO> availbleEntries = priceInputMap.get(DSDPriceEntry.getKey());
					for(RetailPriceDTO existingLevel: availbleEntries){
						
					}
					availbleEntries.add(mostCommonPrice);
					priceInputMap.put(DSDPriceEntry.getKey(), availbleEntries);
				}
			}
		}
	}
	/**
	 * This method returns an array of fields in input file
	 * @param fieldNames
	 * @param columnCount2
	 * @return String array
	 */
	public static String[] mapFileField(String[] fieldNames, int columnCount2) {
		int fieldLength = columnCount2 < fieldNames.length? columnCount2 :fieldNames.length;
		String [] fileFields = new String[fieldLength];
		for ( int i = 0; i < fieldLength; i++)
			fileFields[i] = fieldNames[i];
		return fileFields;
	}
	
	/**
	 * This method add the elements of a list to a hashmap with upc as the key.
	 * @param listobj		List of RetailPriceDTO parsed from input file
	 */
	@Override
	public void processRecords(List listobj) throws GeneralException {
		HashMap<String, List<RetailPriceDTO>> processPriceDataMap = new HashMap<String, List<RetailPriceDTO>>();
		List<String> removeUPClist = new ArrayList<String>();
		List<RetailPriceDTO> retailPriceDataList = (List<RetailPriceDTO>) listobj;
		//To Cache Number of Stores for each UPC
		if(!processActualValues){
			for(RetailPriceDTO retailPriceDTO:retailPriceDataList){
				recordCounts++;
				int numberOfRec = 0;
				if(storeCountBasedOnUPC.containsKey(retailPriceDTO.getUpc())){
					numberOfRec = storeCountBasedOnUPC.get(retailPriceDTO.getUpc());
				}
				numberOfRec = numberOfRec+1;
				storeCountBasedOnUPC.put(retailPriceDTO.getUpc(), numberOfRec);
				if(recordCounts % 1000000 == 0){
					logger.info("No of records Cached in HashMap: "+recordCounts);
				}
				
			}
		}
		if(processActualValues){
			//Actual data are stored in HashMap based om UPC
			for(RetailPriceDTO retailPriceDTO:retailPriceDataList){
				recordCounts++;
				List<RetailPriceDTO> retailPriceDataLists = new ArrayList<RetailPriceDTO>();
				if(cacheRetailPriceData.containsKey(retailPriceDTO.getUpc())){
					retailPriceDataLists.addAll(cacheRetailPriceData.get(retailPriceDTO.getUpc()));
				}
				retailPriceDataLists.add(retailPriceDTO);
				cacheRetailPriceData.put(retailPriceDTO.getUpc(), retailPriceDataLists);
				if(recordCounts % 1000000 == 0){
					logger.info("No of records Cached in HashMap: "+recordCounts);
				}
				
			}
			//Check the list size of each UPC matched with Store count and if it does add them in list and process
			for(Map.Entry<String, List<RetailPriceDTO>> retailPriceMap : cacheRetailPriceData.entrySet()){
				List<RetailPriceDTO> retailPriceDataLists = retailPriceMap.getValue();
				int storeCountForUPC = storeCountBasedOnUPC.get(retailPriceMap.getKey());
				if(storeCountForUPC == retailPriceDataLists.size()){
					processPriceDataMap.put(retailPriceMap.getKey(), retailPriceDataLists);
					removeUPClist.add(retailPriceMap.getKey());
				}
			}
			processAllStoresBasedOnUPC(processPriceDataMap);
			processPriceDataMap.clear();
			//Remove UPC which is processed..
			for(String upc: removeUPClist){
				cacheRetailPriceData.remove(upc);
			}
			
		}
		
	}
	
	private void processAllStoresBasedOnUPC(HashMap<String, List<RetailPriceDTO>> retailPriceData){
		List<RetailPriceDTO> retailPriceDataList = new ArrayList<RetailPriceDTO>();
		for(Map.Entry<String, List<RetailPriceDTO>> entry:retailPriceData.entrySet()){
			List<RetailPriceDTO> tempList = entry.getValue();
			retailPriceDataList.addAll(tempList);
		}
		processPriceRecords(retailPriceDataList);
	}
	
//	private void processPriceList(List<RetailPriceDTO> retailPriceDataList){
//		if(retailPriceDataList != null && retailPriceDataList.size() > 0){
//			if(retailPriceDataList.size() >= commitRecCount){
//				if(currentProcPriceList.size() >= noOfRecsToBeProcessed){
//					String lastUPC = currentProcPriceList.get(currentProcPriceList.size()-1).getUpc();
//					boolean eofFlag = true;
//					for(RetailPriceDTO priceDTO : retailPriceDataList){
//						if(priceDTO.getUpc().equals(lastUPC)){
//							currentProcPriceList.add(priceDTO);
//						}else{
//							if(currentProcPriceList.size() >= noOfRecsToBeProcessed){
//								processPriceRecords(currentProcPriceList);
//								if(currentProcPriceList != null)
//									currentProcPriceList.clear();
//								eofFlag = false;
//							}
//							currentProcPriceList.add(priceDTO);
//						}
//					}
//					if(eofFlag){
//						processPriceRecords(currentProcPriceList);
//						if(currentProcPriceList != null)
//							currentProcPriceList.clear();
//					}
//				}else{
//					currentProcPriceList.addAll(retailPriceDataList);
//				}
//			}else{
//				currentProcPriceList.addAll(retailPriceDataList);
//				processPriceRecords(currentProcPriceList);
//				if(currentProcPriceList != null)
//					currentProcPriceList.clear();
//			}
//		}
//	}
	/**
	 * Processes Price records
	 * (Populates retailPriceDataMap with UPC as key and corresponding list of price records as value)
	 * @param priceList	List of Price records to be processed
	 */
	private void processPriceRecords(List<RetailPriceDTO> priceList){
		// Populate HashMap from the list with UPC as key and list of corresponding RetailPriceDTO as it value
		logger.info("Inside processPriceRecords of RetailPriceSetup()");
		for(RetailPriceDTO retailPriceDTO:priceList){
			recordCount++;
			
			// Changes to restrict items that has price lesser than minItemPrice
			if((retailPriceDTO.getRegPrice() > 0 && retailPriceDTO.getRegPrice() <= minItemPrice) || 
					(retailPriceDTO.getRegMPrice() > 0 && retailPriceDTO.getRegMPrice() <= minItemPrice)){
				continue;
			}
			// Changes to restrict items that has price lesser than minItemPrice - Ends
			
			if(retailPriceDTO.getRegQty() > 1){
				retailPriceDTO.setRegMPrice(retailPriceDTO.getRegPrice());
				retailPriceDTO.setRegPrice(0);
			}
			
			if(retailPriceDTO.getSaleQty() > 1){
				retailPriceDTO.setSaleMPrice(retailPriceDTO.getSalePrice());
				retailPriceDTO.setSalePrice(0);
			}
			
			retailPriceDTO.setCalendarId(calendarId);
			
			
			if(useVendorCodeFromZone){
				String zoneArr[] = retailPriceDTO.getLevelId().split("-");
				if(zoneArr.length > 3){
					retailPriceDTO.setVendorName("VENDOR-" + zoneArr[3]);
					retailPriceDTO.setVendorNumber(zoneArr[3]);
				}
			}
			
			// Changes to retrieve item_code using upc and retailer_item_code
			String priceMapKey = null;
			if(!checkRetailerItemCode)
				priceMapKey = PrestoUtil.castUPC(retailPriceDTO.getUpc(),false);
			else
				priceMapKey = retailPriceDTO.getUpc() + "-" + retailPriceDTO.getRetailerItemCode();
			// Changes to retrieve item_code using upc and retailer_item_code - Ends 
			
			// Populate StoreId in the global Set
			if(retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
				//Cast store number if length is less than 4
				retailPriceDTO.setLevelId(PrestoUtil.castStoreNumber(retailPriceDTO.getLevelId()));
				storeIdSet.add(retailPriceDTO.getLevelId());
			}
			
			// Condition to check whether the Effective Date of Future Price is
			// within 26 Weeks.
			loadFuturePriceXWeeks = Integer.parseInt(PropertyManager.getProperty("LOAD_FUTURE_PRICE_X_WEEKS", "0"));
			if (loadFuturePriceXWeeks != 0) {
				boolean isFutureDateisWithinXWeeks = isFutureDateisWithinXWeeks(retailPriceDTO.getRegEffectiveDate(),
						loadFuturePriceXWeeks);
				if (isFutureDateisWithinXWeeks) {
					recordCountPast26Weeks++;
					continue;
				}
			}
			
			// Changes to handle future price records
			boolean isFuturePrice = isFuturePrice(retailPriceDTO.getRegEffectiveDate(), weekEndDate);
			//Identify if sale price is set to future
			boolean isFutureSalePrice = isFuturePrice(retailPriceDTO.getSaleStartDate(), weekEndDate);
			if((isFuturePrice || isFutureSalePrice) && !loadFuturePrice){
				futurePriceRecordsSkipped++;
				continue;
			}
			
			if(isFuturePrice){
				//Identify future week calendar id based on regular effective date
				populateCalendarIdForFuture(retailPriceDTO.getRegEffectiveDate(), retailPriceDTO);
			}else if(isFutureSalePrice){
				//Identify future week calendar id based on sale start date
				populateCalendarIdForFuture(retailPriceDTO.getSaleStartDate(), retailPriceDTO);
			}
			// Changes to handle future price records Ends
			
			boolean canBeAdded = true;
			if(retailPriceDataMap.get(retailPriceDTO.getCalendarId()) != null){
				HashMap<String, List<RetailPriceDTO>> tMap = retailPriceDataMap.get(retailPriceDTO.getCalendarId());
				if(tMap.get(priceMapKey) != null){
					List<RetailPriceDTO> tempList = tMap.get(priceMapKey);
					
					for(RetailPriceDTO tempDTO:tempList){
						if(tempDTO.getLevelId().equals(retailPriceDTO.getLevelId())){
							canBeAdded = false;
								
							// If dates are not equal compare dates and update retail price dto with most recent price.
							compareDatesInRetailPrice(tempDTO, retailPriceDTO);
						}
					}
					
					
					if(canBeAdded){
						tempList.add(retailPriceDTO);
						tMap.put(priceMapKey, tempList);
					}
				}else{
					List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
					tempList.add(retailPriceDTO);
					tMap.put(priceMapKey, tempList);
				}
				retailPriceDataMap.put(retailPriceDTO.getCalendarId(), tMap);
			}else{
				HashMap<String, List<RetailPriceDTO>> tMap = new HashMap<String, List<RetailPriceDTO>>();
				List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
				tempList.add(retailPriceDTO);
				tMap.put(priceMapKey, tempList);
				retailPriceDataMap.put(retailPriceDTO.getCalendarId(), tMap);
			}
			
			if(recordCount % Constants.RECS_TOBE_PROCESSED == 0)    	
				logger.info("No of price records Processed " + recordCount);
		}
		
//		logger.info("No of price records Processed - " + recordCount);
		
		priceList = null;
		
		try{
			loadPriceData();
		}catch(GeneralException | Exception exception ){
			logger.error("Exception in processPriceRecords of RetailPriceSetup - ", exception);
		}
	}
	
	private boolean isFutureDateisWithinXWeeks(String regEffectiveDate, int EffectiveDateXWeeks) {
		boolean isFuturePrice = false;

		if (regEffectiveDate != null && !Constants.EMPTY.equals(regEffectiveDate)) {
			try {
				Date regEffDate = DateUtil.toDate(regEffectiveDate);
				Date todayDate = new Date();
				int futurePriceXDays = (EffectiveDateXWeeks * 7);
				Date effectiveDateXweeks = DateUtils.addDays(todayDate, futurePriceXDays);

				if (regEffDate.after(effectiveDateXweeks)) {
					logger.info("Effective Date - " + (regEffectiveDate) + ", is not within " + EffectiveDateXWeeks
							+ " Weeks From Current Date ("
							+ new SimpleDateFormat(Constants.APP_DATE_FORMAT).format(todayDate) + ")");
					isFuturePrice = true;
				}
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + regEffectiveDate);
			}
		}
		return isFuturePrice;
	}
	
	/**
	 * Populates calendar id to map for given date string
	 * @param effDate
	 * @param retailPriceDTO
	 */
	private void populateCalendarIdForFuture(String effDate, RetailPriceDTO retailPriceDTO){
		if(calendarMap.get(effDate) != null){
			int calId = calendarMap.get(effDate);
			retailPriceDTO.setCalendarId(calId);
		}else{
			try{
				RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, effDate, Constants.CALENDAR_WEEK);
				int calId = calendarDTO.getCalendarId();
				retailPriceDTO.setCalendarId(calId);
				calendarMap.put(effDate, calId);
				logger.info("Future Price Effective Date - " + effDate + "\t" + calId);
			}catch(GeneralException exception){
				logger.error("Error when retrieving calendar id for " + effDate + " " + exception);
			}
		}
	}
	
	/**
	 * This method compares dates in two retail price dto 
	 * and sets the most recent price values
	 * @param tempDTO			RetailPriceDTO to be compared for the dates
	 * @param retailPriceDTO	RetailPriceDTO to be compared for the dates
	 */
	private void compareDatesInRetailPrice(RetailPriceDTO tempDTO,
			RetailPriceDTO retailPriceDTO) {
		logger.debug("Inside compareDatesInRetailPrice() of RetailPriceSetup");
		try{
			if(!tempDTO.getRegEffectiveDate().isEmpty() && !retailPriceDTO.getRegEffectiveDate().isEmpty())
				if(!tempDTO.getRegEffectiveDate().equals(retailPriceDTO.getRegEffectiveDate())){
					SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yy" );  
					java.util.Date tempDate = sdf.parse( tempDTO.getRegEffectiveDate() );  
					java.util.Date retailDate = sdf.parse( retailPriceDTO.getRegEffectiveDate() );
					// Changes to handle future price data
					boolean isTempDateInFuture = isFuturePrice(tempDTO.getRegEffectiveDate(), weekEndDate);
					boolean isRetailDateInFuture = isFuturePrice(retailPriceDTO.getRegEffectiveDate(), weekEndDate);
					
					if(!isTempDateInFuture && !isRetailDateInFuture)
					// Changes to handle future price data Ends
						if (compareTo(tempDate, retailDate) > 0)   
						{  
							// tempDate is after retailDate
							retailPriceDTO.setRegEffectiveDate(tempDTO.getRegEffectiveDate());
							retailPriceDTO.setRegPrice(tempDTO.getRegPrice());
							retailPriceDTO.setRegQty(tempDTO.getRegQty());
							retailPriceDTO.setRegMPrice(tempDTO.getRegMPrice());
						}  
				}
			
			if(!tempDTO.getSaleStartDate().isEmpty() && !retailPriceDTO.getSaleStartDate().isEmpty())
				if(!tempDTO.getSaleStartDate().equals(retailPriceDTO.getSaleStartDate())){
					SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yy" );  
					java.util.Date tempDate = sdf.parse( tempDTO.getSaleStartDate() );  
					java.util.Date retailDate = sdf.parse( retailPriceDTO.getSaleStartDate() );
					  
					if (compareTo(tempDate, retailDate) > 0)   
					{  
						// tempDate is after retailDate
						retailPriceDTO.setSaleStartDate(tempDTO.getSaleStartDate());
						retailPriceDTO.setSaleEndDate(tempDTO.getSaleEndDate());
						retailPriceDTO.setSalePrice(tempDTO.getSalePrice());
						retailPriceDTO.setSaleQty(tempDTO.getSaleQty());
						retailPriceDTO.setSaleMPrice(tempDTO.getSaleMPrice());
					}  
				}
		}catch(ParseException pe){
			logger.error("Parser Error - " +  pe.getMessage());
		}
	}
	
	/**
	 * This method compares two dates
	 * @param date1		Date to compare
	 * @param date2		Date to compare
	 * @return
	 */
    public static long compareTo( java.util.Date date1, java.util.Date date2 )  
    {  
    	//returns negative value if date1 is before date2  
    	//returns 0 if dates are even  
    	//returns positive value if date1 is after date2  
    	return date1.getTime() - date2.getTime();  
    }  

	/**
	 * Performs price roll up logic for Retail Price Data
	 * @return List		Map Containing Item Code as key and list of RetailPriceDTO as value  
	 * 					to be compared against existing database entries
	 */
	public HashMap<String, List<RetailPriceDTO>> priceRollUp(HashMap<String, List<RetailPriceDTO>> retailPriceDataMap, HashMap<String, String> itemCodeMap, 
															HashMap<String, String> storeInfoMap, Set<String> noItemCodeSet, String chainId){
		//logger.debug("Inside priceRollUp of RetailPriceSetup");
		
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = new HashMap<String, List<RetailPriceDTO>>();
		List<RetailPriceDTO> priceRolledUpList; 
		
		for(List<RetailPriceDTO> retailPriceDTOList:retailPriceDataMap.values()){
			
			HashMap<String, List<RetailPriceDTO>> chainLevelMap = new HashMap<String, List<RetailPriceDTO>>();
			HashMap<String, HashMap<String, List<RetailPriceDTO>>> zoneLevelMap = new HashMap<String, HashMap<String,List<RetailPriceDTO>>>();
			
			priceRolledUpList = new ArrayList<RetailPriceDTO>();
			String itemCode = null;
			String upc = null;
			boolean isPricedAtZoneLevel = false;
			boolean isPricedAtStoreLevel = false;
			for(RetailPriceDTO retailPriceDTO:retailPriceDTOList){
				
				String priceStr = Constants.EMPTY;
				
				retailPriceDTO.setPromotionFlag("N");
				if(retailPriceDTO.getRegPrice() > 0){
					priceStr = priceStr + retailPriceDTO.getRegPrice();
				}else if(retailPriceDTO.getRegMPrice() > 0){
					priceStr = priceStr + retailPriceDTO.getRegMPrice() + 
							Constants.INDEX_DELIMITER + retailPriceDTO.getRegQty();
				}
				
				if(retailPriceDTO.getSalePrice() > 0){
					priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSalePrice();
					retailPriceDTO.setPromotionFlag("Y");
				}else if(retailPriceDTO.getSaleMPrice() > 0){
					priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSaleMPrice()
						+ Constants.INDEX_DELIMITER + retailPriceDTO.getSaleQty();
					retailPriceDTO.setPromotionFlag("Y");
				}
				
				priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getRegEffectiveDate();
				
				
				
				// Set Item Code
				if(retailPriceDTO.getItemcode() != null){
					itemCode = retailPriceDTO.getItemcode();
				}
				
				if(retailPriceDTO.getUpc() != null){
					if(!checkRetailerItemCode)
						itemCode = itemCodeMap.get(PrestoUtil.castUPC(retailPriceDTO.getUpc(),false));
					else{
						String key = PrestoUtil.castUPC(retailPriceDTO.getUpc(), false) + "-" + retailPriceDTO.getRetailerItemCode();
						if(itemCodeMap.get(key) != null)
							itemCode = String.valueOf(itemCodeMap.get(key));
						else{
							noItemCodeSet.add(retailPriceDTO.getUpc());
							continue;
						}	
					}
					
					retailPriceDTO.setItemcode(itemCode);
									
					upc = retailPriceDTO.getUpc();
				}
				
				// Set Zone# and Store#
				if(retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
					retailPriceDTO.setStoreNbr(retailPriceDTO.getLevelId());
					retailPriceDTO.setZoneNbr(storeInfoMap.get(retailPriceDTO.getStoreNbr()));
					// Change to handle stores with no zone
					if(storeInfoMap.get(retailPriceDTO.getStoreNbr()) == null)
						storesWithNoZone.add(retailPriceDTO.getStoreNbr());
					// Change to handle stores with no zone - Ends
				}else if(retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
					retailPriceDTO.setZoneNbr(retailPriceDTO.getLevelId());
				}
				
				
				// If item is priced at zone level populate a hashmap with price as key and list of corresponding retailPriceDTO as its value
				if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					isPricedAtZoneLevel = true;
					List<RetailPriceDTO> tempList = null;
					if(chainLevelMap.get(priceStr) != null){
						tempList = chainLevelMap.get(priceStr);
						tempList.add(retailPriceDTO);
					}else{
						tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(retailPriceDTO);
					}
					chainLevelMap.put(priceStr, tempList);
				}else{
					/* If item is priced at store level populate a hashmap with zone number as key
					 * and hashmap containing price and its list of retail price info as value
					 */
					isPricedAtStoreLevel = true;
					HashMap<String,List<RetailPriceDTO>> zonePriceMap = null;
					if(zoneLevelMap.get(retailPriceDTO.getZoneNbr()) != null){
						zonePriceMap = zoneLevelMap.get(retailPriceDTO.getZoneNbr());
						List<RetailPriceDTO> tempList = null;
						if(zonePriceMap.get(priceStr) != null){
							tempList = zonePriceMap.get(priceStr);
							tempList.add(retailPriceDTO);
						}else{
							tempList = new ArrayList<RetailPriceDTO>();
							tempList.add(retailPriceDTO);
						}
						zonePriceMap.put(priceStr, tempList);
					}else{
						zonePriceMap = new HashMap<String, List<RetailPriceDTO>>();
						List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(retailPriceDTO);
						zonePriceMap.put(priceStr, tempList);
					}
					zoneLevelMap.put(retailPriceDTO.getZoneNbr(), zonePriceMap);
				}
			}
			
			// If item is priced at store level perform an additional step of rolling it up to zone level.
			if(isPricedAtStoreLevel){
				
				for(Map.Entry<String, HashMap<String, List<RetailPriceDTO>>> entry:zoneLevelMap.entrySet()){
					String mostPrevalentPriceAtZone = null;
					int mostPrevalentCntAtZone = 0;
					if(entry.getKey() != null){
						for(Map.Entry<String, List<RetailPriceDTO>> inEntry:entry.getValue().entrySet()){
							if(inEntry.getValue().size() > mostPrevalentCntAtZone){
								mostPrevalentCntAtZone = inEntry.getValue().size();
								mostPrevalentPriceAtZone = inEntry.getKey();
							}
						}
						
						for(Map.Entry<String, List<RetailPriceDTO>> inEntry:entry.getValue().entrySet()){
							if(inEntry.getKey().equals(mostPrevalentPriceAtZone)){
								RetailPriceDTO retailPriceDTO = inEntry.getValue().get(0);
								retailPriceDTO.setLevelId(entry.getKey());
								retailPriceDTO.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
								
								List<RetailPriceDTO> tempList = null;
								if(chainLevelMap.get(mostPrevalentPriceAtZone) != null){
									tempList = chainLevelMap.get(mostPrevalentPriceAtZone);
									tempList.add(retailPriceDTO);
								}else{
									tempList = new ArrayList<RetailPriceDTO>();
									tempList.add(retailPriceDTO);
								}
								chainLevelMap.put(mostPrevalentPriceAtZone, tempList);
							}else{
								List<RetailPriceDTO> tempList = null;
								if(chainLevelMap.get(inEntry.getKey()) != null){
									tempList = chainLevelMap.get(inEntry.getKey());
									tempList.addAll(inEntry.getValue());
								}else{
									tempList = new ArrayList<RetailPriceDTO>();
									tempList.addAll(inEntry.getValue());
								}
								chainLevelMap.put(inEntry.getKey(), tempList);
							}
						}
					}else{
						// Changes for TOPS Cost/Billback dataload - To handle stores with null zone ids
						if(!ignoreStoresNotInDB.equalsIgnoreCase(Constants.IGN_STR_NOT_IN_DB_TRUE)){
							for(Map.Entry<String, List<RetailPriceDTO>> inEntry:entry.getValue().entrySet()){
								for(RetailPriceDTO retailPriceDTO : inEntry.getValue()){
									String priceStr = Constants.EMPTY;
									
									retailPriceDTO.setPromotionFlag("N");
									if(retailPriceDTO.getRegPrice() > 0){
										priceStr = priceStr + retailPriceDTO.getRegPrice();
									}else if(retailPriceDTO.getRegMPrice() > 0){
										priceStr = priceStr + retailPriceDTO.getRegMPrice();
									}
									
									if(retailPriceDTO.getSalePrice() > 0){
										priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSalePrice();
										retailPriceDTO.setPromotionFlag("Y");
									}else if(retailPriceDTO.getSaleMPrice() > 0){
										priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSaleMPrice();
										retailPriceDTO.setPromotionFlag("Y");
									}
									
									List<RetailPriceDTO> tempList = null;
									if(chainLevelMap.get(priceStr) != null){
										tempList = chainLevelMap.get(priceStr);
										retailPriceDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailPriceDTO.setLevelId(retailPriceDTO.getStoreNbr());
										tempList.add(retailPriceDTO);
									}else{
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
			for(Map.Entry<String, List<RetailPriceDTO>> entry:chainLevelMap.entrySet()){
				/*if(entry.getValue().size() > mostPrevalentCnt){
					mostPrevalentCnt = entry.getValue().size();
					mostPrevalentPrice = entry.getKey();
				}*/
				
				
				List<RetailPriceDTO> retailPriceDTOLst = entry.getValue();
				tempCnt = 0;
				for(RetailPriceDTO retailPriceDTO:retailPriceDTOLst){
					if(retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
						// Check if there are two common prices
						// Confusion in picking chain level price
						if(chainLevelMap.size() == 2 && useZoneStoreMapping){
							if(zoneToStoreMapping.get(retailPriceDTO.getLevelId()) != null){
								List<String> stores = zoneToStoreMapping.get(retailPriceDTO.getLevelId());
								tempCnt = tempCnt + stores.size();
							}
						}
						else{
							tempCnt = tempCnt + 1;
						}
					}
				}
				
				if(tempCnt > mostPrevalentCnt){
					mostPrevalentCnt = tempCnt;
					mostPrevalentPrice = entry.getKey();
				}
			}
			
			// Add Items to the final list to be compared against the database
			for(Map.Entry<String, List<RetailPriceDTO>> entry:chainLevelMap.entrySet()){
				if(entry.getKey().equals(mostPrevalentPrice)){
					RetailPriceDTO retailPriceDTO = entry.getValue().get(0);
					RetailPriceDTO chainLevelDTO = new RetailPriceDTO();
					chainLevelDTO.copy(retailPriceDTO);
					//logger.debug("UPC" + retailPriceDTO.getUpc() + "Rolled Up Price " + entry.getKey() +" Level type id " + retailPriceDTO.getLevelTypeId() + " Level id " + retailPriceDTO.getLevelId());
					chainLevelDTO.setLevelId(chainId);
					chainLevelDTO.setLevelTypeId(Constants.CHAIN_LEVEL_TYPE_ID); 
					chainLevelDTO.setWhseZoneRolledUpRecord(false);
					priceRolledUpList.add(chainLevelDTO);
					for(RetailPriceDTO retailPrice:entry.getValue()){
						if(retailPrice.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
							priceRolledUpList.add(retailPrice);
						}
					}
				}else{
					for(RetailPriceDTO retailPriceDTO:entry.getValue()){
						priceRolledUpList.add(retailPriceDTO);
					}
				}
			}
			
			if(itemCode != null)
				priceRolledUpMap.put(itemCode, priceRolledUpList);
			else{
				if(upc != null)
					noItemCodeSet.add(upc);
			}
		}
		return priceRolledUpMap;
	}
	
	/**
	 * This method compares price rolled up records with existing entries in database
	 * and determines what records needs to be inserted, updated, deleted.
	 * @param priceRolledUpMap Contains Item Code as key and list of price rolled up records for that item code as value
	 */
	public void compareExistingEntries(Connection conn, RetailPriceDAO retailPriceDAO,
			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap, List<RetailPriceDTO> toBeInserted,
			List<RetailPriceDTO> toBeUpdated, List<RetailPriceDTO> toBeDeleted, int procCalendarId,
			List<RetailPriceDTO> futureDeleteList)
					throws GeneralException {
		logger.debug("Inside compareExistingEntries() of RetailPriceSetup");
		
		HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = retailPriceDAO.getRetailPriceInfo(conn, priceRolledUpMap.keySet(), procCalendarId, false);
		
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			
			List<RetailPriceDTO> retailPriceDBList = retailPriceDBMap.get(entry.getKey());
			
			// If there are no existing records for that Item Code insert all new records
			if(retailPriceDBList == null || (retailPriceDBList != null && retailPriceDBList.isEmpty())){
				toBeInserted.addAll(entry.getValue());
				
				for(RetailPriceDTO retailPriceNew:entry.getValue()){
					Date regEffDate = DateUtil.toDate(retailPriceNew.getRegEffectiveDate());
					Date currentDate = DateUtil.toDate(startDate);
					//Check if the effective date is current week or future week.
					//if it is current or future, add it to delete list so that any future data will be wiped
					//and latest information will be reflected
					if(regEffDate.after(currentDate) || regEffDate.compareTo(currentDate) == 0){
						futureDeleteList.add(retailPriceNew);
					}
				}
				continue;
			}
			
			if(retailPriceDBList != null && !retailPriceDBList.isEmpty()){
				// Compare new list and db list to find out records that needs to be updated
				for(RetailPriceDTO retailPriceNew:entry.getValue()){
					
					for(RetailPriceDTO retailPriceDB:retailPriceDBList){
						if(retailPriceNew.getLevelTypeId() == retailPriceDB.getLevelTypeId()
								&& retailPriceNew.getLevelId().equals(retailPriceDB.getLevelId())){
							if(retailPriceNew.getPromotionFlag().equals(retailPriceDB.getPromotionFlag())){
								if(!((retailPriceNew.getRegPrice() == retailPriceDB.getRegPrice()) &&
									(retailPriceNew.getRegMPrice() == retailPriceDB.getRegMPrice()) &&
									(retailPriceNew.getRegQty() == retailPriceDB.getRegQty()) &&
									(retailPriceNew.getSalePrice() == retailPriceDB.getSalePrice()) &&
									(retailPriceNew.getSaleMPrice() == retailPriceDB.getSaleMPrice()) &&
									(retailPriceNew.getSaleQty() == retailPriceDB.getSaleQty())
								)){
									toBeUpdated.add(retailPriceNew);
								}
							}else{
								toBeUpdated.add(retailPriceNew);
							}
							retailPriceNew.setProcessedFlag(true);
							retailPriceDB.setProcessedFlag(true);
						}
					}
					
				}
				
				// Check for not processed records in new list for inserting into database
				for(RetailPriceDTO retailPriceNew:entry.getValue()){
					if(!retailPriceNew.isProcessedFlag()){
						toBeInserted.add(retailPriceNew);
						Date regEffDate = DateUtil.toDate(retailPriceNew.getRegEffectiveDate());
						Date currentDate = DateUtil.toDate(startDate);
						//Check if the effective date is current week or future week.
						//if it is current or future, add it to delete list so that any future data will be wiped
						//and latest information will be reflected
						if(regEffDate.after(currentDate) || regEffDate.compareTo(currentDate) == 0){
							futureDeleteList.add(retailPriceNew);
						}
					}
				}
				
				// Check for not processed records in db list for deleting from database
				for(RetailPriceDTO retailPriceDB:retailPriceDBList){
					if(!retailPriceDB.isProcessedFlag()){
						toBeDeleted.add(retailPriceDB);
					}
				}
			}
		}
	}
	
	
	private boolean isFuturePrice(String regEffectiveDate, Date weekEndDate){
		boolean isFuturePrice = false;
		if(regEffectiveDate != null && !Constants.EMPTY.equals(regEffectiveDate)){
			try{
				Date regEffDate = DateUtil.toDate(regEffectiveDate);
				if(compareTo(regEffDate, weekEndDate) > 0){
					isFuturePrice = true;
				}
			}catch(GeneralException exception){
				logger.error("Error when parsing date - " + regEffectiveDate);
			}
		}
		return isFuturePrice;
	}
	
	private void updateLocationId(List<RetailPriceDTO> insertList){
		for(RetailPriceDTO retailPriceDTO: insertList){
			//Update chain id for zone level records
			if(retailPriceDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID)
				retailPriceDTO.setLocationId(Integer.parseInt(retailPriceDTO.getLevelId()));
			//Update price zone id from the cache when there is a zone level record 
			else if(retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null 
				if(retailPriceZone.get(retailPriceDTO.getLevelId()) != null)
					retailPriceDTO.setLocationId(retailPriceZone.get(retailPriceDTO.getLevelId()));
			}
			//Update comp_str_id from the cache when there is a store level record
			else if(retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null
				if(storeIdsMap.get(retailPriceDTO.getLevelId()) != null)
					retailPriceDTO.setLocationId(storeIdsMap.get(retailPriceDTO.getLevelId()));
			}
		}
	}
	
	/**
	 * 
	 * @param fileName
	 * @param delimiter
	 * @return # of coulmns in the file
	 * @throws Exception
	 */
	public int getColumnCount(String fileName, char delimiter) throws Exception{
		int colCount = 0;
		CsvReader reader = new CsvReader(new FileReader(fileName));
	    reader.setDelimiter(delimiter);
	    if(reader.readRecord()){
	    	String[] line = reader.getValues();
	    	colCount = line.length;
	    }
		return colCount;
	}
	
}
