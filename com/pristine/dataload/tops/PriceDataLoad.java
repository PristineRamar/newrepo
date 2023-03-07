package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
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
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.prestoload.CompDataLoadV2;
import com.pristine.dataload.prestoload.RetailPriceSetup;
import com.pristine.dataload.service.PriceAndCostService;
import com.pristine.dataload.service.StoreItemMapService;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;

public class PriceDataLoad extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("PriceDataLoad");
  	
  	private static LinkedHashMap PRICE_FIELD = null;
  	
  	HashMap<ItemDetailKey, List<PriceAndCostDTO>> priceDataMap = new HashMap<ItemDetailKey, List<PriceAndCostDTO>>();
  	HashMap<String, String> storeInfoMap = null;
  	HashMap<String, List<String>> zoneStoreMap = null;
  	HashMap<ItemDetailKey, String> itemCodeMap = null;
  	// HashMap<String, RetailPriceDTO> unrolledPriceDataMap = new HashMap<String, RetailPriceDTO>();
  	HashMap<String, List<RetailPriceDTO>> retailPriceCache = new HashMap<String, List<RetailPriceDTO>>();
  	private static String ignoreStoresNotInDB = "";  	
  	List<PriceAndCostDTO> currentProcPriceList = new ArrayList<PriceAndCostDTO>();
  	private Set<String> storesWithNoZone = new HashSet<String>();  	
  	//Changes for populating store id in Store item map table.
  	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
  	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
  	List<PriceAndCostDTO> ignoredRecords = new ArrayList<PriceAndCostDTO>();
  	int calendarId = -1;
  	int prevCalendarId = -1;
  	int priceRecordCount = 0;
  	int commitRecCount = 1000;
  	static boolean isCopyEnabled = true;
  	String startDate = null;
  	String prevWkStartDate = null;
  	private static String ARG_IGN_STR_NOT_IN_DB = "IGN_STR_NOT_IN_DB=";  	
  	private RetailPriceDAO retailPriceDAO = null;
  	private CostDAO costDAO = null;
  	Set<String> levelIdNotFound= new HashSet<String>(); 
  	
  	String chainId = null;
  	Connection conn = null;
  	
  	float minItemPrice=0;
  	StoreItemMapService storeItemMapService = null;
  	
  	public PriceDataLoad() {
  		super();
  	}
  	
	/**
	 * @param initProps
	 */
	public PriceDataLoad(boolean initProps) {
		super(initProps);
	}

  	/**
  	 * Arguments
  	 * args[0]		Relative path of Price File
  	 * args[1]		weektype - lastweek/currentweek/nextweek/specificweek
  	 * args[2]		Date if weektype is specific week
  	 * args[3] 		IGN_COPY_EXIST= true/false.
  	 * args[4] 		MODE= delta/full.
  	 * @param args
  	 */
    public static void main(String[] args) {
  
    	PriceDataLoad dataload = new PriceDataLoad();
    	PropertyConfigurator.configure("log4j-price-retail.properties");
		 
    	String weekType = null;
		String mode = null;
		String copyExisting = null;
		
    	if(args[args.length - 1].startsWith(Constants.DATA_LOAD_MODE)){
    		mode = args[args.length - 1].substring(Constants.DATA_LOAD_MODE.length());
    	}
    	
    	// Default week type to currentweek if it is not specified
    	if((mode == null && args.length == 1) || (mode != null && args.length == 2)){
    		weekType = Constants.CURRENT_WEEK;
    	}else{
    		weekType = args[1];
    	}

		if(args[2].startsWith(Constants.IGN_COPY_EXISTING_DATA) || args[3].startsWith(Constants.IGN_COPY_EXISTING_DATA)){
    		if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType))
    			copyExisting = args[3].substring(Constants.IGN_COPY_EXISTING_DATA.length());
    		else
    			copyExisting = args[2].substring(Constants.IGN_COPY_EXISTING_DATA.length());
    	}

    	if(!(Constants.NEXT_WEEK.equalsIgnoreCase(weekType) || Constants.CURRENT_WEEK.equalsIgnoreCase(weekType) || Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) || Constants.LAST_WEEK.equalsIgnoreCase(weekType))){
			logger.info("Invalid Argument, args[1] should be lastweek/currentweek/nextweek/specificweek");
        	System.exit(-1);
		}
		
		if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) && 
				((mode == null && copyExisting == null && args.length != 3) 
						|| (mode != null && copyExisting != null && args.length != 5)
						|| (mode == null && copyExisting != null && args.length != 4)
						|| (mode != null && copyExisting == null && args.length != 4))){
			logger.info("Invalid Arguments, args[0] should be relative path of price file, " +
					" args[1] lastweek/currentweek/nextweek/specificweek, args[2] data for a specific week");
        	System.exit(-1);
		}
		
		String dateStr = null;
		if(Constants.CURRENT_WEEK.equalsIgnoreCase(weekType)){
			dateStr = DateUtil.getWeekStartDate(0);
		}else if(Constants.NEXT_WEEK.equalsIgnoreCase(weekType)){
			dateStr = DateUtil.getDateFromCurrentDate(7);
		}else if(Constants.LAST_WEEK.equalsIgnoreCase(weekType)){
			dateStr = DateUtil.getWeekStartDate(1);
		}else if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType)){
			try{
				dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
			}catch(GeneralException exception){
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}
		if(copyExisting.equals(Constants.VALUE_TRUE)){
			isCopyEnabled = false;
		}
		
		for(String arg: args){
			if(arg.equalsIgnoreCase(ARG_IGN_STR_NOT_IN_DB)){
				ignoreStoresNotInDB = arg.substring(ARG_IGN_STR_NOT_IN_DB.length());
			}
		}
		
		dataload.setupObjects(dateStr, mode);
        dataload.processPriceFile(args[0]);
    }
    
    /**
     * Sets up class level objects for the given date
     * @param dateStr
     */
    public void setupObjects(String dateStr, String mode){
    	try {
    		
    		logger.info("******* Setting up required objects *******");
    		String tempCommitCount = PropertyManager.getProperty("DATALOAD.COMMITRECOUNT", "1000");
            commitRecCount=Integer.parseInt(tempCommitCount);
            
            String minPrice = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.MIN_ITEM_PRICE", "0.10");
			minItemPrice = Float.parseFloat(minPrice);
			
    		populateCalendarId(dateStr);
    		
    		costDAO = new CostDAO();
    		retailPriceDAO = new RetailPriceDAO();
			
	    	// Retrieve Subscriber Chain Id
			chainId = retailPriceDAO.getChainId(conn); 
			logger.info("setupObjects() - Subscriber Chain Id - " + chainId);
			
			// Retrieve all stores and its corresponding zone#
			storeInfoMap = costDAO.getStoreZoneInfo(conn, chainId);
			logger.info("setupObjects() - No of store available - " + storeInfoMap.size());
			
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
			
			
			// Populate a map with zone# as key and list of corresponding stores as value
			CostDataLoad costDataLoad = new CostDataLoad();
			costDataLoad.castZoneNumbers(storeInfoMap);
			zoneStoreMap = costDataLoad.getZoneMapping(storeInfoMap);
			logger.info("setupObjects() - No of zones available - " + zoneStoreMap.size());
			
			// Delete from retail_price_info for the week being processed
			if(mode != null && isCopyEnabled)
				retailPriceDAO.deleteRetailPriceData(conn, calendarId);
			
			if(mode != null && mode.equals(Constants.DATA_LOAD_DELTA) && isCopyEnabled){
				// Insert previous week retail price value
				retailPriceDAO.setupRetailPriceData(conn, calendarId, prevCalendarId);
				 PristineDBUtil.commitTransaction(conn, "Retail Price Data Setup");
			}
			storeItemMapService = new StoreItemMapService(storeIdsMap, retailPriceZone, storeInfoMap);
			logger.info("******* Objects setup is done *******");
    	}catch(GeneralException ge){
			logger.error("Error in setting up objects", ge);
			return;
    	}
    }
    
    /**
     * Sets input week's calendar id and its previous week's calendar id
     * @param weekStartDate			Input Date
     * @throws GeneralException
     */
    public void populateCalendarId(String weekStartDate) throws GeneralException{
    	conn = getOracleConnection();
    	
    	RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		calendarId = calendarDTO.getCalendarId();	
		startDate = calendarDTO.getStartDate();
		
		String prevWeekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate),1);
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate, Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();
		logger.info("Previous Week Calendar Id - " + calendarDTO.getCalendarId());
		
		prevCalendarId = calendarDTO.getCalendarId();	
    }
    
    /**
     * Returns HashMap containing Price data fields as key and
     * its start and end position in the input file.
     * @return		LinkedHashMap
     */
    public static LinkedHashMap getPrice_Field() {
        if (PRICE_FIELD == null) {
        	PRICE_FIELD = new LinkedHashMap();
        	PRICE_FIELD.put("recordType", "0-1");
        	PRICE_FIELD.put("vendorNo", "1-7");
            PRICE_FIELD.put("itemNo", "7-13");
            PRICE_FIELD.put("upc", "13-27");
            PRICE_FIELD.put("sourceCode", "27-28");
            PRICE_FIELD.put("zone", "28-32");
            PRICE_FIELD.put("retailEffDate", "32-38");
            PRICE_FIELD.put("strCurrRetail", "38-45");
            PRICE_FIELD.put("rtlQuanity", "45-47");
        }
    
        return PRICE_FIELD;
    }
    
    /**
     * Processes records in price file
     * @param relativePath				Relative path of price file
     */
    private void processPriceFile(String relativePath){
		ArrayList<String> fileList = null;
		
		//get zip files
		ArrayList<String> zipFileList = null;
		
		logger.info( "Setting up Retail Price Information ");
		String zipFilePath = getRootPath() + "/" + relativePath;
		
    	try {
    		zipFileList = getZipFiles(relativePath);
    	}catch(GeneralException ge){
			logger.error("Error in setting up objects", ge);
			return;
    	}

		//Start with -1 so that if any regular files are present, they are processed first
		int curZipFileCount = -1;
		boolean processZipFile = false;
		boolean commit = true;
		do {
		    try{      
		    	
				if( processZipFile)
					PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

				fileList = getFiles(relativePath);
				for (int i = 0; i < fileList.size(); i++) {
				    String files = fileList.get(i);
				    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
				    
				    // parse text file
				    parseTextFile(PriceAndCostDTO.class, files, getPrice_Field(), stopCount);				
				    

					
					if(ignoredRecords.size() > 0){
						//Save all ignored records
						logger.error("Some records were missed while processing price file. "
								+ "Please check IGNORED_PRICE_RECORDS table for calendar_id - " + calendarId);
						logger.error("Reasons for skipped records can be 1. Item is not available in ITEM_LOOKUP \t "
								+ " 2. Invalid retails");
						retailPriceDAO.saveIgnoredData(conn, ignoredRecords);
						//Reuse list for next file.
						ignoredRecords.clear();
					}
					logger.info("No of price records processed - " + priceRecordCount);
				    PristineDBUtil.commitTransaction(conn, "Retail Price Data Setup");
				    PristineDBUtil.close(conn);
				}
			} catch (GeneralException ex) {
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
		}
		while(curZipFileCount < zipFileList.size());
	    if(!levelIdNotFound.isEmpty()){
	    	List<String> skippedStores = new ArrayList<String>(levelIdNotFound);
			logger.error("Level id not found for list of Stores: "
					+ PRCommonUtil.getCommaSeperatedStringFromStrArray(skippedStores));
	    }
		logger.info("Retail Price Data Load successfully completed");

		return;
	
    }

	public void processRecords(List listObject) throws GeneralException {
		List<PriceAndCostDTO> priceList = (List<PriceAndCostDTO>) listObject;
    	
		if(priceList != null && priceList.size() > 0){
			if(priceList.size() >= commitRecCount){
				if(currentProcPriceList.size() >= Constants.RECS_TOBE_PROCESSED){
					String lastUPC = currentProcPriceList.get(currentProcPriceList.size()-1).getUpc();
					boolean eofFlag = true;
					for(PriceAndCostDTO priceandcostDTO : priceList){
						if(priceandcostDTO.getUpc().equals(lastUPC)){
							currentProcPriceList.add(priceandcostDTO);
						}else{
							if(currentProcPriceList.size() >= Constants.RECS_TOBE_PROCESSED){
								processPriceRecords(currentProcPriceList);
								currentProcPriceList.clear();
								eofFlag = false;
							}
							currentProcPriceList.add(priceandcostDTO);
						}
					}
					if(eofFlag){
						processPriceRecords(currentProcPriceList);
						currentProcPriceList.clear();
					}
				}else{
					currentProcPriceList.addAll(priceList);
				}
			}else{
				currentProcPriceList.addAll(priceList);
				processPriceRecords(currentProcPriceList);
				currentProcPriceList.clear();
			}
		}
	}
	
	/**
	 * Processes Price records
	 * (Populates priceDataMap with ItemDetailKey as key and corresponding list of price records as value)
	 * @param priceList	List of Price records to be processed
	 */
	private void processPriceRecords(List<PriceAndCostDTO> priceList){
		priceDataMap = new HashMap<ItemDetailKey, List<PriceAndCostDTO>>();
		for(PriceAndCostDTO priceDTO : priceList){
			priceRecordCount++;
			String upc = PrestoUtil.castUPC(priceDTO.getUpc(),false);
			priceDTO.setUpc(upc);
			//Changes by Pradep on 06/24/2015.
			String sourceVendor = priceDTO.getVendorNo();
			String itemNo = priceDTO.getItemNo();
			priceDTO.setCalendarId(calendarId);
			String ret_item_code = sourceVendor + itemNo;
			ItemDetailKey itemDetailKey = new ItemDetailKey(upc, ret_item_code);
			//Changes Ends.
			boolean canBeAdded = true;
			if(priceDataMap.get(itemDetailKey) != null){
				List<PriceAndCostDTO> tempList = priceDataMap.get(itemDetailKey);
				
				// Logic to handle duplicates in input file
				for(PriceAndCostDTO tempDTO:tempList){
					if(tempDTO.getSourceCode().equals(priceDTO.getSourceCode()) && tempDTO.getZone().equals(priceDTO.getZone())){
						if(tempDTO.getRecordType().equals(priceDTO.getRecordType())){
							canBeAdded = false;
						}
						// Code changes to give priority to record type A than record type C
						else if(Constants.RECORD_TYPE_ADDED.equals(tempDTO.getRecordType()) && Constants.RECORD_TYPE_UPDATED.equals(priceDTO.getRecordType())){
							canBeAdded = false;
						}else if(Constants.RECORD_TYPE_UPDATED.equals(tempDTO.getRecordType()) && Constants.RECORD_TYPE_ADDED.equals(priceDTO.getRecordType())){
							tempDTO.setRecordType(priceDTO.getRecordType());
							tempDTO.setRetailEffDate(priceDTO.getRetailEffDate());
							tempDTO.setStrCurrRetail(priceDTO.getStrCurrRetail());
							tempDTO.setRtlQuanity(priceDTO.getRtlQuanity());
							canBeAdded = false;
						}
					}
				}
				if(canBeAdded){
					tempList.add(priceDTO);
					priceDataMap.put(itemDetailKey, tempList);
				}
			}else{
				List<PriceAndCostDTO> tempList = new ArrayList<PriceAndCostDTO>();
				tempList.add(priceDTO);
				priceDataMap.put(itemDetailKey, tempList);
			}

			if(priceRecordCount % Constants.RECS_TOBE_PROCESSED == 0)    	
				logger.info("No of price records Processed " + priceRecordCount);
		}
		
		try{
			loadPriceData();
			PristineDBUtil.commitTransaction(conn, "Price Data Setup");
		}catch(GeneralException exception){
			logger.error("Exception in processPriceRecords of PriceDataLoad - " + exception);
		}
	}
	
	private void loadPriceData() throws GeneralException{
		// Retrieve Item Code for UPCs in the priceDataMap populated from input file
		//Changes by Pradep on 06/24/2015.
		//Commented following code as we are considering upc + retailer item code.
		//itemCodeMap = retailPriceDAO.getItemCode(conn, priceDataMap.keySet());
		//New method added.
		//Changes ends.
		logger.info("No of price data records with Item Code - " + itemCodeMap.size());
		
		// Insert/Update/Delete item mapping in store_item_map table
		long startTime = System.currentTimeMillis();
		HashMap<String, Long> vendorInfo = storeItemMapService.setupVendorInfo(conn, priceDataMap);
		storeItemMapService.mergeIntoStoreItemMap(conn, priceDataMap, vendorInfo, itemCodeMap, false,levelIdNotFound);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to merge data into store_item_map - " + (endTime - startTime) + "ms");
		
    	// Process every 1000 UPCs in priceDataMap
		Set<ItemDetailKey> itemSet = priceDataMap.keySet();
		
		if(itemSet.size() > 0)
			try {
				processPriceData(storeInfoMap, zoneStoreMap, itemSet);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("loadPriceData() - Error while processing records - " + e.toString()); 
				e.printStackTrace();
			}
	}
	
    
    /**
     * Populates price and cost information in PriceAndCostDTO
     * @param priceandCostDTO
     * @return
     */
    private boolean populatePrices(PriceAndCostDTO priceandCostDTO) {
    	boolean retVal = true;
    	
    	try{
    		float price = Float.parseFloat(priceandCostDTO.getStrCurrRetail().trim());
    		priceandCostDTO.setCurrRetail(price / 100);
    		
    	}catch (Exception e){
    		logger.error("Populate Prices - JavaException" + ", UPC = + " + priceandCostDTO.getUpc() + ", Store = + " + priceandCostDTO.getZone(), e);
        	retVal = false;
    	}
        return retVal;
    }
    
    /**
	 * Sets up RetailPriceDTO for PriceAndCostDTO passed as input
	 * @param priceCostDTO		PriceAndCostDTO from which RetailPriceDTO needs to be setup
	 * @param retailPriceDTO		RetailPriceDTO to be setup
	 */
    private void setupRetailPriceDTO(PriceAndCostDTO priceCostDTO, RetailPriceDTO retailPriceDTO){
    	try{
    		retailPriceDTO.setRegQty(priceCostDTO.getRtlQuanity());
            retailPriceDTO.setRegPrice(priceCostDTO.getCurrRetail());
            retailPriceDTO.setRegEffectiveDate(formatDate(priceCostDTO.getRetailEffDate()));
            if (retailPriceDTO.getRegQty() <= 1) {
            	retailPriceDTO.setRegMPrice(0);
            } else {
            	retailPriceDTO.setRegMPrice(retailPriceDTO.getRegPrice());
            	retailPriceDTO.setRegPrice(0);
            }
            //Added to avoid clearing of sale price when the loader processes current week files.
            if(isCopyEnabled){
	            retailPriceDTO.setSaleQty(0);
	            retailPriceDTO.setSalePrice(0);
	            retailPriceDTO.setSaleMPrice(0);
            }
         
    	} catch (Exception e) {
        	logger.error("Building Retail Price DTO - JavaException" + ", UPC = + " + priceCostDTO.getUpc() + ", STORE = + " + priceCostDTO.getZone(), e);
        	retailPriceDTO = null;
        }
    }
    
    private String formatDate(String inputDate){
    	String formatDate = "";
        if( inputDate.equals("000000"))
        	formatDate = "";  
        else{
        	formatDate =
        		inputDate.substring(0, 2) + "/" + inputDate.substring(2, 4) + "/"
        		+ "20" + inputDate.substring(4);
        	//logger.debug("EffDate = " + inputDate + " -> " + formatDate);
        }
        return formatDate;

    }
    /**
     * This method process price data to be stored in retail_price_info
     * @param storeZoneInfo
     * @param zoneStoreMap
     * @throws Exception 
     */
    private void processPriceData(HashMap<String, String> storeZoneInfo, HashMap<String, List<String>> zoneStoreMap, Set<ItemDetailKey> itemSet) throws GeneralException, Exception{
    	// Item code List to be processed
    	Set<String> itemCodeSet = new HashSet<String>();
    	for(ItemDetailKey itemDetailKey : itemSet){
    		String itemCode = itemCodeMap.get(itemDetailKey);
    		if(itemCode != null){
    			itemCodeSet.add(itemCode);
    		}
    	}
   
    	// Retrieve item store mapping from store_item_map table for items in itemCodeList
    	long startTime = System.currentTimeMillis();
		HashMap<String, List<String>> itemStoreMapping = costDAO.getStoreItemMap(conn, prevWkStartDate, itemCodeSet,
				"PRICE", false);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
		
		
		// Retrieve item store mapping from store_item_map table for items in itemCodeList
    	startTime = System.currentTimeMillis();
		HashMap<String, List<String>> authorizedItemsMap = costDAO.getStoreItemMap(conn, prevWkStartDate, itemCodeSet,
				"PRICE", true);
		endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
		
		// Retrieve from Retail_Price_Info for items in itemCodeList
    	startTime = System.currentTimeMillis();
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMapForItems = retailPriceDAO.getRetailPriceInfo(conn, itemCodeSet, calendarId, false); 
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken to retrieve items from retail_price_info - " + (endTime - startTime));
    	
		// Unroll previous week's price data for items in itemCodeList
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMapForItems = new HashMap<String, List<RetailPriceDTO>>();
		startTime = System.currentTimeMillis();
		if(priceRolledUpMapForItems != null && priceRolledUpMapForItems.size() > 0){
			//CompDataLoadV2 compDataLoad = new CompDataLoadV2(conn);
			HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = unrollRetailPriceInfoWithStoreItemMap(priceRolledUpMapForItems, storeZoneInfo.keySet(), zoneStoreMap, retailPriceDAO, itemStoreMapping, storeZoneInfo);
				
			for(List<RetailPriceDTO> retailPriceDTOList : unrolledPriceMap.values()){
				for(RetailPriceDTO retailPriceDTO : retailPriceDTOList){
					if(unrolledPriceMapForItems.get(retailPriceDTO.getItemcode()) != null){
						List<RetailPriceDTO> tempList = unrolledPriceMapForItems.get(retailPriceDTO.getItemcode());
						tempList.add(retailPriceDTO);
						unrolledPriceMapForItems.put(retailPriceDTO.getItemcode(), tempList);
					}else{
						List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(retailPriceDTO);
						unrolledPriceMapForItems.put(retailPriceDTO.getItemcode(), tempList);
					}
				}
			}
		}
		endTime = System.currentTimeMillis();
		logger.info("Time taken to unroll price data - " + (endTime - startTime));
		
		// Compare items from input files with unrolled price data from retail_price_info
		HashMap<String, List<RetailPriceDTO>> retailPriceDTOMap = new HashMap<String, List<RetailPriceDTO>>();
		startTime = System.currentTimeMillis();
		for(ItemDetailKey itemDetailKey : itemSet){
			List<String> addedStores = new ArrayList<String>(); // Changes to handle zone level items
			List<RetailPriceDTO> finalRetailPriceList = new ArrayList<RetailPriceDTO>(); 
			String itemCode = itemCodeMap.get(itemDetailKey);
			List<PriceAndCostDTO> priceDataList = priceDataMap.get(itemDetailKey);
    		if(itemCode != null){
    			List<RetailPriceDTO> retailPriceList = unrolledPriceMapForItems.get(itemCode);
    			
    			// Changes to handle items for which we received price at both store and zone level
    			boolean isBothStoreAndZonePresent = false;
    			boolean store = false;
				boolean zone = false;
    			for(PriceAndCostDTO priceAndCostDTO : priceDataList){
    				if(Integer.parseInt(priceAndCostDTO.getSourceCode()) == Constants.ZONE_LEVEL_TYPE_ID){
    					zone = true;
    				}
    				if(Integer.parseInt(priceAndCostDTO.getSourceCode()) == Constants.STORE_LEVEL_TYPE_ID){
    					store = true;
    				}
    			}
    			if(store && zone)
    				isBothStoreAndZonePresent = true;
    			// Changes to handle items for which we received price at both store and zone level - Ends
    			
    			// Process price of an item in a store that is present in both input file and retail_price_info 
    			if(retailPriceList != null && retailPriceList.size() > 0){
	    			for(PriceAndCostDTO priceAndCostDTO : priceDataList){
	    				
	    				for(RetailPriceDTO retailPriceDTO : retailPriceList){
	    					String storeZone = priceAndCostDTO.getZone();
	    					if((!isBothStoreAndZonePresent && ((priceAndCostDTO.getSourceCode().equals(String.valueOf(Constants.STORE_LEVEL_TYPE_ID)) && 
	    							storeZone.equals(retailPriceDTO.getLevelId())) || (priceAndCostDTO.getSourceCode().equals(String.valueOf(Constants.ZONE_LEVEL_TYPE_ID)) &&  
	    							zoneStoreMap.get(storeZone) != null && zoneStoreMap.get(storeZone).contains(retailPriceDTO.getLevelId())))) ||
	    							(isBothStoreAndZonePresent && priceAndCostDTO.getSourceCode().equals(String.valueOf(Constants.STORE_LEVEL_TYPE_ID)) && 
	    							storeZone.equals(retailPriceDTO.getLevelId()))){
	    						if(Constants.RECORD_TYPE_ADDED.equals(priceAndCostDTO.getRecordType()) || Constants.RECORD_TYPE_UPDATED.equals(priceAndCostDTO.getRecordType())){
	    							populatePrices(priceAndCostDTO);
	    							if( priceAndCostDTO.getCurrRetail() <= minItemPrice )
	    		    					continue;
	    							setupRetailPriceDTO(priceAndCostDTO, retailPriceDTO);
	    							retailPriceDTO.setCalendarId(calendarId);
	    							retailPriceDTO.setRetailerItemCodeAsItIs(itemDetailKey.getRetailerItemCode());
	    							finalRetailPriceList.add(retailPriceDTO);
	    							retailPriceDTO.setUpc(itemDetailKey.getUpc());
	    							retailPriceDTO.setProcessedFlag(true);
	    							priceAndCostDTO.setProcessedFlag(true);
	    							addedStores.add(retailPriceDTO.getLevelId()); // Changes to handle zone level items
	    						}else{
	    							
	    							String key = "";
									if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
										if (retailPriceZone.get(
												String.valueOf(Integer.parseInt(retailPriceDTO.getLevelId()))) != null) {
											int zoneId = retailPriceZone
													.get(String.valueOf(Integer.parseInt(retailPriceDTO.getLevelId())));
											key = retailPriceDTO.getLevelTypeId() + Constants.INDEX_DELIMITER + zoneId;
										}
									}else if(retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
										if (storeIdsMap.get(retailPriceDTO.getLevelId()) != null) {
											int storeId = storeIdsMap.get(retailPriceDTO.getLevelId());
											key = retailPriceDTO.getLevelTypeId() + Constants.INDEX_DELIMITER + storeId;
										}
									}
									if(authorizedItemsMap.containsKey(key) && key != ""){
										List<String> authorizedItems = authorizedItemsMap.get(key);
										if(authorizedItems.contains(retailPriceDTO.getItemcode())){
											retailPriceDTO.setCalendarId(calendarId);
											retailPriceDTO.setRetailerItemCodeAsItIs(itemDetailKey.getRetailerItemCode());
											retailPriceDTO.setUpc(itemDetailKey.getUpc());
											finalRetailPriceList.add(retailPriceDTO);
											addedStores.add(retailPriceDTO.getLevelId()); 
										}
									}
	    							retailPriceDTO.setProcessedFlag(true);
	    							priceAndCostDTO.setProcessedFlag(true);
	    						}
	    					}
	    				}
	    			}
    			}
    			
    			// Process price of an item in a store that is present only in input file and not in retail_price_info 
    			for(PriceAndCostDTO priceAndCostDTO : priceDataList){
    					
    				if(!priceAndCostDTO.isProcessedFlag()){
    					// Changes to handle items for which we received price at both store and zone level
    					if(isBothStoreAndZonePresent && 
    							(Constants.RECORD_TYPE_ADDED.equals(priceAndCostDTO.getRecordType()) 
    							|| Constants.RECORD_TYPE_UPDATED.equals(priceAndCostDTO.getRecordType())) &&
    							Constants.ZONE_LEVEL_TYPE_ID == Integer.parseInt(priceAndCostDTO.getSourceCode())){
    						String zoneNo = priceAndCostDTO.getZone();
    						if(zoneNo != null && zoneStoreMap.get(zoneNo) != null){
    							for(String storeNo : zoneStoreMap.get(zoneNo)){
    								if(!addedStores.contains(storeNo)){
    									RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
    		    						retailPriceDTO.setCalendarId(calendarId);
    		    						retailPriceDTO.setUpc(priceAndCostDTO.getUpc());
    		    						retailPriceDTO.setRetailerItemCodeAsItIs(itemDetailKey.getRetailerItemCode());
    		    						retailPriceDTO.setItemcode(itemCodeMap.get(itemDetailKey));
    		    						retailPriceDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
    		    						retailPriceDTO.setLevelId(storeNo);
    		    						retailPriceDTO.setUpc(itemDetailKey.getUpc());
    		    						populatePrices(priceAndCostDTO);
    		    						if( priceAndCostDTO.getCurrRetail() <= minItemPrice ){
    		    							ignoredRecords.add(priceAndCostDTO);
    		    	    					continue;
    		    						}
    		    						setupRetailPriceDTO(priceAndCostDTO, retailPriceDTO);
    		    						finalRetailPriceList.add(retailPriceDTO);
    		    						if(retailPriceList != null && retailPriceList.size() > 0)
	    		    						for(RetailPriceDTO retailPriceDTOTemp : retailPriceList){
	    		    							if(retailPriceDTOTemp.getLevelTypeId() ==  Constants.STORE_LEVEL_TYPE_ID && storeNo.equals(retailPriceDTOTemp.getLevelId()))
	    		    								retailPriceDTOTemp.setProcessedFlag(true);
	    		    						}
    								}
    							}
    						}
    					}
    					// Changes to handle items for which we received price at both store and zone level - Ends
    					else{
	    					if(Constants.RECORD_TYPE_ADDED.equals(priceAndCostDTO.getRecordType())
	    							|| Constants.RECORD_TYPE_UPDATED.equals(priceAndCostDTO.getRecordType())){
	    						RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
	    						retailPriceDTO.setCalendarId(calendarId);
	    						retailPriceDTO.setUpc(priceAndCostDTO.getUpc());
	    						retailPriceDTO.setItemcode(itemCodeMap.get(itemDetailKey));
	    						retailPriceDTO.setRetailerItemCodeAsItIs(itemDetailKey.getRetailerItemCode());
	    						retailPriceDTO.setLevelTypeId(Integer.parseInt(priceAndCostDTO.getSourceCode()));
	    						retailPriceDTO.setLevelId(priceAndCostDTO.getZone());
	    						retailPriceDTO.setUpc(itemDetailKey.getUpc());
	    						populatePrices(priceAndCostDTO);
	    						if( priceAndCostDTO.getCurrRetail() <= minItemPrice ){
	    							ignoredRecords.add(priceAndCostDTO);
	    	    					continue;
	    						}
	    						setupRetailPriceDTO(priceAndCostDTO, retailPriceDTO);
	    						finalRetailPriceList.add(retailPriceDTO);
	    						if(retailPriceList != null && retailPriceList.size() > 0)
		    						for(RetailPriceDTO retailPriceDTOTemp : retailPriceList){
		    							if(retailPriceDTOTemp.getLevelTypeId() ==  Constants.STORE_LEVEL_TYPE_ID && priceAndCostDTO.getZone().equals(retailPriceDTOTemp.getLevelId()))
		    								retailPriceDTOTemp.setProcessedFlag(true);
		    						}
	    					}
    					}
    				}
    			}
    		}
    		else{
    			//Add all the records for which the Presto item code is not found
    			ignoredRecords.addAll(priceDataList);
    		}
    		
    		if(retailPriceDTOMap.get(itemCode) != null){
    			List<RetailPriceDTO> tempList = retailPriceDTOMap.get(itemCode);
    			tempList.addAll(finalRetailPriceList);
    			retailPriceDTOMap.put(itemCode, tempList);
    		}
    		else{
    			retailPriceDTOMap.put(itemCode, finalRetailPriceList);
    		}
		}
		
		// Process price of an item in a store that is present only in retail_price_info and not in input file
		for(String itemCode : priceRolledUpMapForItems.keySet()){
			List<RetailPriceDTO> finalRetailPriceList = new ArrayList<RetailPriceDTO>(); 
			List<RetailPriceDTO> retailPriceList = unrolledPriceMapForItems.get(itemCode);
			if(retailPriceList != null && retailPriceList.size() > 0){
    			for(RetailPriceDTO retailPriceDTO : retailPriceList){
    				retailPriceDTO.setCalendarId(calendarId);
    				if(!retailPriceDTO.isProcessedFlag()){
    					finalRetailPriceList.add(retailPriceDTO);
    				}
    			}
			}
			if(retailPriceDTOMap.get(itemCode) != null){
				List<RetailPriceDTO> tempList = retailPriceDTOMap.get(itemCode);
				tempList.addAll(finalRetailPriceList);
				retailPriceDTOMap.put(itemCode, tempList);
			}else{
				retailPriceDTOMap.put(itemCode, finalRetailPriceList);
			}
			
		}
		endTime = System.currentTimeMillis();
		logger.info("Time taken for processing data - " + (endTime - startTime));
		
		priceRolledUpMapForItems = null;
		unrolledPriceMapForItems = null;
		
		// Rollup Price Info
		RetailPriceSetup retailPriceSetup = new RetailPriceSetup();
		startTime = System.currentTimeMillis();
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = priceRollUp(retailPriceDTOMap, itemCodeMap, storeInfoMap, new HashSet<String>(), chainId);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for processing data - " + (endTime - startTime));
		
		// Delete existing data
		startTime = System.currentTimeMillis();
		List<String> itemCodeList = new ArrayList<String>(itemCodeSet);
		retailPriceDAO.deleteRetailPriceData(conn, calendarId, itemCodeList);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for deleting data from retail_price_info - " + (endTime - startTime));
		
		// Insert into retail_price_info table
		//For handling duplicates the following set is added.
		Set<DuplicateKey> duplicateSet = new HashSet<DuplicateKey>();
		List<RetailPriceDTO> insertList = new ArrayList<RetailPriceDTO>();
		for(List<RetailPriceDTO> priceDTOList : priceRolledUpMap.values()){
			for(RetailPriceDTO retailPriceDTO :  priceDTOList){
				DuplicateKey duplicateKey = new DuplicateKey(retailPriceDTO.getItemcode(), retailPriceDTO.getLevelId(), retailPriceDTO.getLevelTypeId(), retailPriceDTO.getCalendarId());
				if(!duplicateSet.contains(duplicateKey)){
					duplicateSet.add(duplicateKey);
					insertList.add(retailPriceDTO);
				}
			}
		}
		//Update proper location id for all the objects
		updateLocationId(insertList);
		
		startTime = System.currentTimeMillis();
		retailPriceDAO.saveRetailPriceData(conn, insertList);
		endTime = System.currentTimeMillis();
		logger.info("Time taken for inserting data into retail_price_info - " + (endTime - startTime));
	}
    
  
    /**
     * Returns unit price of items for a week
     * @param connection		Database Connection
     * @param storeNumOrZoneNum			Competitor Store Number
     * @param itemCodeList		List of	Item Code
     * @param dayCalendarId		Calendar Id of a day in a week 
     * @return HashMap containing item code as key and RetailPriceDTO as value
     */
    public HashMap<String, RetailPriceDTO> getRetailPrice(Connection connection, String storeNumOrZoneNum, 
    		List<String> itemCodeList, int dayCalendarId, 
    		HashMap<String, HashMap<String, List<String>>> itemStoreMapping, int levelOfPrice) throws GeneralException{
    	logger.debug("Inside getRetailPrice() of PriceDataLoad"); 
    	
    	HashMap<String, RetailPriceDTO> priceDataMap = new HashMap<String, RetailPriceDTO>();
    	RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
    	CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(connection);
    	RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
    	
    	RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarIdForWeek(connection, dayCalendarId);
    	int calId = calendarDTO.getCalendarId();
    	List<Integer> historyCalendarList = getCalendarIdHistory(connection, calendarDTO.getStartDate());
    	
    	// Retrieve Store Information
    	if(chainId == null){
    		chainId = retailPriceDAO.getChainId(connection);
    	}
    	StoreDTO storeInfo = null;
    	String zoneNumberLength = PropertyManager.getProperty("ZONENUM_LENGTH");
		if(levelOfPrice == Constants.STORE_LEVEL_TYPE_ID){
			storeInfo = compDataDAO.getStoreInfo(chainId, storeNumOrZoneNum);
			// Assuming this is a store with price_zone_id as null
			if (storeInfo == null) {
				storeInfo = new StoreDTO();
				storeInfo.strNum = storeNumOrZoneNum;
			} else {
				
				if(Integer.parseInt(zoneNumberLength) != -1) {
					storeInfo.zoneNum = PrestoUtil.castZoneNumber(storeInfo.zoneNum);
				}else {
					storeInfo.zoneNum = storeInfo.zoneNum;
				}
				
			}
		}else if(levelOfPrice == Constants.ZONE_LEVEL_TYPE_ID){
			storeInfo = new StoreDTO();
			if(Integer.parseInt(zoneNumberLength) != -1) {
				storeInfo.zoneNum = PrestoUtil.castZoneNumber(storeNumOrZoneNum);
			}else {
				storeInfo.zoneNum = storeNumOrZoneNum;
			}
		} else if (levelOfPrice == Constants.CHAIN_LEVEL_TYPE_ID) {
			storeInfo = new StoreDTO();
			storeInfo.chainId = Integer.parseInt(chainId);
			storeInfo.strNum = storeNumOrZoneNum;
		}
    	
    	if(calId <= 0){
    		logger.error("Error computing calendar id for the week");
    	}
    	logger.debug("Calendar Id for the week - " + calId);
    	String key = calId + "_";
    	logger.debug("No of items in input - " + itemCodeList.size());
    	
	    if(chainId == null){
	    	chainId = retailPriceDAO.getChainId(connection);
	    }
	    	
		Set<String> storeNumbers = new HashSet<String>();
		storeNumbers.add(storeNumOrZoneNum);
		
		
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = new HashMap<String, List<RetailPriceDTO>>();
		
		List<String> itemCodeNotInCache = new ArrayList<String>();
    	for(String itemCode : itemCodeList){
    		if(retailPriceCache.get(key+itemCode) != null){
    			priceRolledUpMap.put(itemCode, retailPriceCache.get(key+itemCode));
    		}else{
    			itemCodeNotInCache.add(itemCode);
    		}
    	}
    	logger.debug("No of items not in cache - " + itemCodeNotInCache.size());
    	
    	if(itemCodeNotInCache.size() > 0){
	    	Set<String> itemCodeSet = new HashSet<String>(itemCodeNotInCache);
	    	
	    	HashMap<String, List<RetailPriceDTO>> priceRolledUpMap1 = retailPriceDAO.getRetailPriceInfo(connection, itemCodeSet, calId, false);
	    	for(Map.Entry<String, List<RetailPriceDTO>> entry : priceRolledUpMap1.entrySet()){
	    		retailPriceCache.put(key+entry.getKey(), entry.getValue());
	    	}
			priceRolledUpMap.putAll(priceRolledUpMap1);
			
			Set<String> itemsNotPresent = new HashSet<String>(); 
			for(String itemCode : itemCodeSet){
				if(priceRolledUpMap.get(itemCode) == null)
					itemsNotPresent.add(itemCode);
			}
			
			if(itemsNotPresent.size() > 0){
				HashMap<String, List<RetailPriceDTO>> priceRolledUpMap2 = retailPriceDAO.getRetailPriceInfoHistory(connection, itemsNotPresent, historyCalendarList);
				for(Map.Entry<String, List<RetailPriceDTO>> entry : priceRolledUpMap2.entrySet()){
		    		retailPriceCache.put(key+entry.getKey(), entry.getValue());
		    	}
				priceRolledUpMap.putAll(priceRolledUpMap2);
			}
    	}
    	
    	HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = null;
		
    	boolean useStoreItemMap = Boolean.parseBoolean(PropertyManager.
				getProperty("USE_STORE_ITEM_MAP_FOR_UNROLLING", "FALSE"));
		if(useStoreItemMap && Constants.STORE_LEVEL_TYPE_ID == levelOfPrice){
			PriceAndCostService priceAndCostService = new PriceAndCostService();
			unrolledPriceMap = priceAndCostService.unrollAndFindGivenStorePrice(connection, priceRolledUpMap,
					storeInfo, priceRolledUpMap.keySet(), itemStoreMapping);
		}else{
			
			if(levelOfPrice == Constants.ZONE_LEVEL_TYPE_ID){
				
				
				unrolledPriceMap = unrollRetailPriceInfoForZone(priceRolledUpMap, storeInfo);
				logger.debug("# in unrollmap: " + unrolledPriceMap.size());
				
			}else if(levelOfPrice == Constants.STORE_LEVEL_TYPE_ID){
			
				unrolledPriceMap = unrollRetailPriceInfo(priceRolledUpMap, storeInfo);
				
			}else if(levelOfPrice == Constants.CHAIN_LEVEL_TYPE_ID){
			
				unrolledPriceMap = unrollRetailPriceInfo(priceRolledUpMap, storeInfo);
				
			}
			
				
		}

		if(unrolledPriceMap != null && unrolledPriceMap.size() > 0){
			List<RetailPriceDTO> unrolledPriceList = unrolledPriceMap.get(storeNumOrZoneNum);
			for(RetailPriceDTO retailPriceDTO : unrolledPriceList){
				boolean isSaleDateCondnMet = false;
				// Check if week end date falls within sale start and end date
				if(retailPriceDTO.getSalePrice() > 0 || retailPriceDTO.getSaleQty() > 1){
					if(retailPriceDTO.getSaleStartDate() != null && retailPriceDTO.getSaleEndDate() != null){
						if(PrestoUtil.compareTo(DateUtil.toDate(calendarDTO.getStartDate()), DateUtil.toDate(retailPriceDTO.getSaleStartDate())) >= 0
							&& PrestoUtil.compareTo(DateUtil.toDate(calendarDTO.getEndDate()), DateUtil.toDate(retailPriceDTO.getSaleEndDate())) <= 0){
								priceDataMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
								isSaleDateCondnMet = true;
								//unrolledPriceDataMap.put(key+retailPriceDTO.getItemcode(), retailPriceDTO);
						}
					}else{
						priceDataMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
						isSaleDateCondnMet = true;
					}
				}
				
				if(!isSaleDateCondnMet){
					retailPriceDTO.setSalePrice(0);
					retailPriceDTO.setSaleMPrice(0);
					retailPriceDTO.setSaleMPack(0);
					retailPriceDTO.setSaleQty(0);
					priceDataMap.put(retailPriceDTO.getItemcode(), retailPriceDTO);
					//unrolledPriceDataMap.put(key+retailPriceDTO.getItemcode(), retailPriceDTO);
				}
			}
			logger.debug("No of items with price - " + priceDataMap.size());
			return priceDataMap;
		}else{
			logger.debug("No price data available for the store - " + storeNumOrZoneNum + " for calendar id " + calId);
			return priceDataMap;
		}	
    }
    
    /**
	 * This method unrolls price data from the rolled up price information passed as input
	 * @param priceRolledUpMap	Map containing rolled up price information
	 * @param storeInfo			Store Information
	 * @return	Map containing unrolled price data for items
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfo(HashMap<String, List<RetailPriceDTO>> priceRolledUpMap, StoreDTO storeInfo) throws GeneralException{
		logger.debug("Inside unrollRetailPriceInfo() of PriceDataLoad");
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		CompDataLoadV2 weeklyDataload = new CompDataLoadV2();
		
				
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			RetailPriceDTO chainLevelData = new RetailPriceDTO();
			RetailPriceDTO zoneLevelData = new RetailPriceDTO();
			boolean isChainLevelPresent = false;
			boolean isPopulated = false;
			
	
			for(RetailPriceDTO retailPriceDTO:entry.getValue()){
				if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					isChainLevelPresent = true;
					chainLevelData.copy(retailPriceDTO);
					
				}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					
					if(!isPopulated){
						
						zoneLevelData.copy(retailPriceDTO);
						// Unroll cost for zone level data
						if(retailPriceDTO.getLevelId().equals(storeInfo.zoneNum) || retailPriceDTO.getLevelId().equals(storeInfo.dept1ZoneNum) ||
								retailPriceDTO.getLevelId().equals(storeInfo.dept2ZoneNum) || retailPriceDTO.getLevelId().equals(storeInfo.dept3ZoneNum)){
							zoneLevelData.setLevelId(storeInfo.strNum);
							weeklyDataload.populateMap(unrolledPriceMap, zoneLevelData);
							isPopulated = true;
						}
					}
				}else{
					
					if(!isPopulated){
						if(storeInfo.strNum.equals(retailPriceDTO.getLevelId())){
							weeklyDataload.populateMap(unrolledPriceMap, retailPriceDTO);
							isPopulated = true;
						}
					}
				}
			}
			
			// Unroll cost for chain level data
			if(isChainLevelPresent){
				if(!isPopulated){
					chainLevelData.setLevelId(storeInfo.strNum);
					
					weeklyDataload.populateMap(unrolledPriceMap, chainLevelData);
				}
			}
		}
		logger.debug("# in unrolledPriceMap: " + unrolledPriceMap.size());
		return unrolledPriceMap;
	}

	/**
	 * This method unrolls price data from the rolled up price information passed as input
	 * @param priceRolledUpMap	Map containing rolled up price information
	 * @param storeInfo			Store Information
	 * @return	Map containing unrolled price data for items
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfoForZone(HashMap<String, List<RetailPriceDTO>> priceRolledUpMap, 
			StoreDTO storeInfo) throws GeneralException{
		logger.debug("Inside unrollRetailPriceInfo() of PriceDataLoad");
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		CompDataLoadV2 weeklyDataload = new CompDataLoadV2();
		
		
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			RetailPriceDTO chainLevelData = new RetailPriceDTO();
			RetailPriceDTO zoneLevelData = new RetailPriceDTO();
			boolean isChainLevelPresent = false;
			boolean isPopulated = false;
			
			for(RetailPriceDTO retailPriceDTO:entry.getValue()){
				
				if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					
					isChainLevelPresent = true;
					chainLevelData.copy(retailPriceDTO);
				}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					
					if(!isPopulated){
						
						zoneLevelData.copy(retailPriceDTO);
						// Unroll cost for zone level data
						if(retailPriceDTO.getLevelId().equals(storeInfo.zoneNum)){
							zoneLevelData.setLevelId(storeInfo.zoneNum);
							weeklyDataload.populateMap(unrolledPriceMap, zoneLevelData);
							isPopulated = true;
						}
					}
				}/*else{
					if(!isPopulated){
						if(storeInfo.strNum.equals(retailPriceDTO.getLevelId())){
							weeklyDataload.populateMap(unrolledPriceMap, retailPriceDTO);
							isPopulated = true;
						}
					}
				}*/
			}
			
			// Unroll cost for chain level data
			if(isChainLevelPresent){
				
				if(!isPopulated){
					
					chainLevelData.setLevelId(storeInfo.zoneNum);
					weeklyDataload.populateMap(unrolledPriceMap, chainLevelData);
				}
			}
		}
		
		return unrolledPriceMap;
	}

	
	
	
	
	
	
	/**
	 * This method set calendar id history in the input list
	 * @param weekStartDate
	 * @param historyCalendarList
	 */
	private List<Integer> getCalendarIdHistory(Connection conn, String weekStartDate) throws GeneralException{
		List<Integer> historyCalendarList = new ArrayList<Integer>();
		String historyStartDateStr = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 4);
		Date historyStartDate = DateUtil.toDate(historyStartDateStr);
		
		String historyEndDateStr = DateUtil.getWeekEndDate(DateUtil.toDate(weekStartDate));
		Date historyEndDate = DateUtil.toDate(historyEndDateStr);
		
		RetailCalendarDAO calDAO = new RetailCalendarDAO();
		List<RetailCalendarDTO> retailCalDTOList = calDAO.RowTypeBasedCalendarList(conn, historyStartDate, historyEndDate, Constants.CALENDAR_WEEK);
		for(RetailCalendarDTO calendarDTO : retailCalDTOList){
			historyCalendarList.add(calendarDTO.getCalendarId());
		}
		return historyCalendarList;
	}
	
	
	/**
	 * Method added for unrolling the data from retail price info based on comp str id and price zone id.
	 * 
	 * This method unrolls data from retail price info
	 * @param priceRolledUpMap		Map containing item code as key and list of rolled up price from retail price info as value
	 * @param storeNumbers			Set of stores available for Price Index
	 * @param zoneStoreMap			Map containing zone number as key and store number as value
	 * @return HashMap containing store number as key and list of unrolled retail price info as value
	 */
	public HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfoWithStoreItemMap(HashMap<String, List<RetailPriceDTO>>priceRolledUpMap, Set<String> storeNumbers, HashMap<String, List<String>> zoneStoreMap,
																		RetailPriceDAO retailPriceDAO, HashMap<String, List<String>> storeItemMap, HashMap<String, String> storeNumberMap) throws GeneralException{
		logger.debug("Inside unrollRetailPriceInfo() of CompDataLoadV2");
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		
		RetailPriceDTO chainLevelData = null;
		boolean isChainLevelPresent = false;
		RetailPriceDTO zoneLevelData = null;
		Set<String> unrolledStores = null;
		HashMap<String,List<String>> deptZoneMap = retailPriceDAO.getDeptZoneMap(conn, chainId);
		HashMap<String,List<String>> storeDeptZoneMap = retailPriceDAO.getStoreDeptZoneMap(conn, chainId);
		
		Set<ItemStoreKey> itemsBasedOnStoreOrZone = new HashSet<ItemStoreKey>();
		storeItemMap.forEach((key, value)->{
			value.forEach(itemCode->{
				ItemStoreKey itemKey = new ItemStoreKey(key, itemCode);
				itemsBasedOnStoreOrZone.add(itemKey);
			});
			
		});
		
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			unrolledStores = new HashSet<String>();
			isChainLevelPresent = false;
			RetailPriceDTO chainLevelDTO = null;
			RetailPriceDTO zoneLevelDTO = null;
			for(RetailPriceDTO retailPriceDTO:entry.getValue()){
				if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					isChainLevelPresent = true;
					chainLevelData = retailPriceDTO;
				}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					zoneLevelData = retailPriceDTO;
					// Unroll price for zone level data
					if(!(null == zoneStoreMap.get(retailPriceDTO.getLevelId()))){
						for(String storeNo:zoneStoreMap.get(retailPriceDTO.getLevelId())){
							if(!unrolledStores.contains(storeNo)){
								zoneLevelDTO = new RetailPriceDTO();
								zoneLevelDTO.copy(zoneLevelData);
								zoneLevelDTO.setLevelId(storeNo);
								zoneLevelDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
								String zoneNo = Integer.toString(Integer.parseInt(zoneLevelData.getLevelId()));
								String zoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + retailPriceZone.get(zoneNo);
								String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeIdsMap.get(zoneLevelDTO.getLevelId());
								// Check if this item exists for this store
								boolean isPopulated = false;
								if(storeItemMap.containsKey(zoneKey)){
									ItemStoreKey key = new ItemStoreKey(zoneKey, zoneLevelDTO.getItemcode());
									if(itemsBasedOnStoreOrZone.contains(key)){
										populateMap(unrolledPriceMap, zoneLevelDTO);
										isPopulated = true;
									}
								}

								if(!isPopulated)
									if(storeItemMap.containsKey(storeKey)){
										ItemStoreKey key = new ItemStoreKey(storeKey, zoneLevelDTO.getItemcode());
										if(itemsBasedOnStoreOrZone.contains(key)){
											populateMap(unrolledPriceMap, zoneLevelDTO);
										}
									}
								
								unrolledStores.add(storeNo);
							}
						}
					}else{
						// To handle items priced at dept zone level
						List<String> stores = deptZoneMap.get(retailPriceDTO.getLevelId());
						if(stores != null && stores.size() > 0){
							for(String store:stores){
								if(!unrolledStores.contains(store)){
									zoneLevelDTO = new RetailPriceDTO();
									zoneLevelDTO.copy(zoneLevelData);
									zoneLevelDTO.setLevelId(store);
									zoneLevelDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
									String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeIdsMap.get(zoneLevelDTO.getLevelId());
									
									
									if(storeItemMap.containsKey(storeKey)){
										ItemStoreKey key = new ItemStoreKey(storeKey, zoneLevelDTO.getItemcode());
										if(itemsBasedOnStoreOrZone.contains(key)){
											populateMap(unrolledPriceMap, zoneLevelDTO);
										}
									}
									
									unrolledStores.add(store);
								}
							}
						}
					}
				}else{
					if(storeNumbers.contains(retailPriceDTO.getLevelId())){
						populateMap(unrolledPriceMap, retailPriceDTO);
						unrolledStores.add(retailPriceDTO.getLevelId());
					}
				}
			}
			
			// Unroll price for chain level data
			if(isChainLevelPresent){
				for(String storeNo:storeNumbers){
					if(!unrolledStores.contains(storeNo)){
						chainLevelDTO = new RetailPriceDTO();
						chainLevelDTO.copy(chainLevelData); 
						chainLevelDTO.setLevelId(storeNo);
						chainLevelDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
						String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeIdsMap.get(chainLevelDTO.getLevelId());
						String zoneNo = Integer.toString(Integer.parseInt(storeNumberMap.get(chainLevelDTO.getLevelId())));
						String zoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + retailPriceZone.get(zoneNo).toString();
						// Check if this item exists for this store
						boolean isPopulated = false;
						if(storeItemMap.containsKey(storeKey)){
							ItemStoreKey key = new ItemStoreKey(storeKey, chainLevelDTO.getItemcode());
							if(itemsBasedOnStoreOrZone.contains(key)){	
								populateMap(unrolledPriceMap, chainLevelDTO);
								isPopulated = true;
							}
						}
						
						if(!isPopulated)
							if(storeItemMap.containsKey(zoneKey)){
								ItemStoreKey key = new ItemStoreKey(zoneKey, chainLevelDTO.getItemcode());
								if(itemsBasedOnStoreOrZone.contains(key)){	
									populateMap(unrolledPriceMap, chainLevelDTO);
									isPopulated = true;
								}
							}
						
						// To handle items priced at dept zone level
						if(!isPopulated){
							if(storeDeptZoneMap != null && storeDeptZoneMap.size() > 0){
								List<String> deptZones = storeDeptZoneMap.get(storeNo);
								if(deptZones != null && deptZones.size() > 0){
									for(String deptZone:deptZones){
										String deptZoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + retailPriceZone.get(deptZone).toString();
										if(storeItemMap.containsKey(deptZoneKey)){
											ItemStoreKey key = new ItemStoreKey(deptZoneKey, chainLevelDTO.getItemcode());
											if(itemsBasedOnStoreOrZone.contains(key)){	
												populateMap(unrolledPriceMap, chainLevelDTO);
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		
		return unrolledPriceMap;
	}
	
	/**
	 * This method populates a HashMap with store number as key and a list of its corresponding retailPriceDTO as its value
	 * @param unrolledPriceMap		Map that needs to be populated with with store number as key and a list of its corresponding retailPriceDTO as its value
	 * @param retailPriceDTO		Retail Price DTO that needs to be added to the Map
	 */
	public void populateMap(HashMap<String, List<RetailPriceDTO>> unrolledPriceMap, RetailPriceDTO retailPriceDTO){
		if(unrolledPriceMap.get(retailPriceDTO.getLevelId()) != null){
			List<RetailPriceDTO> tempList = unrolledPriceMap.get(retailPriceDTO.getLevelId());
			tempList.add(retailPriceDTO);
			unrolledPriceMap.put(retailPriceDTO.getLevelId(),tempList);
		}else{
			List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
			tempList.add(retailPriceDTO);
			unrolledPriceMap.put(retailPriceDTO.getLevelId(),tempList);
		}
	}
	
	/**
	 * Performs price roll up logic for Retail Price Data
	 * @return List		Map Containing Item Code as key and list of RetailPriceDTO as value  
	 * 					to be compared against existing database entries
	 */
	public HashMap<String, List<RetailPriceDTO>> priceRollUp(HashMap<String, List<RetailPriceDTO>> retailPriceDataMap, HashMap<ItemDetailKey, String> itemCodeMap, 
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
					ItemDetailKey itemDetailKey = new ItemDetailKey(retailPriceDTO.getUpc(), retailPriceDTO.getRetailerItemCode());	
						if(itemCodeMap.get(itemDetailKey) != null)
							itemCode = String.valueOf(itemCodeMap.get(itemDetailKey));
						else{
							noItemCodeSet.add(retailPriceDTO.getUpc());
							continue;
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
						tempCnt = tempCnt + 1;
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

	
	
	private void updateLocationId(List<RetailPriceDTO> insertList){
		for(RetailPriceDTO retailPriceDTO: insertList){
			//Update chain id for zone level records
			if(retailPriceDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID)
				retailPriceDTO.setLocationId(Integer.parseInt(retailPriceDTO.getLevelId()));
			//Update price zone id from the cache when there is a zone level record 
			else if(retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null 
				if(retailPriceZone.get(String.valueOf(Integer.parseInt(retailPriceDTO.getLevelId()))) != null)
					retailPriceDTO.setLocationId(retailPriceZone.get(String.valueOf(Integer.parseInt(retailPriceDTO.getLevelId()))));
			}
			//Update comp_str_id from the cache when there is a store level record
			else if(retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null
				if(storeIdsMap.get(retailPriceDTO.getLevelId()) != null)
					retailPriceDTO.setLocationId(storeIdsMap.get(retailPriceDTO.getLevelId()));
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
		private PriceDataLoad getOuterType() {
			return PriceDataLoad.this;
		}
		
		
	}
}
