package com.pristine.dataload.tops;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

import java.text.DateFormat;
import java.text.ParseException;

public class BillbackDataLoad extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("BillbackDataLoad");
	private static LinkedHashMap BILLBACK_FIELD = null;
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private Connection conn = null;
	private RetailCostDAO retailCostDAO = null;
	
	HashMap<ItemDetailKey, HashMap<String, List<PriceAndCostDTO>>> billbackDataMap = new HashMap<ItemDetailKey, HashMap<String, List<PriceAndCostDTO>>>();
	List<PriceAndCostDTO> currentProcBillbkList = new ArrayList<PriceAndCostDTO>();
	HashMap<StartAndEndDateKey, List<PriceAndCostDTO>> groupedByStartAndEndDate = new HashMap<StartAndEndDateKey, List<PriceAndCostDTO>>();
	HashMap<String, List<PriceAndCostDTO>> billBackBasedOnStartDate = new HashMap<String, List<PriceAndCostDTO>>();
	
	int billbackRecordCount = 0;
	int duplicateCount = 0;
	Set<String> duplicateUPC = new HashSet<String>();
	int commitRecCount = 1000;
	int calendarId = -1;
	int processingCalendarId = -1;
	HashMap<ItemDetailKey, String> itemCodeMap = null;
	String weekStartDate = null;
	String processingBBStartDate = null;
	private static final String BILLBACK_TYPE_PERIOD = "A";
	private static final String BILLBACK_TYPE_VELOCITY = "V";
	private static final String BILLBACK_TYPE_SCAN = "S";
	  	
	//Changes for populating store id in Store item map table.
  	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
  	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
  	
	private RetailPriceDAO retailPriceDAO = null;
  	private CostDAO costDAO = null;
  	private String chainId;
  	private static boolean ignoreFutureRecords = false;
  	private HashSet<String> upcNotFoundList = new HashSet<String>();
	public BillbackDataLoad(){
		retailCostDAO = new RetailCostDAO();
		conn = getOracleConnection();
	}
	
	 /**
	  * Arguments
	  * args[0]		Relative path of Billback File
	  * args[2]		weektype - lastweek/currentweek/nextweek/specificweek
	  * args[3]		Date if weektype is specific week
	  * @param args
	  */
	 public static void main(String[] args) {
	  
		 BillbackDataLoad dataload = new BillbackDataLoad();
		 PropertyConfigurator.configure("log4j-billback-data-load.properties");
			
	     String weekType = null;
	     for (String arg : args) {
	    	 if (arg.startsWith("IGNORE_FUTURE_RECORDS")) {
	    		 ignoreFutureRecords = Boolean.parseBoolean(arg.substring("IGNORE_FUTURE_RECORDS=".length()));
				} 
	     }
	     if(args.length == 1){
	    	 weekType = Constants.CURRENT_WEEK;
	     }else{
	         weekType = args[1];
	     }
	    	
	     if(!(Constants.NEXT_WEEK.equalsIgnoreCase(weekType) || Constants.CURRENT_WEEK.equalsIgnoreCase(weekType) || Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) || Constants.LAST_WEEK.equalsIgnoreCase(weekType))){
	    	 logger.info("Invalid Argument, args[1] should be lastweek/currentweek/nextweek/specificweek");
	    	 System.exit(-1);
	     }
			
	     if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) && args.length != 3){
	    	 logger.info("Invalid Arguments, args[0] should be relative path of billback file, " +
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
	     dataload.setDate(dateStr);
	     dataload.prepareData();
	     dataload.processBillbackFile(args[0]);
	}
	 
	private void setDate(String dateStr) {
		try{
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, dateStr, Constants.CALENDAR_WEEK);
			logger.info("Calendar Id - " + calendarDTO.getCalendarId());
			this.weekStartDate = dateStr;
			this.calendarId = calendarDTO.getCalendarId();
		}catch(GeneralException e){
			logger.error("Error when setting calendar id " + e);
			System.exit(-1);
		}
	}

	/**
	 * Clears DEAL_COST, DEAL_START_DATE, DEAL_END_DATE.
	 * Copies LEVEL2_COST, LEVEL2_START_DATE, LEVEL2_END_DATE into DEAL_COST, DEAL_START_DATE, DEAL_END_DATE.
	 */
	private void prepareData(){
		try{
			retailCostDAO.clearDealCost(conn, calendarId);
			retailCostDAO.populateDealAsLevel2(conn, calendarId);
		}catch(GeneralException e){
			logger.error("Error when preparing data " + e);
			System.exit(-1);
		}
	}
	
	private void  processBillbackFile(String relativePath ){
		 String tempCommitCount = PropertyManager.getProperty("DATALOAD.COMMITRECOUNT", "1000");
         commitRecCount=Integer.parseInt(tempCommitCount);
         
		 try{      
			 logger.info("Billback Data Load Started ....");
			 
			 
			 	costDAO = new CostDAO();
				retailPriceDAO = new RetailPriceDAO();
			 	// Retrieve Subscriber Chain Id
				chainId = retailPriceDAO.getChainId(conn); 
				logger.info("setupObjects() - Subscriber Chain Id - " + chainId);
				//Changes for populating store id in Store item map table.
				//Added by Pradeep 06-01-2015
				long startTime  = System.currentTimeMillis();
				storeIdsMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
				long endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to retreive store id mapping - " + (endTime - startTime));
				startTime = System.currentTimeMillis();
				retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
				endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to retreive zone id mapping - " + (endTime - startTime));
				//Changes ends.
				
				//Retrieve all items
				//Changes by Pradeep on 06/24/2014.
				startTime  = System.currentTimeMillis();
				ItemDAO itemDAO = new ItemDAO();
				itemCodeMap = itemDAO.getAllItemsFromItemLookupV2(conn);
				endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to cache all items - " + ((endTime - startTime)/1000) + " seconds");

			 //getzip files
			 ArrayList<String> zipFileList = getZipFiles(relativePath);
				
			 //Start with -1 so that if any reqular files are present, they are processed first
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
						 String files = fileList.get(i);
						 int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));   
						 parseTextFile(PriceAndCostDTO.class, files, getBillback_Field(), stopCount);
						 HashMap<String, List<PriceAndCostDTO>> billbackBasedOnStartDate = getBillbackBasedOnStartDate();
						 processBbBasedOnStartDate(billbackBasedOnStartDate);
						 logger.info("No of billback records processed - " + billbackRecordCount);
						 logger.info("No of duplicate records - " + duplicateCount);
						 StringBuffer sb = new StringBuffer();
						 for(String item : duplicateUPC){
							 sb.append(item).append(",");
						 }
						 logger.info("Duplicate UPCs - " + sb);
						 PristineDBUtil.commitTransaction(conn, "Billback Data Setup");
						 // PristineDBUtil.close(conn);
					 }
				 } catch (GeneralException ex) {
					 logger.error("Inner Exception - GeneralException", ex);
					 commit = false;
				 } catch (Exception ex) {
					 logger.error("Inner Exception - JavaException", ex);
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

		 logger.info("Billback Data Load successfully completed");

		 super.performCloseOperation(conn, true);
		 return;
	}
	
	/**
     * Returns HashMap containing Billback fields as key and
     * its start and end position in the input file.
     * @return		LinkedHashMap
     */
    public static LinkedHashMap getBillback_Field() {
        if (BILLBACK_FIELD == null) {
        	BILLBACK_FIELD = new LinkedHashMap();
        	BILLBACK_FIELD.put("vendorNo", "0-6");
            BILLBACK_FIELD.put("itemNo", "6-12");
            BILLBACK_FIELD.put("upc", "12-26");
            BILLBACK_FIELD.put("zone", "26-30");
            // Changes to process new billback feed from TOPS
            BILLBACK_FIELD.put("billbackType", "30-31"); 
            BILLBACK_FIELD.put("bbEffDate", "31-39");
            BILLBACK_FIELD.put("bbEndDate", "39-47");
            BILLBACK_FIELD.put("strBbAmount", "47-54");
            /*BILLBACK_FIELD.put("baEffDate", "53-61");
            BILLBACK_FIELD.put("strBaAmount", "61-68");
            BILLBACK_FIELD.put("bsEffDate", "68-76");
            BILLBACK_FIELD.put("bsEndDate", "76-84");
            BILLBACK_FIELD.put("strBsAmount", "84-91");*/
            BILLBACK_FIELD.put("companyPack", "54-60");
            // Changes to process new billback feed from TOPS
        }
        return BILLBACK_FIELD;
    }
    
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<PriceAndCostDTO> billbkDataList = (List<PriceAndCostDTO>) listobj; 
	
		for(PriceAndCostDTO priceAndCostDTO: billbkDataList){
			List<PriceAndCostDTO> tempBB = new ArrayList<PriceAndCostDTO>();
			StartAndEndDateKey key = new StartAndEndDateKey(priceAndCostDTO.getBbEffDate(), priceAndCostDTO.getBbEndDate());
			if(groupedByStartAndEndDate.containsKey(key)){
				tempBB = groupedByStartAndEndDate.get(key);
			}
			tempBB.add(priceAndCostDTO);
			groupedByStartAndEndDate.put(key, tempBB);
		}
	}
	/**
	 * Processes Billback records(Populates billbackDataMap with UPC as key and corresponding list of billback records as value)
	 * @param billbackDataList List of Billback records to be processed
	 * @param itemToBeDeleted
	 * @param itemsToBeInserted
	 * @throws GeneralException 
	 */
	private void processBillBackRecords(List<PriceAndCostDTO> billbackDataList, HashMap<Integer, List<String>> itemToBeDeleted, 
			List<RetailCostDTO> itemsToBeInserted) throws GeneralException{
		billbackRecordCount=0;
		billbackDataMap = new HashMap<ItemDetailKey, HashMap<String, List<PriceAndCostDTO>>>();
		for(PriceAndCostDTO priceAndCostDTO : billbackDataList){
			String upc = PrestoUtil.castUPC(priceAndCostDTO.getUpc(),false);
			priceAndCostDTO.setUpc(upc);
			//Changes by Pradep on 06/24/2015.
			String sourceVendor = priceAndCostDTO.getVendorNo();
			String itemNo = priceAndCostDTO.getItemNo();
			
			String ret_item_code = sourceVendor + itemNo;
			ItemDetailKey itemDetailKey = new ItemDetailKey(upc, ret_item_code);
			//Changes Ends.
			populatePrices(priceAndCostDTO);
			boolean canBeAdded = true;
			if(billbackDataMap.get(itemDetailKey) != null){
				HashMap<String, List<PriceAndCostDTO>> tempMap = billbackDataMap.get(itemDetailKey);
				
				/*for(PriceAndCostDTO tempDTO:tempList){
					String tempRetItemCode = tempDTO.getVendorNo() + tempDTO.getItemNo();
					String pcRetItemCode = priceAndCostDTO.getVendorNo() + priceAndCostDTO.getItemNo();
					if(tempDTO.getZone().equals(priceAndCostDTO.getZone()) && tempDTO.getBillbackType().equals(priceAndCostDTO.getBillbackType())
							&& !tempRetItemCode.equals(pcRetItemCode)){
						canBeAdded = false;
					}
				}*/
				/*if(canBeAdded){
					tempList.add(priceAndCostDTO);
					billbackDataMap.put(upc, tempList);
				}else{
					duplicateUPC.add(upc);
					duplicateCount++;
				}*/
				List<PriceAndCostDTO> tempList = null;
				if(tempMap.get(priceAndCostDTO.getZone()) != null)
					tempList = tempMap.get(priceAndCostDTO.getZone());
				else
					tempList = new ArrayList<PriceAndCostDTO>();
				tempList.add(priceAndCostDTO);
				tempMap.put(priceAndCostDTO.getZone(), tempList);
				billbackDataMap.put(itemDetailKey, tempMap);
			}else{
				HashMap<String, List<PriceAndCostDTO>> tempMap = new HashMap<String, List<PriceAndCostDTO>>();
				List<PriceAndCostDTO> tempList = new ArrayList<PriceAndCostDTO>();
				tempList.add(priceAndCostDTO);
				tempMap.put(priceAndCostDTO.getZone(), tempList);
				billbackDataMap.put(itemDetailKey, tempMap);
			}
		
			billbackRecordCount++;
			if(billbackRecordCount % 100000 == 0)    	
				logger.info("No of billback records Processed " + billbackRecordCount);
		}
		logger.info("No of billback records Processed " + billbackRecordCount);
		try{
			loadBillbackData(itemToBeDeleted,itemsToBeInserted);
			PristineDBUtil.commitTransaction(conn, "Billback Data Setup");
		}catch(GeneralException exception){
			logger.error("Exception in processBillbackRecords of BillbackDataLoad General- " + exception);
			throw new GeneralException("Exception in processBillbackRecords of BillbackDataLoad - " + exception);
		} catch (Exception e) {
			logger.error("Exception in processBillbackRecords of BillbackDataLoad Exception- " + e.toString());
			throw new GeneralException("Exception in processBillbackRecords of BillbackDataLoad - " + e);
		}
	}
	
	private void loadBillbackData(HashMap<Integer, List<String>> itemToBeDeleted, 
			List<RetailCostDTO> itemsToBeInserted) throws GeneralException, Exception{
		CostDAO costDAO = new CostDAO();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();	
		
		String chainId = retailPriceDAO.getChainId(conn); 
		
		// Retrieve all stores and its corresponding zone#
		HashMap<String, String> storeInfoMap = costDAO.getStoreZoneInfo(conn, chainId);
		logger.info("No of store available - " + storeInfoMap.size());
					
		// Populate a map with zone# as key and list of corresponding stores as value
		CostDataLoad costDataLoad = new CostDataLoad();
		costDataLoad.castZoneNumbers(storeInfoMap);
		HashMap<String, List<String>> zoneStoreMap = costDataLoad.getZoneMapping(storeInfoMap);
		logger.info("No of zones available - " + zoneStoreMap.size());
		
		Set<ItemDetailKey> itemSet = billbackDataMap.keySet();
		/*HashMap<String, String> itemCodeMap = retailPriceDAO.getItemCode(conn, upcSet);*/
		Set<String> itemCodeSet = new HashSet<String>();
		for(ItemDetailKey itemDetailKey: itemSet){
    		String itemCode = itemCodeMap.get(itemDetailKey);
    		if(itemCode != null){
    			itemCodeSet.add(itemCode);
    		}
    		else{
    			upcNotFoundList.add(itemDetailKey.getUpc());
//    			logger.warn("Item code not found for - UPC: " + itemDetailKey.getUpc() + ", itemcode: " + itemDetailKey.getRetailerItemCode());
    		}
    	}
		
		// Retrieve item store mapping from store_item_map table for items in itemCodeList
    	long startTime = System.currentTimeMillis();
		HashMap<String, List<String>> itemStoreMapping = costDAO.getStoreItemMap(conn, weekStartDate, itemCodeSet,"COST", false);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
		
		// Retrieve from Retail_Cost_Info for items in itemCodeList
    	startTime = System.currentTimeMillis();
    	
		HashMap<String, List<RetailCostDTO>> costRolledUpMapForItems = retailCostDAO.getRetailCostInfo(conn, itemCodeSet, calendarId, false); 
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken to retrieve items from retail_cost_info - " + (endTime - startTime));
    	
    	// Unroll previous week's cost data for items in itemCodeList
    	HashMap<String, List<RetailCostDTO>> unrolledCostMapForItems = new HashMap<String, List<RetailCostDTO>>();
    	startTime = System.currentTimeMillis();
    	if(costRolledUpMapForItems != null && costRolledUpMapForItems.size() > 0){
    		/*MovementWeeklyDataLoadV2 compDataLoad = new MovementWeeklyDataLoadV2(conn);
    		HashMap<String, List<RetailCostDTO>> unrolledCostMap = compDataLoad.unrollRetailCostInfo(costRolledUpMapForItems, storeInfoMap.keySet(), zoneStoreMap, retailPriceDAO, itemStoreMapping, storeInfoMap);
    					*/
    		HashMap<String, List<RetailCostDTO>> unrolledCostMap = unrollRetailCostInfo(costRolledUpMapForItems, storeInfoMap.keySet(), zoneStoreMap, retailPriceDAO, itemStoreMapping, storeInfoMap);
    		for(List<RetailCostDTO> retailCostDTOList : unrolledCostMap.values()){
    			for(RetailCostDTO retailCostDTO : retailCostDTOList){
    				if(unrolledCostMapForItems.get(retailCostDTO.getItemcode()) != null){
    					List<RetailCostDTO> tempList = unrolledCostMapForItems.get(retailCostDTO.getItemcode());
    					tempList.add(retailCostDTO);
    					unrolledCostMapForItems.put(retailCostDTO.getItemcode(), tempList);
    				}else{
    					List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
    					tempList.add(retailCostDTO);
    					unrolledCostMapForItems.put(retailCostDTO.getItemcode(), tempList);
    				}
    			}
    		}
    	}
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken to unroll cost data - " + (endTime - startTime));
    	
    	Set<String> strNoSet = storeInfoMap.keySet();
    	HashMap<String, List<RetailCostDTO>> retailCostDTOMap = new HashMap<String, List<RetailCostDTO>>();
    	try{
    		for(ItemDetailKey itemDetailKey : billbackDataMap.keySet()){
        		String itemCode = itemCodeMap.get(itemDetailKey);
        		if(itemCode != null){
        			List<RetailCostDTO> unrolledCostList = unrolledCostMapForItems.get(itemCode);
        			HashMap<String, List<PriceAndCostDTO>> billbackDataList = billbackDataMap.get(itemDetailKey);
        			List<RetailCostDTO> finalRetailCostList = new ArrayList<RetailCostDTO>(); 
        			if(unrolledCostList != null){
        				for(Map.Entry<String, List<PriceAndCostDTO>> entry : billbackDataList.entrySet()){
        					RetailCostDTO costForStore = new RetailCostDTO();
        					boolean dataFound = false;
        					for(PriceAndCostDTO billbackData : entry.getValue()){
        	    				String storeNo = billbackData.getZone();
        	    				populatePrices(billbackData);
        	    				for(RetailCostDTO retailCostDTO : unrolledCostList){
        	    					if(storeNo.equals(retailCostDTO.getLevelId())){
        	    						setupRetailCostDTO(billbackData, retailCostDTO);
        	    						//Code change to include future calendar id By Dinesh(03/07/2017)
        	    						retailCostDTO.setCalendarId(processingCalendarId);
        	    						retailCostDTO.setProcessedFlag(true);
        	    						retailCostDTO.setRetailerItemCode(itemDetailKey.getRetailerItemCode());
        	    						costForStore.copy(retailCostDTO);
        	    						costForStore.setUpc(itemDetailKey.getUpc());
        	    						costForStore.setRetailerItemCode(itemDetailKey.getRetailerItemCode());
        	    						dataFound = true;
        	    					}
        	    				}
        	    			}
        					if(dataFound)
        						finalRetailCostList.add(costForStore);
        				}
        				
        				if (retailCostDTOMap.get(itemCode) != null) {
        					List<RetailCostDTO> tempList = retailCostDTOMap.get(itemCode);
        					tempList.addAll(finalRetailCostList);
        					retailCostDTOMap.put(itemCode, tempList);
        				} else {
        					retailCostDTOMap.put(itemCode, finalRetailCostList);
        				}
    	    		}
        		}
        		else{
        			upcNotFoundList.add(itemDetailKey.getUpc());
//        			logger.warn("Item code not found for - UPC: " + itemDetailKey.getUpc() + ", itemcode: " + itemDetailKey.getRetailerItemCode());
        		}
        	}
    	}catch(Exception e){
    		logger.error("Error", e);
    		throw new Exception("Error while processing billback data processing",e);
    	}
    	
    	// Process price of an item in a store that is present only in retail_cost_info and not in billback file
    	for(String itemCode : costRolledUpMapForItems.keySet()){
    		List<RetailCostDTO> finalRetailCostList = new ArrayList<RetailCostDTO>();
    		List<RetailCostDTO> retailCostList = unrolledCostMapForItems.get(itemCode);
			if(retailCostList != null && retailCostList.size() > 0){
    			for(RetailCostDTO retailCostDTO : retailCostList){
   	   				retailCostDTO.setCalendarId(processingCalendarId);
   	   				if(!retailCostDTO.isProcessedFlag()){
   	   					finalRetailCostList.add(retailCostDTO);
   	   				}
    			}
    		}
    		if(retailCostDTOMap.get(itemCode) != null){
    			List<RetailCostDTO> tempList = retailCostDTOMap.get(itemCode);
    			tempList.addAll(finalRetailCostList);
    			retailCostDTOMap.put(itemCode, tempList);
    		}else{
    			retailCostDTOMap.put(itemCode, finalRetailCostList);
    		}
    				
    	}
    	
    	//Rollup Cost Info
    	//RetailCostSetup retailCostSetup = new RetailCostSetup();
    	 
    	startTime = System.currentTimeMillis();
    	HashMap<String, List<RetailCostDTO>> costRolledUpMap = costDataLoad.costRollUp(retailCostDTOMap, itemCodeMap, storeInfoMap, new HashSet<String>(), chainId);
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken for processing data - " + (endTime - startTime));
    			
    	// Delete existing data
    	startTime = System.currentTimeMillis();
    	List<String> itemCodeList = new ArrayList<String>(itemCodeSet);
    	itemToBeDeleted.put(processingCalendarId, itemCodeList);
//    	retailCostDAO.deleteRetailCostData(conn, processingCalendarId, itemCodeList);
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken for deleting data from retail_cost_info - " + (endTime - startTime));
    			
    	// Insert into retail_price_info table
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
    	itemsToBeInserted.addAll(insertList);
//    	retailCostDAO.saveRetailCostData(conn, insertList);
//    	endTime = System.currentTimeMillis();
//    	logger.info("Time taken for inserting data into retail_cost_info - " + (endTime - startTime));
	}
	
	
	 private ItemDetailKey getItemDetailKey(String itemCode, Set<ItemDetailKey> itemSet){
	    	ItemDetailKey itemDetailKey = null;
	    		for(Map.Entry<ItemDetailKey, String> itemEntry: itemCodeMap.entrySet()){
	    			Object itemObject = itemEntry.getValue();
	    			if(itemObject.equals(itemCode)){
	    				if(itemSet.contains(itemEntry.getKey()))
	    						itemDetailKey = itemEntry.getKey(); 
	    			}
	    		}
	    		return itemDetailKey;
	    		
	    	}
	/**
	 * Sets up RetailCostDTO for PriceAndCostDTO passed as input
	 * @param priceandCostDTO	PriceAndCostDTO from which RetailCostDTO needs to be setup
	 * @param retailCostDTO		RetailCostDTO to be setup
	 * @throws GeneralException 
	 * @throws ParseException 
	 */
    private void setupRetailCostDTO(PriceAndCostDTO priceandCostDTO, RetailCostDTO retailCostDTO) throws GeneralException, ParseException{
    		float smallerCost = retailCostDTO.getListCost();
    		try{
    		if(retailCostDTO.getDealCost() >0.0 && smallerCost > retailCostDTO.getDealCost()){
    			//To avoid loading off invoice if the processing date doesn't fit within off invoice end date or level2 end date is empty then assign deal cosr
    			if(!Constants.EMPTY.equals(retailCostDTO.getLevel2EndDate())&& retailCostDTO.getLevel2EndDate() != null){
    				//Bill back processing week fall within level 2 end date
    				if(DateUtil.toDate(processingBBStartDate).compareTo(DateUtil.toDate(retailCostDTO.getLevel2EndDate())) < 0){
        				smallerCost = retailCostDTO.getDealCost();
    				}
    			}
    			else{
    				smallerCost = retailCostDTO.getDealCost();
    			}
    		}
    		String bbEffDateStr = formatDate(priceandCostDTO.getBbEffDate());
    		String bbEndDateStr = formatDate(priceandCostDTO.getBbEndDate());
    		Date startDate = DateUtil.toDate(processingBBStartDate);
    		Date bbEffDate = null;
    		Date bbEndDate = null;
    		// As per inputs from customer do not use the date in feed
    		if(!bbEffDateStr.equals(Constants.EMPTY) && !bbEndDateStr.equals(Constants.EMPTY)){
    			bbEffDate = DateUtil.toDate(bbEffDateStr, "yyyyMMdd");
    			bbEndDate = DateUtil.toDate(bbEndDateStr, "yyyyMMdd");
    			
    			if(!(startDate.compareTo(bbEffDate) >= 0 && startDate.compareTo(bbEndDate) <= 0)){
    				logger.warn("Date range out of processing week");
    				//smallerCost = smallerCost - (priceandCostDTO.getBbAmount()/priceandCostDTO.getCasePack());
    			}
    		}else if(!bbEffDateStr.equals(Constants.EMPTY) && bbEndDateStr.equals(Constants.EMPTY)){
    			bbEffDate = DateUtil.toDate(bbEffDateStr, "yyyyMMdd");
    			if(!(startDate.compareTo(bbEffDate) >= 0)){
    				logger.warn("Date range out of processing week");
    				//smallerCost = smallerCost - (priceandCostDTO.getBbAmount()/priceandCostDTO.getCasePack());
    			}
    		}
    		if(priceandCostDTO.getBillbackType().equals(BILLBACK_TYPE_SCAN))
    			smallerCost = smallerCost - priceandCostDTO.getBbAmount();
    		else
    			smallerCost = smallerCost - (priceandCostDTO.getBbAmount()/priceandCostDTO.getCasePack());
    		retailCostDTO.setDealCost(smallerCost);
    		// Changes to process new billback feed from TOPS
    		/*retailCostDTO.setDealCost(smallerCost -  (priceandCostDTO.getBaAmount()/priceandCostDTO.getCasePack()));
    		
    		String velBsEffDateStr = formatDate(priceandCostDTO.getBsEffDate());
    		String velBsEndDateStr = formatDate(priceandCostDTO.getBsEndDate());
    		
    		if(!velBsEffDateStr.equals(Constants.EMPTY) && !velBsEndDateStr.equals(Constants.EMPTY)){
    			Date velBsEffDate = DateUtil.toDate(velBsEffDateStr, "yyyyMMdd");
    			Date velBsEndDate = DateUtil.toDate(velBsEndDateStr, "yyyyMMdd");
    			
    			if(startDate.compareTo(velBsEffDate) >= 0 && startDate.compareTo(velBsEndDate) <= 0){
    				retailCostDTO.setDealCost(retailCostDTO.getDealCost() - priceandCostDTO.getBsAmount());
    			}
    		}*/
    		// Changes to process new billback feed from TOPS
    		
    		if( Math.abs(retailCostDTO.getListCost() - retailCostDTO.getDealCost()) < 0.01f)
    			retailCostDTO.setDealCost(0);
    		//Assign Billback start and end date if deal is cost is available
    		if(retailCostDTO.getDealCost() >0){
    			retailCostDTO.setDealStartDate(DateUtil.dateToString(bbEffDate, "MM/dd/yy"));
    			if(!bbEndDateStr.equals(Constants.EMPTY)){
    				retailCostDTO.setDealEndDate(DateUtil.dateToString(bbEndDate, "MM/dd/yy"));
    			}
    			
    		}
    		}catch (GeneralException | ParseException e){
    			logger.error("Error while processing setupRetailCostDTO", e);
    			throw new GeneralException("Error while processing setupRetailCostDTO", e);
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
	        if(priceandCostDTO.getStrBbAmount() != null){
	        	float bbAmount=Float.parseFloat(priceandCostDTO.getStrBbAmount().trim());
	        	priceandCostDTO.setBbAmount(bbAmount/100);
	        }
	        
	        // Changes to process new billback feed from TOPS
	        /*if(priceandCostDTO.getStrBaAmount() != null){
	        	float baAmount=Float.parseFloat(priceandCostDTO.getStrBaAmount().trim());
	        	priceandCostDTO.setBaAmount(baAmount/100);
	        }
	        
	        if(priceandCostDTO.getStrBsAmount() != null){
	        	float bsAmount=Float.parseFloat(priceandCostDTO.getStrBsAmount().trim());
	        	priceandCostDTO.setBsAmount(bsAmount/100);
	        }*/
	        // Changes to process new billback feed from TOPS
	        
	        if(priceandCostDTO.getCompanyPack() != null){
		        int casePack = Integer.parseInt(priceandCostDTO.getCompanyPack().trim());
		        priceandCostDTO.setCasePack(casePack);
	        }
    	}catch (Exception e){
    		logger.error("Populate Prices - JavaException" + ", UPC = + " + priceandCostDTO.getUpc() + ", Store = + " + priceandCostDTO.getZone(), e);
        	retVal = false;
    	}
        return retVal;
    }
    
    /**
     * Formats given date
     * @param inputDate
     * @return
     */
    private String formatDate(String inputDate) throws ParseException{
    	String formatDate = "";
        if( inputDate.equals("00000000"))
        	formatDate = "";  
        else{
        	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        	formatDate = sdf.format(sdf.parse(inputDate));
        }
        return formatDate;

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
		RetailCostDTO chainLevelData = null;
		boolean isChainLevelPresent = false;
		RetailCostDTO zoneLevelData = null;
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
										populateMap(unrolledCostMap, zoneLevelDTO);
										isPopulated = true;
									}
								}

								if(!isPopulated)
									if(storeItemMap.containsKey(storeKey)){
										ItemStoreKey key = new ItemStoreKey(storeKey, zoneLevelDTO.getItemcode());
										if(itemsBasedOnStoreOrZone.contains(key)){
											populateMap(unrolledCostMap, zoneLevelDTO);
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
											populateMap(unrolledCostMap, zoneLevelDTO);
										}
									}
									
									unrolledStores.add(store);
								}
							}
						}
					}
				}else{
					if(storeNumbers.contains(retailCostDTO.getLevelId())){
						populateMap(unrolledCostMap, retailCostDTO);
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
								populateMap(unrolledCostMap, chainLevelDTO);
								isPopulated = true;
							}
						}
						
						if(!isPopulated)
							if(storeItemMap.containsKey(zoneKey)){
								ItemStoreKey key = new ItemStoreKey(zoneKey, chainLevelDTO.getItemcode());
								if(itemsBasedOnStoreOrZone.contains(key)){	
									populateMap(unrolledCostMap, chainLevelDTO);
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
												populateMap(unrolledCostMap, chainLevelDTO);
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
	 * This method populates a HashMap with store number as key and a list of its corresponding retailCostDTO as its value
	 * @param unrolledCostMap		Map that needs to be populated with with store number as key and a list of its corresponding retailCostDTO as its value
	 * @param retailCostDTO			Retail Cost DTO that needs to be added to the Map
	 */
	public void populateMap(HashMap<String, List<RetailCostDTO>> unrolledCostMap, RetailCostDTO retailCostDTO){
		if(unrolledCostMap.get(retailCostDTO.getLevelId()) != null){
			List<RetailCostDTO> tempList = unrolledCostMap.get(retailCostDTO.getLevelId());
			tempList.add(retailCostDTO);
			unrolledCostMap.put(retailCostDTO.getLevelId(),tempList);
		}else{
			List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
			tempList.add(retailCostDTO);
			unrolledCostMap.put(retailCostDTO.getLevelId(),tempList);
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
		private BillbackDataLoad getOuterType() {
			return BillbackDataLoad.this;
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
	
	/**
	 * To generate key based on Start and End date
	 * @author Dinesh Ramanathan
	 *
	 */
	public class StartAndEndDateKey {
		
		String bbEffDate;
		String bbEndDate;
		
		public StartAndEndDateKey(String bbEffDate, String bbEndDate) {
			this.bbEffDate = bbEffDate;
			this.bbEndDate = bbEndDate;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((bbEffDate == null) ? 0 : bbEffDate.hashCode());
			result = prime * result + ((bbEndDate == null) ? 0 : bbEndDate.hashCode());
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
			StartAndEndDateKey other = (StartAndEndDateKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (bbEffDate == null) {
				if (other.bbEffDate != null)
					return false;
			} else if (!bbEffDate.equals(other.bbEffDate))
				return false;
			if (bbEndDate == null) {
				if (other.bbEndDate != null)
					return false;
			} else if (!bbEndDate.equals(other.bbEndDate))
				return false;
			return true;
		}
		
		public String getBbEffDate() {
			return bbEffDate;
		}
		public void setBbEffDate(String bbEffDate) {
			this.bbEffDate = bbEffDate;
		}
		public String getBbEndDate() {
			return bbEndDate;
		}
		public void setBbEndDate(String bbEndDate) {
			this.bbEndDate = bbEndDate;
		}
		private BillbackDataLoad getOuterType() {
			return BillbackDataLoad.this;
		}
	}
    /**
     * Separate Billback based on the start date 
     * @param groupedByStartAndEndDate
     * @return
     * @throws ParseException
     * @throws GeneralException
     */
	private HashMap<String, List<PriceAndCostDTO>> getBillbackBasedOnStartDate()
			throws ParseException, GeneralException {
		HashMap<String, List<PriceAndCostDTO>> billbackBasedOnStartDate = new HashMap<String, List<PriceAndCostDTO>>();
		int maxNoOfWeeks = Integer.valueOf(PropertyManager.getProperty("MAX_NO_OF_WEEKS_TO_PROCESS", "14"));
		//To process only up to given MaxNoOfWeeks
		String tempStartDate = DateUtil.dateToString(DateUtil.toDate(weekStartDate), Constants.APP_DATE_FORMAT);
		for (int i = 1; i <= maxNoOfWeeks; i++) {
			if (i != 1) {
				tempStartDate = DateUtil.dateToString(
						getNextWeekDate(DateUtil.toDate(tempStartDate)), Constants.APP_DATE_FORMAT);
			}
				List<PriceAndCostDTO> tempBillBack = new ArrayList<PriceAndCostDTO>();
				billbackBasedOnStartDate.put(tempStartDate, tempBillBack);
		}
		for (Map.Entry<StartAndEndDateKey, List<PriceAndCostDTO>> entry : groupedByStartAndEndDate.entrySet()) {
			// Assign actual Week start date in Processing week start date
			String processingWeekStartDate = weekStartDate;
			StartAndEndDateKey key = entry.getKey();

			String bbEffDateStr = formatDate(key.getBbEffDate());
			String bbEndDateStr = formatDate(key.getBbEndDate());
			Date startDate = DateUtil.toDate(weekStartDate);

			// Get the week start date and split them based on number of weeks
			// available.
			if (!bbEffDateStr.equals(Constants.EMPTY) && !bbEndDateStr.equals(Constants.EMPTY)) {
				Date bbEffDate = DateUtil.toDate(bbEffDateStr, "yyyyMMdd");
				Date bbEndDate = DateUtil.toDate(bbEndDateStr, "yyyyMMdd");
				// If End date is older than Current date skip those records.
				if ((startDate.compareTo(bbEndDate) > 0)) {
					logger.warn("Date range out of processing week");
				} else {
					int noOfWeeksToProcess = 0;
					long noOfDays;
					// If Start date is older than current week then process
					// from current to Given end date
					if ((startDate.compareTo(bbEffDate) > 0)) {
						noOfDays = updateNoOfDaysInPromoDuration(startDate, bbEndDate);
						noOfWeeksToProcess = (int) Math.ceil((double) noOfDays / 7);
						processingWeekStartDate = DateUtil.dateToString(startDate, Constants.APP_DATE_FORMAT);
					}
					// If start date and current date are same or future date
					// then assign actual start date
					else if ((startDate.compareTo(bbEffDate) <= 0)) {
						noOfDays = updateNoOfDaysInPromoDuration(bbEffDate, bbEndDate);
						noOfWeeksToProcess = (int) Math.ceil((double) noOfDays / 7);
						processingWeekStartDate = DateUtil.dateToString(bbEffDate, Constants.APP_DATE_FORMAT);
					}
					for (int i = 1; i <= noOfWeeksToProcess; i++) {
						if (i != 1) {
							processingWeekStartDate = DateUtil.dateToString(
									getNextWeekDate(DateUtil.toDate(processingWeekStartDate)),
									Constants.APP_DATE_FORMAT);
						}
						// Assign values based on Week Start date and restrict
						// future Billback processing only up to 13 weeks
//						if (i <= maxNoOfWeeks) {
							for (PriceAndCostDTO priceAndCostDTO : entry.getValue()) {
								List<PriceAndCostDTO> tempBillBack = new ArrayList<PriceAndCostDTO>();
								if (billbackBasedOnStartDate.containsKey(processingWeekStartDate)) {
									tempBillBack = billbackBasedOnStartDate.get(processingWeekStartDate);
									tempBillBack.add(priceAndCostDTO);
									billbackBasedOnStartDate.put(processingWeekStartDate, tempBillBack);
								}
								
//							}
						}
					}
				}
			}
			// If end date is empty or like 000000. Then use week start date to
			// assign week end date.
			else if (!bbEffDateStr.equals(Constants.EMPTY) && bbEndDateStr.equals(Constants.EMPTY)) {
				Date bbEffDate = DateUtil.toDate(bbEffDateStr, "yyyyMMdd");
				// If Start date is older than Current date then for those
				// records assign from current week start date.
				if ((startDate.compareTo(bbEffDate) >= 0)) {
					processingWeekStartDate = DateUtil.dateToString(startDate, Constants.APP_DATE_FORMAT);
				} else {
					processingWeekStartDate = DateUtil.dateToString(bbEffDate, Constants.APP_DATE_FORMAT);
				}
				for (int i = 1; i <= maxNoOfWeeks; i++) {
					if (i != 1) {
						processingWeekStartDate = DateUtil.dateToString(
								getNextWeekDate(DateUtil.toDate(processingWeekStartDate)), Constants.APP_DATE_FORMAT);
					}
					for (PriceAndCostDTO priceAndCostDTO : entry.getValue()) {
						List<PriceAndCostDTO> tempBillBack = new ArrayList<PriceAndCostDTO>();
						if (billbackBasedOnStartDate.containsKey(processingWeekStartDate)) {
							tempBillBack = billbackBasedOnStartDate.get(processingWeekStartDate);
							tempBillBack.add(priceAndCostDTO);
							billbackBasedOnStartDate.put(processingWeekStartDate, tempBillBack);
						}
						
					}
				}
			}
		}
		return billbackBasedOnStartDate;
	}
	
	/**
	 * Process each week and its items to get final deal cost and its details
	 * @param billbackBasedOnStartDate
	 * @throws GeneralException
	 */
	private void processBbBasedOnStartDate(HashMap<String, List<PriceAndCostDTO>> billbackBasedOnStartDate) throws GeneralException{

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		//Delete retail cost list is used to delete before loading billback items
		HashMap<Integer, List<String>> itemToBeDeleted = new HashMap<Integer, List<String>>();
		//List of items to be inserted into the Retail cost info table
		List<RetailCostDTO> itemsToBeInserted = new ArrayList<RetailCostDTO>();
		logger.info("# of weeks to Process: "+billbackBasedOnStartDate.size());
		for(Map.Entry<String, List<PriceAndCostDTO>> finalEntry: billbackBasedOnStartDate.entrySet()){
				//To get calendar id for the processing week start date
			
				RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, finalEntry.getKey(), Constants.CALENDAR_WEEK);
				processingBBStartDate = calendarDTO.getStartDate();
				processingCalendarId = calendarDTO.getCalendarId();
				logger.info("Processing week calendar id: "+processingCalendarId);
				// To ignore future records and to process only current week
				if(ignoreFutureRecords && processingCalendarId == calendarId){
					processBillBackRecords(finalEntry.getValue(),itemToBeDeleted, itemsToBeInserted );
				}else if(!ignoreFutureRecords){
					processBillBackRecords(finalEntry.getValue(),itemToBeDeleted, itemsToBeInserted );
				}
		}
		//Delete the items with respect to given calendar id
		for(Map.Entry<Integer, List<String>> itemListTODelete: itemToBeDeleted.entrySet()){
			retailCostDAO.deleteRetailCostData(conn, itemListTODelete.getKey(), itemListTODelete.getValue());
		}
		if(!ignoreFutureRecords){
			retailCostDAO.deleteFutureRetailCostData(conn,calendarId);
		}
		retailCostDAO.saveRetailCostData(conn, itemsToBeInserted);
		if(!upcNotFoundList.isEmpty()){
	    	List<String> skippedUPC = new ArrayList<String>(upcNotFoundList);
			logger.warn("Level id not found for list of Stores: "
					+ PRCommonUtil.getCommaSeperatedStringFromStrArray(skippedUPC));
	    }
		itemToBeDeleted = new HashMap<Integer, List<String>>();
		itemsToBeInserted = new ArrayList<RetailCostDTO>();
	}
	private long updateNoOfDaysInPromoDuration(Date startDate, Date endDate) throws ParseException, GeneralException {
		
		Date endDateOfPromo = getLastDateOfWeek(endDate);
		Date startDateTemp = getFirstDateOfWeek(startDate);

		long diff = endDateOfPromo.getTime() - startDateTemp.getTime();
		return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}
	private Date getNextWeekDate(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 7);
		return outputDate;
	}
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}

	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date endDate = appDateFormatter.parse(strDate);
		Date outputDate = DateUtil.incrementDate(endDate, 6);
		return outputDate;
	}
}
