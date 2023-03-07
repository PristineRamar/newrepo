package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.PromoDataLoader;
import com.pristine.dataload.prestoload.CompDataLoadV2;
import com.pristine.dataload.prestoload.RetailPriceSetup;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.PromoDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PromoDataLoad extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("PromoDataLoad");
	private Connection conn = null;
	
	HashMap<String, List<PromoDataDTO>> promoDataMap = new HashMap<String, List<PromoDataDTO>>();
	List<PromoDataDTO> currentProcPromoList = new ArrayList<PromoDataDTO>();
	
	int promoRecordCount = 0;
	int commitRecCount = 1000;
	
	String weekStartDate = null;
	
	float minItemPrice=0;
	//Changes for populating store id in Store item map table.
  	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
  	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
  	
	private RetailPriceDAO retailPriceDAO = null;
  	private CostDAO costDAO = null;
  	private String chainId;
  	
	 /**
	  * Arguments
	  * args[0]		Relative path of Price File
	  * args[2]		weektype - lastweek/currentweek/nextweek/specificweek
	  * args[3]		Date if weektype is specific week
	  * @param args
	  */
	 public static void main(String[] args) {
	  
		 PromoDataLoad dataload = new PromoDataLoad();
		 PropertyConfigurator.configure("log4j-promo-data-load.properties");
			
	     String weekType = null;
		 
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
	    	 logger.info("Invalid Arguments, args[0] should be relative path of promo file, " +
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
	     dataload.setWeekStartDate(dateStr);
	     dataload.processPromoFile(args[0]);
	}
	 
	private void setWeekStartDate(String dateStr) {
		this.weekStartDate = dateStr;	
	}

	private void  processPromoFile(String relativePath ){
		 conn = getOracleConnection();

		 String tempCommitCount = PropertyManager.getProperty("DATALOAD.COMMITRECOUNT", "1000");
         commitRecCount=Integer.parseInt(tempCommitCount);
         
         String minPrice = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.MIN_ITEM_PRICE", "0.10");
		 minItemPrice = Float.parseFloat(minPrice);
		 
		 try{      
			 logger.info("Promo Data Load Started ....");
			 
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
						 String fieldNames[] = new String[22];
						 fieldNames[0] = "buyerCode";
						 fieldNames[1] = "adDate1";
						 fieldNames[2] = "adDate2";
						 fieldNames[3] = "adDate3";
						 fieldNames[4] = "adDate4";
						 fieldNames[5] = "promoStartDate";
						 fieldNames[6] = "promoEndDate";
						 fieldNames[7] = "sourceVendorNo";
						 fieldNames[8] = "itemNo";
						 fieldNames[9] = "itemDesc";
						 fieldNames[10] = "priceZone";
						 fieldNames[11] = "storeNo";
						 fieldNames[12] = "storeZoneData";
						 fieldNames[13] = "regQty"; 
						 fieldNames[14] = "regPrice";
						 fieldNames[15] = "saleQty";
						 fieldNames[16] = "salePrice";
						 fieldNames[17] = "saveAmt";
						 fieldNames[18] = "pageNo";
						 fieldNames[19] = "blockNo";
						 fieldNames[20] = "userChange";
						 fieldNames[21] = "promoNo";
			
							
						 super.headerPresent = true;
						 super.parseDelimitedFile(PromoDataDTO.class, files, '|',fieldNames, stopCount);							      
						 
						 logger.info("No of promo records processed - " + promoRecordCount);
						 PristineDBUtil.commitTransaction(conn, "Promo Data Setup");
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

		 logger.info("Promo Data Load successfully completed");

		 super.performCloseOperation(conn, true);
		 return;
	}
	 
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<PromoDataDTO> promoDataList = (List<PromoDataDTO>) listobj; 
		if(promoDataList != null && promoDataList.size() > 0){
			if(promoDataList.size() >= commitRecCount){
				if(currentProcPromoList.size() >= Constants.RECS_TOBE_PROCESSED){
					PromoDataDTO lastPromoData = currentProcPromoList.get(currentProcPromoList.size()-1);
					String lastRetItemCode = lastPromoData.getSourceVendorNo()+lastPromoData.getItemNo();
					boolean eofFlag = true;
					for(PromoDataDTO promoDataDTO : promoDataList){
						String tempRetItemCode = promoDataDTO.getSourceVendorNo()+promoDataDTO.getItemNo();
						if(tempRetItemCode.equals(lastRetItemCode)){
							currentProcPromoList.add(promoDataDTO);
						}else{
							if(currentProcPromoList.size() >= Constants.RECS_TOBE_PROCESSED){
								processPromoRecords(currentProcPromoList);
								currentProcPromoList.clear();
								eofFlag = false;
							}
							currentProcPromoList.add(promoDataDTO);
						}
					}
					if(eofFlag){
						processPromoRecords(currentProcPromoList);
						currentProcPromoList.clear();
					}
				}else{
					currentProcPromoList.addAll(promoDataList);
				}
			}else{
				currentProcPromoList.addAll(promoDataList);
				processPromoRecords(currentProcPromoList);
				currentProcPromoList.clear();
			}
		}
	}
	
	/**
	 * Processes Promo records
	 * (Populates promoDataMap with UPC as key and corresponding list of promo records as value)
	 * @param promoDataList	List of Promo records to be processed
	 */
	private void processPromoRecords(List<PromoDataDTO> promoDataList){
		promoDataMap = new HashMap<String, List<PromoDataDTO>>();
		for(PromoDataDTO promoDTO : promoDataList){
			String retItemCode = promoDTO.getSourceVendorNo()+promoDTO.getItemNo();
			
			boolean canBeAdded = true;
			if(promoDataMap.get(retItemCode) != null){
				List<PromoDataDTO> tempList = promoDataMap.get(retItemCode);
				tempList.add(promoDTO);
				promoDataMap.put(retItemCode, tempList);
			}else{
				List<PromoDataDTO> tempList = new ArrayList<PromoDataDTO>();
				tempList.add(promoDTO);
				promoDataMap.put(retItemCode, tempList);
			}
		
			promoRecordCount++;
			if(promoRecordCount % 10000 == 0)    	
				logger.info("No of promo records Processed " + promoRecordCount);
		}
		
		try{
			loadPromoData();
			
			PristineDBUtil.commitTransaction(conn, "Promo Data Setup");
		}catch(GeneralException exception){
			logger.error("Exception in processPromoRecords of PromoDataLoad - " + exception);
		}
	}
	
	private void loadPromoData() throws GeneralException{
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		CostDAO costDAO = new CostDAO();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		int calendarId = calendarDTO.getCalendarId();	
		String startDateStr = calendarDTO.getStartDate();
		Date startDate = DateUtil.toDate(startDateStr);
		
		String chainId = retailPriceDAO.getChainId(conn); 
		
		// Retrieve all stores and its corresponding zone#
		HashMap<String, String> storeInfoMap = costDAO.getStoreZoneInfo(conn, chainId);
		logger.info("No of store available - " + storeInfoMap.size());
					
		// Populate a map with zone# as key and list of corresponding stores as value
		CostDataLoad costDataLoad = new CostDataLoad();
		costDataLoad.castZoneNumbers(storeInfoMap);
		HashMap<String, List<String>> zoneStoreMap = costDataLoad.getZoneMapping(storeInfoMap);
		logger.info("No of zones available - " + zoneStoreMap.size());
		
		ItemDAO itemDAO = new ItemDAO();
		Set<String> retItemCodeSet = promoDataMap.keySet();
		HashMap<String, List<String>> itemCodeMap = itemDAO.getItemCodeList(conn, retItemCodeSet);
		//HashMap<String, List<String>> itemCodeMap  = itemDAO.getItemCodeForUPC(conn, upcMap);
		
		Set<String> itemCodeSet = new HashSet<String>();
		for(String retItemCode : retItemCodeSet){
			List<String> itemCodeList = itemCodeMap.get(retItemCode);
    		if(itemCodeList != null)
				for(String itemCode : itemCodeList){
	    			itemCodeSet.add(itemCode);
	    		}
		}
		
		// Retrieve item store mapping from store_item_map table for items in itemCodeList
    	long startTime = System.currentTimeMillis();
		HashMap<String, List<String>> itemStoreMapping = costDAO.getStoreItemMap(conn, weekStartDate, itemCodeSet,"PRICE", false);
		long endTime = System.currentTimeMillis();
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
    		//commented by Pradeep on 06/01/2015
    		//CompDataLoadV2 compDataLoad = new CompDataLoadV2(conn);
    		//HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = compDataLoad.unrollRetailPriceInfo(priceRolledUpMapForItems, storeInfoMap.keySet(), zoneStoreMap, retailPriceDAO, itemStoreMapping, storeInfoMap);
    		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = unrollRetailPriceInfo(priceRolledUpMapForItems, storeInfoMap.keySet(), 
    				zoneStoreMap, retailPriceDAO, itemStoreMapping,   storeInfoMap);
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
    	
    	HashMap<String, List<RetailPriceDTO>> retailPriceDTOMap = new HashMap<String, List<RetailPriceDTO>>();
    	for(String retItemCode : promoDataMap.keySet()){
    		List<String> itemCodeList = itemCodeMap.get(retItemCode);
    		
    		if(itemCodeList != null){
    			for(String itemCode : itemCodeList){
	    			List<RetailPriceDTO> unrolledPriceList = unrolledPriceMapForItems.get(itemCode);
	    			List<PromoDataDTO> promoDataList = promoDataMap.get(retItemCode);
	    			List<RetailPriceDTO> finalRetailPriceList = new ArrayList<RetailPriceDTO>();
	    			if(retailPriceDTOMap.get(itemCode) != null)
	    				finalRetailPriceList = retailPriceDTOMap.get(itemCode);
	    			for(PromoDataDTO promoData : promoDataList){
	    				String promoStartDateStr = DateUtil.getWeekStartDate(DateUtil.toDate(promoData.getPromoStartDate()), 0);
	    				String promoEndDateStr = DateUtil.getWeekEndDate(DateUtil.toDate(promoData.getPromoEndDate()));
	    				promoData.setPromoStartDate(promoStartDateStr);
	    				promoData.setPromoEndDate(promoEndDateStr);
	    				
	    				Date promoStartDate = DateUtil.toDate(promoStartDateStr);
	    				Date promoEndDate = DateUtil.toDate(promoEndDateStr);
	    				
	    				if(startDate.compareTo(promoStartDate) < 0 || startDate.compareTo(promoEndDate) > 0){
	    					continue;
	    				}
	    				if( promoData.getSalePrice() < 0.01f && promoData.getSaleQty() < 0.01f){
	    					continue;
	    				}
	    				
	    				if(!isValidPromoNo(promoData.getPromoNo())){
	    					continue;
	    				}
	    				
	    				if(promoData.getRegPrice() <= minItemPrice )
	    					continue;
	    				
	    				String storeNo = null;
	    				String zoneNo = null;
	    				if(promoData.getStoreNo().equals("000") ){
	    	        		if (promoData.getStoreZoneData().equals("000")) { //Zone Priced
	    	        			zoneNo = "0" + promoData.getPriceZone();
	    	        		}
	    	        		else
	    	        			storeNo = "0" + promoData.getPriceZone(); //DSD Price, Store # will be in Price Zone field
	    	        	}
	    	        	else{
	    	        		storeNo= "0" + promoData.getStoreNo(); //Store Specific Pricing
	    	        	}
	    				if(unrolledPriceList != null){
	    					for(RetailPriceDTO retailPriceDTO : unrolledPriceList){
		    					// float salePrice = (retailPriceDTO.getSalePrice()>0)?retailPriceDTO.getSalePrice():retailPriceDTO.getSaleMPrice();
		    					// if(salePrice <= 0)
			    					if((storeNo != null && storeNo.equals(retailPriceDTO.getLevelId())) || 
			    							(zoneNo != null && zoneStoreMap.get(zoneNo) != null && zoneStoreMap.get(zoneNo).contains(retailPriceDTO.getLevelId()))){
			    						populatePromoData(retailPriceDTO, promoData);
			    						retailPriceDTO.setProcessedFlag(true);
			    			            // Logic to handle duplicates in input file
			    			            boolean canBeAdded = true;
			    						for(RetailPriceDTO tempDTO:finalRetailPriceList){
			    							if(tempDTO.getLevelId().equals(retailPriceDTO.getLevelId())){
			    								canBeAdded = false;
			    							}
			    						}
			    						if(canBeAdded)
			    							finalRetailPriceList.add(retailPriceDTO);
			    					}
		    				}
	    				}else{
	    					RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
	    					retailPriceDTO.setCalendarId(calendarId);
	    					retailPriceDTO.setItemcode(itemCode);
	    					if(storeNo != null){
	    						retailPriceDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
	    						retailPriceDTO.setLevelId(storeNo);
	    					}else if(zoneNo != null){
	    						retailPriceDTO.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
	    						retailPriceDTO.setLevelId(zoneNo);
	    					}
	    					populatePromoData(retailPriceDTO, promoData);
				            // Logic to handle duplicates in input file
				            boolean canBeAdded = true;
							for(RetailPriceDTO tempDTO:finalRetailPriceList){
								if(tempDTO.getLevelId().equals(retailPriceDTO.getLevelId())){
									canBeAdded = false;
								}
							}
							if(canBeAdded)
								finalRetailPriceList.add(retailPriceDTO);
	    				}
	    			}
	    			if(finalRetailPriceList.size() > 0)
	    				retailPriceDTOMap.put(itemCode, finalRetailPriceList);
    			}
    		}
    	}
    	
    	// Process price of an item in a store that is present only in retail_price_info and not in promo file
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
    	
    	// Rollup Price Info
    	RetailPriceSetup retailPriceSetup = new RetailPriceSetup();
    	startTime = System.currentTimeMillis();
    	HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = retailPriceSetup.priceRollUp(retailPriceDTOMap, new HashMap<String, String>(), storeInfoMap, new HashSet<String>(), chainId);
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken for processing data - " + (endTime - startTime));
    			
    	// Delete existing data
    	startTime = System.currentTimeMillis();
    	List<String> itemCodeList = new ArrayList<String>(itemCodeSet);
    	retailPriceDAO.deleteRetailPriceData(conn, calendarId, itemCodeList);
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken for deleting data from retail_price_info - " + (endTime - startTime));
    			
    	// Insert into retail_price_info table
    	List<RetailPriceDTO> insertList = new ArrayList<RetailPriceDTO>();
    	for(List<RetailPriceDTO> priceDTOList : priceRolledUpMap.values()){
    		for(RetailPriceDTO retailPriceDTO :  priceDTOList){
    			insertList.add(retailPriceDTO);
    		}
    	}
    	
    	updateLocationId(insertList);
    	startTime = System.currentTimeMillis();
    	retailPriceDAO.saveRetailPriceData(conn, insertList);
    	endTime = System.currentTimeMillis();
    	logger.info("Time taken for inserting data into retail_price_info - " + (endTime - startTime));
	}
	
	private void populatePromoData(RetailPriceDTO retailPriceDTO, PromoDataDTO promoData){
		/*retailPriceDTO.setRegQty(promoData.getRegQty());
		retailPriceDTO.setRegPrice(promoData.getRegPrice());

        if (retailPriceDTO.getRegQty() <= 1) {
        	retailPriceDTO.setRegMPrice(0);
        } else {
        	retailPriceDTO.setRegMPrice(promoData.getRegPrice());
        	retailPriceDTO.setRegPrice(0);
        }*/

        retailPriceDTO.setSaleQty(promoData.getSaleQty());
		retailPriceDTO.setSalePrice(promoData.getSalePrice());
		// Changes to capture promo start and end date in retail_price_info table
		retailPriceDTO.setSaleStartDate(promoData.getPromoStartDate());
		retailPriceDTO.setSaleEndDate(promoData.getPromoEndDate());
				
        //Bogo Adjustment
        if(Math.abs(promoData.getRegPrice() - retailPriceDTO.getSalePrice()) < 0.01f && retailPriceDTO.getSaleQty() == 1 && 
        		promoData.getRegPrice() > 0 ){
        		retailPriceDTO.setSaleQty(2);
        }
        	
        if (retailPriceDTO.getSaleQty() <= 1) {
        	retailPriceDTO.setSaleMPrice(0);
        	retailPriceDTO.setSaleQty(0);
        }else{
        	retailPriceDTO.setSaleMPrice(promoData.getSalePrice());
        	retailPriceDTO.setSalePrice(0);
        }

        retailPriceDTO.setProcessedFlag(true);
	}
	
	private boolean isValidPromoNo(String promoNo) {
		boolean retVal = true;
		
		try{
			int intPromoNo = Integer.parseInt(promoNo);
			if ( intPromoNo < 10000){
				retVal = false;
			}
		}catch(Exception e){
			
		}
		return retVal;
	}
	
	private String getAdIndicator(PromoDataDTO promoData, String weekEndDate) throws GeneralException {
		String onAd = "N";
		
		Date currWeekEndDate = DateUtil.toDate(weekEndDate);
		weekEndDate = DateUtil.dateToString(currWeekEndDate, Constants.APP_DATE_MMDDYYFORMAT);
		if(promoData.getAdDate1().equals(weekEndDate) ||  promoData.getAdDate2().equals(weekEndDate) ||
		   promoData.getAdDate3().equals(weekEndDate) || promoData.getAdDate4().equals(weekEndDate) ){
			onAd = "Y";
		}
		return onAd;
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
	public HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfo(HashMap<String, List<RetailPriceDTO>>priceRolledUpMap, Set<String> storeNumbers, HashMap<String, List<String>> zoneStoreMap,
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

}
