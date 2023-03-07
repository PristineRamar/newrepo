/*
 * Title: Load Competitive Data for Price Index
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/12/2012	Janani			Initial Version 
 * Version 0.2	04/26/2012	Janani			Changes for Unrolling price using  
 * 											store to item mapping
 * Version 0.3	06/20/2012	Janani			Change in place where commit is 
 * 											invoked
 * Version 0.4	01/08/2012	Janani			Fix the issue with 
 * 											retrieveStoreItemMap() trying to 
 * 											retrieve data for all items
 *******************************************************************************
 */
package com.pristine.dataload.prestoload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.CompDataLIGRollup;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompDataLoadV2 {

	private static Logger logger = Logger.getLogger("CompDataLoadV2");
	
	private int calendarId = -1;
	private long noOfRecInserted = -1;
	private long noOfRecUpdated = -1;
	
	private HashMap<String, String> storeNumberMap = new HashMap<String, String>(); 
	private HashMap<String, List<String>> zoneStoreMap = null;
	private HashMap<Integer, Integer> scheduleIdMap = null;
	private HashMap<String, Integer> storeIdMap = null; 
	
	private String chainId = null;
	private String weekStartDate = null;
	private String weekEndDate = null;
	
	private CompetitiveDataDAOV2 compDataDAO = null;
	private RetailPriceDAO retailPriceDAO = null;
	private Connection conn = null;
	
	public CompDataLoadV2(){
		
	}
	
	public CompDataLoadV2(Connection connection){
		this.conn = connection;
	}
	
	/**
	 * Main method of Competitive Data Load V2 batch
	 * @param args[0]	lastweek/specificweek/currentweek/nextweek
	 * @param args[1]	Will be present only if args[0] is specificweek
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-comp-data-load.properties");
		PropertyManager.initialize("analysis.properties");
		CompDataLoadV2 compDataLoad = new CompDataLoadV2();
		
		logger.info("Competitive Data Load started");
	
		if(!(Constants.NEXT_WEEK.equalsIgnoreCase(args[0]) || Constants.CURRENT_WEEK.equalsIgnoreCase(args[0]) || Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[0]) || Constants.LAST_WEEK.equalsIgnoreCase(args[0]))){
			logger.info("Invalid Argument, args[0] should be lastweek/currentweek/nextweek/specificweek");
        	System.exit(-1);
		}
		
		if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[0]) && (args.length != 2)){
			logger.info("Invalid Arguments, args[0] lastweek/currentweek/nextweek/specificweek, args[1] data for a specific week");
        	System.exit(-1);
		}
		
		String dateStr = null;
		Calendar c = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if(Constants.CURRENT_WEEK.equalsIgnoreCase(args[0])){
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.NEXT_WEEK.equalsIgnoreCase(args[0])){
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.LAST_WEEK.equalsIgnoreCase(args[0])){
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[0])){
			dateStr = args[1];
		}
		
		long startTime = System.currentTimeMillis();
		compDataLoad.loadCompetitiveData(dateStr);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to load competitive data - " + (endTime - startTime) + "ms");
	}
	
	/**
	 * This method populates store and zone related info
	 * @param inputDate		Date of the week
	 * @throws GeneralException
	 */
	public void populateStoreZoneDetails(String inputDate) throws GeneralException{
		logger.debug("Inside populateStoreZoneDetails() of CompDataLoadV2");
		conn = DBManager.getConnection();
		
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_WEEK);
		
		weekStartDate = calendarDTO.getStartDate();
		weekEndDate = calendarDTO.getEndDate();
		calendarId = calendarDTO.getCalendarId();
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		
		retailPriceDAO = new RetailPriceDAO();
		
		// Retrieve Chain Id
		chainId = retailPriceDAO.getChainId(conn);
		logger.info("Chain Id - " + chainId);
		
		compDataDAO = new CompetitiveDataDAOV2(conn);
		
		// Retrieve store# and its corresponding zone# for stores that are available for PI
		long retStoreInfoStartTime = System.currentTimeMillis();
		storeNumberMap = compDataDAO.getStoreNumbers(chainId); 
		storeIdMap = compDataDAO.getStoreIds(chainId);
		logger.info("Number of stores available for PI - " + storeNumberMap.size());
		long retStoreInfoEndTime = System.currentTimeMillis();
		logger.debug("Time taken to retrieve store info - " + (retStoreInfoEndTime - retStoreInfoStartTime) + "ms");
		
		// Compute a map containing zone# as key and set of stores under that zone to handle items priced at zone level
		zoneStoreMap = new HashMap<String, List<String>>();
		String storeNo = null, zoneNo = null;
		for(Map.Entry<String, String> entry:storeNumberMap.entrySet()){
			storeNo = entry.getKey();
			zoneNo = entry.getValue();
			if(zoneStoreMap.get(zoneNo) != null){
				List<String> storeNoList = zoneStoreMap.get(zoneNo);
				storeNoList.add(storeNo);
				zoneStoreMap.put(zoneNo, storeNoList);
			}else{
				List<String> storeNoList = new ArrayList<String>();
				storeNoList.add(storeNo);
				zoneStoreMap.put(zoneNo, storeNoList);
			}
		}
		
		// Retrieve/Insert schedule Id for stores retrieved and the specified week
		long populateSchIdStartTime = System.currentTimeMillis();
		scheduleIdMap = compDataDAO.populateScheduleId(storeIdMap.values(), weekStartDate, weekEndDate);
		logger.info("Number of stores for which schedule id is present - " + scheduleIdMap.size());
		long populateSchIdEndTime = System.currentTimeMillis();
		logger.debug("Time taken for populating schedule id - " + (populateSchIdEndTime - populateSchIdStartTime) + "ms");
	}
	
	/**
	 * This method loads competitive data table
	 * @param inputDate		Date of the week
	 */
	public void loadCompetitiveData(String inputDate){
		
		try{
			populateStoreZoneDetails(inputDate);
			
			// Get departments
			List<Integer> deptLst = compDataDAO.getDepartments();
			logger.info("Number of departments - " + deptLst.size());
			
			String piSetupItems = PropertyManager.getProperty("PI_SETUP_ITEMS");
			boolean isPIAnalyzeItemsOnly = true;
			if(Constants.PI_SETUP_ITEMS.equalsIgnoreCase(piSetupItems)){
				isPIAnalyzeItemsOnly = false;
			}
			
			// Load competitive data for each item belonging to a department
			Set<String> itemCodeSet = null;
			for(Integer deptId:deptLst){
				long deptLoadStartTime = System.currentTimeMillis();
				
				// Select all item codes belonging to a department
				itemCodeSet = compDataDAO.getItemCodes(deptId, isPIAnalyzeItemsOnly);
				logger.debug("Number of items for department " + deptId + " - " + itemCodeSet.size());
				
				// Process Competitive Data
				// If itemCodeSet has more than 100K items, process competitive data for every 100k items
				if(itemCodeSet.size() > Constants.MAX_ITEMS_ALLOWED){
					Set<String> itemCodeTemp = new HashSet<String>();
					int counter = 0;
					for(String itemCode : itemCodeSet){
						counter++;
						itemCodeTemp.add(itemCode);
						if(counter % Constants.MAX_ITEMS_ALLOWED == 0){
							processCompetitiveData(itemCodeTemp, String.valueOf(Constants.YES), isPIAnalyzeItemsOnly, retailPriceDAO);
							itemCodeTemp.clear();
							itemCodeTemp = new HashSet<String>();
							counter = 0;
						}
					}
					processCompetitiveData(itemCodeTemp, String.valueOf(Constants.YES), isPIAnalyzeItemsOnly, retailPriceDAO);
				}else{
					processCompetitiveData(itemCodeSet, String.valueOf(Constants.YES), isPIAnalyzeItemsOnly, retailPriceDAO);
				}
								
				long deptLoadEndTime = System.currentTimeMillis();
				logger.info("Time taken to load records for dept id " + deptId + " - " + (deptLoadEndTime - deptLoadStartTime) + "ms");
			}

			logger.info("Total number or records inserted - " + noOfRecInserted);
			logger.info("Total number or records updated - " + noOfRecUpdated);
			
			// Perform LIG rollup
			performLIGRollup();		
		}catch(GeneralException ge){
			logger.error("Error in Competitive Data Load", ge);
			PristineDBUtil.rollbackTransaction(conn, "Competitive Data Load V2");
			System.exit(1);
		}
	}
	
	/**
	 * This method invokes LIG rollup function for the given schedules.
	 * @throws GeneralException
	 */
	public void performLIGRollup() throws GeneralException{
		CompDataLIGRollup ligRollup = new CompDataLIGRollup (); 

        for ( int schId : scheduleIdMap.values()){
        	ligRollup.LIGLevelRollUp(conn, schId);
        	PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
		}
	}
	
	/**
	 * Thie method populates competitive data and inserts/updates in database.
	 * @param itemCodeSet				Item codes to be processed
	 * @param priceIndex				Y/N based on if all items or only specified items needs to be processes
	 * @param isPIAnalyzeItemsOnly		If processing needs to be done for items with pi_analyze_flag as Y
	 * @throws GeneralException
	 */
	public void processCompetitiveData(Set<String> itemCodeSet, String priceIndex, boolean isPIAnalyzeItemsOnly, RetailPriceDAO retailPriceDAO) throws GeneralException{
		
		// Retrieve store item map
		// if(storeItemMap != null && storeItemMap.size() > 0){
		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
			long retStoreItemMapStartTime = System.currentTimeMillis();
			if("Y".equalsIgnoreCase(priceIndex) && isPIAnalyzeItemsOnly)
				storeItemMap = compDataDAO.retrieveStoreItemMap();
			else
				storeItemMap = compDataDAO.retrieveStoreItemMapForItems(itemCodeSet);
			long retStoreItemMapEndTime = System.currentTimeMillis();
			logger.debug("Time taken to retrieve store item map - " + (retStoreItemMapEndTime - retStoreItemMapStartTime) + "ms");
			logger.debug("Number of Items in store item map - " + storeItemMap.size());
		//}
		
		// Calendar Id is being passed as -1 to retrieve latest retail price info for the item codes
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = retailPriceDAO.getRetailPriceInfo(conn, itemCodeSet, calendarId, true);
		logger.debug("Size of Price rolled up map - " + priceRolledUpMap.size());
		
		//Unroll price data
		long unrollStartTime = System.currentTimeMillis();
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = unrollRetailPriceInfo(priceRolledUpMap, storeNumberMap.keySet(), zoneStoreMap, retailPriceDAO, storeItemMap, storeNumberMap);
		logger.debug("Size of Unrolled price list - " + unrolledPriceMap.size());
		long unrollEndTime = System.currentTimeMillis();
		logger.debug("Time taken for unrolling price data - " + (unrollEndTime - unrollStartTime) + "ms");
		
		// Process competitive data for each store
		for(List<RetailPriceDTO> unrolledPriceLst:unrolledPriceMap.values()){
			logger.debug("Size of Unrolled price list inside loop - " + unrolledPriceLst.size());
			long storeProcStartTime = System.currentTimeMillis();
			HashMap<String, CompetitiveDataDTO> compDataMap = new HashMap<String, CompetitiveDataDTO>();
			CompetitiveDataDTO compData = null;
			for(RetailPriceDTO retailPriceDTO:unrolledPriceLst){
				compData = new CompetitiveDataDTO();
				compData.compStrNo = retailPriceDTO.getLevelId();
				compData.compStrId = storeIdMap.get(compData.compStrNo);
				compData.scheduleId = scheduleIdMap.get(compData.compStrId);
				compData.itemcode = Integer.parseInt(retailPriceDTO.getItemcode());
				compData.regPrice = retailPriceDTO.getRegPrice();
				compData.regMPrice = retailPriceDTO.getRegMPrice();
				compData.regMPack = retailPriceDTO.getRegQty();
				compData.fSalePrice = retailPriceDTO.getSalePrice();
				compData.fSaleMPrice = retailPriceDTO.getSaleMPrice();
				compData.saleMPack = retailPriceDTO.getSaleQty();
				compData.saleInd = retailPriceDTO.getPromotionFlag();
				compData.effRegRetailStartDate = retailPriceDTO.getRegEffectiveDate();
				compData.effSaleStartDate = retailPriceDTO.getSaleStartDate();
				compData.effSaleEndDate = retailPriceDTO.getSaleEndDate();
				compData.weekStartDate = weekStartDate;
				compData.weekEndDate = weekEndDate;
				compDataMap.put(compData.scheduleId+","+compData.itemcode,compData);
			}
			logger.debug("No of items to be compared against competitive data table - " + compDataMap.size());
			
			// Retrieve Check Data Id
			long populateChkIdStartTime = System.currentTimeMillis();
			compDataDAO.populateCheckDataId(compDataMap);
			long populateChkIdEndTime = System.currentTimeMillis();
			logger.debug("Time taken for populating check data id - " + (populateChkIdEndTime - populateChkIdStartTime) + "ms");
			
			List<CompetitiveDataDTO> toBeInsertedList = new ArrayList<CompetitiveDataDTO>();
			List<CompetitiveDataDTO> toBeUpdatedList = new ArrayList<CompetitiveDataDTO>();
			
			for(CompetitiveDataDTO compDataDTO:compDataMap.values()){ 
				if(compDataDTO.checkItemId != 0){
					toBeUpdatedList.add(compDataDTO);
				}else{
					toBeInsertedList.add(compDataDTO);
				}
			}
			noOfRecInserted = noOfRecInserted + toBeInsertedList.size();
			noOfRecUpdated = noOfRecUpdated + toBeUpdatedList.size();
			logger.debug("Number of records to be inserted - " + toBeInsertedList.size());
			logger.debug("Number of records to be updated - " + toBeUpdatedList.size());
			long insertStartTime = System.currentTimeMillis();
			compDataDAO.insertCompetitiveData(toBeInsertedList);
			long insertEndTime = System.currentTimeMillis();
			logger.debug("Time taken for inserting records - " + (insertEndTime - insertStartTime) + "ms");
			
			long updateStartTime = System.currentTimeMillis();
			compDataDAO.updateCompetitiveData(toBeUpdatedList);
			long updateEndTime = System.currentTimeMillis();
			logger.debug("Time taken for updating records - " + (updateEndTime - updateStartTime) + "ms");
			
			priceRolledUpMap = null; 
			unrolledPriceMap = null;
			toBeInsertedList = null;
			toBeUpdatedList = null;
			
			long storeProcEndTime = System.currentTimeMillis();
			logger.debug("Time taken to process a store - " + (storeProcEndTime - storeProcStartTime));
			PristineDBUtil.commitTransaction(conn, "Competitive Data Load");
		}
	}

	/**
	 * This method unrolls data from retail price info to be loaded in competitive data table
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
								String zoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + zoneLevelData.getLevelId();
								String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + zoneLevelDTO.getLevelId();
								// Check if this item exists for this store
								boolean isPopulated = false;
								if(storeItemMap.containsKey(zoneKey)){
									if(storeItemMap.get(zoneKey).contains(zoneLevelDTO.getItemcode())){
										populateMap(unrolledPriceMap, zoneLevelDTO);
										isPopulated = true;
									}
								}

								if(!isPopulated)
									if(storeItemMap.containsKey(storeKey)){
										if(storeItemMap.get(storeKey).contains(zoneLevelDTO.getItemcode())){
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
									String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + zoneLevelDTO.getLevelId();
									
									
									if(storeItemMap.containsKey(storeKey)){
										if(storeItemMap.get(storeKey).contains(zoneLevelDTO.getItemcode())){
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
						String storeKey = Constants.STORE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + chainLevelDTO.getLevelId();
						String zoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + storeNumberMap.get(chainLevelDTO.getLevelId());
						// Check if this item exists for this store
						boolean isPopulated = false;
						if(storeItemMap.containsKey(storeKey)){
							if(storeItemMap.get(storeKey).contains(chainLevelDTO.getItemcode())){	
								populateMap(unrolledPriceMap, chainLevelDTO);
								isPopulated = true;
							}
						}
						
						if(!isPopulated)
							if(storeItemMap.containsKey(zoneKey)){
								if(storeItemMap.get(zoneKey).contains(chainLevelDTO.getItemcode())){	
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
										String deptZoneKey = Constants.ZONE_LEVEL_TYPE_ID + Constants.INDEX_DELIMITER + deptZone;
										if(storeItemMap.containsKey(deptZoneKey)){
											if(storeItemMap.get(deptZoneKey).contains(chainLevelDTO.getItemcode())){	
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
}
