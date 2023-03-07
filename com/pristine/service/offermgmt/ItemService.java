package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
//import java.util.Optional;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.ItemDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.SecondaryZoneRecDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class ItemService {
 	private static Logger logger = Logger.getLogger("ItemService");
	private List<ExecutionTimeLog> executionTimeLogs;
	private ItemDAO itemDAO = null;
	private ExecutionTimeLog executionTimeLog = null;

	public ItemService(List<ExecutionTimeLog> executionTimeLogs) {
		if(executionTimeLogs == null)
			executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		this.executionTimeLogs = executionTimeLogs;
	}
	
	public List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, PRStrategyDTO inputDTO,
			List<Integer> priceZoneStores) throws OfferManagementException {
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_ITEMS);
		itemDAO = new ItemDAO();
		boolean byPassAuth = Boolean.parseBoolean(PropertyManager.getProperty("BY_PASS_AUTHORIZATION", "FALSE"));
		try {
			if (inputDTO.getRunType() == PRConstants.RUN_TYPE_TEMP) {
				logger.info("Getting items from past recommendation...");

				long runId = itemDAO.getPastRecommendationRunId(conn, inputDTO);

				if (runId > 0) {
					logger.info("Past recommendation run id: " + runId);

					itemList = itemDAO.getItemsFromPastRec(conn, inputDTO, runId);

					logger.info("Getting items from past recommendation is completed");
				} else {
					if(byPassAuth) {
						PriceZoneDTO priceZoneDTO = new RetailPriceZoneDAO().getPriceZoneInfo(conn, inputDTO.getLocationId());
						String priceZoneNum = priceZoneDTO.getPriceZoneNum();
						inputDTO.setZoneNum(priceZoneNum);
						itemList = itemDAO.getAllItems(conn, inputDTO, priceZoneStores, false, null);
					}else {
						itemList = itemDAO.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);	
					}
				}
			} else {
				if(byPassAuth) {
					PriceZoneDTO priceZoneDTO = new RetailPriceZoneDAO().getPriceZoneInfo(conn, inputDTO.getLocationId());
					String priceZoneNum = priceZoneDTO.getPriceZoneNum();
					inputDTO.setZoneNum(priceZoneNum);
					itemList = itemDAO.getAllItems(conn, inputDTO, priceZoneStores, false, null);
				}else {
					itemList = itemDAO.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);	
				}
			}

			executionTimeLog.setEndTime();
		} catch (Exception | GeneralException ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfZoneAndStore() " + ex,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		}
		executionTimeLogs.add(executionTimeLog);
		return itemList;
	}
	
	public List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, int productLevelId, int productId,
			List<Integer> items, List<Integer> priceZoneStores) throws OfferManagementException {
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_ITEMS);
		itemDAO = new ItemDAO();
		try {
			itemList = itemDAO.getAuthorizedItemsOfZoneAndStore(conn, productLevelId, productId, items, priceZoneStores);
			executionTimeLog.setEndTime();
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfZoneAndStore() " + ex,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		}
		executionTimeLogs.add(executionTimeLog);
		return itemList;
	}
	
	public List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, int productLevelId, int productId,
			int locationLevelId, int locationId, List<Integer> priceZoneStores) throws OfferManagementException {
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_ITEMS);
		itemDAO = new ItemDAO();
		try {
			itemList = itemDAO.getAuthorizedItemsOfZoneAndStore(conn, productLevelId, productId, locationLevelId, locationId, priceZoneStores);
			executionTimeLog.setEndTime();
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfZoneAndStore() " + ex,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		}
		executionTimeLogs.add(executionTimeLog);
		return itemList;
	}
	
	public List<Integer> getPriceZoneStores(Connection conn, int productLevelId, int productId, 
			int locationLevelId, int locationId)
			throws GeneralException {
		List<Integer> storeList = new ArrayList<Integer>();
		itemDAO = new ItemDAO();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO(executionTimeLogs);
		try {
			storeList = pricingEngineDAO.getPriceZoneStores(conn, productLevelId, productId, locationLevelId, locationId);
		} catch (Exception ex) {
			throw new GeneralException("Exception in getPriceZoneStores() " + ex);
		}
		return storeList;
	}

	public HashMap<ItemKey, PRItemDTO> populateAuthorizedItemsOfZone(Connection conn, long runId, PRStrategyDTO inputDTO, List<PRItemDTO> itemList)
			throws OfferManagementException {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.PROCESS_ZONE_ITEMS);
		try {
			ItemKey itemKey;
			PRItemDTO zoneItem;
			for (PRItemDTO itemInfo : itemList) {
				//zoneItem = (PRItemDTO) itemInfo.clone();
				zoneItem = itemInfo;
				zoneItem.setRunId(runId);
				zoneItem.setChildLocationLevelId(inputDTO.getLocationLevelId());
				zoneItem.setChildLocationId(inputDTO.getLocationId());
				if (inputDTO.isPriceTestZone())
					zoneItem.setChildLocationId(inputDTO.getTempLocationID());
				else
					zoneItem.setChildLocationId(inputDTO.getLocationId());
				//zoneItem.copyZoneItemInfo(itemInfo);
				itemKey = new ItemKey(zoneItem.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
				if (itemDataMap.get(itemKey) == null) {
					itemDataMap.put(itemKey, zoneItem);
				} else {
					// Update no of stores in which items are authorized to sell
					PRItemDTO itemDTO = itemDataMap.get(itemKey);
					if(zoneItem.isAuthorized()) {
						itemDTO.setAuthorized(true);
						itemDTO.setNoOfStoresItemAuthorized(itemDTO.getNoOfStoresItemAuthorized() + 1);
					}
				}
				
			}
			//Find correct zone id for each item
			boolean useStoreItemMapForZone = Boolean.parseBoolean(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
			if(useStoreItemMapForZone) {
				findStrategyZoneOfEachTimeForGE(conn, inputDTO, itemList, itemDataMap);
			}else{
				findStrategyZoneOfEachTime(conn, inputDTO, itemList, itemDataMap);
			}
			addLigItemInItemMap(itemDataMap);
		} catch (Exception | GeneralException ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfZone() " + ex,
					RecommendationErrorCode.PROCESS_ZONE_ITEMS);
		}
		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);
//		for (PRItemDTO zoneItem : itemDataMap.values()) {
//			
//			if (zoneItem.getItemCode() == 2591 || zoneItem.getItemCode() == 113704) {
//				logger.debug("stop log:" + zoneItem.getVendorId());
//				logger.debug("stop log:" + zoneItem.getItemCode() + "-" + zoneItem.isLir()  + "-" + zoneItem.getRetLirId() + "-" + zoneItem.getRetLirItemCode());
//			}
//		}
		return itemDataMap;
	}

	public HashMap<Integer, HashMap<ItemKey, PRItemDTO>> populateAuthorizedItemsOfStore(Connection conn, long runId, PRStrategyDTO inputDTO,
			List<PRItemDTO> itemList) throws OfferManagementException {
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.PROCESS_STORE_ITEMS);
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		try {
			HashMap<Integer, Integer> stateOfStores = pricingEngineDAO.getStateOfStores(conn, inputDTO.getLocationId());
			ItemKey itemKey;
			PRItemDTO storeItem;
			//Loop all store items
			for (PRItemDTO itemInfo : itemList) {
				HashMap<ItemKey, PRItemDTO> tMap = null;
				storeItem = new PRItemDTO();
				storeItem.setRunId(runId);
				storeItem.copyStoreItemInfo(itemInfo);
				itemKey = new ItemKey(storeItem.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
				
				if (itemDataMapStore.get(storeItem.getChildLocationId()) != null) {
					tMap = itemDataMapStore.get(storeItem.getChildLocationId());
				} else {
					tMap = new HashMap<ItemKey, PRItemDTO>();
				}

				if (stateOfStores.get(storeItem.getChildLocationId()) != null)
					storeItem.setStateId(stateOfStores.get(storeItem.getChildLocationId()));
				tMap.put(itemKey, storeItem);
				itemDataMapStore.put(storeItem.getChildLocationId(), tMap);
			}
			
			for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> storeItemsMap : itemDataMapStore.entrySet()) {
//				logger.debug("Setting Lig items for Store:" + storeItemsMap.getKey());
				addLigItemInItemMap(storeItemsMap.getValue());
//				logger.debug("Completed Setting Lig items for Store:" + storeItemsMap.getKey());
			}
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfStore() " + ex,
					RecommendationErrorCode.PROCESS_STORE_ITEMS);
		}
		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);
		return itemDataMapStore;
	}
	
	public void addLigItemInItemMap(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<Integer, PRItemDTO> ligItems = new HashMap<Integer, PRItemDTO>();
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		
		// Loop through each item
		for (Map.Entry<ItemKey, PRItemDTO> itemDTOEntry : itemDataMap.entrySet()) {
			PRItemDTO itemDTO = itemDTOEntry.getValue();
			// Check if it is part of lig & it is not already in the map
			if (itemDTO.getRetLirId() > 0 && ligItems.get(itemDTO.getRetLirId()) == null) {
				PRItemDTO ligItemDTO = new PRItemDTO();
				ligItemDTO.setItemCode(itemDTO.getRetLirId());
				ligItemDTO.setLir(true);
				// Copy property to lig representing item
				ligItemDTO.copyToLigRepItem(itemDTO);
				// Keep it in the hashmap
				ligItems.put(ligItemDTO.getItemCode(), ligItemDTO);
//				logger.debug("Ret Lir Id:" + ligItemDTO.getItemCode());
			}
		}

		//Update most common data at lig level for few properties
		// put it in the map
		for (Map.Entry<Integer, PRItemDTO> ligRepItemDTOEntry : ligItems.entrySet()) {
			ItemKey itemKey = new ItemKey(ligRepItemDTOEntry.getKey(), PRConstants.LIG_ITEM_INDICATOR);
			List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
			PRItemDTO ligItem = ligRepItemDTOEntry.getValue();
			// Get all its member
			for (PRItemDTO prItem : itemDataMap.values()) {
				if (prItem.getRetLirId() == itemKey.getItemCodeOrRetLirId() && !prItem.isLir()) {
					ligMembers.add(prItem);
				}
			}
			
//			if(ligItem.getItemCode() == 103675){
//				logger.debug("Stop Log");
//			}
			//Update Item Size
			if((Double)mostOccurrenceData.getMaxOccurance(ligMembers, "ItemSize") != null){
				ligItem.setItemSize((double)mostOccurrenceData.getMaxOccurance(ligMembers, "ItemSize"));
			}
			
			//Update UOM Id
			ligItem.setUOMId((String)mostOccurrenceData.getMaxOccurance(ligMembers, "UOMId"));
			
			//Update UOM Name
			ligItem.setUOMName((String)mostOccurrenceData.getMaxOccurance(ligMembers, "UOM"));
			
			//Update Dept Product Id
			if((Integer)mostOccurrenceData.getMaxOccurance(ligMembers, "DeptProductId") != null){
				ligItem.setDeptProductId((int)mostOccurrenceData.getMaxOccurance(ligMembers, "DeptProductId"));
			}
			
			//Update portfolio Product Id
			if((Integer)mostOccurrenceData.getMaxOccurance(ligMembers, "PortProductId") != null){
				ligItem.setPortfolioProductId((int)mostOccurrenceData.getMaxOccurance(ligMembers, "PortProductId"));
			}
			
			//Update category Product Id
			if((Integer)mostOccurrenceData.getMaxOccurance(ligMembers, "CatProductId") != null){
				ligItem.setCategoryProductId((int)mostOccurrenceData.getMaxOccurance(ligMembers, "CatProductId"));
			}
			
			//Update sub-category Product Id
			if((Integer)mostOccurrenceData.getMaxOccurance(ligMembers, "SubCatProductId") != null){
				ligItem.setSubCatProductId((int)mostOccurrenceData.getMaxOccurance(ligMembers, "SubCatProductId"));
			}
			
			//Update segment Product Id
			if((Integer)mostOccurrenceData.getMaxOccurance(ligMembers, "SegProductId") != null){
				ligItem.setSegmentProductId((int)mostOccurrenceData.getMaxOccurance(ligMembers, "SegProductId"));
			}
			
			if((int)mostOccurrenceData.getMaxOccurance(ligMembers, "NoOfAuthorizedStores") > 0){
				ligItem.setNoOfStoresItemAuthorized((int)mostOccurrenceData.getMaxOccurance(ligMembers, "NoOfAuthorizedStores"));
			}
			
			// 15th July 2016, if all the lig members is shipper item, then mark
			ligItem.setAllLigMemIsShipperItem(isAllLigMemShipperItem(ligMembers));
			
			
			int activeItemsCount = (int) ligMembers.stream().filter(p -> p.isActive()).count();
			int authItemsCount = (int) ligMembers.stream().filter(p -> p.isAuthorized()).count();
			
			if (activeItemsCount == 0) {
				ligItem.setActive(false);
			} else {
				ligItem.setActive(true);
			}

			if (authItemsCount == 0) {
				ligItem.setAuthorized(false);
			} else {
				ligItem.setAuthorized(true);
			}
			
			itemDataMap.put(itemKey, ligItem);
		}
	}
	
	private boolean isAllLigMemShipperItem(List<PRItemDTO> ligMembers) {
		// this Handle scenarios, all lig members are not shipper item, few member
		// alone shipper item  no member is shipper item
		boolean isAllLigMemShipperItem = false;
		boolean isShipperItemPresent = false;
		// Is there shipper item
		for (PRItemDTO lig : ligMembers) {
			if (lig.isShipperItem()) {
				isShipperItemPresent = true;
				break;
			}
		}
		// Is all item is shipper or only few
		if (isShipperItemPresent) {
			isAllLigMemShipperItem = true;
			for (PRItemDTO lig : ligMembers) {
				if (!lig.isShipperItem()) {
					isAllLigMemShipperItem = false;
					break;
				}
			}
		}
		return isAllLigMemShipperItem;
	}
	
	public HashMap<Integer, List<PRItemDTO>> populateRetLirDetailsInMap(HashMap<ItemKey, 
			PRItemDTO> itemDataMap) throws OfferManagementException{
		// HashMap<RET_LIR_ID, ArrayList<ITEM_CODE>>
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();

		try {
			for (PRItemDTO prItem : itemDataMap.values()) {
				if (prItem.getRetLirId() > 0 && !prItem.isLir()) {
					if (retLirMap.get(prItem.getRetLirId()) != null) {
						List<PRItemDTO> tList = retLirMap.get(prItem.getRetLirId());
						tList.add(prItem);
						retLirMap.put(prItem.getRetLirId(), tList);
					} else {
						List<PRItemDTO> tList = new ArrayList<PRItemDTO>();
						tList.add(prItem);
						retLirMap.put(prItem.getRetLirId(), tList);
					}
				}
			}
		} catch (Exception ex) {
			throw new OfferManagementException("Error in populateRetLirDetailsInMap() - " + ex, 
					RecommendationErrorCode.POPULATE_LIG);
		}
		return retLirMap;
	}
	
	public HashMap<Integer, HashMap<ItemKey, PRItemDTO>> removeWarehouseItems(HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore) {
		HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStoreDsdItemsOnly = new 
				HashMap<Integer, HashMap<ItemKey, PRItemDTO>>();
		// Loop through each store
		for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> storeItemMap : itemDataMapStore.entrySet()) {
			HashMap<ItemKey, PRItemDTO> dsdItems = new HashMap<ItemKey, PRItemDTO>();
			// Loop through each item in the store
			for (Map.Entry<ItemKey, PRItemDTO> storeItems : storeItemMap.getValue().entrySet()) {
				PRItemDTO storeItemDTO = storeItems.getValue();
				//Add ligs (irrespective of the dsd flag), 
				//To handle lig where members are with both warehouse and dsd flag
				//e.g. a lig may have marked as warehouse, but it may items with dsd flag
				//This will remove that lig and keep few of the lig members, this will cause issue while applying lig constraint
				if (storeItemDTO.isLir() || storeItemDTO.getDistFlag() != Constants.WAREHOUSE) {
					dsdItems.put(storeItems.getKey(), storeItemDTO);
				}
			}
			if(dsdItems.size() > 0){
				HashMap<ItemKey, PRItemDTO> finalDsdItems = new HashMap<ItemKey, PRItemDTO>();
				HashMap<Integer, Integer> lirAndItsMemberCount = new HashMap<Integer, Integer>();
				//If all the members are removed, then remove its lig also
				//Get distinct lir id
				for (PRItemDTO dsdItem : dsdItems.values()){
					if (dsdItem.isLir()) {
						lirAndItsMemberCount.put(dsdItem.getItemCode(), 0);
					}
				}
				
				//Update lig member cnt for each lig
				for (PRItemDTO dsdItem : dsdItems.values()){
					//Only lig members
					if (!dsdItem.isLir() && dsdItem.getRetLirId() > 0) {
						int totalMembers = lirAndItsMemberCount.get(dsdItem.getRetLirId());
						totalMembers = totalMembers + 1;
						lirAndItsMemberCount.put(dsdItem.getRetLirId(), totalMembers);
					}
				}

				//Get lir id and its member count
				//if there is no member then ignore that lig
				String removeLirId = "";
				for (Entry<ItemKey, PRItemDTO> dsdItem : dsdItems.entrySet()){
					PRItemDTO itemDTO = dsdItem.getValue();
					if (itemDTO.isLir()) {
						//Check if the lig has atleast one member
						if (lirAndItsMemberCount.get(itemDTO.getItemCode()) > 0) {
							finalDsdItems.put(dsdItem.getKey(), itemDTO);
						}else{
							removeLirId = removeLirId + "," + itemDTO.getItemCode();
						}
					} else {
						// add all lig members and non lig's
						finalDsdItems.put(dsdItem.getKey(), itemDTO);
					}
				}
				if(removeLirId != "")
					logger.debug("Removed Lig as there are no lig members:" + removeLirId + ", for store:" + storeItemMap.getKey());
				itemDataMapStoreDsdItemsOnly.put(storeItemMap.getKey(), finalDsdItems);
			}
		}
		//Clear existing data
		itemDataMapStore.clear();
		//itemDataMapStore = itemDataMapStoreDsdItemsOnly;
		return itemDataMapStoreDsdItemsOnly;
	}
	
	public void updateVendorIdForZoneItems(HashMap<ItemKey, PRItemDTO> itemDataMap, 
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> storeAndItsItems) {
		executionTimeLog = new ExecutionTimeLog(PRConstants.UPDATE_VENDOR_ID);		
		
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		// Loop through each zone item(lig members and non lig)
		for (Map.Entry<ItemKey, PRItemDTO> zoneItem : itemDataMap.entrySet()) {
//			 if(zoneItem.getValue().getItemCode() == 2591){
//				 logger.debug("Stop Log");
//			 }
			if (!zoneItem.getValue().isLir()) {
				List<PRItemDTO> correspondingStoreItems = new ArrayList<PRItemDTO>();
				// Get the corresponding item from all stores
				//Loop each store
				for (HashMap<ItemKey, PRItemDTO> storeItems : storeAndItsItems.values()) {
					//Loop each item in a store
//					for (Map.Entry<ItemKey, PRItemDTO> storeItemSet : storeItems.entrySet()) {
//						if (zoneItem.getKey().equals(storeItemSet.getKey())){
//							correspondingStoreItems.add(storeItemSet.getValue());
//							break;
//						}
//					}
					
					//3rd Mar 2016, Commented above lines and added above 3 lines as part of performance
					//improvement
					if(storeItems.get(zoneItem.getKey()) != null) {
						correspondingStoreItems.add(storeItems.get(zoneItem.getKey()));
					}
				}

				//If the item is present in at-least one of the store
				if (correspondingStoreItems.size() > 0) {
					// Get most common vendor id (if more than one vendor id is found as most common, then pick what
					// ever comes first)
					Object mostCommonVendorId = mostOccurrenceData.getMaxOccurance(correspondingStoreItems, "VendorId");
					if (mostCommonVendorId != null) {
						int vendorId = (int) mostCommonVendorId;
						zoneItem.getValue().setVendorId(vendorId);
					}
				}
			}
		}
		
		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);
	}
	
	public void copyPrePriceToAllMembers(HashMap<ItemKey, PRItemDTO> itemDataMap){
		HashMap<Integer, Integer> distinctRetLirId = new HashMap<Integer, Integer>();

		//get distinct ret lir id which has any one of its member as pre-priced
		for (PRItemDTO zoneItem : itemDataMap.values()) {
			if(zoneItem.getIsPrePriced() == 1 && zoneItem.getRetLirId() > 0){
				distinctRetLirId.put(zoneItem.getRetLirId(), 0);
			}
		}
		
		//Update all members of a lig as pre-priced for lig which has atleast one member marked as pre-priced
		for (PRItemDTO zoneItem : itemDataMap.values()) {
			if(distinctRetLirId.get(zoneItem.getRetLirId()) != null){
				zoneItem.setIsPrePriced(1);
			}
		}
	}

	//Find which zone each item is belongs to. A price zone may have stores from other strategy zones. For e.g. 
	//Price Zone 693 will have stores from zones 693 and 643. Assigning zone id to item will be useful in finding price,cost for items present in 
	//strategy zones 
	private void findStrategyZoneOfEachTime(Connection conn, PRStrategyDTO inputDTO, List<PRItemDTO> allStoresItem,
			HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException {
		List<Integer> distinctStrategyZoneIds = new ArrayList<Integer>();
		List<PRItemDTO> itemsNotSoldInPriceZone = new ArrayList<PRItemDTO>();
		List<Integer> distinctZoneIds = new ArrayList<Integer>();
		//6th June 2016 - bug fix, as hashmap doesn't maintain the element order,
		//zone with higher no of store is not taken always. changed to linkedhashmap
//		HashMap<Integer, Integer> zoneAndItsStoreCount = new HashMap<Integer, Integer>();
		LinkedHashMap<Integer, Integer> zoneAndItsStoreCount = new LinkedHashMap<Integer, Integer>();
		
		/***PROM-2214 changes**/
		HashMap<Integer, Integer> itemAndStrCount = new HashMap<Integer, Integer>();
		/***PROM-2214 changes**/

		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO(executionTimeLogs);
		PricingEngineService pricingEngineService = new PricingEngineService();
		int newPriceZoneId = 0;
		boolean byPassAuth = Boolean.parseBoolean(PropertyManager.getProperty("BY_PASS_AUTHORIZATION", "FALSE"));
		try {
			//31st May 2015: the zones which has more no of stores will be considered as price zone
			//If more than one zone has same no of stores, then what ever comes first it will be taken
			//The picked price zone may be different from actual price zone for which recommendation is running
			//this is done to handle where zone 693 has store only from zone 641 and 642 and not even a
			//single store from 693
			distinctZoneIds = getDistinctZoneIds(allStoresItem);
			// Find the no. of stores inside each zone, sorted by no of stores in descending
			
			//NU:: 15th Feb 2018, for GE stores are not directly linked to zones,
			//it has to take it from store item map
			boolean useStoreItemMapForZone = Boolean.parseBoolean(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
			
			/***PROM-2214 changes**/
			boolean getauthorisedItStores = Boolean.parseBoolean(PropertyManager.getProperty("GET_STORES_FROM_STORE_ITEM_MAP", "FALSE"));
			/***PROM-2214 changes end **/
			
			if (useStoreItemMapForZone) {
				zoneAndItsStoreCount = pricingEngineService.getZoneStoreCount(allStoresItem);
			}
			else {
				zoneAndItsStoreCount =  pricingEngineDAO.getZoneStoreCount(conn, distinctZoneIds);	
			}
			
			/*** PROM-2214 changes **/
			// Added for AZ by Karishma on 10/27/21 to get the store count for each item
			// from new table created
			if (getauthorisedItStores) {
				itemAndStrCount = pricingEngineDAO.getItemStoreCount(conn, inputDTO.getLocationId(),
						inputDTO.isGlobalZone(), inputDTO.getProductId());
			}
			/*** PROM-2214 changes end **/
			for(Map.Entry<Integer, Integer> zoneAndStoreCount : zoneAndItsStoreCount.entrySet()){
				newPriceZoneId = zoneAndStoreCount.getKey();
				break;
			}
			logger.info("Actual Price Zone: " + inputDTO.getLocationId() + ",New" +
					" Price Zone: " + newPriceZoneId);

			// Get distinct zone id's
			distinctStrategyZoneIds = getStrategyZoneIds(newPriceZoneId, allStoresItem);

			
			Map<Integer, List<PRItemDTO>> zoneItemMap = getZoneItemMap(allStoresItem, newPriceZoneId);
			
			// Update item with price zone and find items which are not sold in
			// price zone
			// Loop each zone item
			for (PRItemDTO itemDTO : itemDataMap.values()) {
				// Ignore lig items
				if (!itemDTO.isLir()) {
					boolean isItemSoldInPriceZone = false;
					String priceZoneNum = "";
					/*
					 * for (PRItemDTO itemInfo : allStoresItem) { // If an item is sold in even any
					 * of the price zone // store, then // consider that as price zone level item if
					 * (itemDTO.getItemCode() == itemInfo.getItemCode() && itemInfo.getPriceZoneId()
					 * == newPriceZoneId) { isItemSoldInPriceZone = true; priceZoneNum =
					 * itemInfo.getPriceZoneNo(); break; } }
					 */
					
					if (zoneItemMap.containsKey(itemDTO.getItemCode())) {
						List<PRItemDTO> items = zoneItemMap.get(itemDTO.getItemCode());
						PRItemDTO itemInfo = items.stream().findFirst().get();
						priceZoneNum = itemInfo.getPriceZoneNo();
						isItemSoldInPriceZone = true;
					}
					
					// Keep items not sold in price zone in a separate list
					if (!isItemSoldInPriceZone) {
						itemsNotSoldInPriceZone.add(itemDTO);
					} else {
						itemDTO.setPriceZoneId(newPriceZoneId);
						itemDTO.setPriceZoneNo(priceZoneNum);
					}
					
					if(byPassAuth) {
						/*** PROM-2214 changes **/
						// Added for AZ by Karishma on 10/27/21 to get the store count for each item
						if (getauthorisedItStores) {
							if (itemAndStrCount.containsKey(itemDTO.getItemCode())) {
								int authCount = itemAndStrCount.get(itemDTO.getItemCode());
								itemDTO.setNoOfStoresItemAuthorized(authCount);
							} else {
								itemDTO.setNoOfStoresItemAuthorized(0);
								/*** PROM-2214 changes end **/
							}

						} else if (zoneAndItsStoreCount.containsKey(itemDTO.getPriceZoneId())) {
							int authCount = zoneAndItsStoreCount.get(itemDTO.getPriceZoneId());
							itemDTO.setNoOfStoresItemAuthorized(authCount);
						}	
					}
				}
			}
			logger.info("There are " + itemsNotSoldInPriceZone.size() + " items which are not sold in Price Zone : "
					+ inputDTO.getLocationId() + " but sold in strategy zones ");
			// Update items sold in strategy zone with appropriate zone id
			assignStrategyZoneToItem(conn, distinctStrategyZoneIds, itemsNotSoldInPriceZone, allStoresItem);
		} catch (Exception ex) {
			logger.error("Exception in findStrategyZoneOfEachTime" + ex.toString(), ex);
			throw new GeneralException(ex.toString());
		}
	}
	
	private Map<Integer, List<PRItemDTO>> getZoneItemMap(List<PRItemDTO> allStoreItem, int newZoneId) {
		return allStoreItem.parallelStream().filter(p -> p.getPriceZoneId() == newZoneId)
				.collect(Collectors.groupingBy(PRItemDTO::getItemCode));
	}
	
	private void findStrategyZoneOfEachTimeForGE(Connection conn, PRStrategyDTO inputDTO, List<PRItemDTO> allStoresItem,
			HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException {
		List<Integer> distinctStrategyZoneIds = new ArrayList<Integer>();
		List<PRItemDTO> itemsNotSoldInPriceZone = new ArrayList<PRItemDTO>();
		int actualPriceZoneId = 0;
		
		try {
			boolean recommendDSDZonesSeparately = Boolean.parseBoolean(PropertyManager.getProperty("RECOMMEND_DSD_ZONES_SEPARATE", "FALSE"));
			// Dinesh:: 01 Mar 2018, Assign actual Zone id as a Primary Zone id.
			// In GE, Primary zone and Matching zone items were recommended together, 
			// but for DSD zone items and Primary zone items assign Primary zone id and 
			// for matching zone items assign Warehouse zone id and donot consider DSD Zones in matching zones 
			// As the price and cost are setup at primary zone id of DSD zones
			actualPriceZoneId = inputDTO.getLocationId();
			PriceZoneDTO priceZoneDTO = new RetailPriceZoneDAO().getPriceZoneInfo(conn, actualPriceZoneId);
			String priceZoneNum = priceZoneDTO.getPriceZoneNum();
			// To find Primary Zone Number
			for (PRItemDTO prItemDTO : allStoresItem) {
				if (prItemDTO.getPriceZoneId() == actualPriceZoneId && prItemDTO.getPriceZoneNo() != null
						&& !prItemDTO.getPriceZoneNo().trim().isEmpty()) {
					priceZoneNum = prItemDTO.getPriceZoneNo();
					break;
				}
			}
			logger.info("Actual Price Zone: " + inputDTO.getLocationId()+
					" Price Zone Number: " + priceZoneNum);

			// Get distinct zone id's
			// filter DSD Zone items from allStoreItem list to get distinctStrategyZoneIds other than Primary Zone
			List<PRItemDTO> nonDSDZoneItems = allStoresItem;
			if(!recommendDSDZonesSeparately) {
				nonDSDZoneItems = allStoresItem.stream().filter(item-> !item.isDSDItem()).collect(Collectors.toList());	
			}
			distinctStrategyZoneIds = getStrategyZoneIds(actualPriceZoneId, nonDSDZoneItems);

			// Update item with price zone and find items which are not sold in
			// price zone
			// Loop each zone item
			for (PRItemDTO itemDTO : itemDataMap.values()) {
				// Ignore lig items
				if (!itemDTO.isLir()) {
					boolean isItemSoldInPriceZone = false;
					
					for (PRItemDTO itemInfo : allStoresItem) {
						// If an item is sold in even any of the price zone
						// store, then
						// consider that as price zone level item
						if (itemDTO.getItemCode() == itemInfo.getItemCode()
								&& itemInfo.getPriceZoneId() == actualPriceZoneId) {
							isItemSoldInPriceZone = true;
						}
						// if it DSD Zone item, then assign primary zone number
						else if(itemDTO.getItemCode() == itemInfo.getItemCode()
								&& itemInfo.isDSDItem() && !recommendDSDZonesSeparately){
							isItemSoldInPriceZone = true;
						}
						if(isItemSoldInPriceZone){
							break;
						}
					}
					// Keep items not sold in price zone in a separate list
					if (!isItemSoldInPriceZone) {
						itemsNotSoldInPriceZone.add(itemDTO);
					} else {
						itemDTO.setPriceZoneId(actualPriceZoneId);
						itemDTO.setPriceZoneNo(priceZoneNum);
					}
				}
			}
			logger.info("There are " + itemsNotSoldInPriceZone.size() + " items which are not sold in Price Zone : "
					+ inputDTO.getLocationId() + " but sold in strategy zones ");
			// Update items sold in strategy zone with appropriate zone id
			assignStrategyZoneToItem(conn, distinctStrategyZoneIds, itemsNotSoldInPriceZone, nonDSDZoneItems);
		} catch (Exception ex) {
			logger.error("Exception in findStrategyZoneOfEachTime" + ex.toString(), ex);
			throw new GeneralException(ex.toString());
		}
	}
	
	private void assignStrategyZoneToItem(Connection conn, List<Integer> distinctStrategyZoneIds,
			List<PRItemDTO> itemsNotSoldInPriceZone, List<PRItemDTO> allStoresItem) throws Exception {
		// Decide which price zone to pick, for items not sold in price zones	
		//Pick zone which has more no of stores in it
		HashMap<Integer, String> tempDistinctZoneId = new HashMap<Integer, String>();
		//6th June 2016 - bug fix, as hashmap doesn't maintain the element order,
		// zone with higher no of store is not taken always. 
		// changed to linkedhashmap
		// HashMap<Integer, Integer> zoneAndItsStoreCount = new HashMap<Integer, Integer>();
		LinkedHashMap<Integer, Integer> zoneAndItsStoreCount = new LinkedHashMap<Integer, Integer>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO(executionTimeLogs);
		
		if(itemsNotSoldInPriceZone.size() > 0){
			// Find the no. of stores inside each zone
//			zoneAndItsStoreCount = pricingEngineDAO.getZoneStoreCount(conn, distinctStrategyZoneIds);
			
			//NU:: 15th Feb 2018, for GE stores are not directly linked to zones,
			//it has to take it from store item map
			boolean useStoreItemMapForZone = Boolean.parseBoolean(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
			if(useStoreItemMapForZone) {
				zoneAndItsStoreCount =  new PricingEngineService().getZoneStoreCount(allStoresItem);
			} else {
				zoneAndItsStoreCount =  pricingEngineDAO.getZoneStoreCount(conn, distinctStrategyZoneIds);	
			}
			
			//Loop each item
			for(PRItemDTO itemDTO : itemsNotSoldInPriceZone){
				tempDistinctZoneId.clear();
				for (PRItemDTO itemInfo : allStoresItem) {
					if(itemDTO.getItemCode() == itemInfo.getItemCode() && itemInfo.getPriceZoneId() > 0){
						// Get distinct zone id's
						tempDistinctZoneId.put(itemInfo.getPriceZoneId(), itemInfo.getPriceZoneNo());
					}
				}
		
				if(tempDistinctZoneId.size() > 0){
					int choosenStrategyZoneId = 0;
					String choosenStrategyZoneNo = "";
					// Assign price zone which has most number of stores		
					for(Map.Entry<Integer, Integer> zoneAndStoreCount : zoneAndItsStoreCount.entrySet()){
						//Check if zone is present in distinct zone, as the map is sorted by no' of store, first occurrence
						//is picked 
						if(tempDistinctZoneId.get(zoneAndStoreCount.getKey()) != null){
							choosenStrategyZoneId = zoneAndStoreCount.getKey();
							choosenStrategyZoneNo = tempDistinctZoneId.get(zoneAndStoreCount.getKey());
							break;
						}
					}
					if(useStoreItemMapForZone) {
						List<SecondaryZoneRecDTO> secondaryZones = new ArrayList<>();
						tempDistinctZoneId.forEach((priceZoneId, priceZoneNo) -> {
							SecondaryZoneRecDTO secondaryZoneRecDTO =  new SecondaryZoneRecDTO();
							secondaryZoneRecDTO.setPriceZoneId(priceZoneId);
							secondaryZoneRecDTO.setPriceZoneNo(priceZoneNo);
							secondaryZoneRecDTO.setProductLevelId(Constants.ITEMLEVELID);
							secondaryZoneRecDTO.setProductId(itemDTO.getItemCode());
							secondaryZones.add(secondaryZoneRecDTO);
						});
						itemDTO.setSecondaryZones(secondaryZones);
						logger.debug("Item " + itemDTO.getItemCode() + " is assigned with secondary zones: " + secondaryZones.size());
						logger.debug("Item " + itemDTO.getItemCode() + " is assigned with strategy zone Id: " + choosenStrategyZoneId);	
					}
					itemDTO.setPriceZoneId(choosenStrategyZoneId);
					itemDTO.setPriceZoneNo(choosenStrategyZoneNo);
				}
			}
		}
	}
	
	private List<Integer> getStrategyZoneIds(int priceZoneId, List<PRItemDTO> allStoresItem){
		HashMap<Integer, Integer> distinctStrategyZones = new HashMap<Integer, Integer>();
		List<Integer> distinctStrategyZoneIds = new ArrayList<Integer>();
		for (PRItemDTO itemInfo : allStoresItem) {
			// Ignore price zones
			if (distinctStrategyZones.get(itemInfo.getPriceZoneId()) == null
					&& itemInfo.getPriceZoneId() != priceZoneId && itemInfo.getPriceZoneId() > 0) {
				distinctStrategyZones.put(itemInfo.getPriceZoneId(), 0);
				distinctStrategyZoneIds.add(itemInfo.getPriceZoneId());
			}
		}
		return distinctStrategyZoneIds;
	}
	
	private List<Integer> getDistinctZoneIds(List<PRItemDTO> allStoresItem) {
		HashMap<Integer, Integer> distinctZones = new HashMap<Integer, Integer>();
		List<Integer> distinctZoneIds = new ArrayList<Integer>();
		for (PRItemDTO itemInfo : allStoresItem) {
			if (distinctZones.get(itemInfo.getPriceZoneId()) == null && itemInfo.getPriceZoneId() > 0) {
				distinctZones.put(itemInfo.getPriceZoneId(), 0);
				distinctZoneIds.add(itemInfo.getPriceZoneId());
			}
		}
		return distinctZoneIds;
	}
	
	public List<String> getPriceAndStrategyZoneNos(List<PRItemDTO> allStoresItem) {
		HashMap<Integer, Integer> distinctStrategyZones = new HashMap<Integer, Integer>();
		List<String> distinctStrategyZoneNos = new ArrayList<String>();
		for (PRItemDTO itemInfo : allStoresItem) {
			// Ignore price zones
			if (distinctStrategyZones.get(itemInfo.getPriceZoneId()) == null && itemInfo.getPriceZoneId() > 0) {
				distinctStrategyZones.put(itemInfo.getPriceZoneId(), 0);
				distinctStrategyZoneNos.add(itemInfo.getPriceZoneNo());
			}
			
			if (itemInfo.getSecondaryZones() != null && itemInfo.getSecondaryZones().size() > 0) {
				itemInfo.getSecondaryZones().forEach(secZone -> {
					if (distinctStrategyZones.get(secZone.getPriceZoneId()) == null && secZone.getPriceZoneId() > 0) {
						distinctStrategyZones.put(secZone.getPriceZoneId(), 0);
						distinctStrategyZoneNos.add(secZone.getPriceZoneNo());
					}		
				});
			}
		}
		return distinctStrategyZoneNos;
	}
	
	public List<PRItemDTO> getAuthorizedItems(Connection conn, int locationLevelId, int locationId, int productLeveId, int productId) throws OfferManagementException {
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		itemDAO = new ItemDAO();
		try {
			itemList = itemDAO.getAuthorizedItems(conn, locationLevelId, locationId, productLeveId, productId);
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfZoneAndStore() " + ex,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		}
		return itemList;
	}
	
	public HashMap<ItemKey, PRItemDTO> populateAuthorizedItemsOfZone(PRStrategyDTO inputDTO, List<PRItemDTO> itemList)
			throws OfferManagementException {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.PROCESS_ZONE_ITEMS);
		try {
			ItemKey itemKey;
			PRItemDTO zoneItem;

			for (PRItemDTO itemInfo : itemList) {
				zoneItem = new PRItemDTO();
				zoneItem.setChildLocationLevelId(inputDTO.getLocationLevelId());
				zoneItem.setChildLocationId(inputDTO.getLocationId());
				zoneItem.copyZoneItemInfo(itemInfo);
				itemKey = new ItemKey(zoneItem.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
				if (itemDataMap.get(itemKey) == null) {
					itemDataMap.put(itemKey, zoneItem);
				} else {
					// Update no of stores in which items are authorized to sell
					PRItemDTO itemDTO = itemDataMap.get(itemKey);
					itemDTO.setNoOfStoresItemAuthorized(itemDTO.getNoOfStoresItemAuthorized() + 1);
				}
				
			}
			
			//Find correct zone id for each item
			addLigItemInItemMap(itemDataMap);
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in getAuthorizedItemsOfZone() " + ex,
					RecommendationErrorCode.PROCESS_ZONE_ITEMS);
		}
		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);
		return itemDataMap;
	}
	
	public List<PRItemDTO> getAuthorizedItems(Connection conn, int productLevelId, int productId, List<Integer> stores)
			throws OfferManagementException {
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_ITEMS);
		itemDAO = new ItemDAO();
		try {
			itemList = itemDAO.getAuthorizedItems(conn, productLevelId, productId, stores);
			executionTimeLog.setEndTime();
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in getAuthorizedItems() " + ex,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		}
		executionTimeLogs.add(executionTimeLog);
		return itemList;
	}
}
