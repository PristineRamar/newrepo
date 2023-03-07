package com.pristine.service.offermgmt.oos;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dao.offermgmt.prediction.PredictionReportDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dto.AdKey;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;

public class OOSCandidateItemImpl implements OOSCandidateItem{

	private static Logger  logger = Logger.getLogger("OOSCandidateItem");
	
	
	public List<PRItemDTO> getAuthorizedItemOfWeeklyAd(Connection conn, int productLevelId, int productId, int locationLevelId, int locationId,
			int weekCalendarId, int chainId) throws GeneralException {
		List<PRItemDTO> finalItemList = null;
		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			finalItemList = getStoreAuthorizedItemOfWeeklyAd(conn, productLevelId, productId, locationLevelId, locationId, weekCalendarId, chainId);
		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
			finalItemList = getStoreListAuthorizedItemOfWeeklyAd(conn, productLevelId, productId, locationLevelId, locationId, weekCalendarId,
					chainId);
		} else if (locationLevelId == Constants.ZONE_LEVEL_ID) {
			PredictionReportDAO predictionReportDAO = new PredictionReportDAO();
			finalItemList = predictionReportDAO.getCandidateItemsForZoneForecastFromRec(conn, locationLevelId, locationId, productLevelId, productId,
					weekCalendarId);
		}
		return finalItemList;
	}

	/**
	 * Return item codes of authorized items advertised in first page of the weekly ad
	 */
	private List<PRItemDTO> getStoreAuthorizedItemOfWeeklyAd(Connection conn, int productLevelId, int productId,
			int locationLevelId, int locationId, int weekCalendarId, int chainId) throws GeneralException {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		
		// Get all authorized items for given location and product combination
		// The following method will support only store level location
		List<PRItemDTO> authorizedItems = oosAnalysisDAO.getAuthorizedItems(conn, productLevelId, productId, locationLevelId, locationId);
		logger.debug("Total No of Authorized Items:" + authorizedItems.size());
		
		// Get all the authorized and promoted items from weekly ad
		List<PRItemDTO> promotedItems = oosAnalysisDAO.getPromotionDetails(conn, weekCalendarId, chainId, locationId, locationLevelId, productLevelId,
				productId);
		logger.debug("Total No of Promotion Items:" + promotedItems.size());
		
		//Get only 1st and last page, gate a and gate c items 
		///List<PRItemDTO> filteredList =  filterByWeeklyPageNO(promotedItems);
		
		//Keep only distinct items. It is possible that same item falls in different promotion (probably bug in promo loader)
		//List<PRItemDTO> finalList = getDistinctItem(promotedItems, locationLevelId, locationId);
		
		logger.debug("Before clearing duplicate items promoted list size was " + promotedItems.size());
		//Eliminate duplicates.
		List<PRItemDTO> finalList = clearDuplicates(promotedItems);
		logger.debug("After clearing duplicate items promoted list size is " + promotedItems.size());
		
		// Validate promotion list with authorized items
		List<PRItemDTO> finalItemList = filterAuthorizedItems(authorizedItems, finalList);
		logger.debug("Item count after authorization:" + finalItemList.size());
		
		//Clear duplicates from finalList
		//NU: 9th Sep 2016, not needed as already covered in clearDuplicates()
//		finalItemList = filterDiffPromoSameItem(finalItemList);
//		logger.debug("Item count after filterDiffPromoSameItem:" + finalItemList.size());
		
		logger.debug("Total No of Items for OOS Items:" + finalItemList.size());
		return finalItemList;
	}
	
	
	public List<PRItemDTO> clearDuplicates(List<PRItemDTO> itemList){
		List<PRItemDTO> filteredItems = new ArrayList<>();
		filteredItems = filterSamePagePromos(itemList);
		filteredItems = filterDiffPageItems(filteredItems);
		filteredItems = filterDiffPromoSameLig(filteredItems);
		filteredItems = filterDiffPromoSameItem(filteredItems);
		return filteredItems;
	}
	
	/**
	 * eliminates items if same promo is duplicated in same page.
	 * @param itemList
	 * @param distinctItems
	 */
	private List<PRItemDTO> filterSamePagePromos(List<PRItemDTO> itemList){
		HashMap<Integer, List<PRItemDTO>> promoPageMap = new HashMap<>();  
		List<PRItemDTO> filteredItems = new ArrayList<>();
		for(PRItemDTO prItemDTO: itemList){
			//If the promotion appears more than once in the same page, then any one of the block is picked 
			//(there is no definite logic in picking the block)
			if(promoPageMap.get(prItemDTO.getPageNumber()) == null){
				List<PRItemDTO> promotions = new ArrayList<>();
				promotions.add(prItemDTO);
				promoPageMap.put(prItemDTO.getPageNumber(), promotions);
			}
			else{
				List<PRItemDTO> promotions = promoPageMap.get(prItemDTO.getPageNumber());
				boolean canBeAdded = true;
				for(PRItemDTO tempDTO: promotions){
					//if same item is duplicated in same page, ignore it and keep adding other items.
					if(tempDTO.getPromoDefinitionId() == prItemDTO.getPromoDefinitionId()
							&& tempDTO.getItemCode() == prItemDTO.getItemCode()){
						canBeAdded = false;
					}
				}
				if(canBeAdded){
					promotions.add(prItemDTO);
					promoPageMap.put(prItemDTO.getPageNumber(), promotions);
				}
			}
		}
		//Add all the distinct entries across the pages. 
		for(Map.Entry<Integer, List<PRItemDTO>> entry: promoPageMap.entrySet()){
			filteredItems.addAll(entry.getValue());
		}
		
		return filteredItems;
	}
	
	/**
	 * 
	 * @param itemList
	 * @return filteredItems
	 */
	private List<PRItemDTO> filterDiffPageItems(List<PRItemDTO> itemList){
		HashMap<Long, List<PRItemDTO>> promoPageMap = new HashMap<>(); 
		List<PRItemDTO> filteredItems = new ArrayList<>();
		for(PRItemDTO prItemDTO: itemList){
			//If the promotion appears more than once in the same page, then any one of the block is picked 
			//(there is no definite logic in picking the block)
			if(promoPageMap.get(prItemDTO.getPromoDefinitionId()) == null){
				List<PRItemDTO> promotions = new ArrayList<>();
				promotions.add(prItemDTO);
				promoPageMap.put(prItemDTO.getPromoDefinitionId(), promotions);
			}
			else{
				List<PRItemDTO> promotions = promoPageMap.get(prItemDTO.getPromoDefinitionId());
				boolean canBeAdded = true;
				for(PRItemDTO tempDTO: promotions){
					//if same item is duplicated in different page, ignore it and keep adding other items.
					if(tempDTO.getPageNumber() != prItemDTO.getPageNumber()
							&& tempDTO.getItemCode() == prItemDTO.getItemCode()){
						canBeAdded = false;
					}
				}
				if(canBeAdded){
					promotions.add(prItemDTO);
					promoPageMap.put(prItemDTO.getPromoDefinitionId(), promotions);
				}
			}
		}
		//Add all the distinct entries across the pages. 
		for(Map.Entry<Long, List<PRItemDTO>> entry: promoPageMap.entrySet()){
			filteredItems.addAll(entry.getValue());
		}
		
		return filteredItems;
	}
	
	/**
	 * eliminates items if same LIG is loaded in to different promotions.
	 * @param itemList
	 * @param distinctItems
	 */
	private List<PRItemDTO> filterDiffPromoSameLig(List<PRItemDTO> itemList){
		HashMap<Integer, List<PRItemDTO>> promoMap = new HashMap<>(); 
		List<PRItemDTO> filteredItems = new ArrayList<>();
		for(PRItemDTO prItemDTO: itemList){
			//If the promotion appears more than once in the same page, then any one of the block is picked 
			//(there is no definite logic in picking the block)
			if(prItemDTO.getRetLirPromoKey() != -1){
				if(promoMap.get(prItemDTO.getRetLirPromoKey()) == null){
					List<PRItemDTO> promotions = new ArrayList<>();
					promotions.add(prItemDTO);
					promoMap.put(prItemDTO.getRetLirPromoKey(), promotions);
				}
				else{
					List<PRItemDTO> promoItems = promoMap.get(prItemDTO.getRetLirPromoKey());
					boolean canBeAdded = true;
					PRItemDTO itemToBeDeleted = null;
					for(PRItemDTO tempDTO: promoItems){
						//if same item is duplicated in different page, ignore it and keep adding other items.
						if(tempDTO.getItemCode() == prItemDTO.getItemCode()){
							Date existingPromoDate = tempDTO.getPromoModifiedDate() == null ? tempDTO.getPromoCreatedDate()
									: tempDTO.getPromoModifiedDate();
							Date currentPromoDate = prItemDTO.getPromoModifiedDate() == null ? prItemDTO.getPromoCreatedDate()
									: prItemDTO.getPromoModifiedDate();
							if(existingPromoDate.after(currentPromoDate)){
								canBeAdded = false;
							}
							else{
								itemToBeDeleted = tempDTO;
							}
						}
					}
					
					if(canBeAdded){
						promoItems.add(prItemDTO);
						promoMap.put(prItemDTO.getRetLirPromoKey(), promoItems);
						if(itemToBeDeleted != null){
							promoItems.remove(itemToBeDeleted);
							promoMap.put(prItemDTO.getRetLirPromoKey(), promoItems);
						}
					}
				}
			}
			else{
				filteredItems.add(prItemDTO);
			}
		}
		//Add all the distinct entries across the pages. 
		for(Map.Entry<Integer, List<PRItemDTO>> entry: promoMap.entrySet()){
			filteredItems.addAll(entry.getValue());
		}
		
		return filteredItems;
	}
	
	/**
	 * eliminates items if same item is loaded in to different promotions. 
	 * @param itemList
	 * @param distinctItems
	 */
	private List<PRItemDTO> filterDiffPromoSameItem(List<PRItemDTO> itemList){
		HashMap<Integer, PRItemDTO> promoMap = new HashMap<>(); 
		List<PRItemDTO> filteredItems = new ArrayList<>();
		for(PRItemDTO prItemDTO: itemList){
			//If the promotion appears more than once in the same page, then any one of the block is picked 
			//(there is no definite logic in picking the block)
				if(promoMap.get(prItemDTO.getItemCode()) == null){
					promoMap.put(prItemDTO.getItemCode(), prItemDTO);
				}
				else{
					PRItemDTO tempDTO = promoMap.get(prItemDTO.getItemCode());
					boolean canBeAdded = true;
						//if same item is duplicated in different page, ignore it and keep adding other items.
						if(tempDTO.getItemCode() == prItemDTO.getItemCode()){
							Date existingPromoDate = tempDTO.getPromoModifiedDate() == null ? tempDTO.getPromoCreatedDate()
									: tempDTO.getPromoModifiedDate();
							Date currentPromoDate = prItemDTO.getPromoModifiedDate() == null ? prItemDTO.getPromoCreatedDate()
									: prItemDTO.getPromoModifiedDate();
							if(existingPromoDate.after(currentPromoDate)){
								canBeAdded = false;
							}
							else{
								canBeAdded = true;
							}
						}
					if(canBeAdded){
						promoMap.put(prItemDTO.getItemCode(), prItemDTO);
					}
				}
			}
		//Add all the distinct entries across the pages. 
		filteredItems.addAll(promoMap.values());
		
		return filteredItems;
	}
	
	
//	private List<PRItemDTO> filterByWeeklyPageNO(List<PRItemDTO> promotedItems){
//		List<PRItemDTO> finalItemList = new ArrayList<PRItemDTO>();
//		String gateAAndGateCPageNo = PropertyManager.getProperty("GATE_A_AND_GATE_C_PAGE_NO");
//		HashSet<Integer> candidatePageNo = new HashSet<Integer>();
//		
//		//add gate A and gate C items
//		if (gateAAndGateCPageNo != "") {
//			String[] pageNos = gateAAndGateCPageNo.split(",");
//			for (String pageNo : pageNos) {
//				candidatePageNo.add(Integer.valueOf(pageNo));
//			}
//		}
//		
//		//Find max page no
////		int maxPageNo = 0;
////		for(PRItemDTO prItemDTO: promotedItems){
////			//ignore gate page no's
////			if(!candidatePageNo.contains(prItemDTO.getPageNumber())){
////				if(prItemDTO.getPageNumber() > maxPageNo)
////					maxPageNo = prItemDTO.getPageNumber();	
////			}
////		}
//		
//		//add page 1
//		candidatePageNo.add(1);
////		candidatePageNo.add(maxPageNo);
//		for(PRItemDTO prItemDTO: promotedItems){
//			//If page number belongs to candidate page no's
//			if(candidatePageNo.contains(prItemDTO.getPageNumber())){
//				finalItemList.add(prItemDTO);
//			}
//		}
//		return finalItemList;
//	}
	
	
	/*private List<PRItemDTO> getDistinctItem(List<PRItemDTO> itemList, int locationLevelId, int locationId) {
		List<PRItemDTO> finalList = new ArrayList<PRItemDTO>();
		HashMap<OOSCandidateDistinctItemKey, PRItemDTO> distinctItems = new HashMap<OOSCandidateDistinctItemKey, PRItemDTO>();

		for (PRItemDTO prItemDTO : itemList) {
			OOSCandidateDistinctItemKey distinctItemKey;
			distinctItemKey = new OOSCandidateDistinctItemKey(locationLevelId, locationId, prItemDTO.getItemCode(),
					prItemDTO.getPageNumber(), prItemDTO.getBlockNumber(), prItemDTO.getPromoTypeId(),
					prItemDTO.getDisplayTypeId());

			if (distinctItems.get(distinctItemKey) == null)
				distinctItems.put(distinctItemKey, prItemDTO);
		}
		finalList.addAll(distinctItems.values());
		return finalList;
	}*/
	
	private List<PRItemDTO> filterAuthorizedItems(List<PRItemDTO> authorizedList, List<PRItemDTO> promotionalList) {
		List<PRItemDTO> finalItemList = new ArrayList<PRItemDTO>();
		Set<Integer> unauthorizedItems = new HashSet<Integer>();
		HashMap<Integer, PRItemDTO> authorizedItemCodes  = new HashMap<Integer, PRItemDTO>();
		for (PRItemDTO authorizedItem : authorizedList) {
			authorizedItemCodes.put(authorizedItem.getItemCode(), authorizedItem);
		}
		
		for (PRItemDTO prItemDTO : promotionalList) {
			if (authorizedItemCodes.get(prItemDTO.getItemCode()) != null) {
				PRItemDTO authorizedItem = authorizedItemCodes.get(prItemDTO.getItemCode());
				prItemDTO.setDeptProductId(authorizedItem.getDeptProductId());
				prItemDTO.setPortfolioProductId(authorizedItem.getPortfolioProductId());
				prItemDTO.setCategoryProductId(authorizedItem.getCategoryProductId());
				prItemDTO.setSubCatProductId(authorizedItem.getSubCatProductId());
				prItemDTO.setSegmentProductId(authorizedItem.getSegmentProductId());
				prItemDTO.setCategoryName(authorizedItem.getCategoryName());
				prItemDTO.setStoreNo(authorizedItem.getStoreNo());
				prItemDTO.setDistrictName(authorizedItem.getDistrictName());
				prItemDTO.setRetLirId(authorizedItem.getRetLirId());
				finalItemList.add(prItemDTO);
			} else {
				unauthorizedItems.add(prItemDTO.getItemCode());
			}
		}
		
//		for (PRItemDTO authorizedItem : authorizedList) {
//			for (PRItemDTO prItemDTO : promotionalList) {
//				if (authorizedItem.getItemCode() == prItemDTO.getItemCode()) {
//					prItemDTO.setDeptProductId(authorizedItem.getDeptProductId());
//					prItemDTO.setPortfolioProductId(authorizedItem.getPortfolioProductId());
//					prItemDTO.setCategoryProductId(authorizedItem.getCategoryProductId());
//					prItemDTO.setSubCatProductId(authorizedItem.getSubCatProductId());
//					prItemDTO.setSegmentProductId(authorizedItem.getSegmentProductId());
//					prItemDTO.setCategoryName(authorizedItem.getCategoryName());
//					prItemDTO.setStoreNo(authorizedItem.getStoreNo());
//					prItemDTO.setDistrictName(authorizedItem.getDistrictName());
//					prItemDTO.setRetLirId(authorizedItem.getRetLirId());
//					finalItemList.add(prItemDTO);
//					break;
//				} else {
//					unauthorizedItems.add(prItemDTO.getItemCode());
//				}
//			}
//		}
		logger.debug("unauthorized items:" + PRCommonUtil.getCommaSeperatedStringFromIntSet(unauthorizedItems));
		return finalItemList;
	}
	
	public List<OOSItemDTO> getOOSCandidateItems(Connection conn, int locationLevelId, int locationId,
			int calendarId) throws GeneralException{
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		List<OOSItemDTO> oosCandidateItems = oosAnalysisDAO.getOOSCandidateItems(conn, locationLevelId, locationId, calendarId);
		return oosCandidateItems;
	}
	
	
	public List<OOSItemDTO> getItemsWithChainLevelForecast(Connection conn, int locationLevelId, String locationIds, int calendarId,
			int topsStoreListId, int guStoreListId, boolean isPageBlockLevelReport) throws GeneralException, CloneNotSupportedException{
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		List<OOSItemDTO> finalItemList = new ArrayList<>();
		List<OOSItemDTO> oosCandidateItems = oosAnalysisDAO.getOOSCandidateItemsForChainForecast(conn, locationLevelId,
				locationIds, calendarId, topsStoreListId, guStoreListId);
		HashMap<Integer, List<OOSItemDTO>> itemMap = new HashMap<>();
		
		//Group by item
		for(OOSItemDTO oosItemDTO: oosCandidateItems){
			//Filter blocks with noOfLigOrNonLig as 1
			if (isPageBlockLevelReport || oosItemDTO.getNoOfLigOrNonLig() == 1) {
				List<OOSItemDTO> itemList = null;
				if (itemMap.get(oosItemDTO.getProductId()) == null) {
					itemList = new ArrayList<>();
				} else {
					itemList = itemMap.get(oosItemDTO.getProductId());
				}
				itemList.add(oosItemDTO);
				itemMap.put(oosItemDTO.getProductId(), itemList);
			}
		}

		//Loop each item
		for(Map.Entry<Integer, List<OOSItemDTO>> entry: itemMap.entrySet()){
			OOSItemDTO finalItem = null;
				for(OOSItemDTO oosItemDTO: entry.getValue()){
					if(finalItem == null){
						finalItem = (OOSItemDTO) oosItemDTO.clone();
					}
					else{
						//setData(finalItem, oosItemDTO);
						if(oosItemDTO.getLocationId() == topsStoreListId){
							finalItem.setWeeklyPredictedMovementTops(oosItemDTO.getWeeklyPredictedMovementTops());
							finalItem.setAdPageNo(oosItemDTO.getAdPageNo());
							finalItem.setBlockNo(oosItemDTO.getBlockNo());	
						}
						else{
							finalItem.setWeeklyPredictedMovementGU(oosItemDTO.getWeeklyPredictedMovementGU());
							finalItem.setAdPageNoGU(oosItemDTO.getAdPageNoGU());
							finalItem.setBlockNoGU(oosItemDTO.getBlockNoGU());	
						}
						
						//Sum TOPS and GU Predicted Movement
						finalItem.setWeeklyPredictedMovement(finalItem.getWeeklyPredictedMovementTops() 
								+ finalItem.getWeeklyPredictedMovementGU());
						/*//Sum TOPS and GU Client forecast
						finalItem.setClientWeeklyPredictedMovement(finalItem.getClientWeeklyPredictedMovement() 
								+ oosItemDTO.getClientWeeklyPredictedMovement());*/
						finalItem.setWeeklyActualMovement(finalItem.getWeeklyActualMovement() 
								+ oosItemDTO.getWeeklyActualMovement());
					}
				}
				
				if(finalItem != null){
					finalItemList.add(finalItem);
				}
			}
		
		return finalItemList;
	}
	

	
	private List<PRItemDTO> getStoreListAuthorizedItemOfWeeklyAd(Connection conn, int productLevelId, int productId, int locationLevelId,
			int locationId, int weekCalendarId, int chainId) throws GeneralException {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		WeeklyAdDAO weeklyAdDAO = new WeeklyAdDAO(conn);
		
		// Get all authorized items for given location and product combination
		// The following method will support only store level location
		List<PRItemDTO> authorizedItems = oosAnalysisDAO.getAuthorizedItems(conn, productLevelId, productId, locationLevelId, locationId);
		logger.debug("Total No of Authorized Items:" + authorizedItems.size());
		
		// Get all the authorized and promoted items from weekly ad
		List<PRItemDTO> promotedItems = oosAnalysisDAO.getPromotionDetails(conn, weekCalendarId, chainId, locationId, locationLevelId,
				productLevelId, productId);
		logger.debug("Total No of Promotion Items:" + promotedItems.size());
		
		// Group by item code to find duplicate items
		HashMap<Integer, List<PRItemDTO>> promoItemMap = new HashMap<Integer, List<PRItemDTO>>();
		HashSet<Long> duplicateItemPromoIds = new HashSet<Long>();
		List<PRItemDTO> nonDuplicateItemList = new ArrayList<PRItemDTO>();
		HashMap<Integer, PRItemDTO> clearedList = new HashMap<Integer, PRItemDTO>();
		HashMap<AdKey, List<String>> adplexItemsMap = weeklyAdDAO.getAdplexItems(locationLevelId, locationId, weekCalendarId);
		
		groupItemByItemCode(promotedItems,promoItemMap,duplicateItemPromoIds,nonDuplicateItemList);
		handleItemAppearsMoreThanOnce(conn, duplicateItemPromoIds, promoItemMap, clearedList);
		handleItemAppearsInMultiplePage(conn, locationLevelId, locationId, weekCalendarId, promoItemMap, clearedList, adplexItemsMap);

		// Add all duplicate removed items
		nonDuplicateItemList.addAll(clearedList.values());

		//Remove items which are not in adplex feed
		List<PRItemDTO> finalList = new ArrayList<PRItemDTO>();
		removeItemsNotInAdPlexFeed(finalList, nonDuplicateItemList,adplexItemsMap);
		
		//Get only 1st and last page, gate a and gate c items 
		///List<PRItemDTO> filteredList =  filterByWeeklyPageNO(promotedItems);
		
		//Keep only distinct items. It is possible that same item falls in different promotion (probably bug in promo loader)
		//List<PRItemDTO> finalList = getDistinctItem(promotedItems, locationLevelId, locationId);
		
//		logger.debug("Before clearing duplicate items promoted list size was " + promotedItems.size());
		//Eliminate duplicates.
//		List<PRItemDTO> finalList = clearDuplicates(promotedItems);
//		logger.debug("After clearing duplicate items promoted list size is " + finalList.size());
		
		// Validate promotion list with authorized items
		List<PRItemDTO> finalItemList = filterAuthorizedItems(authorizedItems, finalList);
		logger.debug("Item count after authorization:" + finalItemList.size());
		
		//Clear duplicates from finalList
		//NU: 9th Sep 2016, not needed as already covered in clearDuplicates()
//		finalItemList = filterDiffPromoSameItem(finalItemList);
//		logger.debug("Item count after filterDiffPromoSameItem:" + finalItemList.size());

//		logger.debug("Total No of Items for OOS Items:" + finalItemList.size());
		return finalItemList;
	}
	
	private void groupItemByItemCode(List<PRItemDTO> promotedItems, HashMap<Integer, List<PRItemDTO>> promoItemMap,
			HashSet<Long> duplicateItemPromoIds, List<PRItemDTO> nonDuplicateItemList) {
		int duplicateItems = 0;
		for (PRItemDTO itemDTO : promotedItems) {
			List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
			if (promoItemMap.get(itemDTO.getItemCode()) != null) {
				itemList = promoItemMap.get(itemDTO.getItemCode());
				duplicateItems++;
			}
			itemList.add(itemDTO);
			promoItemMap.put(itemDTO.getItemCode(), itemList);
		}

		for (Map.Entry<Integer, List<PRItemDTO>> entry : promoItemMap.entrySet()) {
			List<PRItemDTO> items = entry.getValue();
			if (items.size() == 1) {
				nonDuplicateItemList.addAll(items);
			} else {
				for (PRItemDTO item : items) {
					duplicateItemPromoIds.add(item.getPromoDefinitionId());
				}
			}
		}
		logger.debug("No of duplicate items:" + duplicateItems);
		logger.debug("No of unique items:" + nonDuplicateItemList);
	}
	
	private void handleItemAppearsMoreThanOnce(Connection conn, HashSet<Long> duplicateItemPromoIds, HashMap<Integer, List<PRItemDTO>> promoItemMap,
			HashMap<Integer, PRItemDTO> clearedList) throws GeneralException {
		// Get duplicate items location's
		HashMap<Long, List<LocationKey>> itemsLocation = new HashMap<Long, List<LocationKey>>();
		PromotionDAO promotionDAO = new PromotionDAO(conn);
		itemsLocation = promotionDAO.getPromoLocations(conn, duplicateItemPromoIds);
		OOSAnalysisDAO oosAnlaysisDAO = new OOSAnalysisDAO();
		HashMap<Integer, Integer> zoneAndStoreCount = oosAnlaysisDAO.getNoOfStoresInPriceZone(conn);
		// Loop duplicate items
		for (Map.Entry<Integer, List<PRItemDTO>> entry : promoItemMap.entrySet()) {
			List<PRItemDTO> items = entry.getValue();
			if (items.size() > 1) {
				logger.debug("Item appear more than once:" + entry.getKey());
				// clearedList.put(entry.getKey(), items.get(0));
				//
				PRItemDTO pickedItem = null;
				// Pick item which is available in many locations

				//Check if chain level is there
				for (PRItemDTO item : items) {
					List<LocationKey> locations = itemsLocation.get(item.getPromoDefinitionId());
					for (LocationKey location : locations) {
						if (location.getLocationLevelId() == Constants.CHAIN_LEVEL_ID) {
							pickedItem = item;
							break;
						}
					}
				}
				
				// Check no of zones
				if (pickedItem == null) {
					int noOfStores = 0;
					for (PRItemDTO item : items) {
						int tempNoOfStores = 0;
						List<LocationKey> locations = itemsLocation.get(item.getPromoDefinitionId());
						for (LocationKey location : locations) {
							if (location.getLocationLevelId() == Constants.ZONE_LEVEL_ID) {
								tempNoOfStores = tempNoOfStores + zoneAndStoreCount.get(location.getLocationId());
							} else if (location.getLocationLevelId() == Constants.STORE_LEVEL_ID) {
								tempNoOfStores++;
							}
						}
						if (tempNoOfStores > noOfStores) {
							pickedItem = item;
							noOfStores = tempNoOfStores;
						}
					}
				}

				// if no zones, then check store
//				int noOfStores = 0;
//				if (pickedItem == null) {
//					for (PRItemDTO item : items) {
//						int tempNoOfStores = 0;
//						List<LocationKey> locations = itemsLocation.get(item.getPromoDefinitionId());
//						for (LocationKey location : locations) {
//							if (location.getLocationLevelId() == Constants.STORE_LEVEL_ID) {
//								tempNoOfStores++;
//							}
//						}
//						if (tempNoOfStores > noOfStores) {
//							pickedItem = item;
//							noOfStores = tempNoOfStores;
//						}
//						
//					}
//				}

				// If item is not cleared, mostly it should not happen
				if (pickedItem == null) {
					pickedItem = entry.getValue().get(0);
				}
				logger.debug("Picked item sale price:"
						+ PRCommonUtil.getMultiplePrice(pickedItem.getSaleMPack(), pickedItem.getSalePrice(), pickedItem.getSaleMPrice()));
				clearedList.put(entry.getKey(), pickedItem);
			}
		}
	}
	
	private void handleItemAppearsInMultiplePage(Connection conn, int locationLevelId, int locationId, int weekCalendarId,
			HashMap<Integer, List<PRItemDTO>> promoItemMap, HashMap<Integer, PRItemDTO> clearedList,
			HashMap<AdKey, List<String>> adplexItemsMap) throws GeneralException {
		// Loop duplicate items
		for (Map.Entry<Integer, List<PRItemDTO>> entry : promoItemMap.entrySet()) {
			List<PRItemDTO> items = entry.getValue();
			List<AdKey> adKeys = new ArrayList<AdKey>();
			// If items appears more than once
			if (items.size() > 1) {
				AdKey preAdKey = null;
				boolean isItemInDiffPage = false;
				for (PRItemDTO item : items) {
					AdKey adKey = new AdKey(item.getPageNumber(), item.getBlockNumber());
					if (preAdKey != null) {
						// if the item appears in different page/block
						if (!preAdKey.equals(adKey) && !isItemInDiffPage) {
							isItemInDiffPage = true;
						}
					}
					adKeys.add(adKey);
					preAdKey = adKey;
				}

				// get page and block from ad plex feed
				if (isItemInDiffPage) {
					logger.debug("Item appear in more than one page:" + entry.getKey());
					AdKey finalAdKeyOfItem = null;
					// Loop all pages of item
					for (AdKey adKey : adKeys) {
						if (adplexItemsMap.get(adKey) != null) {
							// check if item present in the ad plex
							for (String retailerItemCode : adplexItemsMap.get(adKey)) {
//								logger.debug("items.get(0).getRetailerItemCode():" + items.get(0).getRetailerItemCode());
								if (items.get(0).getRetailerItemCode().equals(retailerItemCode)) {
									finalAdKeyOfItem = adKey;
									break;
								}
							}
						}
					}

					PRItemDTO itemDTO = clearedList.get(entry.getKey());
					//if there is no matching retailer item code found in ad plex feed
					// update page and block number
					if (finalAdKeyOfItem != null) {
						itemDTO.setPageNumber(finalAdKeyOfItem.getPageNumber());
						itemDTO.setBlockNumber(finalAdKeyOfItem.getBlockNumber());
						logger.debug("New page and block:" + finalAdKeyOfItem.toString());
						// Get items with that page and block to get the client
						// forecast
						for (PRItemDTO item : items) {
							AdKey adKey = new AdKey(item.getPageNumber(), item.getBlockNumber());
							if (adKey.equals(finalAdKeyOfItem)) {
								itemDTO.setAdjustedUnits(item.getAdjustedUnits());
								break;
							}
						}
					}
				}
			}
		}
	}
	
	private void removeItemsNotInAdPlexFeed(List<PRItemDTO> finalList, List<PRItemDTO> nonDuplicateItemList,
			HashMap<AdKey, List<String>> adplexItemsMap) {
		for (PRItemDTO itemDTO : nonDuplicateItemList) {
			boolean itemPresentInAdplexFeed = false;
			for (List<String> retailerItemCodes : adplexItemsMap.values()) {
				for (String retItemCode : retailerItemCodes) {
					if (itemDTO.getRetailerItemCode().equals(retItemCode)) {
						itemPresentInAdplexFeed = true;
						break;
					}
				}
			}
			if (itemPresentInAdplexFeed) {
				finalList.add(itemDTO);
			}
		}
	}
}

class OOSCandidateDistinctItemKey {
	private long itemCode;
	private int adPageNo;
	private int blockNo;
	private int displayTypeId;
	private int promoTypeId;
	
	public OOSCandidateDistinctItemKey(long lirOrItemCode, int adPageNo,
			int blockNo, int promoTypeId, int displayTypeId) {
		this.itemCode = lirOrItemCode;
		this.promoTypeId = promoTypeId;
		this.adPageNo = adPageNo;
		this.blockNo = blockNo;
		this.displayTypeId = displayTypeId;
	}
	
	public long getItemCode() {
		return itemCode;
	}

	public void setItemCode(long itemCode) {
		this.itemCode = itemCode;
	}

	public int getAdPageNo() {
		return adPageNo;
	}

	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}

	public int getBlockNo() {
		return blockNo;
	}

	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}

	public int getDisplayTypeId() {
		return displayTypeId;
	}

	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	public int getPromoTypeId() {
		return promoTypeId;
	}

	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + adPageNo;
		result = prime * result + blockNo;
		result = prime * result + displayTypeId;
		result = prime * result + (int) (itemCode ^ (itemCode >>> 32));
		result = prime * result + promoTypeId;
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
		OOSCandidateDistinctItemKey other = (OOSCandidateDistinctItemKey) obj;
		if (adPageNo != other.adPageNo)
			return false;
		if (blockNo != other.blockNo)
			return false;
		if (displayTypeId != other.displayTypeId)
			return false;
		if (itemCode != other.itemCode)
			return false;
		if (promoTypeId != other.promoTypeId)
			return false;
		return true;
	}
	
	
}
