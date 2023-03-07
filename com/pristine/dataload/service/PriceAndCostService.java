package com.pristine.dataload.service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCostDAO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PriceAndCostService {

	static Logger logger = Logger.getLogger("PriceAndCostService");

	/**
	 * 
	 * @param priceRolledUpMap
	 * @param priceZoneInfo
	 * @param itemInfoMap
	 * @return unrolled price data
	 * @throws CloneNotSupportedException
	 */
	public HashMap<String, List<RetailPriceDTO>> identifyStoresAndFindCommonPrice(
			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap, PriceZoneDTO priceZoneInfo,
			HashMap<String, HashMap<String, Integer>> itemInfoMap,
			HashMap<String, HashMap<String, List<String>>> itemStoreMapping, int processingCalId) throws CloneNotSupportedException {
		HashMap<String, List<RetailPriceDTO>> bannerLevelRolledUpMap = new HashMap<String, List<RetailPriceDTO>>();

		for (Map.Entry<String, List<RetailPriceDTO>> entry : priceRolledUpMap.entrySet()) {

			HashMap<String, RetailPriceDTO> priceData = getStoreLevelEntries(entry, priceZoneInfo.getCompStrNo(),
					itemStoreMapping, priceZoneInfo, processingCalId);
			if(priceData.size() > 0){
				HashMap<String, List<RetailPriceDTO>> priceGroupMap = groupPrices(priceData, priceZoneInfo);
	
				RetailPriceDTO mostCommonPriceDTO = findMostCommonPrice(priceGroupMap, priceZoneInfo);
	
				if(mostCommonPriceDTO.getItemcode() == null){
					logger.debug("Item code is null : " + mostCommonPriceDTO.toString());
				}
				
				if (bannerLevelRolledUpMap.get(mostCommonPriceDTO.getItemcode()) == null) {
					List<RetailPriceDTO> tempLst = new ArrayList<>();
					tempLst.add(mostCommonPriceDTO);
					bannerLevelRolledUpMap.put(mostCommonPriceDTO.getItemcode(), tempLst);
				} else {
					List<RetailPriceDTO> tempLst = bannerLevelRolledUpMap.get(mostCommonPriceDTO.getItemcode());
					tempLst.add(mostCommonPriceDTO);
					bannerLevelRolledUpMap.put(mostCommonPriceDTO.getItemcode(), tempLst);
				}
			}else{
				logger.debug("Item is not auntorized: " + entry.getKey());
			}
		}
		return bannerLevelRolledUpMap;
	}

	/**
	 * 
	 * @param entry
	 * @param priceZoneInfo
	 * @return store level price data
	 * @throws CloneNotSupportedException
	 */
	private HashMap<String, RetailPriceDTO> getStoreLevelEntries(Map.Entry<String, List<RetailPriceDTO>> entry,
			List<String> stores, HashMap<String, HashMap<String, List<String>>> itemStoreMapping, 
			PriceZoneDTO priceZoneInfo, int calendarId)
					throws CloneNotSupportedException {
		// Unroll chain level price to store level price.
		int exceptionLocationId = Integer.parseInt(PropertyManager.getProperty("EXCEPTION_LOCATION_ID", "0"));
		HashMap<String, RetailPriceDTO> priceData = new HashMap<>();
		for (RetailPriceDTO retailPriceDTO : entry.getValue()) {
			if (Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()) {
				for (String store : stores) {
					RetailPriceDTO priceDTONew = (RetailPriceDTO) retailPriceDTO.clone();
					priceDTONew.setLevelId(store);
					priceDTONew.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
					HashMap<String, List<String>> zoneStoreMap = itemStoreMapping.get(priceDTONew.getItemcode());
					boolean canBeAdded = false;
					if(zoneStoreMap != null){
						for (Map.Entry<String, List<String>> zoneStoreEntry : zoneStoreMap.entrySet()) {
							if (zoneStoreEntry.getValue().contains(store)) {
								canBeAdded = true;
							}
						}
						if (canBeAdded) {
							String storeKey = priceDTONew.getLevelTypeId() + "-" + priceDTONew.getLevelId();
							priceData.put(storeKey, priceDTONew);
						}
					}
				}
			}
		}

		
		// NU:: 7th June 2017, Item is incorrectly marked as (different price across zone), even though it's same across
		// zones. Previously if there is more than one entry for an item, it is assumed there is multiple price
		// but there can be multiple entry in retail_price_info table, if the effective date is different for different locations
		// so, code is changed checking if there is different price in different authorized location
		boolean isZonePriceVariation =  isZonePriceVaries(entry, stores, itemStoreMapping, calendarId);
				
		int noOfZoneEntries = 0;
		boolean markZonePriceForException = false;
		for (RetailPriceDTO retailPriceDTO : entry.getValue()) {
			if (Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()
					&& (calendarId == 0 || calendarId == retailPriceDTO.getCalendarId())) {
				noOfZoneEntries++;
				HashMap<String, List<String>> zoneStoreMap = itemStoreMapping.get(retailPriceDTO.getItemcode());
				if(zoneStoreMap != null){
					if (zoneStoreMap.get(retailPriceDTO.getLevelId()) != null) {
						boolean isZoneAuthorizedForStore = false;
						for (String store : zoneStoreMap.get(retailPriceDTO.getLevelId())) {
							if (stores.contains(store)) {
								isZoneAuthorizedForStore = true;
								RetailPriceDTO priceDTONew = (RetailPriceDTO) retailPriceDTO.clone();
								priceDTONew.setLevelId(store);
								priceDTONew.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
								if(priceZoneInfo != null && exceptionLocationId > 0
										&& priceZoneInfo.getLocationId() == exceptionLocationId){
									if(noOfZoneEntries > 1 && markZonePriceForException){
										//priceDTONew.setZonePriceDiff(true);		
									}
								}else{
									//priceDTONew.setZonePriceDiff(true);
								}
								
								priceDTONew.setZonePriceDiff(isZonePriceVariation);
								
								String storeKey = priceDTONew.getLevelTypeId() + "-" + priceDTONew.getLevelId();
								priceData.put(storeKey, priceDTONew);
							}
						}
						if(isZoneAuthorizedForStore){
							markZonePriceForException = true;
						}
					}
				}
			}
		}
		
		return priceData;
	}
	
	private boolean isZonePriceVaries(Map.Entry<String, List<RetailPriceDTO>> retailPriceInfoMap, List<String> stores,
			HashMap<String, HashMap<String, List<String>>> itemStoreMapping, int calendarId) {
		HashSet<MultiplePrice> distinctPrices = new HashSet<MultiplePrice>();
		boolean isZonePriceVariation = false;
		for (RetailPriceDTO retailPriceDTO : retailPriceInfoMap.getValue()) {
			boolean isAuthorized = false;

			MultiplePrice multiplePrice = null;
			// Find final price
			if (retailPriceDTO.getSalePrice() > 0 || retailPriceDTO.getSaleMPrice() > 0) {
				multiplePrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getSaleQty(), (double) retailPriceDTO.getSalePrice(),
						(double) retailPriceDTO.getSaleMPrice());
			} else {
				multiplePrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getRegQty(), (double) retailPriceDTO.getRegPrice(),
						(double) retailPriceDTO.getRegMPrice());
			}
			if (Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()) {
				if (multiplePrice != null) {
					distinctPrices.add(multiplePrice);
				}
			} else if (Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId() && (calendarId == 0 || calendarId == retailPriceDTO.getCalendarId())) {
				HashMap<String, List<String>> zoneStoreMap = itemStoreMapping.get(retailPriceDTO.getItemcode());
				// If item is present in store item map table
				if (zoneStoreMap != null) {
					if (zoneStoreMap.get(retailPriceDTO.getLevelId()) != null) {
						// if item is authorized for the zone
						for (String store : zoneStoreMap.get(retailPriceDTO.getLevelId())) {
							if (stores.contains(store)) {
								isAuthorized = true;
								break;
							}
						}
						if (isAuthorized) {
							// Put it in a map
							if (multiplePrice != null) {
								distinctPrices.add(multiplePrice);
							}
						}
					}
				}
			}
		}

		// If the map size > 1, mark as different price
		if (distinctPrices.size() > 1) {
			isZonePriceVariation = true;
		}

		return isZonePriceVariation;
	}

	/**
	 * 
	 * @param priceData
	 */
	private HashMap<String, List<RetailPriceDTO>> groupPrices(HashMap<String, RetailPriceDTO> priceData,
			PriceZoneDTO priceZoneInfo) {
		HashMap<String, List<RetailPriceDTO>> priceGroupMap = new HashMap<>();
		for (Map.Entry<String, RetailPriceDTO> priceEntry : priceData.entrySet()) {
			RetailPriceDTO retailPriceDTO = priceEntry.getValue();
			String priceStr = Constants.EMPTY;
			if (retailPriceDTO.getRegPrice() > 0) {
				priceStr = priceStr + retailPriceDTO.getRegPrice();
			} else if (retailPriceDTO.getRegMPrice() > 0) {
				priceStr = priceStr + retailPriceDTO.getRegMPrice() + Constants.INDEX_DELIMITER
						+ retailPriceDTO.getRegQty();
			}
			if (retailPriceDTO.getSalePrice() > 0) {
				priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSalePrice();
			} else if (retailPriceDTO.getSaleMPrice() > 0) {
				priceStr = priceStr + Constants.INDEX_DELIMITER + retailPriceDTO.getSaleMPrice()
						+ Constants.INDEX_DELIMITER + retailPriceDTO.getSaleQty();
			}

			// Group store prices to find most common price
			if (priceGroupMap.get(priceStr) == null) {
				List<RetailPriceDTO> tempList = new ArrayList<>();
				tempList.add(retailPriceDTO);
				priceGroupMap.put(priceStr, tempList);
			} else {
				List<RetailPriceDTO> tempList = priceGroupMap.get(priceStr);
				tempList.add(retailPriceDTO);
				priceGroupMap.put(priceStr, tempList);
			}
		}
		return priceGroupMap;
	}

	/**
	 * 
	 * @param priceGroupMap
	 * @return most common price across the stores within a banner
	 */
	private RetailPriceDTO findMostCommonPrice(HashMap<String, List<RetailPriceDTO>> priceGroupMap,
			PriceZoneDTO priceZoneInfo) {
		// Determine most prevalent price for this UPC
		RetailPriceDTO mostCommonPrice = new RetailPriceDTO();
		String mostPrevalentPrice = null;
		int mostPrevalentCnt = 0;
		int tempCnt = 0;
		for (Map.Entry<String, List<RetailPriceDTO>> entry : priceGroupMap.entrySet()) {
			/*
			 * if(entry.getValue().size() > mostPrevalentCnt){ mostPrevalentCnt
			 * = entry.getValue().size(); mostPrevalentPrice = entry.getKey(); }
			 */
			List<RetailPriceDTO> retailPriceDTOLst = entry.getValue();
			tempCnt = 0;
			for (RetailPriceDTO retailPriceDTO : retailPriceDTOLst) {
				if (retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
					tempCnt = tempCnt + 1;
				}
			}

			if (tempCnt > mostPrevalentCnt) {
				mostPrevalentCnt = tempCnt;
				mostPrevalentPrice = entry.getKey();
			}
		}

		boolean isZonePriceDiff = false;
		for (Map.Entry<String, List<RetailPriceDTO>> entry : priceGroupMap.entrySet()) {
			for (RetailPriceDTO retailPrice : entry.getValue()) {
				if (retailPrice.isZonePriceDiff()) {
					isZonePriceDiff = true;
				}
			}
			if (entry.getKey().equals(mostPrevalentPrice)) {
				mostCommonPrice = entry.getValue().get(0);
				mostCommonPrice.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
				mostCommonPrice.setLevelTypeId(Constants.CHAIN_LEVEL_TYPE_ID);
			}
		}
		mostCommonPrice.setZonePriceDiff(isZonePriceDiff);

		return mostCommonPrice;
	}

	/**
	 * 
	 * @param costRolledUpMap
	 * @param priceZoneInfo
	 * @param itemInfoMap
	 * @return unrolled price data
	 * @throws CloneNotSupportedException
	 */
	public HashMap<String, List<RetailCostDTO>> identifyStoresAndFindCommonCost(
			HashMap<String, List<RetailCostDTO>> costRolledUpMap, PriceZoneDTO priceZoneInfo,
			HashMap<String, HashMap<String, Integer>> itemInfoMap,
			HashMap<String, HashMap<String, List<String>>> itemStoreMapping) throws CloneNotSupportedException {
		HashMap<String, List<RetailCostDTO>> bannerLevelRolledUpMap = new HashMap<String, List<RetailCostDTO>>();

		for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap.entrySet()) {

			HashMap<String, RetailCostDTO> costData = getStoreLevelCostEntries(entry, priceZoneInfo.getCompStrNo(),
					itemStoreMapping);

			if(costData.size() > 0){
				HashMap<String, List<RetailCostDTO>> costGroupMap = groupCosts(costData, priceZoneInfo);
	
				RetailCostDTO mostCommonCostDTO = findMostCommonCost(costGroupMap, priceZoneInfo);
	
				if (bannerLevelRolledUpMap.get(mostCommonCostDTO.getItemcode()) == null) {
					List<RetailCostDTO> tempLst = new ArrayList<>();
					tempLst.add(mostCommonCostDTO);
					bannerLevelRolledUpMap.put(mostCommonCostDTO.getItemcode(), tempLst);
				} else {
					List<RetailCostDTO> tempLst = bannerLevelRolledUpMap.get(mostCommonCostDTO.getItemcode());
					tempLst.add(mostCommonCostDTO);
					bannerLevelRolledUpMap.put(mostCommonCostDTO.getItemcode(), tempLst);
				}
			}else{
				logger.debug("Item is not auntorized: " + entry.getKey());
			}
		}
		return bannerLevelRolledUpMap;
	}

	/**
	 * 
	 * @param entry
	 * @param priceZoneInfo
	 * @return store level price data
	 * @throws CloneNotSupportedException
	 */
	private HashMap<String, RetailCostDTO> getStoreLevelCostEntries(Map.Entry<String, List<RetailCostDTO>> entry,
			List<String> stores, HashMap<String, HashMap<String, List<String>>> itemStoreMapping)
					throws CloneNotSupportedException {
		// Unroll chain level price to store level price.
		HashMap<String, RetailCostDTO> costData = new HashMap<>();
		for (RetailCostDTO retailCostDTO : entry.getValue()) {
			if (Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()) {
				for (String store : stores) {
					RetailCostDTO costDTONew = (RetailCostDTO) retailCostDTO.clone();
					costDTONew.setLevelId(store);
					costDTONew.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
					HashMap<String, List<String>> zoneStoreMap = itemStoreMapping.get(costDTONew.getItemcode());
					boolean canBeAdded = false;
					if(zoneStoreMap != null){
						for (Map.Entry<String, List<String>> zoneStoreEntry : zoneStoreMap.entrySet()) {
							if (zoneStoreEntry.getValue().contains(store)) {
								canBeAdded = true;
							}
						}
						if (canBeAdded) {
							String storeKey = costDTONew.getLevelTypeId() + "-" + costDTONew.getLevelId();
							costData.put(storeKey, costDTONew);
						}
					}
				}
			}
		}
		for (RetailCostDTO retailCostDTO : entry.getValue()) {
			if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()) {
				HashMap<String, List<String>> zoneStoreMap = itemStoreMapping.get(retailCostDTO.getItemcode());
				if(zoneStoreMap != null){
					if (zoneStoreMap.get(retailCostDTO.getLevelId()) != null) {
						for (String store : zoneStoreMap.get(retailCostDTO.getLevelId())) {
							if (stores.contains(store)) {
								RetailCostDTO costDTONew = (RetailCostDTO) retailCostDTO.clone();
								costDTONew.setLevelId(store);
								costDTONew.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
								String storeKey = costDTONew.getLevelTypeId() + "-" + costDTONew.getLevelId();
								costData.put(storeKey, costDTONew);
							}
						}
					}
				}
			}
		}

		return costData;
	}

	/**
	 * 
	 * @param costData
	 */
	private HashMap<String, List<RetailCostDTO>> groupCosts(HashMap<String, RetailCostDTO> costData,
			PriceZoneDTO priceZoneInfo) {
		HashMap<String, List<RetailCostDTO>> costGroupMap = new HashMap<>();
		for (Map.Entry<String, RetailCostDTO> costEntry : costData.entrySet()) {
			RetailCostDTO retailCostDTO = costEntry.getValue();
			String costStr = Constants.EMPTY;

			retailCostDTO.setPromotionFlag("N");

			/*
			 * if(retailCostDTO.getListCost() >= 0){ costStr = costStr +
			 * retailCostDTO.getListCost(); }
			 */

			costStr = costStr + retailCostDTO.getListCost();

			if (retailCostDTO.getDealCost() > 0) {
				costStr = costStr + Constants.INDEX_DELIMITER + retailCostDTO.getDealCost();
			}

			// Changes for TOPS Cost/Billback dataload
			if (retailCostDTO.getLevel2Cost() > 0) {
				costStr = costStr + Constants.INDEX_DELIMITER + retailCostDTO.getLevel2Cost();
			}

			// Changes for Ahold VIP Cost
			if (retailCostDTO.getVipCost() > 0) {
				costStr = costStr + Constants.INDEX_DELIMITER + retailCostDTO.getVipCost();
			}

			costStr = costStr + Constants.INDEX_DELIMITER + retailCostDTO.getEffListCostDate();

			// Group store prices to find most common price
			if (costGroupMap.get(costStr) == null) {
				List<RetailCostDTO> tempList = new ArrayList<>();
				tempList.add(retailCostDTO);
				costGroupMap.put(costStr, tempList);
			} else {
				List<RetailCostDTO> tempList = costGroupMap.get(costStr);
				tempList.add(retailCostDTO);
				costGroupMap.put(costStr, tempList);
			}
		}
		return costGroupMap;
	}

	/**
	 * 
	 * @param costGroupMap
	 * @return most common price across the stores within a banner
	 */
	private RetailCostDTO findMostCommonCost(HashMap<String, List<RetailCostDTO>> costGroupMap,
			PriceZoneDTO priceZoneInfo) {
		// Determine most prevalent price for this UPC
		RetailCostDTO mostCommonCost = new RetailCostDTO();
		String mostPrevalentCost = null;
		int mostPrevalentCnt = 0;
		int tempCnt = 0;
		for (Map.Entry<String, List<RetailCostDTO>> entry : costGroupMap.entrySet()) {
			/*
			 * if(entry.getValue().size() > mostPrevalentCnt){ mostPrevalentCnt
			 * = entry.getValue().size(); mostPrevalentPrice = entry.getKey(); }
			 */
			List<RetailCostDTO> retailCostDTOLst = entry.getValue();
			tempCnt = 0;
			for (RetailCostDTO retailCostDTO : retailCostDTOLst) {
				if (retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
					tempCnt = tempCnt + 1;
				}
			}

			if (tempCnt > mostPrevalentCnt) {
				mostPrevalentCnt = tempCnt;
				mostPrevalentCost = entry.getKey();
			}
		}

		for (Map.Entry<String, List<RetailCostDTO>> entry : costGroupMap.entrySet()) {
			if (entry.getKey().equals(mostPrevalentCost)) {
				mostCommonCost = entry.getValue().get(0);
				mostCommonCost.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
				mostCommonCost.setLevelTypeId(Constants.CHAIN_LEVEL_TYPE_ID);
			}
		}

		return mostCommonCost;
	}
	
	/**
	 * 
	 * @param conn
	 * @param costRolledUpMap
	 * @param storeInfo
	 * @param itemCodeList
	 * @return unrolled cost map
	 * @throws GeneralException
	 */

	public HashMap<String, List<RetailCostDTO>> unrollAndFindGivenStoreCost(Connection conn,
			HashMap<String, List<RetailCostDTO>> costRolledUpMap, StoreDTO storeInfo, Set<String> itemCodeList,
			HashMap<String, HashMap<String, List<String>>> itemStoreMapping)
					throws GeneralException {

		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<>();
		try {
			HashMap<String, HashMap<String, List<String>>> storeItemMap = itemStoreMapping;
			if(storeItemMap == null){
				long startTime = System.currentTimeMillis();
				RetailCostDAO retailCostDAO = new RetailCostDAO();
				storeItemMap = retailCostDAO
						.getStoreItemMapAtZonelevel(conn, itemCodeList, storeInfo.strNum);
				long endTime = System.currentTimeMillis();
				logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
				logger.info("store_item_map size - " + storeItemMap.size());
			}
			List<String> stores = new ArrayList<>();
			stores.add(storeInfo.strNum);
			for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap.entrySet()) {

				HashMap<String, RetailCostDTO> costData = getStoreLevelCostEntries(entry, stores, storeItemMap);

				for (Map.Entry<String, RetailCostDTO> costEntry : costData.entrySet()) {
					RetailCostDTO retailCostDTO = costEntry.getValue();
					if (unrolledCostMap.get(retailCostDTO.getLevelId()) != null) {
						List<RetailCostDTO> tempList = unrolledCostMap.get(retailCostDTO.getLevelId());
						tempList.add(retailCostDTO);
						unrolledCostMap.put(retailCostDTO.getLevelId(), tempList);
					} else {
						List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
						tempList.add(retailCostDTO);
						unrolledCostMap.put(retailCostDTO.getLevelId(), tempList);
					}
				}

			}
		} catch (GeneralException | Exception e) {
			logger.error("Error -- unrollAndFindGivenStoreCost() ", e );
			throw new GeneralException("Error -- unrollAndFindGivenStoreCost() ", e);
		}
		return unrolledCostMap;
	}
	
	/**
	 * 
	 * @param conn
	 * @param priceRolledUpMap
	 * @param storeInfo
	 * @param itemCodeList
	 * @return unrolled price map
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailPriceDTO>> unrollAndFindGivenStorePrice(Connection conn,
			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap, StoreDTO storeInfo, 
			Set<String> itemCodeList, HashMap<String, HashMap<String, List<String>>> itemStoreMapping)
					throws GeneralException {

		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<>();
		try {

			HashMap<String, HashMap<String, List<String>>> storeItemMap = itemStoreMapping;
			if(storeItemMap == null){
				long startTime = System.currentTimeMillis();
				RetailCostDAO retailCostDAO = new RetailCostDAO();
				storeItemMap = retailCostDAO
						.getStoreItemMapAtZonelevel(conn, itemCodeList, storeInfo.strNum);
				long endTime = System.currentTimeMillis();
				logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
				logger.info("store_item_map size - " + storeItemMap.size());
			}

			List<String> stores = new ArrayList<>();
			stores.add(storeInfo.strNum);
			for (Map.Entry<String, List<RetailPriceDTO>> entry : priceRolledUpMap.entrySet()) {

				HashMap<String, RetailPriceDTO> priceData = getStoreLevelEntries(entry, stores, storeItemMap, null, 0);

				for (Map.Entry<String, RetailPriceDTO> priceEntry : priceData.entrySet()) {
					RetailPriceDTO retailPriceDTO = priceEntry.getValue();
					if (unrolledPriceMap.get(retailPriceDTO.getLevelId()) != null) {
						List<RetailPriceDTO> tempList = unrolledPriceMap.get(retailPriceDTO.getLevelId());
						tempList.add(retailPriceDTO);
						unrolledPriceMap.put(retailPriceDTO.getLevelId(), tempList);
					} else {
						List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
						tempList.add(retailPriceDTO);
						unrolledPriceMap.put(retailPriceDTO.getLevelId(), tempList);
					}
				}

			}
		} catch (GeneralException | Exception e) {
			logger.error("Error -- unrollAndFindGivenStorePrice() ", e );
			throw new GeneralException("Error -- unrollAndFindGivenStorePrice() ", e);
		}
		return unrolledPriceMap;
	}

}
