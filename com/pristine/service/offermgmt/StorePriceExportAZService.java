package com.pristine.service.offermgmt;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class StorePriceExportAZService {
	
	private static Logger logger = Logger.getLogger("StorePriceExportAZSerive");

	public List<PRItemDTO> filterSalesFloorItemsByAbsoluteImpact(List<PRItemDTO> salesFloorItems,
			String sFItemLimitStr) {


		logger.info("filterSalesFloorItemsByAbsoluteImpact() - filtering sales floor items starts ");
		
		int SFItemLimit = Integer.parseInt(sFItemLimitStr);

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> itemsWithOverallImpact = new ArrayList<PRItemDTO>();
		String thresholdStr = PropertyManager.getProperty("THRESHOLD");
		int threshold = Integer.parseInt(thresholdStr);
		
		//priorities previously approved items
		//List<PRItemDTO> recentlyApprovedItems = prioritiesPrevAprvdItem(salesFloorItems, finalExportList);
			
		//logger.debug("# of recently approved SF items: " + recentlyApprovedItems.stream().collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode)).size());

		// map produced to set key as item code of non lig member
		HashMap<Integer, List<PRItemDTO>> totalExportListByItem = (HashMap<Integer, List<PRItemDTO>>) salesFloorItems
				.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		// map produced to set key as item code of lig member
		HashMap<Integer, List<PRItemDTO>> exportListByLigItem = (HashMap<Integer, List<PRItemDTO>>) salesFloorItems
				.stream().filter(p -> p.getRetLirId() > 0).collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

		for (Map.Entry<Integer, List<PRItemDTO>> totalExportListEntry : totalExportListByItem.entrySet()) {

			List<PRItemDTO> exportItemValues = totalExportListEntry.getValue();
			PRItemDTO exportItem = totalExportListEntry.getValue().get(0);
			PRItemDTO clonedObjOfExportItem;
			try {
				clonedObjOfExportItem = (PRItemDTO) exportItem.clone();

				double totalImpact = exportItemValues.stream().mapToDouble(p -> p.getImpact()).sum();
				clonedObjOfExportItem.setImpact(Math.abs(totalImpact));
				itemsWithOverallImpact.add(clonedObjOfExportItem);
				// exportItem.setImpact(exportItem.getI);
				logger.debug("filterSalesFloorItemsByAbsoluteImpact() - Total Impact: " + totalImpact);
				// sort in descending order of price change impact
				Comparator<PRItemDTO> compareByImpact = (PRItemDTO o1, PRItemDTO o2) -> o1.getImpact()
						.compareTo(o2.getImpact());

				Collections.sort(itemsWithOverallImpact, compareByImpact.reversed());

			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		int counter = 0;
		//int counter = finalExportList.size();
		int thresholdMax = (int) (SFItemLimit + (SFItemLimit * (threshold / 100)));
		// iterating the ranking list
		for (PRItemDTO itemWithConsolidatedRanking : itemsWithOverallImpact) {

			List<PRItemDTO> membersInAllLocations = exportListByLigItem.get(itemWithConsolidatedRanking.getRetLirId());

			List<PRItemDTO> itemInAllLocations = totalExportListByItem.get(itemWithConsolidatedRanking.getItemCode());

			// group the ranking list by ret lir id
			HashMap<Integer, List<PRItemDTO>> rankingOrderByLig = (HashMap<Integer, List<PRItemDTO>>) itemsWithOverallImpact
					.stream().collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

			if (!itemWithConsolidatedRanking.getIsMemberProcessed()) {
				// int diffPct = 0;

				// lig items
				if (itemWithConsolidatedRanking.getRetLirId() > 0) {

					logger.debug(
							"filterSalesFloorItemsByImpact() - LIG member: " + itemWithConsolidatedRanking.getItemCode()
									+ ", impact: " + itemWithConsolidatedRanking.getImpact());

					List<PRItemDTO> memberListObj = rankingOrderByLig.get(itemWithConsolidatedRanking.getRetLirId());
					counter = counter + memberListObj.size();
					
					if (counter <= SFItemLimit || (counter > SFItemLimit && counter == thresholdMax)) {
						logger.debug("filterSalesFloorItemsByImpact() - Added " + memberListObj.size()
								+ " members for LIG: " + itemWithConsolidatedRanking.getRetLirId());
						memberListObj.forEach(item -> {
							logger.debug("filterSalesFloorItemsByAbsoluteImpact() - Member items of LIG group are: "
									+ item.getItemCode() + ", its impact: " + item.getImpact());
						});

						finalExportList.addAll(membersInAllLocations);
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter);
						for (PRItemDTO flagSet : memberListObj) {
							flagSet.setIsMemberProcessed(true);
						}
					} else if (counter > SFItemLimit) {
						logger.debug("filterSalesFloorItemsByImpact() - Threshold Limit exceeded. Skipping "
								+ memberListObj.size() + " LIG members for LIG: "
								+ itemWithConsolidatedRanking.getRetLirId());
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter
								+ ". Stopped adding items.");
						break;
					}

				}
				// non lig items
				else {
					logger.debug("filterSalesFloorItemsByImpact() - Non LIG item: "
							+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
							+ itemWithConsolidatedRanking.getImpact());

					counter++;
					if (counter <= SFItemLimit) {
						logger.debug("filterSalesFloorItemsByImpact() - Adding non LIG item: "
								+ itemWithConsolidatedRanking.getItemCode());
						finalExportList.addAll(itemInAllLocations);
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter);
					} else if (counter > SFItemLimit) {
						logger.debug("filterSalesFloorItemsByImpact() - Limit exceeded. Skipping item: "
								+ itemWithConsolidatedRanking.getItemCode());
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter
								+ ". Stopped adding items.");
						break;
					}
				}
			}
		}

		logger.debug("# of items filtered: " + finalExportList.stream().collect(Collectors.groupingBy(PRItemDTO :: getRetailerItemCode)).size());
		logger.info("filterSalesFloorItemsByAbsoluteImpact() - filtering sales floor items ends ");
		return finalExportList;
		
	}

	private List<PRItemDTO> prioritiesPrevAprvdItem(List<PRItemDTO> salesFloorItems, List<PRItemDTO> finalExportList) {
		List<PRItemDTO> recentlyApprovedItems = new ArrayList<>();
		HashMap<LocalDate, List<PRItemDTO>> dataByAprvdDate = (HashMap<LocalDate, List<PRItemDTO>>) salesFloorItems
				.stream().collect(Collectors.groupingBy(PRItemDTO::getAprvdDateAsLocalDate));

		logger.debug("dataByAprvdDate size: " + dataByAprvdDate.size());
		if (dataByAprvdDate.size() > 1) {
			TreeMap<LocalDate, List<PRItemDTO>> orderByAprvdDate = new TreeMap<LocalDate, List<PRItemDTO>>(
					dataByAprvdDate);

			// HashMap<LocalDate, List<PRItemDTO>> firstlyAprvdItems = (HashMap<LocalDate,
			// List<PRItemDTO>>) orderByAprvdDate.entrySet().stream().findFirst().get();
			boolean firstElementAdded = false;
			for (Map.Entry<LocalDate, List<PRItemDTO>> entry : orderByAprvdDate.entrySet()) {
				if (!firstElementAdded) {
					finalExportList.addAll(entry.getValue());
					logger.debug("# of previously approved SF items: "
							+ finalExportList.stream().collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode)).size());
					firstElementAdded = true;
				} else {
					recentlyApprovedItems.addAll(entry.getValue());
				}
			}
		}
		else if (dataByAprvdDate.size() == 1) {
			recentlyApprovedItems = salesFloorItems;
		}
		return recentlyApprovedItems;
	}

	public List<PRItemDTO> setZoneLevelPrice(List<PRItemDTO> exportList, List<Integer> excludeZoneIdForVirtualZone, 
			HashMap<Integer, List<StoreDTO>> zoneIdAndStoreNumMap) {
		
		HashMap<Integer, List<PRItemDTO>> exportDataMap = (HashMap<Integer, List<PRItemDTO>>) exportList.stream()
				.distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();

		if(exportDataMap.size()>0) {
		exportDataMap.forEach((itemCode, exportObj) -> {
			exportObj.forEach(itemDTO -> {

				List<String> excludeEcomStores = getExcludedEcomStores();
				List<String> excludedStores = getStoresExcludeZoneLevelData();
				 

				PRItemDTO zoneLevelData = itemDTO;
				zoneLevelData.setChildLocationLevelId(Constants.ZONE_LEVEL_TYPE_ID);
				zoneLevelData.setStoreLockExpiryFlag("A");
				finalExportList.add(zoneLevelData);
				
				if(excludeZoneIdForVirtualZone.contains(zoneLevelData.getPriceZoneId())) {
				// gets store list for given zone
				// List<String> storeNumList = getStoreNoOfZones(itemDTO.getPriceZoneId());
				 List<StoreDTO> storeNumList = zoneIdAndStoreNumMap.get(itemDTO.getPriceZoneId());

				// get excluded stores from item list

					/*HashMap<String, Integer> excludedStoresFromItemList = priceExportDao.getExcludedStoresFromItemList(
							conn, itemDTO.getItemCode(), itemDTO.getPriceZoneId(), emergencyInSaleFloor, false, false);*/

					//List<String> excludedStoresFromExcel = priceExportDao.getExcludedStoresFromList(conn);
				 	if(storeNumList == null) {
				 		logger.info("No Stores are available for zone: " + itemDTO.getPriceZoneNo());
				 	}
				 	else if (storeNumList.size() > 0) {
						storeNumList.forEach(storeNo -> {

							if (!excludedStores.contains(storeNo.strNum)) {
								if(!excludeEcomStores.contains(storeNo.strNum)) {
								/*if (excludedStoresFromItemList.size() < 0 || excludedStoresFromItemList == null
										|| !excludedStoresFromItemList.containsKey(storeNo)) {*/

									try {
										PRItemDTO storeLevelData = (PRItemDTO) zoneLevelData.clone();
										storeLevelData.setStoreNo(storeNo.strNum); 
										storeLevelData.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
										storeLevelData.setStoreLockExpiryFlag("A");
										finalExportList.add(storeLevelData);
									} catch (CloneNotSupportedException e) {
										e.printStackTrace();
									}

								}
							}

						});
					}
			}
			});
		});
		}
		return finalExportList;
		
	}
	
	public void filterAndAddItemsBasedOnPriceTestData(List<String> itemStoreCombinationsFromPriceTest,
			List<PRItemDTO> source, List<PRItemDTO> destination, HashMap<String, String> storeNumIDMap) {
				for(PRItemDTO S : source) {
					String storeId = storeNumIDMap.get(S.getStoreNo());
					String key = S.getItemCode()+"_"+storeId;
					if(!itemStoreCombinationsFromPriceTest.contains(key)) 
					{
						destination.add(S);
					}
				}
		}
	
	/*
	 * Export test zone data which are not in clearance and emergency list
	 */
	public void insertPriceTestItemsBasedOnEmergencyClearenceItems(List<PRItemDTO> finalExportList,
			List<PRItemDTO> PriceTestItemStoreLevelEntries, List<PRItemDTO> eCItemsToExport,
			List<String> itemStoreCombinationsFromPriceTest, HashMap<String, String> storeNumIDMap) {
		List<String> DictReference = new ArrayList<>();
		for (PRItemDTO I : eCItemsToExport) {
			if (I.getChildLocationLevelId() == Constants.STORE_LEVEL_TYPE_ID) {
				String storeId = storeNumIDMap.get(I.getStoreNo());
				String Key = I.getItemCode() + "_" + storeId;
				DictReference.add(Key);
			}
		}

		for (PRItemDTO I : PriceTestItemStoreLevelEntries) {
			String storeId = storeNumIDMap.get(I.getStoreNo());
			String Key = I.getItemCode() + "_" + storeId;
			if (!DictReference.contains(Key)) {
				finalExportList.add(I);
			}
		}

	}
	
	public void setCurrentPriceForLockedStore(HashMap<String, List<RetailPriceDTO>> priceDataMap,
			PRItemDTO itemStoreObj, String itemCodeStr) {
		if (priceDataMap.containsKey(itemCodeStr)) {
			List<RetailPriceDTO> priceList = priceDataMap.get(itemCodeStr);
			for (RetailPriceDTO priceDTO : priceList) {
				if (priceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID
						&& itemStoreObj.getStoreNo().equals(priceDTO.getLevelId())) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					itemStoreObj.setRecommendedRegPrice(currentPrice);
					itemStoreObj.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
					itemStoreObj.setDiffRetail(0D);
					break;
				} else if (priceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID
						&& itemStoreObj.getPriceZoneNo().equals(priceDTO.getLevelId())) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					itemStoreObj.setRecommendedRegPrice(currentPrice);
					itemStoreObj.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
					itemStoreObj.setDiffRetail(0D);
					break;
				} else if (priceDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					itemStoreObj.setRecommendedRegPrice(currentPrice);
					itemStoreObj.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
					itemStoreObj.setDiffRetail(0D);
					break;
				}
			}
		}
	}

	
	
	// helper method deriving the excluded Ecommerce stores
	public List<String> getExcludedEcomStores() {
		String ecommerceStores = PropertyManager.getProperty("ECOMMERCE_STORES");
		String[] excludedEcomStoresArray = ecommerceStores.split(",");
		List<String> excludedEcomStoresList = Arrays.asList(excludedEcomStoresArray);
		return excludedEcomStoresList;
	}

	// helper method deriving the excluded stores
	public List<String> getStoresExcludeZoneLevelData() {
		String excludedStores = PropertyManager.getProperty("STORES_EXCLUDE_ZONE_LEVEL_PRICES");
		String[] excludedStoresArray = excludedStores.split(",");
		List<String> excludedStoresList = Arrays.asList(excludedStoresArray);
		return excludedStoresList;
	}

}
