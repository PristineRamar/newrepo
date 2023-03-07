package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;


public class StorePriceExportAZServiceV2 {

	private static Logger logger = Logger.getLogger("StorePriceExportAZServiceV2");

	public List<PRItemDTO> filterSalesFloorItemsByAbsoluteImpact(List<PRItemDTO> salesFloorItems,
			String sFItemLimitStr) {

		String virtualZone = PropertyManager.getProperty("VIRTUAL_ZONE");

		int SFItemLimit = Integer.parseInt(sFItemLimitStr);

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();

		String thresholdStr = PropertyManager.getProperty("THRESHOLD");
		float threshold = Float.parseFloat(thresholdStr);

		int thresholdMax = (int) (SFItemLimit + (new Double(SFItemLimit) * new Double((threshold / 100))));
		SFItemLimit = thresholdMax;
		logger.debug("SF limit + threshold count: " + thresholdMax);

		// GROUP THE SF DATA BY ZONE
		HashMap<String, List<PRItemDTO>> salesFloorItemsByZone = (HashMap<String, List<PRItemDTO>>) salesFloorItems
				.stream().collect(Collectors.groupingBy(PRItemDTO::getPriceZoneNo));

		// ITERATE DATA FOR EACH ZONE
		for (Map.Entry<String, List<PRItemDTO>> dataByZoneEntry : salesFloorItemsByZone.entrySet()) {
			int countOfDistinctItemsInExport = 0;
			List<PRItemDTO> sfExportListByZone = new ArrayList<PRItemDTO>();
			
			// CHANGES FOR AI #109
			// MANDATORY EXPORT FOR ITEMS WITH PRIORITY
			List<PRItemDTO> priorityItems = dataByZoneEntry.getValue().stream().filter(e -> e.getPriority().equals("Y"))
					.collect(Collectors.toList());

			logger.info("# in priorityItems: " + priorityItems.size());
			if (priorityItems != null && priorityItems.size() > 0) {
				sfExportListByZone.addAll(priorityItems);
				finalExportList.addAll(priorityItems);
				countOfDistinctItemsInExport = sfExportListByZone.stream()
						.collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
			}
		
			// do not process virtual zone separately, group along with individual zones
			if (!dataByZoneEntry.getKey().equals(virtualZone)) {

				//CHANGED FOR AI #109
				List<PRItemDTO> regularItems = dataByZoneEntry.getValue().stream().filter(e -> e.getPriority().equals("N")).collect(Collectors.toList());

				// Sf map by approved week rank - early approved gets higher rank
			//	HashMap<Integer, List<PRItemDTO>> salesFloorItemsByWeekRank = (HashMap<Integer, List<PRItemDTO>>) dataByZoneEntry
					//	.getValue().stream().collect(Collectors.groupingBy(PRItemDTO::getSF_week_rank));
				
				HashMap<Integer, List<PRItemDTO>> salesFloorItemsByWeekRank = (HashMap<Integer, List<PRItemDTO>>) regularItems
						.stream().collect(Collectors.groupingBy(PRItemDTO::getSF_week_rank));

				TreeMap<Integer, List<PRItemDTO>> sfItemsByWeekRankInOrder = new TreeMap<>(salesFloorItemsByWeekRank);

				logger.info("filterSalesFloorItemsByAbsoluteImpact() - filtering sales floor items starts ");

				// ITERATE DATA FOR ORDERED WEEK_RANK
				for (Map.Entry<Integer, List<PRItemDTO>> referenceMapWeekImpactEntry : sfItemsByWeekRankInOrder
						.entrySet()) {

					logger.debug("Iterating week - " + referenceMapWeekImpactEntry.getKey());

					// form order wise map based on partially exported/ approved
					HashMap<Integer, List<PRItemDTO>> ruBasedOnExportStatus = (HashMap<Integer, List<PRItemDTO>>) referenceMapWeekImpactEntry
							.getValue().stream().collect(Collectors.groupingBy(PRItemDTO::getSF_export_rank));

					TreeMap<Integer, List<PRItemDTO>> ruBasedOnExportStatusInOrder = new TreeMap<>(
							ruBasedOnExportStatus);

					// ITERATE DATA FOR EXPORT STATUS
					for (Map.Entry<Integer, List<PRItemDTO>> sfDataMapEntryForExportStatus : ruBasedOnExportStatusInOrder
							.entrySet()) {

						logger.debug("Iterating export status with higher rank (partially/fully export) - "
								+ sfDataMapEntryForExportStatus.getKey());

						// form order wise map based on total impact of RU
						HashMap<Integer, List<PRItemDTO>> ruBasedOnImpact = (HashMap<Integer, List<PRItemDTO>>) sfDataMapEntryForExportStatus
								.getValue().stream().collect(Collectors.groupingBy(PRItemDTO::getSF_RU_rank));

						TreeMap<Integer, List<PRItemDTO>> ruBasedOnImpactInOrder = new TreeMap<>(ruBasedOnImpact);

						// iterate data for total impact of RU
						for (Map.Entry<Integer, List<PRItemDTO>> sfDataMapEntryForImpact : ruBasedOnImpactInOrder
								.entrySet()) {

							if (sfDataMapEntryForImpact.getValue() != null
									&& sfDataMapEntryForImpact.getValue().get(0) != null) {

								logger.debug("Iterating RU - "
										+ sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
										+ " of highest impact: "
										+ sfDataMapEntryForImpact.getValue().get(0).getTotal_Impact());
							}

							logger.debug("Existing count of items in final list - " + countOfDistinctItemsInExport);
							if (countOfDistinctItemsInExport < SFItemLimit) {

								List<Integer> distinctItemsOfRU = new ArrayList<>();
								for (PRItemDTO impactData : sfDataMapEntryForImpact.getValue()) {
									distinctItemsOfRU.add(impactData.getItemCode());
								}

								int remaningItemCount = SFItemLimit - countOfDistinctItemsInExport;
								int iteratingCount = 0;

								if (sfDataMapEntryForImpact.getValue() != null
										&& sfDataMapEntryForImpact.getValue().get(0) != null) {
									logger.debug("# of remaining Items to add from week - "
											+ referenceMapWeekImpactEntry.getKey() + " of RU "
											+ sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
											+ " is: " + remaningItemCount);
								}

								List<PRItemDTO> tempList = new ArrayList<>();

								if (distinctItemsOfRU.size() <= remaningItemCount) {
									iteratingCount = iteratingCount + distinctItemsOfRU.size();
									logger.debug("iteratingCount - " + iteratingCount);
									if (sfDataMapEntryForImpact.getValue() != null
											&& sfDataMapEntryForImpact.getValue().get(0) != null) {
										logger.debug("All items (" + distinctItemsOfRU.size() + " items) of "
												+ sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
												+ " added to final list");
									}
									tempList.addAll(sfDataMapEntryForImpact.getValue());
									finalExportList.addAll(tempList);
									sfExportListByZone.addAll(tempList);
									countOfDistinctItemsInExport = sfExportListByZone.stream()
											.collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
								} else {
									// map produced to set key as item code of non family items
									HashMap<Integer, List<PRItemDTO>> totalExportListByItem = (HashMap<Integer, List<PRItemDTO>>) sfDataMapEntryForImpact
											.getValue().stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

									HashMap<String, List<PRItemDTO>> totalExportListByFamilyItem = (HashMap<String, List<PRItemDTO>>) sfDataMapEntryForImpact
											.getValue().stream()
											.filter(e -> (e.getFamilyName() != null || !e.getFamilyName().equals("")))
											.collect(Collectors.groupingBy(PRItemDTO::getFamilyName));

									List<PRItemDTO> itemsWithOverallImpact = new ArrayList<PRItemDTO>();

									// sort all items by item level impact in descending order across approved zones
									for (Map.Entry<Integer, List<PRItemDTO>> totalExportListEntry : totalExportListByItem
											.entrySet()) {

										List<PRItemDTO> exportItemValues = totalExportListEntry.getValue();
										PRItemDTO exportItem = null;

										PRItemDTO clonedObjOfExportItem;
										try {
											if (totalExportListEntry.getValue() != null) {
												exportItem = totalExportListEntry.getValue().get(0);
											}
											if (exportItem != null) {
												clonedObjOfExportItem = (PRItemDTO) exportItem.clone();

												double totalImpact = exportItemValues.stream()
														.mapToDouble(p -> p.getImpact()).sum();
												//COMMENTED BY KIRTHI
												//12/13/2021 TO NOT TAKE ABSOLUTE IMPACT
												//clonedObjOfExportItem.setImpact(Math.abs(totalImpact));												
												clonedObjOfExportItem.setImpact(totalImpact);
												itemsWithOverallImpact.add(clonedObjOfExportItem);
											}
											// sort in descending order of price change impact
											Comparator<PRItemDTO> compareByImpact = (PRItemDTO o1, PRItemDTO o2) -> o1
													.getImpact().compareTo(o2.getImpact());

											Collections.sort(itemsWithOverallImpact, compareByImpact.reversed());

										} catch (CloneNotSupportedException e) {
											e.printStackTrace();
										}
									}

									// iterating the item level impact ranking list
									for (PRItemDTO itemWithConsolidatedRanking : itemsWithOverallImpact) {

										List<PRItemDTO> familiesInAllLocations = new ArrayList<>();
										if (totalExportListByFamilyItem != null) {
											familiesInAllLocations = totalExportListByFamilyItem
													.get(itemWithConsolidatedRanking.getFamilyName());
										}

										List<PRItemDTO> itemInAllLocations = totalExportListByItem
												.get(itemWithConsolidatedRanking.getItemCode());

										// group the ranking list by family name
										HashMap<String, List<PRItemDTO>> rankingOrderByFamily = (HashMap<String, List<PRItemDTO>>) itemsWithOverallImpact
												.stream().collect(Collectors.groupingBy(PRItemDTO::getFamilyName));

										int counter = 0;
										if (!itemWithConsolidatedRanking.isFamilyProcessed()) {

											if (itemWithConsolidatedRanking.getFamilyName() != null
													&& !itemWithConsolidatedRanking.getFamilyName().equals("")) {

												logger.debug("filterSalesFloorItemsByImpact() - Family item: "
														+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
														+ itemWithConsolidatedRanking.getImpact());

												List<PRItemDTO> familyListObj = rankingOrderByFamily
														.get(itemWithConsolidatedRanking.getFamilyName());
												counter = counter + familyListObj.size();
												iteratingCount = iteratingCount + counter;
												logger.debug("iteratingCount - " + iteratingCount);
												/*
												 * if ((countOfDistinctItemsInExport + counter) <= remaningItemCount ||
												 * ((countOfDistinctItemsInExport + counter) > remaningItemCount &&
												 * (countOfDistinctItemsInExport + counter) <= thresholdMax)) {
												 */
												/*
												 * if (iteratingCount <= remaningItemCount || ((iteratingCount >
												 * remaningItemCount) && (iteratingCount <= thresholdMax))) {
												 */
												if (iteratingCount <= remaningItemCount) {
													logger.debug("filterSalesFloorItemsByImpact() - Added "
															+ familyListObj.size() + " family items for family name: "
															+ itemWithConsolidatedRanking.getFamilyName());
													familyListObj.forEach(item -> {
														logger.debug(
																"filterSalesFloorItemsByAbsoluteImpact() - Family items of Family name "
																		+ item.getFamilyName()
																		+ " are: Presto Itemcode - "
																		+ item.getItemCode() + ", its impact: "
																		+ item.getImpact());
													});

													tempList.addAll(familiesInAllLocations);
													finalExportList.addAll(tempList);
													sfExportListByZone.addAll(tempList);
													countOfDistinctItemsInExport = sfExportListByZone.stream()
															.collect(Collectors.groupingBy(PRItemDTO::getItemCode))
															.size();
													logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
															+ iteratingCount);
													for (PRItemDTO flagSet : familyListObj) {
														flagSet.setFamilyProcessed(true);
													}
												} else if (iteratingCount > remaningItemCount) {
													// else if ((countOfDistinctItemsInExport + counter) >
													// remaningItemCount) {
													logger.debug(
															"filterSalesFloorItemsByImpact() - Threshold Limit exceeded. Skipping "
																	+ familyListObj.size()
																	+ " Family items for family name: "
																	+ itemWithConsolidatedRanking.getFamilyName());
													// break;
												}
											}

											else {
												logger.debug("filterSalesFloorItemsByImpact() - Non Family item: "
														+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
														+ itemWithConsolidatedRanking.getImpact());

												counter++;
												iteratingCount = iteratingCount + counter;
												logger.debug("iteratingCount - " + iteratingCount);
												if (iteratingCount <= remaningItemCount) {
													logger.debug(
															"filterSalesFloorItemsByImpact() - Adding non Family item: "
																	+ itemWithConsolidatedRanking.getItemCode());
													tempList.addAll(itemInAllLocations);
													finalExportList.addAll(tempList);
													sfExportListByZone.addAll(tempList);
													countOfDistinctItemsInExport = sfExportListByZone.stream()
															.collect(Collectors.groupingBy(PRItemDTO::getItemCode))
															.size();
													logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
															+ iteratingCount);
												} else if (iteratingCount > remaningItemCount) {
													logger.debug(
															"filterSalesFloorItemsByImpact() - Limit exceeded. Skipping item: "
																	+ itemWithConsolidatedRanking.getItemCode());
													logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
															+ counter + ". Stopped adding items.");
													break;
												}
											}

										}
									}
								}
							}
						}
					}
				}
				logger.debug("# of items filtered: "
						+ finalExportList.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size());
				logger.info("filterSalesFloorItemsByAbsoluteImpact() - filtering sales floor items ends ");
			}
		}
		
		Set<Integer> uniqueExportItems = finalExportList.stream().map(PRItemDTO::getItemCode).collect(Collectors.toSet());
		Map<Integer, List<PRItemDTO>> virtualZnData = salesFloorItemsByZone.get(virtualZone).stream()
				.collect(Collectors.groupingBy(PRItemDTO::getItemCode));
		for(Integer item : uniqueExportItems) {
			finalExportList.addAll(virtualZnData.get(item));
		}
		return finalExportList;

	}

	public List<PRItemDTO> setZoneLevelPrice(List<PRItemDTO> exportList, List<Integer> excludeZoneIdForVirtualZone,
			HashMap<Integer, List<StoreDTO>> zoneIdAndStoreNumMap) {

		HashMap<Integer, List<PRItemDTO>> exportDataMap = (HashMap<Integer, List<PRItemDTO>>) exportList.stream()
				.distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();

		if (exportDataMap.size() > 0) {
			exportDataMap.forEach((itemCode, exportObj) -> {
				exportObj.forEach(itemDTO -> {

					List<String> excludeEcomStores = getExcludedEcomStores();
					List<String> excludedStores = getStoresExcludeZoneLevelData();

					PRItemDTO zoneLevelData = itemDTO;
					zoneLevelData.setChildLocationLevelId(Constants.ZONE_LEVEL_TYPE_ID);
					zoneLevelData.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
					finalExportList.add(zoneLevelData);

					if (excludeZoneIdForVirtualZone.contains(zoneLevelData.getPriceZoneId())) {
						logger.info("setZoneLevelPrice() - Exploding store level data for zone id- "
								+ zoneLevelData.getPriceZoneId());
						// gets store list for given zone
						// List<String> storeNumList = getStoreNoOfZones(itemDTO.getPriceZoneId());
						List<StoreDTO> storeNumList = zoneIdAndStoreNumMap.get(itemDTO.getPriceZoneId());

						if (storeNumList == null) {
							logger.info("No Stores are available for zone: " + itemDTO.getPriceZoneNo());
						} else if (storeNumList.size() > 0) {
							storeNumList.forEach(storeNo -> {

								if (!excludedStores.contains(storeNo.strNum)) {
									if (!excludeEcomStores.contains(storeNo.strNum)) {

										try {
											PRItemDTO storeLevelData = (PRItemDTO) zoneLevelData.clone();
											storeLevelData.setStoreNo(storeNo.strNum);
											storeLevelData.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
											storeLevelData.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
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
		for (PRItemDTO S : source) {
			String storeId = storeNumIDMap.get(S.getStoreNo());
			String key = S.getItemCode() + "_" + storeId;
			if (!itemStoreCombinationsFromPriceTest.contains(key)) {
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

	public void setDiffPriceForECitems(HashMap<String, List<RetailPriceDTO>> priceDataMap, PRItemDTO zoneItem,
			String itemCodeStr) {
		if (priceDataMap.containsKey(itemCodeStr)) {
			List<RetailPriceDTO> priceList = priceDataMap.get(itemCodeStr);
			for (RetailPriceDTO priceDTO : priceList) {
				if (priceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID
						&& !zoneItem.getStoreNo().equals("") && zoneItem.getStoreNo().equals(priceDTO.getLevelId())) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					zoneItem.setCurrentRegPrice(currentPrice);
					zoneItem.setDiffRetail(round(zoneItem.getVdpRetail()-zoneItem.getCurrentRegPrice().getUnitPrice(),2));
					break;
				} else if (priceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID
						&& zoneItem.getPriceZoneNo().equals(priceDTO.getLevelId())) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					zoneItem.setCurrentRegPrice(currentPrice);
					zoneItem.setDiffRetail(round(zoneItem.getVdpRetail()-zoneItem.getCurrentRegPrice().getUnitPrice(),2));
					break;
				} else if (priceDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					zoneItem.setCurrentRegPrice(currentPrice);
					zoneItem.setDiffRetail(round(zoneItem.getVdpRetail()-zoneItem.getCurrentRegPrice().getUnitPrice(),2));
					break;
				}
			}
		}
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}
}
