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
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
@SuppressWarnings("unused")
public class StorePriceExportAZServiceV3 {
	
	private static Logger logger = Logger.getLogger("StorePriceExportAZServiceV3");

	/**
	 * 
	 * @param salesFloorItemsTobeFiltered
	 * @param saleFloorLimitStr
	 * @param globalZoneRecommended 
	 * @param azZoneData 
	 * @return
	 * 
	 * FOR REFERENCE OF DESIGN DOC, CHECK SVN PATH \SVN\Documents\Autozone\Design\StorePriceExport\AI 19-b design.docx
	 * @throws CloneNotSupportedException 
	 */
	public List<PriceExportDTO> filterSalesFloorItemsByAbsoluteImpactV3(List<PriceExportDTO> salesFloorItems,
			String saleFloorLimitStr, List<ZoneDTO> zonesUnderGlobalZone)
			throws CloneNotSupportedException {

		List<PriceExportDTO> finalExportList = new ArrayList<>();

		// input value initiation
		String virtualZone = PropertyManager.getProperty("VIRTUAL_ZONE");
		int SFItemLimit = Integer.parseInt(saleFloorLimitStr);
		String thresholdStr = PropertyManager.getProperty("THRESHOLD");
		float threshold = Float.parseFloat(thresholdStr);
		int thresholdPct = (int) (new Double(SFItemLimit) * new Double((threshold / 100)));
		logger.debug("thresholdPct count: " + thresholdPct);
		List<String> zoneNumsUnderGlobalZone = new ArrayList<>();
		for (ZoneDTO zones : zonesUnderGlobalZone) {
			zoneNumsUnderGlobalZone.add(zones.getZnNo());
		}

		// 1. Filter priority items and add all priority items in to export
		// HashMap<String, Integer> distinctItemByZone = new HashMap<>();
		finalExportList = exportAllPriorityItemsV3(salesFloorItems, zonesUnderGlobalZone);

		List<PriceExportDTO> aggregatedPriorityItems = aggregatePriorityItemsV3(salesFloorItems, zonesUnderGlobalZone);

		// 2. Group all priority items by zone. Key = zone, value = list of distinct
		// item data. Store it as map called alreadyAddedSFloorItemsByZone
		// do this only when priority items are available
		Map<String, List<PriceExportDTO>> alreadyAddedSFloorItemsByZone = new HashMap<>();
		if (aggregatedPriorityItems.size() > 0) {
			alreadyAddedSFloorItemsByZone = aggregatedPriorityItems.stream()
					.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneNo));
		}

		// 3. Filter non-priority items
		List<PriceExportDTO> nonPriorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("N"))
				.collect(Collectors.toList());

		// 4. Populate a list called 'dataOfzonesUnderGlobalZone' by filtering zones
		// which is under zone 1000 (zone 4 and zone 16)
		// Do this only if atleast 1 global zone recommended RU is available
		// List<PRItemDTO> dataOfzonesUnderGlobalZone = new ArrayList<>();
		// if(globalZoneRecommended) {
		// dataOfzonesUnderGlobalZone = nonPriorityItems.stream().filter(e ->
		// zoneNumsUnderGlobalZone.contains(e.getPriceZoneNo())).collect(Collectors.toList());

		// FIND THE ORDER OF THE RUs BASED ON RANKING ORDERS
		TreeMap<Integer, List<PriceExportDTO>> gZoneRUDataInOrderOfRanking = orderTheRUsBasedOnRankingCriteriaV3(
				nonPriorityItems);

		// 8. Iterate the impact rank map, i.e., iterate individual RU
		for (Map.Entry<Integer, List<PriceExportDTO>> sfDataMapEntryForImpact : gZoneRUDataInOrderOfRanking.entrySet()) {

			if (sfDataMapEntryForImpact.getValue() != null && sfDataMapEntryForImpact.getValue().get(0) != null) {

				logger.debug("Iterating RU - " + sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
						+ " of highest impact: " + sfDataMapEntryForImpact.getValue().get(0).getTotal_Impact());

				// group the RU items by zone
				Map<String, List<PriceExportDTO>> itemsByZone = sfDataMapEntryForImpact.getValue().stream()
						.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneNo));

				for (Map.Entry<String, List<PriceExportDTO>> zoneEntry : itemsByZone.entrySet()) {

					int remainingItemForZone = findRemainingItemCountOfZoneV3(alreadyAddedSFloorItemsByZone, SFItemLimit,
							zoneEntry.getKey(), zoneNumsUnderGlobalZone);

					// if iterating zone is global zone and remaining item count for the zone is <
					// limit
					// then process the RU
					if (zoneEntry.getKey().equals(Constants.AZ_GLOBAL_ZONE) && remainingItemForZone > 0 
							&& remainingItemForZone < SFItemLimit) {

						// if count of items already added hit sales floor limit in any one of the zones
						// between zone 16 and zone 4 then
						// skip the RU and move to next RU.
						if (remainingItemForZone < SFItemLimit) {
							List<PriceExportDTO> filterSFItems = getFilteredSFItemsBasedOnItemImpactV3(remainingItemForZone,
									thresholdPct, sfDataMapEntryForImpact.getValue());

							// populate export data by zone map. Put key1 = zone 4,
							// value = list of distinct items added in export, key2 = zone 16, value = list
							// of distinct items added in export
							for (String gZones : zoneNumsUnderGlobalZone) {
								List<PriceExportDTO> templist = new ArrayList<>();
								if (alreadyAddedSFloorItemsByZone.containsKey(gZones)) {
									templist = alreadyAddedSFloorItemsByZone.get(gZones);
								}
								templist.addAll(filterSFItems);
								alreadyAddedSFloorItemsByZone.put(gZones, templist);

								finalExportList.addAll(filterSFItems);
							}
							
						} else {
							logger.debug("Items added in global zone has hit SF limit. Hence RU - "
									+ sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
									+ " is skipped to add in export");
						}
					}

					// if iterating zone is not global zone and remaining item count of the zone is
					// < limit
					// then process the RU
					else if (remainingItemForZone > 0 && remainingItemForZone < SFItemLimit) {

						List<PriceExportDTO> filterSFItems = getFilteredSFItemsBasedOnItemImpactV3(remainingItemForZone,
								thresholdPct, sfDataMapEntryForImpact.getValue());

						List<PriceExportDTO> templist = new ArrayList<>();
						if (alreadyAddedSFloorItemsByZone.containsKey(zoneEntry.getKey())) {
							templist = alreadyAddedSFloorItemsByZone.get(zoneEntry.getKey());
						}
						templist.addAll(filterSFItems);
						alreadyAddedSFloorItemsByZone.put(zoneEntry.getKey(), templist);

						finalExportList.addAll(filterSFItems);
					} else {
						logger.debug("RU " + sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
								+ " approved in zone: " + zoneEntry.getKey()
								+ " has reached the limit, hence items of this RU is not added in export");
					}
				}

			}

			// ITERATE DATA FOR EACH ZONE
			Set<Integer> uniqueExportItems = finalExportList.stream().map(PriceExportDTO::getItemCode)
					.collect(Collectors.toSet());
			Map<Integer, List<PriceExportDTO>> virtualZnData = salesFloorItems.stream()
					.filter(e -> e.getPriceZoneNo().equals(virtualZone)).collect(Collectors.toList()).stream()
					.collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

			for (Integer item : uniqueExportItems) {
				finalExportList.addAll(virtualZnData.get(item));
			}
		}
		return finalExportList;		
	}
	
	
	public List<PriceExportDTO> filterSalesFloorItemsByAbsoluteImpact(List<PriceExportDTO> salesFloorItems,
			String sFItemLimitStr) {

		int SFItemLimit = Integer.parseInt(sFItemLimitStr);

		List<PriceExportDTO> finalExportList = new ArrayList<PriceExportDTO>();		
		String thresholdStr = PropertyManager.getProperty("THRESHOLD");
		float threshold = Float.parseFloat(thresholdStr);

		int thresholdMax = (int) (SFItemLimit + (new Double(SFItemLimit) * new Double((threshold / 100))));
		SFItemLimit = thresholdMax;
		logger.debug("SF limit + threshold count: " + thresholdMax);
		/*logger.debug("threshold / 100: " + new Double((threshold / 100)));
		logger.debug("mul: " + new Double((new Double(SFItemLimit) * new Double((threshold / 100)))));*/

		int countOfDistinctItemsInExport = 0;
		List<PriceExportDTO> sfExportListByZone = new ArrayList<>();

		//new logic starts for Priority and Hard Date	
		//priority items logic - not considering the limit
		List<PriceExportDTO> priorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("Y")).collect(Collectors.toList());

		if(priorityItems != null && priorityItems.size() > 0) {
			logger.info("# in priorityItems: " + priorityItems.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size());
			sfExportListByZone.addAll(priorityItems);
			finalExportList.addAll(priorityItems);
			countOfDistinctItemsInExport = sfExportListByZone.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
		}

		//hard date items - considering the limit
		List<PriceExportDTO> hardDateItems = salesFloorItems.stream().filter(e -> e.getHdFlag().equals("Y")).collect(Collectors.toList());

		int remaningItemCountHardDates = SFItemLimit - countOfDistinctItemsInExport;
		logger.info("remaningItemCountHardDates: " + remaningItemCountHardDates);
		if(hardDateItems != null && hardDateItems.size() > 0) {
			int hdSize = hardDateItems.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
			logger.info("# in hardDateItems: " + hdSize);
			if(hdSize < remaningItemCountHardDates)
			{
				logger.info("hardDateItems.size() < remaningItemCountHardDates");
				sfExportListByZone.addAll(hardDateItems);
				finalExportList.addAll(hardDateItems);
				countOfDistinctItemsInExport = sfExportListByZone.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
			}
			else
			{
				int count = remaningItemCountHardDates;
				logger.info("# of hard date items exceeds the limit : remaningItemCountHardDates: " + remaningItemCountHardDates);
				//1. group by itemcode 
				HashMap<Integer, List<PriceExportDTO>> limithardDateItems = (HashMap<Integer, List<PriceExportDTO>>) hardDateItems
						.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

				//2. iterate over the map on itemcode to pull all the zones together
				for (Map.Entry<Integer, List<PriceExportDTO>> limithardDateItemsEntry : limithardDateItems.entrySet()) 
				{
					if(count > 0)
					{
						count--;
						sfExportListByZone.addAll(limithardDateItemsEntry.getValue());
						finalExportList.addAll(limithardDateItemsEntry.getValue());
					}

				}
				countOfDistinctItemsInExport = sfExportListByZone.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
			}

		}

		//Filter non priority items
		List<PriceExportDTO> nonpriorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("N")).collect(Collectors.toList());

		//filter items not marked as hard dates from non priority items list
		List<PriceExportDTO> regularItems = nonpriorityItems.stream().filter(e -> e.getHdFlag().equals("N")).collect(Collectors.toList());

		//new  logic ends for Priority and Hard Date
		// Sf map by approved week rank - early approved gets higher rank
		HashMap<Integer, List<PriceExportDTO>> salesFloorItemsByWeekRank = (HashMap<Integer, List<PriceExportDTO>>) regularItems
				.stream().collect(Collectors.groupingBy(PriceExportDTO::getSF_week_rank));

		TreeMap<Integer, List<PriceExportDTO>> sfItemsByWeekRankInOrder = new TreeMap<>(salesFloorItemsByWeekRank);

		logger.info("filterSalesFloorItemsByAbsoluteImpact() - filtering sales floor items starts ");

		// Iterate data for ordered week_rank
		for (Map.Entry<Integer, List<PriceExportDTO>> referenceMapWeekImpactEntry : sfItemsByWeekRankInOrder.entrySet()) 
		{

			logger.debug("Iterating week - " + referenceMapWeekImpactEntry.getKey());

			// form order wise map based on partially exported/ approved
			HashMap<Integer, List<PriceExportDTO>> ruBasedOnExportStatus = (HashMap<Integer, List<PriceExportDTO>>) referenceMapWeekImpactEntry
					.getValue().stream().collect(Collectors.groupingBy(PriceExportDTO::getSF_export_rank));

			TreeMap<Integer, List<PriceExportDTO>> ruBasedOnExportStatusInOrder = new TreeMap<>(ruBasedOnExportStatus);

			// iterate data for export status
			for (Map.Entry<Integer, List<PriceExportDTO>> sfDataMapEntryForExportStatus : ruBasedOnExportStatusInOrder
					.entrySet()) {

				logger.debug("Iterating export status with higher rank (partially/fully export) - "+ sfDataMapEntryForExportStatus.getKey());

				// form order wise map based on total impact of RU
				HashMap<Integer, List<PriceExportDTO>> ruBasedOnImpact = (HashMap<Integer, List<PriceExportDTO>>) sfDataMapEntryForExportStatus
						.getValue().stream().collect(Collectors.groupingBy(PriceExportDTO::getSF_RU_rank));

				TreeMap<Integer, List<PriceExportDTO>> ruBasedOnImpactInOrder = new TreeMap<>(ruBasedOnImpact);

				// iterate data for total impact of RU
				for (Map.Entry<Integer, List<PriceExportDTO>> sfDataMapEntryForImpact : ruBasedOnImpactInOrder.entrySet()) {

					if (sfDataMapEntryForImpact.getValue() != null
							&& sfDataMapEntryForImpact.getValue().get(0) != null) {

						logger.debug("Iterating RU - "+ sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
								+ " of highest impact: " + sfDataMapEntryForImpact.getValue().get(0).getTotal_Impact());
					}

					logger.debug("Existing count of items in final list - " + countOfDistinctItemsInExport);
					if (countOfDistinctItemsInExport < SFItemLimit) {

						List<Integer> distinctItemsOfRU = new ArrayList<>();
						for (PriceExportDTO impactData : sfDataMapEntryForImpact.getValue()) {
							distinctItemsOfRU.add(impactData.getItemCode());
						}

						int remaningItemCount = SFItemLimit - countOfDistinctItemsInExport;
						int iteratingCount = 0;

						if (sfDataMapEntryForImpact.getValue() != null
								&& sfDataMapEntryForImpact.getValue().get(0) != null) {
							logger.debug("# of remaining Items to add from week - " + referenceMapWeekImpactEntry.getKey()
							+ " of RU " + sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
							+ " is: " + remaningItemCount);
						}

						List<PriceExportDTO> tempList = new ArrayList<>();

						if (distinctItemsOfRU.size() <= remaningItemCount) {
							iteratingCount = iteratingCount + distinctItemsOfRU.size();
							logger.debug("iteratingCount - " + iteratingCount);	
							if (sfDataMapEntryForImpact.getValue() != null && sfDataMapEntryForImpact.getValue().get(0) != null) {
								logger.debug("All items (" + distinctItemsOfRU.size() + " items) of "+ sfDataMapEntryForImpact.getValue().get(0).getRecommendationUnit()
										+ " added to final list");
							}
							tempList.addAll(sfDataMapEntryForImpact.getValue());
							finalExportList.addAll(tempList);
							countOfDistinctItemsInExport = finalExportList.stream()
									.collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
						} else {
							// map produced to set key as item code of non family items
							HashMap<Integer, List<PriceExportDTO>> totalExportListByItem = (HashMap<Integer, List<PriceExportDTO>>) sfDataMapEntryForImpact
									.getValue().stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

							HashMap<String, List<PriceExportDTO>> totalExportListByFamilyItem = (HashMap<String, List<PriceExportDTO>>) sfDataMapEntryForImpact
									.getValue().stream()
									.filter(e -> (e.getFamilyName() != null || !e.getFamilyName().equals("")))
									.collect(Collectors.groupingBy(PriceExportDTO::getFamilyName));

							List<PriceExportDTO> itemsWithOverallImpact = new ArrayList<PriceExportDTO>();

							// sort all items by item level impact in descending order across approved zones
							for (Map.Entry<Integer, List<PriceExportDTO>> totalExportListEntry : totalExportListByItem
									.entrySet()) {

								List<PriceExportDTO> exportItemValues = totalExportListEntry.getValue();
								PriceExportDTO exportItem = null;

								PriceExportDTO clonedObjOfExportItem;
								try {
									if (totalExportListEntry.getValue() != null) {
										exportItem = totalExportListEntry.getValue().get(0);
									}
									if (exportItem != null) {
										clonedObjOfExportItem = (PriceExportDTO) exportItem.clone();

										double totalImpact = exportItemValues.stream().mapToDouble(p -> p.getImpact())
												.sum();
//										clonedObjOfExportItem.setImpact(Math.abs(totalImpact));
										clonedObjOfExportItem.setImpact(totalImpact);
										itemsWithOverallImpact.add(clonedObjOfExportItem);
									}
									// sort in descending order of price change impact
									Comparator<PriceExportDTO> compareByImpact = (PriceExportDTO o1, PriceExportDTO o2) -> o1
											.getImpact().compareTo(o2.getImpact());

									Collections.sort(itemsWithOverallImpact, compareByImpact.reversed());

								} catch (CloneNotSupportedException e) {
									e.printStackTrace();
								}
							}

							// iterating the item level impact ranking list
							for (PriceExportDTO itemWithConsolidatedRanking : itemsWithOverallImpact) {

								List<PriceExportDTO> familiesInAllLocations = new ArrayList<>();
								if (totalExportListByFamilyItem != null) {
									familiesInAllLocations = totalExportListByFamilyItem
											.get(itemWithConsolidatedRanking.getFamilyName());
								}

								List<PriceExportDTO> itemInAllLocations = totalExportListByItem
										.get(itemWithConsolidatedRanking.getItemCode());

								// group the ranking list by family name
								HashMap<String, List<PriceExportDTO>> rankingOrderByFamily = (HashMap<String, List<PriceExportDTO>>) itemsWithOverallImpact
										.stream().collect(Collectors.groupingBy(PriceExportDTO::getFamilyName));

								int counter = 0;
								if (!itemWithConsolidatedRanking.isFamilyProcessed()) {

									if (itemWithConsolidatedRanking.getFamilyName() != null
											&& !itemWithConsolidatedRanking.getFamilyName().equals("")) {

										logger.debug("filterSalesFloorItemsByImpact() - Family item: "
												+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
												+ itemWithConsolidatedRanking.getImpact());

										List<PriceExportDTO> familyListObj = rankingOrderByFamily
												.get(itemWithConsolidatedRanking.getFamilyName());
										counter = counter + familyListObj.size();
										iteratingCount = iteratingCount + counter;
										logger.debug("iteratingCount - " + iteratingCount);	
										/*if ((countOfDistinctItemsInExport + counter) <= remaningItemCount
												|| ((countOfDistinctItemsInExport + counter) > remaningItemCount
														&& (countOfDistinctItemsInExport + counter) <= thresholdMax)) {*/
										if (iteratingCount <= remaningItemCount
												/*|| ((iteratingCount > remaningItemCount)
															&& (iteratingCount <= thresholdMax))*/) {
											logger.debug("filterSalesFloorItemsByImpact() - Added "
													+ familyListObj.size() + " family items for family name: "
													+ itemWithConsolidatedRanking.getFamilyName());
											familyListObj.forEach(item -> {
												logger.debug(
														"filterSalesFloorItemsByAbsoluteImpact() - Family items of Family name "
																+ item.getFamilyName() + " are: Presto Itemcode - "
																+ item.getItemCode() + ", its impact: "
																+ item.getImpact());
											});

											tempList.addAll(familiesInAllLocations);
											finalExportList.addAll(tempList);
											countOfDistinctItemsInExport = finalExportList.stream()
													.collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
											logger.debug(
													"filterSalesFloorItemsByImpact() - # of items added: " + iteratingCount);
											for (PriceExportDTO flagSet : familyListObj) {
												flagSet.setFamilyProcessed(true);
											}
										} else if (iteratingCount > remaningItemCount) {
											logger.debug(
													"filterSalesFloorItemsByImpact() - Threshold Limit exceeded. Skipping "
															+ familyListObj.size() + " Family items for family name: "
															+ itemWithConsolidatedRanking.getFamilyName());
											break;
										}
									}

									else {
										logger.debug("filterSalesFloorItemsByImpact() - Non Family item: "
												+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
												+ itemWithConsolidatedRanking.getImpact());

										counter++;
										iteratingCount = iteratingCount+counter;
										logger.debug("iteratingCount - " + iteratingCount);	
										if (iteratingCount <= remaningItemCount) {
											logger.debug("filterSalesFloorItemsByImpact() - Adding non Family item: "
													+ itemWithConsolidatedRanking.getItemCode());
											tempList.addAll(itemInAllLocations);
											finalExportList.addAll(tempList);
											countOfDistinctItemsInExport = finalExportList.stream()
													.collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
											logger.debug(
													"filterSalesFloorItemsByImpact() - # of items added: " + iteratingCount);
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
		logger.info("# of items filtered: "
				+ finalExportList.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size());
		logger.info("filterSalesFloorItemsByAbsoluteImpact() - filtering sales floor items ends ");
		return finalExportList;

	}

	private List<PriceExportDTO> exportAllPriorityItemsV3(List<PriceExportDTO> salesFloorItems,
			List<ZoneDTO> zonesUnderGlobalZone) {
		List<PriceExportDTO> priorityItems = new ArrayList<>();	
		priorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("Y"))
				.collect(Collectors.toList());
				
		return priorityItems;
	}
	
	
	private List<PRItemDTO> exportAllPriorityItems(List<PRItemDTO> salesFloorItems,
			List<ZoneDTO> zonesUnderGlobalZone) {
		List<PRItemDTO> priorityItems = new ArrayList<>();	
		priorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("Y"))
				.collect(Collectors.toList());
				
		return priorityItems;
	}

	private int findRemainingItemCountOfZone(Map<String, List<PRItemDTO>> alreadyAddedSFloorItemsByZone,
			int sFItemLimit, String zoneNum, List<String> zoneNumsUnderGlobalZone) {
		int remainingItem = 0;
			
		if(zoneNum.equals(Constants.AZ_GLOBAL_ZONE)) {
			int maxItemCount = 0;
			
			int itemsInGlobalZone = 0;
			int maxItemCountOfGZones = 0;
			if(alreadyAddedSFloorItemsByZone.containsKey(zoneNum)) {
				itemsInGlobalZone = alreadyAddedSFloorItemsByZone.get(zoneNum).stream()
						.collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
			}
			
			for (Map.Entry<String, List<PRItemDTO>> gzoneEntry : alreadyAddedSFloorItemsByZone.entrySet()) {
				// Finding max distinct item count added between/among zones of global zone
				if (zoneNumsUnderGlobalZone.contains(gzoneEntry.getKey())) {
					int itemCount = gzoneEntry.getValue().stream()
							.collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
					if (itemCount > maxItemCountOfGZones) {
						maxItemCountOfGZones = itemCount;
						itemCount = 0;
					}
				}
			}
			
			maxItemCount = maxItemCountOfGZones + itemsInGlobalZone;
			
			remainingItem = sFItemLimit - maxItemCount;
		}
		else {
			int distinctItemInZone = 0;
			if(alreadyAddedSFloorItemsByZone.get(zoneNum) != null) {
				distinctItemInZone = alreadyAddedSFloorItemsByZone.get(zoneNum).stream()
					.collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
			}
			remainingItem = sFItemLimit - distinctItemInZone;
		}
		
		return remainingItem;
	}

	private int findRemainingItemCountOfZoneV3(Map<String, List<PriceExportDTO>> alreadyAddedSFloorItemsByZone,
			int sFItemLimit, String zoneNum, List<String> zoneNumsUnderGlobalZone) {
		int remainingItem = 0;
			
		if(zoneNum.equals(Constants.AZ_GLOBAL_ZONE)) {
			int maxItemCount = 0;
			
			int itemsInGlobalZone = 0;
			int maxItemCountOfGZones = 0;
			if(alreadyAddedSFloorItemsByZone.containsKey(zoneNum)) {
				itemsInGlobalZone = alreadyAddedSFloorItemsByZone.get(zoneNum).stream()
						.collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
			}
			
			for (Map.Entry<String, List<PriceExportDTO>> gzoneEntry : alreadyAddedSFloorItemsByZone.entrySet()) {
				// Finding max distinct item count added between/among zones of global zone
				if (zoneNumsUnderGlobalZone.contains(gzoneEntry.getKey())) {
					int itemCount = gzoneEntry.getValue().stream()
							.collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
					if (itemCount > maxItemCountOfGZones) {
						maxItemCountOfGZones = itemCount;
						itemCount = 0;
					}
				}
			}
			
			maxItemCount = maxItemCountOfGZones + itemsInGlobalZone;
			
			remainingItem = sFItemLimit - maxItemCount;
		}
		else {
			int distinctItemInZone = 0;
			if(alreadyAddedSFloorItemsByZone.get(zoneNum) != null) {
				distinctItemInZone = alreadyAddedSFloorItemsByZone.get(zoneNum).stream()
					.collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).size();
			}
			remainingItem = sFItemLimit - distinctItemInZone;
		}
		
		return remainingItem;
	}
	
	/**
	 * 
	 * @param remainingItemToAdd
	 * @param thresholdPct
	 * @param value
	 * @return filtered salesfloor item
	 */
	private List<PRItemDTO> getFilteredSFItemsBasedOnItemImpact(int remainingItemToAdd, int thresholdPct,
			List<PRItemDTO> sfItems) {

		
		List<PRItemDTO> finalExportList = new ArrayList<>();
		
	//	if (countOfDistinctItemsInExport < remainingItemToAdd) {
			
			
			//If distinct item count of RU is within remainingItemToAdd count then add all the items
			//else filter items in order of item impact
			List<Integer> distinctItemsOfRU = new ArrayList<>();
			for (PRItemDTO impactData : sfItems) {
				distinctItemsOfRU.add(impactData.getItemCode());
			}
			
			int iteratingCount = 0;
			//List<PRItemDTO> tempList = new ArrayList<>();
			if (distinctItemsOfRU.size() <= remainingItemToAdd) {
				iteratingCount = iteratingCount + distinctItemsOfRU.size();
				logger.debug("iteratingCount - " + iteratingCount);
				if (sfItems != null && sfItems.get(0) != null) {
					logger.debug("All items (" + distinctItemsOfRU.size() + " items) of "
							+ sfItems.get(0).getRecommendationUnit()+ " added to final list");
				}
				//tempList.addAll(sfItems);
				finalExportList.addAll(sfItems);
				//sfExportListByZone.addAll(tempList);
				//countOfDistinctItemsInExport = sfExportListByZone.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
			}
			else {
				//  Group the data by item code and form a map called totalExportListByItem
				HashMap<Integer, List<PRItemDTO>> totalExportListByItem = (HashMap<Integer, List<PRItemDTO>>) sfItems.stream()
						.collect(Collectors.groupingBy(PRItemDTO::getItemCode));

				//	Group the data by family name and form a map called FamilyItemMap
				HashMap<String, List<PRItemDTO>> totalExportListByFamilyItem = 
						(HashMap<String, List<PRItemDTO>>) sfItems.stream()
						.filter(e -> (e.getFamilyName() != null || !e.getFamilyName().equals("")))
						.collect(Collectors.groupingBy(PRItemDTO::getFamilyName));

				List<PRItemDTO> itemsWithOverallImpact = new ArrayList<PRItemDTO>();

				// Iterate individual item
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

					//If iterating item data is family item, then collect all family member items in list called familiesInAllLocations
					List<PRItemDTO> familiesInAllLocations = new ArrayList<>();
					if (totalExportListByFamilyItem != null) {
						familiesInAllLocations = totalExportListByFamilyItem
								.get(itemWithConsolidatedRanking.getFamilyName());
					}

					//Get Items in all location
					List<PRItemDTO> itemInAllLocations = totalExportListByItem
							.get(itemWithConsolidatedRanking.getItemCode());

					// group the ranking list by family name
					HashMap<String, List<PRItemDTO>> rankingOrderByFamily = 
							(HashMap<String, List<PRItemDTO>>) itemsWithOverallImpact
							.stream().collect(Collectors.groupingBy(PRItemDTO::getFamilyName));

					int counter = 0;
					//if the iterating item is a family item and it is not processed already then match the family name 
					//and pick the relative items from FamilyItemMap
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
							int tempCount = 0;
							if(iteratingCount > remainingItemToAdd) {
								tempCount = iteratingCount - remainingItemToAdd;
							}
							else if(iteratingCount <= thresholdPct) {
								tempCount = iteratingCount;
							}
							if ((iteratingCount <= remainingItemToAdd) || ((iteratingCount > remainingItemToAdd)
											&& (tempCount <= thresholdPct) && tempCount != 0)) {												 
							//if (iteratingCount <= remaningItemCount) {
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

								//tempList.addAll(familiesInAllLocations);
								finalExportList.addAll(familiesInAllLocations);
								/*sfExportListByZone.addAll(tempList);
								countOfDistinctItemsInExport = sfExportListByZone.stream()
										.collect(Collectors.groupingBy(PRItemDTO::getItemCode))
										.size();*/
								logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
										+ iteratingCount);
								for (PRItemDTO flagSet : familyListObj) {
									flagSet.setFamilyProcessed(true);
								}
											
							} 
							else if (iteratingCount > remainingItemToAdd) {
								iteratingCount = iteratingCount - counter;
								counter = counter - familyListObj.size();
								
								logger.info("new counter: " + counter);
								logger.info("new iteratingCount: " + iteratingCount);
								// else if ((countOfDistinctItemsInExport + counter) >
								// remaningItemCount) {
								logger.debug(
										"filterSalesFloorItemsByImpact() - Threshold Limit exceeded. Skipping "
												+ familyListObj.size()
												+ " Family items for family name: "
												+ itemWithConsolidatedRanking.getFamilyName());
								//break;
							}
						}

						else {
							logger.debug("filterSalesFloorItemsByImpact() - Non Family item: "
									+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
									+ itemWithConsolidatedRanking.getImpact());

							counter++;
							iteratingCount = iteratingCount + counter;
							logger.debug("iteratingCount - " + iteratingCount);
							if (iteratingCount <= remainingItemToAdd) {
								logger.debug(
										"filterSalesFloorItemsByImpact() - Adding non Family item: "
												+ itemWithConsolidatedRanking.getItemCode());
								//tempList.addAll(itemInAllLocations);
								finalExportList.addAll(itemInAllLocations);
								/*sfExportListByZone.addAll(tempList);
								countOfDistinctItemsInExport = sfExportListByZone.stream()
										.collect(Collectors.groupingBy(PRItemDTO::getItemCode))
										.size();*/
								logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
										+ iteratingCount);
							} else if (iteratingCount > remainingItemToAdd) {
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
		//}
		return finalExportList;	
	}
	
	/**
	 * 
	 * @param remainingItemToAdd
	 * @param thresholdPct
	 * @param value
	 * @return filtered salesfloor item
	 */
	private List<PriceExportDTO> getFilteredSFItemsBasedOnItemImpactV3(int remainingItemToAdd, int thresholdPct,
			List<PriceExportDTO> sfItems) {

	//	int countOfDistinctItemsInExport = 0;		
		
		List<PriceExportDTO> finalExportList = new ArrayList<>();
		
	//	if (countOfDistinctItemsInExport < remainingItemToAdd) {
			
			
			//If distinct item count of RU is within remainingItemToAdd count then add all the items
			//else filter items in order of item impact
			List<Integer> distinctItemsOfRU = new ArrayList<>();
			for (PriceExportDTO impactData : sfItems) {
				distinctItemsOfRU.add(impactData.getItemCode());
			}
			
			int iteratingCount = 0;
			//List<PRItemDTO> tempList = new ArrayList<>();
			if (distinctItemsOfRU.size() <= remainingItemToAdd) {
				iteratingCount = iteratingCount + distinctItemsOfRU.size();
				logger.debug("iteratingCount - " + iteratingCount);
				if (sfItems != null && sfItems.get(0) != null) {
					logger.debug("All items (" + distinctItemsOfRU.size() + " items) of "
							+ sfItems.get(0).getRecommendationUnit()+ " added to final list");
				}
				//tempList.addAll(sfItems);
				finalExportList.addAll(sfItems);
				//sfExportListByZone.addAll(tempList);
				//countOfDistinctItemsInExport = sfExportListByZone.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode)).size();
			}
			else {
				//  Group the data by item code and form a map called totalExportListByItem
				HashMap<Integer, List<PriceExportDTO>> totalExportListByItem = (HashMap<Integer, List<PriceExportDTO>>) sfItems.stream()
						.collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

				//	Group the data by family name and form a map called FamilyItemMap
				HashMap<String, List<PriceExportDTO>> totalExportListByFamilyItem = 
						(HashMap<String, List<PriceExportDTO>>) sfItems.stream()
						.filter(e -> (e.getFamilyName() != null || !e.getFamilyName().equals("")))
						.collect(Collectors.groupingBy(PriceExportDTO::getFamilyName));

				List<PriceExportDTO> itemsWithOverallImpact = new ArrayList<>();

				// Iterate individual item
				for (Map.Entry<Integer, List<PriceExportDTO>> totalExportListEntry : totalExportListByItem
						.entrySet()) {

					List<PriceExportDTO> exportItemValues = totalExportListEntry.getValue();
					PriceExportDTO exportItem = null;

					PriceExportDTO clonedObjOfExportItem;
					try {
						if (totalExportListEntry.getValue() != null) {
							exportItem = totalExportListEntry.getValue().get(0);
						}
						if (exportItem != null) {
							clonedObjOfExportItem = (PriceExportDTO) exportItem.clone();
							double totalImpact = exportItemValues.stream()
									.mapToDouble(p -> p.getImpact()).sum();																	
							clonedObjOfExportItem.setImpact(totalImpact);
							itemsWithOverallImpact.add(clonedObjOfExportItem);
						}						
						
						// sort in descending order of price change impact
						Comparator<PriceExportDTO> compareByImpact = (PriceExportDTO o1, PriceExportDTO o2) -> o1
								.getImpact().compareTo(o2.getImpact());

						Collections.sort(itemsWithOverallImpact, compareByImpact.reversed());

					} catch (CloneNotSupportedException e) {
						e.printStackTrace();
					}
				}

				// iterating the item level impact ranking list
				for (PriceExportDTO itemWithConsolidatedRanking : itemsWithOverallImpact) {

					//If iterating item data is family item, then collect all family member items in list called familiesInAllLocations
					List<PriceExportDTO> familiesInAllLocations = new ArrayList<>();
					if (totalExportListByFamilyItem != null) {
						familiesInAllLocations = totalExportListByFamilyItem
								.get(itemWithConsolidatedRanking.getFamilyName());
					}

					//Get Items in all location
					List<PriceExportDTO> itemInAllLocations = totalExportListByItem
							.get(itemWithConsolidatedRanking.getItemCode());

					// group the ranking list by family name
					HashMap<String, List<PriceExportDTO>> rankingOrderByFamily = 
							(HashMap<String, List<PriceExportDTO>>) itemsWithOverallImpact
							.stream().collect(Collectors.groupingBy(PriceExportDTO::getFamilyName));

					int counter = 0;
					//if the iterating item is a family item and it is not processed already then match the family name 
					//and pick the relative items from FamilyItemMap
					if (!itemWithConsolidatedRanking.isFamilyProcessed()) {

						if (itemWithConsolidatedRanking.getFamilyName() != null
								&& !itemWithConsolidatedRanking.getFamilyName().equals("")) {

							logger.debug("filterSalesFloorItemsByImpact() - Family item: "
									+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
									+ itemWithConsolidatedRanking.getImpact());

							List<PriceExportDTO> familyListObj = rankingOrderByFamily
									.get(itemWithConsolidatedRanking.getFamilyName());
							counter = counter + familyListObj.size();
							iteratingCount = iteratingCount + counter;
							logger.debug("iteratingCount - " + iteratingCount);
							int tempCount = 0;
							if(iteratingCount > remainingItemToAdd) {
								tempCount = iteratingCount - remainingItemToAdd;
							}
							else if(iteratingCount <= thresholdPct) {
								tempCount = iteratingCount;
							}
							if ((iteratingCount <= remainingItemToAdd) || ((iteratingCount > remainingItemToAdd)
											&& (tempCount <= thresholdPct) && tempCount != 0)) {												 
							//if (iteratingCount <= remaningItemCount) {
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

								//tempList.addAll(familiesInAllLocations);
								finalExportList.addAll(familiesInAllLocations);
								/*sfExportListByZone.addAll(tempList);
								countOfDistinctItemsInExport = sfExportListByZone.stream()
										.collect(Collectors.groupingBy(PRItemDTO::getItemCode))
										.size();*/
								logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
										+ iteratingCount);
								for (PriceExportDTO flagSet : familyListObj) {
									flagSet.setFamilyProcessed(true);
								}
											
							} 
							else if (iteratingCount > remainingItemToAdd) {
								iteratingCount = iteratingCount - counter;
								counter = counter - familyListObj.size();
								
								logger.info("new counter: " + counter);
								logger.info("new iteratingCount: " + iteratingCount);
								// else if ((countOfDistinctItemsInExport + counter) >
								// remaningItemCount) {
								logger.debug(
										"filterSalesFloorItemsByImpact() - Threshold Limit exceeded. Skipping "
												+ familyListObj.size()
												+ " Family items for family name: "
												+ itemWithConsolidatedRanking.getFamilyName());
								//break;
							}
						}

						else {
							logger.debug("filterSalesFloorItemsByImpact() - Non Family item: "
									+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
									+ itemWithConsolidatedRanking.getImpact());

							counter++;
							iteratingCount = iteratingCount + counter;
							logger.debug("iteratingCount - " + iteratingCount);
							if (iteratingCount <= remainingItemToAdd) {
								logger.debug(
										"filterSalesFloorItemsByImpact() - Adding non Family item: "
												+ itemWithConsolidatedRanking.getItemCode());
								//tempList.addAll(itemInAllLocations);
								finalExportList.addAll(itemInAllLocations);
								/*sfExportListByZone.addAll(tempList);
								countOfDistinctItemsInExport = sfExportListByZone.stream()
										.collect(Collectors.groupingBy(PRItemDTO::getItemCode))
										.size();*/
								logger.debug("filterSalesFloorItemsByImpact() - # of items added: "
										+ iteratingCount);
							} else if (iteratingCount > remainingItemToAdd) {
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
		//}
		return finalExportList;	
	}


	private TreeMap<Integer, List<PRItemDTO>> orderTheRUsBasedOnRankingCriteria(List<PRItemDTO> salesfloorItem ) {
		
		TreeMap<Integer, List<PRItemDTO>> ruBasedOnImpactInOrder = new TreeMap<>();
		
		//5.	Group the filtered list by approved week and form a map by key as week rank, value as list of items. 		
		HashMap<Integer, List<PRItemDTO>> salesFloorItemsByWeekRank = (HashMap<Integer, List<PRItemDTO>>) salesfloorItem
				.stream().collect(Collectors.groupingBy(PRItemDTO::getSF_week_rank));
		
		//      Order week rank map in ascending order.
		TreeMap<Integer, List<PRItemDTO>> sfItemsByWeekRankInOrder = new TreeMap<>(salesFloorItemsByWeekRank);
		
		//6.	Iterate the week rank map, 		
		for (Map.Entry<Integer, List<PRItemDTO>> referenceMapWeekImpactEntry : sfItemsByWeekRankInOrder
				.entrySet()) {
			logger.debug("Iterating week - " + referenceMapWeekImpactEntry.getKey());

			//			for each week rank, Form export status rank map by key as export status rank, value as list of items. 			
			HashMap<Integer, List<PRItemDTO>> ruBasedOnExportStatus = (HashMap<Integer, List<PRItemDTO>>) referenceMapWeekImpactEntry
					.getValue().stream().collect(Collectors.groupingBy(PRItemDTO::getSF_export_rank));
		
			//			Order export status (partially exported/ exported) map in ascending order. 
			TreeMap<Integer, List<PRItemDTO>> ruBasedOnExportStatusInOrder = new TreeMap<>(ruBasedOnExportStatus);
			
			//7.	Iterate the status rank map
			for (Map.Entry<Integer, List<PRItemDTO>> sfDataMapEntryForExportStatus : ruBasedOnExportStatusInOrder
					.entrySet()) {
				logger.debug("Iterating export status with higher rank (partially/fully export) - "
						+ sfDataMapEntryForExportStatus.getKey());
				
			//		for each status rank, Form impact rank map by key as impact rank, value as list of items. 
				HashMap<Integer, List<PRItemDTO>> ruBasedOnImpact = (HashMap<Integer, List<PRItemDTO>>) sfDataMapEntryForExportStatus
						.getValue().stream().collect(Collectors.groupingBy(PRItemDTO::getSF_RU_rank));
				//		Order impact rank map in ascending order.
				ruBasedOnImpactInOrder = new TreeMap<>(ruBasedOnImpact);
			}
		}
		return ruBasedOnImpactInOrder;		
	}
	
	private TreeMap<Integer, List<PriceExportDTO>> orderTheRUsBasedOnRankingCriteriaV3(List<PriceExportDTO> salesfloorItem ) {
		
		TreeMap<Integer, List<PriceExportDTO>> ruBasedOnImpactInOrder = new TreeMap<>();
		
		//5.	Group the filtered list by approved week and form a map by key as week rank, value as list of items. 		
		HashMap<Integer, List<PriceExportDTO>> salesFloorItemsByWeekRank = (HashMap<Integer, List<PriceExportDTO>>) salesfloorItem
				.stream().collect(Collectors.groupingBy(PriceExportDTO::getSF_week_rank));
		
		//      Order week rank map in ascending order.
		TreeMap<Integer, List<PriceExportDTO>> sfItemsByWeekRankInOrder = new TreeMap<>(salesFloorItemsByWeekRank);
		
		//6.	Iterate the week rank map, 		
		for (Map.Entry<Integer, List<PriceExportDTO>> referenceMapWeekImpactEntry : sfItemsByWeekRankInOrder
				.entrySet()) {
			logger.debug("Iterating week - " + referenceMapWeekImpactEntry.getKey());

			//			for each week rank, Form export status rank map by key as export status rank, value as list of items. 			
			HashMap<Integer, List<PriceExportDTO>> ruBasedOnExportStatus = (HashMap<Integer, List<PriceExportDTO>>) referenceMapWeekImpactEntry
					.getValue().stream().collect(Collectors.groupingBy(PriceExportDTO::getSF_export_rank));
		
			//			Order export status (partially exported/ exported) map in ascending order. 
			TreeMap<Integer, List<PriceExportDTO>> ruBasedOnExportStatusInOrder = new TreeMap<>(ruBasedOnExportStatus);
			
			//7.	Iterate the status rank map
			for (Map.Entry<Integer, List<PriceExportDTO>> sfDataMapEntryForExportStatus : ruBasedOnExportStatusInOrder
					.entrySet()) {
				logger.debug("Iterating export status with higher rank (partially/fully export) - "
						+ sfDataMapEntryForExportStatus.getKey());
				
			//		for each status rank, Form impact rank map by key as impact rank, value as list of items. 
				HashMap<Integer, List<PriceExportDTO>> ruBasedOnImpact = (HashMap<Integer, List<PriceExportDTO>>) sfDataMapEntryForExportStatus
						.getValue().stream().collect(Collectors.groupingBy(PriceExportDTO::getSF_RU_rank));
				//		Order impact rank map in ascending order.
				ruBasedOnImpactInOrder = new TreeMap<>(ruBasedOnImpact);
			}
		}
		return ruBasedOnImpactInOrder;		
	}
	

	/**
	 * 
	 * @param salesFloorItems
	 * @param zonesUnderGlobalZone 
	 * @return list of priority item data
	 * 
	 * Filter all priority items in salesfloor items
	 * @throws CloneNotSupportedException 
	 */
	private List<PRItemDTO> aggregatePriorityItems(List<PRItemDTO> salesFloorItems, List<ZoneDTO> zonesUnderGlobalZone) 
			throws CloneNotSupportedException {
		List<PRItemDTO> priorityItems = new ArrayList<>();	
		priorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("Y"))
				.collect(Collectors.toList());
		
		List<PRItemDTO> aggregatedPriorityItems = new ArrayList<>();
		for(PRItemDTO pitem : priorityItems) {
			if(pitem.getPriceZoneNo().equals(Constants.AZ_GLOBAL_ZONE)) {
				aggregatedPriorityItems.add(pitem);
				for(ZoneDTO gZnoes : zonesUnderGlobalZone) {
					PRItemDTO clonedItem = (PRItemDTO) pitem.clone();
					clonedItem.setPriceZoneNo(gZnoes.getZnNo());
					clonedItem.setPriceZoneId(gZnoes.getZnId());
					aggregatedPriorityItems.add(clonedItem);
				}
			}
			else {
				aggregatedPriorityItems.add(pitem);
			}
		}
		
		return aggregatedPriorityItems;
	}
	
	
	/**
	 * 
	 * @param salesFloorItems
	 * @param zonesUnderGlobalZone 
	 * @return list of priority item data
	 * 
	 * Filter all priority items in salesfloor items
	 * @throws CloneNotSupportedException 
	 */
	private List<PriceExportDTO> aggregatePriorityItemsV3(List<PriceExportDTO> salesFloorItems, List<ZoneDTO> zonesUnderGlobalZone) 
			throws CloneNotSupportedException {
		List<PriceExportDTO> priorityItems = new ArrayList<>();	
		priorityItems = salesFloorItems.stream().filter(e -> e.getPriority().equals("Y"))
				.collect(Collectors.toList());
		
		List<PriceExportDTO> aggregatedPriorityItems = new ArrayList<>();
		for(PriceExportDTO pitem : priorityItems) {
			if(pitem.getPriceZoneNo().equals(Constants.AZ_GLOBAL_ZONE)) {
				aggregatedPriorityItems.add(pitem);
				for(ZoneDTO gZnoes : zonesUnderGlobalZone) {
					PriceExportDTO clonedItem = (PriceExportDTO) pitem.clone();
					clonedItem.setPriceZoneNo(gZnoes.getZnNo());
					clonedItem.setPriceZoneId(gZnoes.getZnId());
					aggregatedPriorityItems.add(clonedItem);
				}
			}
			else {
				aggregatedPriorityItems.add(pitem);
			}
		}
		
		return aggregatedPriorityItems;
	}
	
	
	public List<PriceExportDTO> setZoneLevelPriceV3(List<PriceExportDTO> exportList, List<Integer> excludeZoneIdForVirtualZone,
			HashMap<Integer, List<StoreDTO>> zoneIdAndStoreNumMap) {

		HashMap<Integer, List<PriceExportDTO>> exportDataMap = (HashMap<Integer, List<PriceExportDTO>>) exportList.stream()
				.distinct().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

		List<PriceExportDTO> finalExportList = new ArrayList<PriceExportDTO>();

		if (exportDataMap.size() > 0) {
			exportDataMap.forEach((itemCode, exportObj) -> {
				exportObj.forEach(itemDTO -> {

					List<String> excludeEcomStores = getExcludedEcomStores();
					List<String> excludedStores = getStoresExcludeZoneLevelData();

					PriceExportDTO zoneLevelData = itemDTO;
					zoneLevelData.setChildLocationLevelId(Constants.ZONE_LEVEL_TYPE_ID);
					zoneLevelData.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
					finalExportList.add(zoneLevelData);

					if (excludeZoneIdForVirtualZone.contains(zoneLevelData.getPriceZoneId())) {
						//logs commented by bhargavi
//						logger.info("setZoneLevelPrice() - Exploding store level data for zone id- "
//								+ zoneLevelData.getPriceZoneId());
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
											PriceExportDTO storeLevelData = (PriceExportDTO) zoneLevelData.clone();
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


	public List<PriceExportDTO>  filterAndAddItemsBasedOnPriceTestDataV3(HashMap<Integer,List<PriceExportDTO>> itemStoreCombinationsFromPriceTest,
			List<PriceExportDTO> storeLockData, HashMap<String, String> storeNumIDMap) {		
		
		List<PriceExportDTO> destination = new ArrayList<>();
		
		Map<Integer, List<PriceExportDTO>> storeLockByItem = storeLockData.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));
		
		for (Map.Entry<Integer, List<PriceExportDTO>> sEntry : storeLockByItem.entrySet()) {
			if(itemStoreCombinationsFromPriceTest.containsKey(sEntry.getKey())) {					
				Set<String> stores = itemStoreCombinationsFromPriceTest.get(sEntry.getKey()).get(0).getStoreNums();
				List<PriceExportDTO> filteredItems = sEntry.getValue().stream().filter(e->!stores.contains(e.getStoreNo()))
						.collect(Collectors.toList());
				destination.addAll(filteredItems);
			}
			else {
				destination.addAll(sEntry.getValue());
			}
		}		
		return destination;
	}
	
	public List<PRItemDTO>  filterAndAddItemsBasedOnPriceTestData(HashMap<Integer,List<PRItemDTO>> itemStoreCombinationsFromPriceTest,
			List<PRItemDTO> storeLockData, HashMap<String, String> storeNumIDMap) {		
		
		List<PRItemDTO> destination = new ArrayList<>();
		
		Map<Integer, List<PRItemDTO>> storeLockByItem = storeLockData.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode));
		
		for (Map.Entry<Integer, List<PRItemDTO>> sEntry : storeLockByItem.entrySet()) {
			if(itemStoreCombinationsFromPriceTest.containsKey(sEntry.getKey())) {					
				Set<String> stores = itemStoreCombinationsFromPriceTest.get(sEntry.getKey()).get(0).getStoreNums();
				List<PRItemDTO> filteredItems = sEntry.getValue().stream().filter(e->!stores.contains(e.getStoreNo()))
						.collect(Collectors.toList());
				destination.addAll(filteredItems);
			}
			else {
				destination.addAll(sEntry.getValue());
			}
		}		
		return destination;
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
		//if (priceDataMap.containsKey(itemCodeStr)) {
			List<RetailPriceDTO> priceList = priceDataMap.get(itemCodeStr);
			logger.debug("# in priceList: " + priceList.size());
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
		//}
	}

	/**
	 * Retrieves the property ECOMMERCE_STORES. This property is expected to be a comma separated list of e-commerce store numbers.
	 * @return List of strings with each element representing a store numbers assigned to an e-commerce store.
	 */
	public List<String> getExcludedEcomStores() {
		String ecommerceStores = PropertyManager.getProperty("ECOMMERCE_STORES");
		String[] excludedEcomStoresArray = ecommerceStores.split(",");
		List<String> excludedEcomStoresList = Arrays.asList(excludedEcomStoresArray);
		return excludedEcomStoresList;
	}

	/**
	 * Retrieves the property STORES_EXCLUDE_ZONE_LEVEL_PRICES. This property is expected to be a comma separated list of store numbers. 
	 * These stores do NOT follow the pricing of their respective zones.
	 * @return List of strings with each element representing a store number.
	 */
	public List<String> getStoresExcludeZoneLevelData() {
		String excludedStores = PropertyManager.getProperty("STORES_EXCLUDE_ZONE_LEVEL_PRICES");
		String[] excludedStoresArray = excludedStores.split(",");
		List<String> excludedStoresList = Arrays.asList(excludedStoresArray);
		return excludedStoresList;
	}

	public void setDiffPriceForECitems(HashMap<String, List<RetailPriceDTO>> priceDataMap, PriceExportDTO zoneItem,
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


	
	public HashMap<String, RetailPriceDTO> getZoneOrChainLevelPrice(List<RetailPriceDTO> priceDataForItem, Set<String> storesUnderStoreLock,
			String zoneNum){
		HashMap<String, RetailPriceDTO> storeLockPrice = new HashMap<>();
		HashMap<String,List<RetailPriceDTO>> zoneLevelPrice = (HashMap<String, List<RetailPriceDTO>>) priceDataForItem.stream()
				.filter(e -> e.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID)
				.collect(Collectors.groupingBy(RetailPriceDTO::getLevelId));
		//only chain level
		if(zoneLevelPrice == null || !zoneLevelPrice.containsKey(zoneNum)) {
			
			if(storesUnderStoreLock == null) {
				storeLockPrice.put(zoneNum,priceDataForItem.get(0));
			}
			else {
				storesUnderStoreLock.forEach(storeLockStores -> {
					storeLockPrice.put(storeLockStores, priceDataForItem.get(0));
				});
			}
		}
		//zone level price
		else {
			RetailPriceDTO zonePrice = zoneLevelPrice.get(zoneNum).get(0);
			
			if(storesUnderStoreLock == null) {
				storeLockPrice.put(zoneNum,zonePrice);
			}
			else {
				storesUnderStoreLock.forEach(storeLockStores -> {
					storeLockPrice.put(storeLockStores, zonePrice);
				});
			}
		}
		return storeLockPrice;
	}


	private List<PriceExportDTO> getGlobalZoneData(PriceExportDTO referenceItem, HashMap<String, Integer> zoneMap,
			HashMap<Integer, String> zoneIdandNameMap) {

		List<PriceExportDTO> referenceList = new ArrayList<>();
		zoneMap.forEach((zoneNum, priceZoneId) -> {

			String zoneName = zoneIdandNameMap.get(priceZoneId);
			try {
				PriceExportDTO zoneItem = (PriceExportDTO) referenceItem.clone();
				zoneItem.setPriceZoneId(priceZoneId);
				zoneItem.setPriceZoneNo(zoneNum);
				zoneItem.setZoneName(zoneName);
				zoneItem.setGlobalZoneRecommended(true);
				referenceList.add(zoneItem);

			} catch (Exception e) {
				logger.debug("applyGlobalZonePriceToAllZones() - Error while setting global zone's price");
			}
		});
		return referenceList;
	
	}

	public HashMap<String, RetailPriceDTO> getCurrentPriceForItems(Set<String> storeNums, String zoneNum, Integer item,
			List<RetailPriceDTO> priceDataForItem) {
		HashMap<String, RetailPriceDTO> itemPrice = new HashMap<>();
		
		HashMap<String,List<RetailPriceDTO>> storeLevelPrice = (HashMap<String, List<RetailPriceDTO>>) priceDataForItem.stream()
				.filter(e -> e.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID)
				.collect(Collectors.groupingBy(RetailPriceDTO::getLevelId));
		
		//only zone level/chain level prices
		if(storeNums == null || storeLevelPrice == null) {
			itemPrice = getZoneOrChainLevelPrice(priceDataForItem, storeNums, zoneNum);
		}
		//store level prices
		else {
			Set<String> availableStoreLevelPrice = storeLevelPrice.keySet().stream()
					.filter(store-> storeNums.contains(store)).collect(Collectors.toSet());
			//zone level/chain level prices
			if(availableStoreLevelPrice == null) {
				itemPrice = getZoneOrChainLevelPrice(priceDataForItem, storeNums, zoneNum);
			}			
			//store level prices
			else {	
				for(String stores : availableStoreLevelPrice) {
					itemPrice.put(stores, storeLevelPrice.get(stores).get(0));
				}				
			}
		}
		
		return itemPrice;
	}

	/**
	 * @param storesUnderStoreLock
	 * @param item
	 * @param priceDataForItem
	 * @param zoneIdAndNoMap
	 * @param storeZoneMap
	 * @param baseChainId
	 * @return
	 */
	public HashMap<String, RetailPriceDTO> getCurrentPriceForStoreLockItems(Set<String> storesUnderStoreLock,
			 Integer item, List<RetailPriceDTO> priceDataForItem, HashMap<Integer, String> zoneIdAndNoMap, HashMap<String, Integer> storeZoneMap, String baseChainId) {
		
		HashMap<String, RetailPriceDTO> storeLockPrice = new HashMap<>();
		//This map will have zoneNo as key and list of str nos whose store level price is not found from priceMap.
		HashMap<String, List<String>> zoneStrnotFoundMap = new HashMap<>();
		
		 boolean storeLevelPricePopulated=false;
		
		HashMap<String,List<RetailPriceDTO>> storeLevelPrice = (HashMap<String, List<RetailPriceDTO>>) priceDataForItem.stream()
				.filter(e -> e.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID)
				.collect(Collectors.groupingBy(RetailPriceDTO::getLevelId));
		
		
		HashMap<String, List<RetailPriceDTO>> zoneLevelPrice = (HashMap<String, List<RetailPriceDTO>>) priceDataForItem
				.stream().filter(e -> e.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID)
				.collect(Collectors.groupingBy(RetailPriceDTO::getLevelId));

		HashMap<String, List<RetailPriceDTO>> chainLevelPrice = (HashMap<String, List<RetailPriceDTO>>) priceDataForItem
				.stream().filter(e -> e.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID)
				.collect(Collectors.groupingBy(RetailPriceDTO::getLevelId));

		//Populate the store price available for the stores in store lock
		if (storeLevelPrice.size() > 0) {
			storeLevelPricePopulated=true;
			for (String storeNo : storesUnderStoreLock) {
				if (storeLevelPrice.containsKey(storeNo)) {
					storeLockPrice.put(storeNo, storeLevelPrice.get(storeNo).get(0));
				} else {

					if (storeZoneMap.containsKey(storeNo)) {
						if (zoneIdAndNoMap.get(storeZoneMap.get(storeNo)) != null) {
							List<String> temp = new ArrayList<>();
							if (zoneStrnotFoundMap.containsKey(zoneIdAndNoMap.get(storeZoneMap.get(storeNo)))) {
								temp = zoneStrnotFoundMap.get(zoneIdAndNoMap.get(storeZoneMap.get(storeNo)));
							}
							temp.add(storeNo);
							zoneStrnotFoundMap.put(zoneIdAndNoMap.get(storeZoneMap.get(storeNo)), temp);
						}
					}

				}
			}
		}
		
		// if there is no store level price present for stores from storelock list then
		// check for zone or chain level Price
		if (zoneStrnotFoundMap.size() > 0) {
			for (Map.Entry<String, List<String>> zoneStrEntry : zoneStrnotFoundMap.entrySet()) {
				populateZoneOrChainPrice(zoneStrEntry.getKey(),zoneStrEntry.getValue(),zoneLevelPrice,chainLevelPrice,baseChainId,storeLockPrice);
			}
		}
		
		if (!storeLevelPricePopulated && zoneStrnotFoundMap.size() == 0) {

			HashMap<String, List<String>> zoneNoStrNoMap = new HashMap<>();

			for (String storeNo : storesUnderStoreLock) {

				if (storeZoneMap.containsKey(storeNo)) {
					String zoneNum = zoneIdAndNoMap.get(storeZoneMap.get(storeNo));
					if (zoneNum != null) {
						List<String> temp = new ArrayList<>();

						if (zoneNoStrNoMap.containsKey(zoneNum)) {
							temp = zoneNoStrNoMap.get(zoneNum);
						}
						temp.add(storeNo);
						zoneNoStrNoMap.put(zoneNum, temp);
					}

				}
			}

			
			for (Map.Entry<String, List<String>> zoneStrEntry : zoneNoStrNoMap.entrySet()) {
				populateZoneOrChainPrice(zoneStrEntry.getKey(), zoneStrEntry.getValue(), zoneLevelPrice,
						chainLevelPrice, baseChainId, storeLockPrice);
			}
			
		}
		return storeLockPrice;
	}
	
	/**
	 * This function will populate zone or Chain Price
	 * @param ZoneNo
	 * @param storeList
	 * @param zoneLevelPrice
	 * @param chainLevelPrice
	 * @param baseChainId
	 * @param storeLockPrice
	 */
	public void populateZoneOrChainPrice(String ZoneNo, List<String> storeList,
			HashMap<String, List<RetailPriceDTO>> zoneLevelPrice, HashMap<String, List<RetailPriceDTO>> chainLevelPrice,
			String baseChainId, HashMap<String, RetailPriceDTO> storeLockPrice) {
		
		// If zone Price is present then use that else use chain price for all the
		// remaining stores
		if (zoneLevelPrice.containsKey(ZoneNo)) {

			RetailPriceDTO zonePrice = zoneLevelPrice.get(ZoneNo).get(0);

			for (String storeNo : storeList) {
				storeLockPrice.put(storeNo, zonePrice);
			}

		} else {
//			if (baseChainId != "") {
			if (chainLevelPrice.get(baseChainId) != null) {
				RetailPriceDTO chainPrice = chainLevelPrice.get(baseChainId).get(0);
				for (String storeNo : storeList) {
					storeLockPrice.put(storeNo, chainPrice);
				}
			}else
			{
				logger.error("Price for the BaseChainId is not Present...");
			}
		}

	}
}
