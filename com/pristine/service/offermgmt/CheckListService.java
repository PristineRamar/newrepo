package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRZoneStoreReccommendationFlag;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.PriceCheckListTypeLookup;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class CheckListService {

	private static Logger logger = Logger.getLogger("CheckListService");

	public void populatePriceCheckListDetailsZone(Connection conn, int chainId, int divisionId, int locationLevelId,
			int locationId, int productLevelId, int productId, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo, int leadZoneId,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, PRZoneStoreReccommendationFlag zoneStoreRecFlag,
			HashMap<Integer, Integer> productParentChildRelationMap, PricingEngineDAO pricingEngineDAO,
			StrategyService strategyService, PRStrategyDTO leadInputDTO, PRStrategyDTO inputDTO, int leadZoneDivisionId)
			throws GeneralException, OfferManagementException {
		ArrayList<Integer> locationListId = pricingEngineDAO.getLocationListId(conn, locationLevelId, locationId);
		populatePriceCheckListDetails(conn, chainId, divisionId, locationId, locationLevelId, locationId,
				productLevelId, productId, itemDataMap, priceCheckListInfo, leadZoneId, strategyMap, retLirMap,
				productListMap, zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAO, strategyService,
				leadInputDTO, inputDTO, leadZoneDivisionId, locationListId, null, locationId);
	}

	public void populatePriceCheckListDetailsStore(Connection conn, int chainId, int divisionId, int zoneId,
			int productLevelId, int productId, HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore,
			HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo, int leadZoneId,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, PRZoneStoreReccommendationFlag zoneStoreRecFlag,
			HashMap<Integer, Integer> productParentChildRelationMap, PricingEngineDAO pricingEngineDAO,
			StrategyService strategyService, PRStrategyDTO leadInputDTO, PRStrategyDTO inputDTO, int leadZoneDivisionId)
			throws GeneralException, OfferManagementException {
		for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> allStoresAndItsItems : itemDataMapStore.entrySet()) {
			populatePriceCheckListDetails(conn, chainId, divisionId, zoneId, Constants.STORE_LEVEL_ID,
					allStoresAndItsItems.getKey(), productLevelId, productId, allStoresAndItsItems.getValue(),
					priceCheckListInfo, leadZoneId, strategyMap, retLirMap, productListMap, zoneStoreRecFlag,
					productParentChildRelationMap, pricingEngineDAO, strategyService, leadInputDTO, inputDTO,
					leadZoneDivisionId, null, null, zoneId);
		}
	}

	public void populatePriceCheckListDetails(Connection conn, int chainId, int divisionId, int zoneId,
			int locationLevelId, int locationId, int productLevelId, int productId,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo,
			int leadZoneId, HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<String, ArrayList<Integer>> productListMap,
			PRZoneStoreReccommendationFlag zoneStoreRecFlag, HashMap<Integer, Integer> productParentChildRelationMap,
			PricingEngineDAO pricingEngineDAO, StrategyService strategyService, PRStrategyDTO leadInputDTO,
			PRStrategyDTO inputDTO, int leadZoneDivisionId, ArrayList<Integer> locationListId,
			ArrayList<Integer> storeLocationListId, int priceZoneId) throws GeneralException, OfferManagementException {
		ItemKey itemKey;
		List<PriceCheckListDTO> allCheckList = null;
		PriceCheckListDTO pDTO;
		PriceCheckListDTO secondaryCheckListDTO;
		int checkListId, checkListTypeId;
		LocalDate recStartDate = LocalDate.parse(inputDTO.getStartDate(), PRCommonUtil.getDateFormatter());
		for (Map.Entry<ItemKey, List<PriceCheckListDTO>> itemAndItsList : priceCheckListInfo.entrySet()) {
			itemKey = itemAndItsList.getKey();
			allCheckList = itemAndItsList.getValue();

			if (allCheckList.size() > 0) {
				// First occurrence of check list
				// If an item is falling in more than one check list, then pick
				// one, based on location
				// if(allCheckList.size() > 1)
				pDTO = getPriceCheckListOfItem(conn, chainId, divisionId, zoneId, locationLevelId, locationId, itemKey,
						allCheckList, leadZoneId, strategyMap, retLirMap, productListMap, zoneStoreRecFlag,
						productParentChildRelationMap, pricingEngineDAO, itemDataMap, strategyService, leadInputDTO,
						inputDTO, leadZoneDivisionId, locationListId, storeLocationListId, priceZoneId);
				// else
				// pDTO = allCheckList.get(0);
				// 12th July 2016 AHOLD, item may fall in multiple check list,
				// but all the check list are not belong to the location of
				// current zone
				if (pDTO != null) {
					checkListId = pDTO.getPriceCheckListId();
					checkListTypeId = pDTO.getPriceCheckListTypeId();
					secondaryCheckListDTO=pDTO.getCheckListDTO();
					
					PRItemDTO item = itemDataMap.get(itemKey);
					if (item != null) {
						if (itemKey.getLirIndicator() == PRConstants.NON_LIG_ITEM_INDICATOR) {
							// non-lig,
							item.setPriceCheckListId(checkListId);
							item.setPriceCheckListTypeId(checkListTypeId);
							item.setUseLeadZoneStrategy(pDTO.getUseLeadZoneStrategy());
							if(pDTO.getEndDate() != null && !Constants.EMPTY.equals(pDTO.getEndDate())) {
								LocalDate restrictionEndDate = LocalDate.parse(pDTO.getEndDate(), PRCommonUtil.getDateFormatter());
								if(restrictionEndDate.isAfter(recStartDate)) {
									/** AI-123 change **/
									if (item.getMapRetail() > 0) {
										item.setMinRetail(item.getMapRetail());
									} else {
										item.setMinRetail(pDTO.getMinRetail());
									}
									item.setMaxRetail(pDTO.getMaxRetail());
									item.setLockedRetail(pDTO.getLockedRetail());
									item.setEndDate(pDTO.getEndDate());		
									if (secondaryCheckListDTO!=null && secondaryCheckListDTO.getPriceCheckListId() > 0) {
										/**AI-123 change**/
										if (item.getMapRetail() > 0) {
											secondaryCheckListDTO.setMinRetail(item.getMapRetail());
											if (item.getMinRetail() == 0) {
												item.setMinRetail(item.getMapRetail());
											}
										}
										item.setSecondaryPriceCheckList(secondaryCheckListDTO);
									}
								}
							}else {
								item.setMinRetail(pDTO.getMinRetail());
								item.setMaxRetail(pDTO.getMaxRetail());
								item.setLockedRetail(pDTO.getLockedRetail());
								item.setEndDate(pDTO.getEndDate());
								if (secondaryCheckListDTO!=null && secondaryCheckListDTO.getPriceCheckListId() > 0) {
									/**AI-123 change**/
									if (item.getMapRetail() > 0) {
										secondaryCheckListDTO.setMinRetail(item.getMapRetail());
										if (item.getMinRetail() == 0) {
											item.setMinRetail(item.getMapRetail());
										}
									}
									item.setSecondaryPriceCheckList(secondaryCheckListDTO);
								}
							}
						} else {
							// lig
							item.setPriceCheckListId(checkListId);
							item.setPriceCheckListTypeId(checkListTypeId);
							item.setUseLeadZoneStrategy(pDTO.getUseLeadZoneStrategy());
							// lig members
							for (PRItemDTO itemDTO : itemDataMap.values()) {
								if (itemDTO.getRetLirId() == itemKey.getItemCodeOrRetLirId()) {
									itemDTO.setPriceCheckListId(checkListId);
									itemDTO.setPriceCheckListTypeId(checkListTypeId);
									itemDTO.setUseLeadZoneStrategy(pDTO.getUseLeadZoneStrategy());
									for (PriceCheckListDTO priceCheckListDTO : allCheckList) {
										if (priceCheckListDTO.getPriceCheckListId() == checkListId
												&& priceCheckListDTO.getPriceCheckListTypeId() == checkListTypeId
												&& priceCheckListDTO.getItemCode() == itemDTO.getItemCode()) {
											if (priceCheckListDTO.getEndDate() != null
													&& !Constants.EMPTY.equals(priceCheckListDTO.getEndDate())) {
												LocalDate restrictionEndDate = LocalDate.parse(
														priceCheckListDTO.getEndDate(),
														PRCommonUtil.getDateFormatter());
												if (restrictionEndDate.isAfter(recStartDate)) {
													/** AI-123 change **/
													if (item.getMapRetail() > 0) {
														itemDTO.setMinRetail(item.getMapRetail());
													} else {
														itemDTO.setMinRetail(priceCheckListDTO.getMinRetail());
													}
													itemDTO.setMaxRetail(priceCheckListDTO.getMaxRetail());
													itemDTO.setLockedRetail(priceCheckListDTO.getLockedRetail());
													itemDTO.setEndDate(priceCheckListDTO.getEndDate());
													if (secondaryCheckListDTO!=null && secondaryCheckListDTO.getPriceCheckListId() > 0) {
														/**AI-123 change**/
														if (item.getMapRetail() > 0) {
															secondaryCheckListDTO.setMinRetail(item.getMapRetail());
															if (item.getMinRetail() == 0) {
																item.setMinRetail(item.getMapRetail());
															}
														}
														item.setSecondaryPriceCheckList(secondaryCheckListDTO);
													}
													
												
												}
												
											} else {
												/** AI-123 change **/
												if (item.getMapRetail() > 0) {
													itemDTO.setMinRetail(item.getMapRetail());
												} else {
													itemDTO.setMinRetail(priceCheckListDTO.getMinRetail());
												}
												itemDTO.setMaxRetail(priceCheckListDTO.getMaxRetail());
												itemDTO.setLockedRetail(priceCheckListDTO.getLockedRetail());
												itemDTO.setEndDate(priceCheckListDTO.getEndDate());
												if (secondaryCheckListDTO!=null && secondaryCheckListDTO.getPriceCheckListId() > 0) {
													/**AI-123 change**/
													if (item.getMapRetail() > 0) {
														secondaryCheckListDTO.setMinRetail(item.getMapRetail());
														if (item.getMinRetail() == 0) {
															item.setMinRetail(item.getMapRetail());
														}
													}
													item.setSecondaryPriceCheckList(secondaryCheckListDTO);
												}
												
												
											}
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
	}

	// private PriceCheckListDTO getPriceCheckListOfItem(int chainId, int
	// divisionId, int zoneId, int locationLevelId,
	// int locationId, List<PriceCheckListDTO> itemCheckListIds) {
	// PriceCheckListDTO pDTO = null;
	//
	// //Check if item list present at store level
	// if(locationLevelId == Constants.STORE_LEVEL_ID){
	// pDTO = getPriceCheckListOfItem(locationLevelId, locationId,
	// itemCheckListIds);
	// }
	//
	// //If not present then check at zone level
	// if(locationLevelId == Constants.STORE_LEVEL_ID && pDTO == null){
	// pDTO = getPriceCheckListOfItem(Constants.ZONE_LEVEL_ID, zoneId,
	// itemCheckListIds);
	// }else{
	// pDTO = getPriceCheckListOfItem(locationLevelId, locationId,
	// itemCheckListIds);
	// }
	//
	// //Still not present. Check if present at division level
	// if(pDTO == null)
	// pDTO = getPriceCheckListOfItem(Constants.DIVISION_LEVEL_ID, divisionId,
	// itemCheckListIds);
	//
	// //Still not present. Check if present at division level
	// if (pDTO == null)
	// pDTO = getPriceCheckListOfItem(Constants.CHAIN_LEVEL_ID, chainId,
	// itemCheckListIds);
	//
	// //If it is not available at above levels, return the first one
	// if(pDTO == null) {
	// //Changed on 2nd July 2015 for TOPS
	// //Give preference to check list which has check list type id
	// for(PriceCheckListDTO priceCheckListDTO: itemCheckListIds){
	// if(priceCheckListDTO.getPriceCheckListTypeId() > 0){
	// pDTO = priceCheckListDTO;
	// }
	// }
	// //Still if the check list is not found
	// if(pDTO == null)
	// pDTO = itemCheckListIds.get(0);
	// }
	// return pDTO;
	// }

	public PriceCheckListDTO getPriceCheckListOfItem(Connection conn, int chainId, int divisionId, int zoneId,
			int locationLevelId, int locationId, ItemKey itemKey, List<PriceCheckListDTO> allCheckList, int leadZoneId,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, PRZoneStoreReccommendationFlag zoneStoreRecFlag,
			HashMap<Integer, Integer> productParentChildRelationMap, PricingEngineDAO pricingEngineDAO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, StrategyService strategyService, PRStrategyDTO leadInputDTO,
			PRStrategyDTO inputDTO, int leadZoneDivisionId, ArrayList<Integer> locationListId,
			ArrayList<Integer> storeLocationListId, int priceZoneId) throws GeneralException, OfferManagementException {

		PriceCheckListDTO checkListOfItem = null;
		List<PriceCheckListDTO> filteredCheckListsTemp = new ArrayList<PriceCheckListDTO>();
		List<PriceCheckListDTO> filteredCheckLists = new ArrayList<PriceCheckListDTO>();

		// 23-May-2017: First Filter: Consider check list based on precedence,
		// pick only highest precedence check list
		int ligIndicator = itemKey.getLirIndicator();
		try {

			// Start of logic to pick check list based on precedence
			if (allCheckList != null && allCheckList.size() > 0) {

				// Grouping check lists by Precedence value
				HashMap<Integer, List<PriceCheckListDTO>> checkListByCheckListPrecedence = new HashMap<Integer, List<PriceCheckListDTO>>();

				List<PriceCheckListDTO> filteredByLocationList = filterCheckListsByLocation(chainId, divisionId, zoneId,
						locationLevelId, locationId, locationListId, allCheckList);
				
				if(filteredByLocationList != null) {

					// Added for handling min max values as part of KVI/K2 lists
					// This is added for RiteAid
					// Added by Pradeep on 09/28
					if(Boolean.parseBoolean(PropertyManager.getProperty("COPY_MIN_MAX_TO_OTHER_ITEM_LISTS", "FALSE"))) {
						copyMinMaxToOtherLists(filteredByLocationList);
					}
					
					for (PriceCheckListDTO checkList : filteredByLocationList) {

						Integer precedenceValue = checkList.getPrecedence();
						List<PriceCheckListDTO> precedenceFilteredList = new ArrayList<PriceCheckListDTO>();

						// In case the price check list type id is not found in
						// HashMap then the check list will
						// be mapped to null precedence

						if (checkListByCheckListPrecedence.get(precedenceValue) != null) {
							precedenceFilteredList = checkListByCheckListPrecedence.get(precedenceValue);
						}

						precedenceFilteredList.add(checkList);
						checkListByCheckListPrecedence.put(precedenceValue, precedenceFilteredList);
					}
				}
				

				boolean hasNullPrecedence = true;
				int iterationLevel = 1;
				// filteredCheckLists.clear();
				
				
				// Ligic to sort precendence
				// Added by Pradeep for fixing LIG with multiple checklists with multiple precedence
				// Sorted in ascending. So, lower precedence will take lead
				List<Integer> precedenceList = new ArrayList<>(checkListByCheckListPrecedence.keySet());
				
				precedenceList.sort(Comparator.nullsLast(Integer::compareTo));
				
				boolean checkOtherListTypes = Boolean
						.parseBoolean(PropertyManager.getProperty("CHECK_DIFF_ITEM_LIST", "FALSE"));

				if (checkOtherListTypes) {

					for (Integer precedence : precedenceList) {
						if (precedence != null ) {
							// Code added by Karishma on 08/20/21 for giving prefrence to other lists as
							// well when item is part of multiple lists including min max for AZ
							List<PriceCheckListDTO> highestPrecendenceList = checkListByCheckListPrecedence
									.get(precedence);

							Map<Integer, List<PriceCheckListDTO>> listById = highestPrecendenceList.stream()
									.collect(Collectors.groupingBy(PriceCheckListDTO::getPriceCheckListId));

							if (highestPrecendenceList.get(0).getCheckListTypeName()!=null && highestPrecendenceList.get(0).getCheckListTypeName().equalsIgnoreCase("MIN-MAX")) {
								if (highestPrecendenceList.size() > 1) {
									//get the latest itemList if two lists of same precedence present
									int checkListId = getLatestItemList(highestPrecendenceList);
									filteredCheckListsTemp.addAll(listById.get(checkListId));
								} else
									filteredCheckListsTemp.addAll(checkListByCheckListPrecedence.get(precedence));

								// if there is only min max list then break the loop else check list with second
								// highest precedence
								if (iterationLevel == 1 && precedenceList.size() == 1) {
									break;
								}

							} else {
								if (highestPrecendenceList.size() > 1) {
									//get the latest itemList if two lists of same precedence present
									int checkListId = getLatestItemList(highestPrecendenceList);
									filteredCheckListsTemp.addAll(listById.get(checkListId));
									break;
								} else {
									filteredCheckListsTemp.addAll(checkListByCheckListPrecedence.get(precedence));
									break;
								}

							}

						}

					}
				} else {
					for (Integer precedence : precedenceList) {
						if (precedence != null && hasNullPrecedence && iterationLevel == 1) {
							hasNullPrecedence = false;
							// No null precedences then only fetch the price check
							// list with highest precedence
							filteredCheckListsTemp.addAll(checkListByCheckListPrecedence.get(precedence));
							break;
						}

						if (hasNullPrecedence && checkListByCheckListPrecedence.keySet().size() < 2
								&& iterationLevel == 1) {
							// Add all price check list with null precedence if only
							// null precedences are present
							filteredCheckListsTemp.addAll(checkListByCheckListPrecedence.get(precedence));
							break;
						}

						if (iterationLevel == 2) {
							// Add all price check list with not null and highest
							// precedence if there are
							// multiple precedences with null precedence as a value
							filteredCheckListsTemp.addAll(checkListByCheckListPrecedence.get(precedence));
							break;
						}
						iterationLevel++;
					}

				}

			}
			// 23-May-2017 : End of precedence logic
			// To filter only WIC items(includes an item falls in more than one
			// WIC check list)
			List<PriceCheckListDTO> wicItemList = new ArrayList<PriceCheckListDTO>();
			for (PriceCheckListDTO priceCheckListDTO : filteredCheckListsTemp) {

				if (priceCheckListDTO.getPriceCheckListTypeId() == PriceCheckListTypeLookup.WIC
						.getCheckListTypeLookupId()) {
					wicItemList.add(priceCheckListDTO);
				} else {
					filteredCheckLists.add(priceCheckListDTO);
				}
			}
			// Get WIC items checklist and check lead or Dependent strategy is
			// available
			getWICItemCheckListBasedOnStrategy(conn, wicItemList, filteredCheckLists, chainId, divisionId, zoneId,
					locationLevelId, locationId, itemKey, allCheckList, leadZoneId, strategyMap, retLirMap,
					productListMap, zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAO, itemDataMap,
					strategyService, leadInputDTO, inputDTO, leadZoneDivisionId, locationListId, storeLocationListId,
					priceZoneId);

			/*
			 * // 17th Jun 2016, // Second Filter: Consider check list based on location, //
			 * pick only lowest level check list
			 * 
			 * // Group by level id for (PriceCheckListDTO checkList : filteredCheckLists) {
			 * LocationKey locationKey = new LocationKey(checkList.getLocationLevelId(),
			 * checkList.getLocationId()); List<PriceCheckListDTO> checkLists = new
			 * ArrayList<PriceCheckListDTO>();
			 * 
			 * if (checkListByLevelId.get(locationKey) != null) { checkLists =
			 * checkListByLevelId.get(locationKey); } checkLists.add(checkList);
			 * checkListByLevelId.put(locationKey, checkLists); }
			 * 
			 * // If the recommendation is for stores if (locationLevelId ==
			 * Constants.STORE_LEVEL_ID) { // pick all stores check list filteredCheckLists
			 * = getPriceCheckListOfItem(locationLevelId, locationId, checkListByLevelId); }
			 * 
			 * // If not present then check at zone level if (locationLevelId ==
			 * Constants.STORE_LEVEL_ID && filteredCheckLists == null) { // pick all zone
			 * check list filteredCheckLists =
			 * getPriceCheckListOfItem(Constants.ZONE_LEVEL_ID, zoneId, checkListByLevelId);
			 * } else { // pick all zone check list filteredCheckLists =
			 * getPriceCheckListOfItem(locationLevelId, locationId, checkListByLevelId); }
			 * 
			 * 
			 * // Check zone list level item list if (filteredCheckLists == null) { if
			 * (locationListId != null) { List<PriceCheckListDTO> zoneListLevelLists = new
			 * ArrayList<>(); for (Integer zoneListId : locationListId) {
			 * List<PriceCheckListDTO> itemLists =
			 * getPriceCheckListOfItem(Constants.ZONE_LIST_LEVEL_ID, zoneListId,
			 * checkListByLevelId); if (itemLists != null) {
			 * zoneListLevelLists.addAll(itemLists); } } if (zoneListLevelLists.size() > 0)
			 * { filteredCheckLists = new ArrayList<>();
			 * filteredCheckLists.addAll(zoneListLevelLists); } } }
			 * 
			 * // Still not present. pick check list at division level if
			 * (filteredCheckLists == null) filteredCheckLists =
			 * getPriceCheckListOfItem(Constants.DIVISION_LEVEL_ID, divisionId,
			 * checkListByLevelId);
			 * 
			 * // Still not present. pick check list at division level if
			 * (filteredCheckLists == null) filteredCheckLists =
			 * getPriceCheckListOfItem(Constants.CHAIN_LEVEL_ID, chainId,
			 * checkListByLevelId);
			 * 
			 * // Still not present. pick check list where location is not defined if
			 * (filteredCheckLists == null) { filteredCheckLists =
			 * getPriceCheckListOfItem(0, 0, checkListByLevelId); }
			 */

			// If still there are more than one strategy, then pick the first
			// one
			if (filteredCheckLists != null && filteredCheckLists.size() > 0) {
				// if it is lig, pick check list with highest occurrence
				if (ligIndicator == PRConstants.LIG_ITEM_INDICATOR) {
					
					boolean usePrecedenceForLIGLevelList = Boolean
							.parseBoolean(PropertyManager.getProperty("USE_PRECEDENCE_FOR_LIG_LEVEL_LIST", "FALSE"));
					
					HashMap<Integer, List<PriceCheckListDTO>> checkListByCheckListId = new HashMap<Integer, List<PriceCheckListDTO>>();

					// group by check list id
					for (PriceCheckListDTO checkList : filteredCheckLists) {
						List<PriceCheckListDTO> checkLists = new ArrayList<PriceCheckListDTO>();
						if (checkListByCheckListId.get(checkList.getPriceCheckListId()) != null) {
							checkLists = checkListByCheckListId.get(checkList.getPriceCheckListId());
						}
						checkLists.add(checkList);
						checkListByCheckListId.put(checkList.getPriceCheckListId(), checkLists);
					}

					int highestCount = 0;
					int precedence = 0;
					PriceCheckListDTO highestOccCheckList = filteredCheckLists.get(0);
					for (Map.Entry<Integer, List<PriceCheckListDTO>> entry : checkListByCheckListId.entrySet()) {
						PriceCheckListDTO currentList = entry.getValue().get(0);
						int currentPrecedence = currentList.getPrecedence() == null ? 999999 : currentList.getPrecedence();
						if(usePrecedenceForLIGLevelList) {
							if (currentPrecedence < precedence || precedence == 0) {
								highestCount = entry.getValue().size();
								highestOccCheckList = entry.getValue().get(0);
								precedence = currentPrecedence;
							}
						} else {
							if (entry.getValue().size() > highestCount) {
								highestCount = entry.getValue().size();
								highestOccCheckList = entry.getValue().get(0);
								precedence = currentPrecedence;
							}							
						}
					}
					checkListOfItem = highestOccCheckList;
				} else {
					if (filteredCheckLists.size() == 2) {
						PriceCheckListDTO minMxDTO = null;
						for (PriceCheckListDTO PriceCheckListDTO : filteredCheckLists) {
							// store min max LIST as secondary Option
							if (PriceCheckListDTO.getCheckListTypeName().equalsIgnoreCase("MIN-MAX")) {
								minMxDTO = PriceCheckListDTO;
							} else {
								checkListOfItem = PriceCheckListDTO;
							}

						}
						if (minMxDTO != null) {
							checkListOfItem.setCheckListDTO(minMxDTO);
						}

					} else {
						checkListOfItem = filteredCheckLists.get(0);
					}
				}
			}
		} catch (Exception e) {
			logger.error("Exception occurred in getPriceCheckListOfItem()", e);
			throw new GeneralException(
					"CheckListService: Exception occurred in getPriceCheckListOfItem() : " + e.getMessage());
		}

		// if(checkListByLocation != null && checkListByLocation.size() > 0) {
		// if(checkListByLocation.size() == 1) {
		// checkListOfItem = checkListByLocation.get(0);
		// } else {
		// //If still there are 2 check list
		// //Give weight to check list which has strategy
		// PriceCheckListDTO checkListWithStrategy = null;
		// for(PriceCheckListDTO priceCheckListDTO : checkListByLocation) {
		// //TODO:: check if strategy present
		//
		// if(checkListWithStrategy != null) {
		// checkListOfItem = checkListWithStrategy;
		// break;
		// }
		// }
		//
		// //If strategy is not available for any of the check list
		// //pick first occurrence
		// if(checkListWithStrategy == null) {
		// checkListOfItem = checkListByLocation.get(0);
		// }
		// }
		// }
		return checkListOfItem;
	}
	
	/**
	 * 
	 * @param chainId
	 * @param divisionId
	 * @param zoneId
	 * @param locationLevelId
	 * @param locationId
	 * @param locationListId
	 * @return item lists filtered by location
	 */
	private List<PriceCheckListDTO> filterCheckListsByLocation(int chainId, int divisionId, int zoneId,
			int locationLevelId, int locationId, ArrayList<Integer> locationListId,
			List<PriceCheckListDTO> allCheckList) {
		List<PriceCheckListDTO> filteredCheckLists = null;
		HashMap<LocationKey, List<PriceCheckListDTO>> checkListByLevelId = new HashMap<LocationKey, List<PriceCheckListDTO>>();
		// Group by level id
		for (PriceCheckListDTO checkList : allCheckList) {
			LocationKey locationKey = new LocationKey(checkList.getLocationLevelId(), checkList.getLocationId());
			List<PriceCheckListDTO> checkLists = new ArrayList<PriceCheckListDTO>();

			if (checkListByLevelId.get(locationKey) != null) {
				checkLists = checkListByLevelId.get(locationKey);
			}
			checkLists.add(checkList);
			checkListByLevelId.put(locationKey, checkLists);
		}

		// If the recommendation is for stores
		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			// pick all stores check list
			filteredCheckLists = getPriceCheckListOfItem(locationLevelId, locationId, checkListByLevelId);
		}

		// If not present then check at zone level
		if (locationLevelId == Constants.STORE_LEVEL_ID && filteredCheckLists == null) {
			// pick all zone check list
			filteredCheckLists = getPriceCheckListOfItem(Constants.ZONE_LEVEL_ID, zoneId, checkListByLevelId);
		} else {
			// pick all zone check list
			filteredCheckLists = getPriceCheckListOfItem(locationLevelId, locationId, checkListByLevelId);
		}

		// Check zone list level item list
		if (filteredCheckLists == null) {
			if (locationListId != null) {
				List<PriceCheckListDTO> zoneListLevelLists = new ArrayList<>();
				for (Integer zoneListId : locationListId) {
					List<PriceCheckListDTO> itemLists = getPriceCheckListOfItem(Constants.ZONE_LIST_LEVEL_ID,
							zoneListId, checkListByLevelId);
					if (itemLists != null) {
						zoneListLevelLists.addAll(itemLists);
					}
				}
				if (zoneListLevelLists.size() > 0) {
					filteredCheckLists = new ArrayList<>();
					filteredCheckLists.addAll(zoneListLevelLists);
				}
			}
		}

		// Still not present. pick check list at division level
		if (filteredCheckLists == null)
			filteredCheckLists = getPriceCheckListOfItem(Constants.DIVISION_LEVEL_ID, divisionId, checkListByLevelId);

		// Still not present. pick check list at division level
		if (filteredCheckLists == null)
			filteredCheckLists = getPriceCheckListOfItem(Constants.CHAIN_LEVEL_ID, chainId, checkListByLevelId);

		// Still not present. pick check list where location is not defined
		if (filteredCheckLists == null) {
			filteredCheckLists = getPriceCheckListOfItem(0, 0, checkListByLevelId);
		}

		return filteredCheckLists;
	}

	private List<PriceCheckListDTO> getPriceCheckListOfItem(int locationLevelId, int locationId,
			HashMap<LocationKey, List<PriceCheckListDTO>> checkListByLevelId) {
		List<PriceCheckListDTO> pDTO = null;
		LocationKey locationKey = new LocationKey(locationLevelId, locationId);
		pDTO = checkListByLevelId.get(locationKey);
		return pDTO;
	}

	// private PriceCheckListDTO getPriceCheckListOfItem(int locationLevelId,
	// int locationId, List<PriceCheckListDTO> itemCheckListIds){
	// PriceCheckListDTO pDTO = null;
	// for(PriceCheckListDTO checkList : itemCheckListIds) {
	// if(checkList.getLocationLevelId() == locationLevelId &&
	// checkList.getLocationId() == locationId){
	// pDTO = checkList;
	// break;
	// }
	// }
	// return pDTO;
	// }

	private List<PriceCheckListDTO> checkStrategyForGivenCheckList(Connection conn, int chainId, int divisionId,
			int zoneId, ItemKey itemKey, HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<String, ArrayList<Integer>> productListMap,
			PRZoneStoreReccommendationFlag zoneStoreRecFlag, HashMap<Integer, Integer> productParentChildRelationMap,
			PricingEngineDAO pricingEngineDAO, HashMap<ItemKey, PRItemDTO> itemDataMap, StrategyService strategyService,
			PRStrategyDTO inputDTO, PriceCheckListDTO priceCheckListDTO, PRStrategyDTO leadInputDTO,
			ArrayList<Integer> locationListId, ArrayList<Integer> storeLocationListId, int priceZoneId)
			throws OfferManagementException, GeneralException {

		List<PriceCheckListDTO> strategyItemsBasedOnCheckList = new ArrayList<PriceCheckListDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataToGetStrategyMap = new HashMap<ItemKey, PRItemDTO>();
		// Add Check list id with respect to item to get strategy id's.
		PRItemDTO item = itemDataMap.get(itemKey);
		if (item != null) {
			if (itemKey.getLirIndicator() == PRConstants.NON_LIG_ITEM_INDICATOR) {
				// non-lig,
				item.setPriceCheckListId(priceCheckListDTO.getPriceCheckListId());
				item.setPriceCheckListTypeId(priceCheckListDTO.getPriceCheckListTypeId());
			} else {
				// lig
				item.setPriceCheckListId(priceCheckListDTO.getPriceCheckListId());
				item.setPriceCheckListTypeId(priceCheckListDTO.getPriceCheckListTypeId());
				// lig members
				for (PRItemDTO itemDTO : itemDataMap.values()) {
					if (itemDTO.getRetLirId() == itemKey.getItemCodeOrRetLirId()) {
						itemDTO.setPriceCheckListId(priceCheckListDTO.getPriceCheckListId());
						itemDTO.setPriceCheckListTypeId(priceCheckListDTO.getPriceCheckListTypeId());
					}
				}
			}
			itemDataToGetStrategyMap.put(itemKey, item);
			
			// Added below condition to avoid connection in setting up check list
			// If Location list is passed from outside, connection is not required
			if(locationListId == null){
				
				// Get Strategies for the given item
				strategyService.getStrategiesForEachItem(conn, inputDTO, pricingEngineDAO, strategyMap,
						itemDataToGetStrategyMap, productParentChildRelationMap, retLirMap, productListMap,
						String.valueOf(chainId), divisionId, true, zoneStoreRecFlag, leadInputDTO, 0, 0);	
			}else{
				strategyService.getStrategies(inputDTO, strategyMap, itemDataMap, productParentChildRelationMap,
						retLirMap, productListMap, String.valueOf(chainId), divisionId, true, zoneStoreRecFlag, leadInputDTO, 0, 0,
						locationListId, storeLocationListId, priceZoneId);
			}
			
			
			PRItemDTO strategyItemDetail = itemDataToGetStrategyMap.get(itemKey);
			// If Strategy DTO is available
			if (strategyItemDetail.getStrategyDTO() != null) {
				PRStrategyDTO strategyForItem = strategyItemDetail.getStrategyDTO();
				if (strategyForItem.getStrategyId() > 0 && strategyForItem.getPriceCheckListId() == priceCheckListDTO.getPriceCheckListId()) {
					priceCheckListDTO.setStrategyId(strategyForItem.getStrategyId());
					strategyItemsBasedOnCheckList.add(priceCheckListDTO);
				}
			}
		}
		return strategyItemsBasedOnCheckList;
	}

	/**
	 * To get WIC item check list based on the strategy available for the
	 * processing zone. If recommendation for Dependent zone, then dependent
	 * zone strategy needs to be considered, if dependent zone is not available
	 * then lead zone strategy needs to be checked and applied if applicable
	 */
	private void getWICItemCheckListBasedOnStrategy(Connection conn, List<PriceCheckListDTO> wicItemList,
			List<PriceCheckListDTO> filteredCheckLists, int chainId, int divisionId, int zoneId, int locationLevelId,
			int locationId, ItemKey itemKey, List<PriceCheckListDTO> allCheckList, int leadZoneId,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, PRZoneStoreReccommendationFlag zoneStoreRecFlag,
			HashMap<Integer, Integer> productParentChildRelationMap, PricingEngineDAO pricingEngineDAO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, StrategyService strategyService, PRStrategyDTO leadInputDTO,
			PRStrategyDTO inputDTO, int leadZoneDivisionId, ArrayList<Integer> locationListId,
			ArrayList<Integer> storeLocationListId, int priceZoneId) throws OfferManagementException, GeneralException {
		// Check strategy are available for the list of WIC items
		List<PriceCheckListDTO> strategyItemsBasedOnCheckList = new ArrayList<PriceCheckListDTO>();

		// Group all Strategy based on Location level id
		HashMap<Integer, HashMap<StrategyKey, List<PRStrategyDTO>>> strategyGroupedByLocationLevel = new HashMap<Integer, HashMap<StrategyKey, List<PRStrategyDTO>>>();
		for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : strategyMap.entrySet()) {
			StrategyKey key = entry.getKey();
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMapBasedOnLocationlevel = new HashMap<StrategyKey, List<PRStrategyDTO>>();
			if (strategyGroupedByLocationLevel.containsKey(key.locationLevelId)) {
				strategyMapBasedOnLocationlevel = strategyGroupedByLocationLevel.get(key.locationLevelId);
			}
			strategyMapBasedOnLocationlevel.put(key, entry.getValue());
			strategyGroupedByLocationLevel.put(key.locationLevelId, strategyMapBasedOnLocationlevel);
		}

		// Consider strategies which is defined only at Zone level.
		if (strategyGroupedByLocationLevel.containsKey(Constants.ZONE_LEVEL_ID)) {
			HashMap<StrategyKey, List<PRStrategyDTO>> filteredStrategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
			HashMap<StrategyKey, List<PRStrategyDTO>> zoneLevelStrategyMap = strategyGroupedByLocationLevel.get(Constants.ZONE_LEVEL_ID);
			if (leadZoneId > 0) {
				// Dinesh:: 22-Nov-2017 Filter only dependent strategy. If Both Lead and dependent strategy were in list
				// and WIC item list of lead Strategy processed first, then wrong Strategy were assigned to items.
				// To avoid that case changes made to pass only Dependent strategy first, if none of price check list has Strategy
				// then consider Lead Strategy
				for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : zoneLevelStrategyMap.entrySet()) {
					if (entry.getKey().locationId == inputDTO.getLocationId()) {
						filteredStrategyMap.put(entry.getKey(), entry.getValue());
					}
				}

				strategyItemsBasedOnCheckList = getStrategyItemBasedOnCheckList(conn, wicItemList, filteredCheckLists, chainId, divisionId, zoneId,
						locationLevelId, locationId, itemKey, allCheckList, leadZoneId, filteredStrategyMap, retLirMap, productListMap,
						zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAO, itemDataMap, strategyService, leadInputDTO, inputDTO,
						leadZoneDivisionId, locationListId, storeLocationListId, priceZoneId);
			}
			if (strategyItemsBasedOnCheckList != null && strategyItemsBasedOnCheckList.size() == 0) {
				filteredStrategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
				for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : zoneLevelStrategyMap.entrySet()) {
					PRStrategyDTO leadZoneDTO = (leadZoneId > 0 ? leadInputDTO: inputDTO);
					if (entry.getKey().locationId == leadZoneDTO.getLocationId()) {
						filteredStrategyMap.put(entry.getKey(), entry.getValue());
					}
				}

				strategyItemsBasedOnCheckList = getStrategyItemBasedOnCheckList(conn, wicItemList, filteredCheckLists, chainId, divisionId, zoneId,
						locationLevelId, locationId, itemKey, allCheckList, leadZoneId, filteredStrategyMap, retLirMap, productListMap,
						zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAO, itemDataMap, strategyService, leadInputDTO, inputDTO,
						leadZoneDivisionId, locationListId, storeLocationListId, priceZoneId);
			}

			// If there is no strategy, keep the list unchanged
			if (strategyItemsBasedOnCheckList.size() > 0) {
				filteredCheckLists.clear();
				filteredCheckLists.addAll(strategyItemsBasedOnCheckList);
			} else if (wicItemList.size() > 0) {
				filteredCheckLists.addAll(wicItemList);
			}
		}
	}
	
	private List<PriceCheckListDTO> getStrategyItemBasedOnCheckList(Connection conn, List<PriceCheckListDTO> wicItemList,
			List<PriceCheckListDTO> filteredCheckLists, int chainId, int divisionId, int zoneId, int locationLevelId,
			int locationId, ItemKey itemKey, List<PriceCheckListDTO> allCheckList, int leadZoneId,
			HashMap<StrategyKey, List<PRStrategyDTO>> filteredStrategyMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, PRZoneStoreReccommendationFlag zoneStoreRecFlag,
			HashMap<Integer, Integer> productParentChildRelationMap, PricingEngineDAO pricingEngineDAO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, StrategyService strategyService, PRStrategyDTO leadInputDTO,
			PRStrategyDTO inputDTO, int leadZoneDivisionId, ArrayList<Integer> locationListId,
			ArrayList<Integer> storeLocationListId, int priceZoneId) throws OfferManagementException, GeneralException{
		
		List<PriceCheckListDTO> strategyItemsBasedOnCheckList = new ArrayList<PriceCheckListDTO>();
		
		for (PriceCheckListDTO priceCheckListDTO : wicItemList) {
			List<PriceCheckListDTO> itemBasedOnCheckList = new ArrayList<PriceCheckListDTO>();
			
			// If recommendation for dependent zone, then Check strategy available for Dependent zone
			if (leadZoneId > 0) {
				List<PriceCheckListDTO> strategyItemsList = checkStrategyForGivenCheckList(conn, chainId,
						divisionId, zoneId, itemKey, filteredStrategyMap, retLirMap, productListMap,
						zoneStoreRecFlag, productParentChildRelationMap, pricingEngineDAO, itemDataMap,
						strategyService, inputDTO, priceCheckListDTO, leadInputDTO, locationListId, storeLocationListId, priceZoneId);
				if (strategyItemsList != null && strategyItemsList.size() > 0) {
					for (PriceCheckListDTO priceCheckListDTO2 : strategyItemsList) {
						//To skip strategy at chain level. If given strategy is at chain level, then use it in lead zone strategy
							priceCheckListDTO2.setUseLeadZoneStrategy(String.valueOf(Constants.NO));
							itemBasedOnCheckList.add(priceCheckListDTO2);
					}
				}
			}
			
			// Check strategy available for Lead zone
			if (itemBasedOnCheckList.isEmpty()) {
				List<PriceCheckListDTO> strategyItemsList = checkStrategyForGivenCheckList(conn, chainId,
						(leadZoneId > 0) ? leadZoneDivisionId : divisionId, (leadZoneId > 0) ? leadZoneId : zoneId,
						itemKey, filteredStrategyMap, retLirMap, productListMap, zoneStoreRecFlag,
						productParentChildRelationMap, pricingEngineDAO, itemDataMap, strategyService,
						(leadZoneId > 0) ? leadInputDTO : inputDTO, priceCheckListDTO, leadInputDTO, 
								locationListId, storeLocationListId, priceZoneId);
				if (strategyItemsList != null && strategyItemsList.size() > 0) {
					for (PriceCheckListDTO priceCheckListDTO2 : strategyItemsList) {
						priceCheckListDTO2.setUseLeadZoneStrategy(String.valueOf(Constants.YES));
						itemBasedOnCheckList.add(priceCheckListDTO2);
					}
				}
			}
			if (itemBasedOnCheckList.size() > 0) {
				strategyItemsBasedOnCheckList.addAll(itemBasedOnCheckList);
				break;
			}
		}
		return strategyItemsBasedOnCheckList;
	}
	
	
	/**
	 * 
	 * @param itemListsByLocation
	 */
	private void copyMinMaxToOtherLists(List<PriceCheckListDTO> itemListsByLocation) {
		PriceCheckListDTO minMaxList = null;
		for(PriceCheckListDTO itemList: itemListsByLocation) {
			if(itemList.getMinRetail() > 0 || itemList.getMaxRetail() > 0) {
				minMaxList = itemList;
				break;
			}
		}
		
		if(minMaxList != null) {
			for(PriceCheckListDTO itemList: itemListsByLocation) {
				itemList.setMinRetail(minMaxList.getMinRetail());
				itemList.setMaxRetail(minMaxList.getMaxRetail());
			}
		}
	}

	/**
	 * @throws GeneralException
	 * 
	 */
	// If item is part of multiple item list of same precendence, get the latest
	// created/updated itemList
	public int getLatestItemList(List<PriceCheckListDTO> itemList) throws GeneralException {
		String createListDate = "";
		String updatedListDate = "";
		int itemLististId = 0;
		int entryFlag = 0;
		for (PriceCheckListDTO PriceCheckListDto : itemList) {
			if (entryFlag == 0) {
				createListDate = PriceCheckListDto.getCreateDate();
				updatedListDate = PriceCheckListDto.getUpdateDate();
				itemLististId = PriceCheckListDto.getPriceCheckListId();
				entryFlag++;
			} else {

				if (PriceCheckListDto.getCreateDate() != null && PriceCheckListDto.getUpdateDate() != null
						&& DateUtil.toDate(PriceCheckListDto.getCreateDate(), Constants.APP_DATE_FORMAT).compareTo(
								DateUtil.toDate(PriceCheckListDto.getUpdateDate(), Constants.APP_DATE_FORMAT)) == 0
						&& createListDate != null && updatedListDate != null
						&& DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT)
								.compareTo(DateUtil.toDate(updatedListDate, Constants.APP_DATE_FORMAT)) == 0) {
					if (DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT).compareTo(
							DateUtil.toDate(PriceCheckListDto.getCreateDate(), Constants.APP_DATE_FORMAT)) < 0) {

						createListDate = PriceCheckListDto.getCreateDate();
						updatedListDate = PriceCheckListDto.getUpdateDate();
						itemLististId = PriceCheckListDto.getPriceCheckListId();
					}
				} else if (PriceCheckListDto.getCreateDate() != null && PriceCheckListDto.getUpdateDate() != null
						&& createListDate != null && updatedListDate != null) {

					if (DateUtil.toDate(updatedListDate, Constants.APP_DATE_FORMAT)
							.compareTo(DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT)) > 0) {

						if (DateUtil.toDate(updatedListDate, Constants.APP_DATE_FORMAT).compareTo(
								DateUtil.toDate(PriceCheckListDto.getCreateDate(), Constants.APP_DATE_FORMAT)) < 0) {
							createListDate = PriceCheckListDto.getCreateDate();
							updatedListDate = PriceCheckListDto.getUpdateDate();
							itemLististId = PriceCheckListDto.getPriceCheckListId();

						} else if (DateUtil.toDate(updatedListDate, Constants.APP_DATE_FORMAT).compareTo(
								DateUtil.toDate(PriceCheckListDto.getUpdateDate(), Constants.APP_DATE_FORMAT)) < 0) {
							createListDate = PriceCheckListDto.getCreateDate();
							updatedListDate = PriceCheckListDto.getUpdateDate();
							itemLististId = PriceCheckListDto.getPriceCheckListId();
						}

					} else if (DateUtil.toDate(updatedListDate, Constants.APP_DATE_FORMAT)
							.compareTo(DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT)) < 0) {

						if (DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT).compareTo(
								DateUtil.toDate(PriceCheckListDto.getCreateDate(), Constants.APP_DATE_FORMAT)) < 0) {
							createListDate = PriceCheckListDto.getCreateDate();
							updatedListDate = PriceCheckListDto.getUpdateDate();
							itemLististId = PriceCheckListDto.getPriceCheckListId();

						} else if (DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT).compareTo(
								DateUtil.toDate(PriceCheckListDto.getUpdateDate(), Constants.APP_DATE_FORMAT)) < 0) {
							createListDate = PriceCheckListDto.getCreateDate();
							updatedListDate = PriceCheckListDto.getUpdateDate();
							itemLististId = PriceCheckListDto.getPriceCheckListId();
						}

					} else {
						if (DateUtil.toDate(createListDate, Constants.APP_DATE_FORMAT).compareTo(
								DateUtil.toDate(PriceCheckListDto.getUpdateDate(), Constants.APP_DATE_FORMAT)) < 0) {

							createListDate = PriceCheckListDto.getCreateDate();
							updatedListDate = PriceCheckListDto.getUpdateDate();
							itemLististId = PriceCheckListDto.getPriceCheckListId();
						}
					}

				}
			}

		}
		return itemLististId;
	}
}
