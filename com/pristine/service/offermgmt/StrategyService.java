package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRConstraintLocPrice;
import com.pristine.dto.offermgmt.PRConstraintMinMax;
import com.pristine.dto.offermgmt.PRConstraintsDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRZoneStoreReccommendationFlag;
//import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class StrategyService {
	private static Logger logger = Logger.getLogger("Strategy");
	private List<ExecutionTimeLog> executionTimeLogs;
	private StrategyDAO strategyDAO = null;
	private ExecutionTimeLog executionTimeLog = null;
	public Boolean isStrategyFound = false;
	
	public StrategyService(List<ExecutionTimeLog> executionTimeLogs) {
		this.executionTimeLogs = executionTimeLogs;
	}

	public HashMap<StrategyKey, List<PRStrategyDTO>> getAllActiveStrategies(Connection conn, PRStrategyDTO strategyInput,
			int divisionIdOfZone)
			throws OfferManagementException {
		HashMap<StrategyKey, List<PRStrategyDTO>> locationProductStrategyMap = null;
		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_STRATEGIES);
		strategyDAO = new StrategyDAO();
		
		locationProductStrategyMap = strategyDAO.getAllActiveStrategies(conn, strategyInput, divisionIdOfZone);
			
		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);
		return locationProductStrategyMap;
	}

	public void getStrategiesForEachItem(Connection conn, PRStrategyDTO dependentZoneInputDTO,
			PricingEngineDAO pricingEngineDAO, HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, Integer> productParentChildRelationMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<String, ArrayList<Integer>> productListMap,
			String chainId, int dependentDivisionId, boolean isZoneItem,
			PRZoneStoreReccommendationFlag zoneStoreRecFlag, PRStrategyDTO leadInputDTO, int leadZoneDivisionId,
			int leadZoneId) throws OfferManagementException, GeneralException {
		PRStrategyDTO inputDTO = (leadZoneId > 0 ? leadInputDTO : dependentZoneInputDTO);
		int locationLevelId = inputDTO.getLocationLevelId();
		int locationId = inputDTO.getLocationId();
		isStrategyFound = false;

		// Get location list ids containing input location.
		// ArrayList<LocationListId>
		ArrayList<Integer> locationListId = pricingEngineDAO.getLocationListId(conn, locationLevelId, locationId);

		int priceZoneId = -1;
		ArrayList<Integer> storeLocationListId = null;

		// Trying to assign store level strategy for an item
		if (locationLevelId == 5) {
			// Get location list ids containing input location.
			// ArrayList<LocationListId>
			storeLocationListId = pricingEngineDAO.getStoreLocationListId(conn, locationLevelId, locationId);
			// Get price zone id for store
			priceZoneId = pricingEngineDAO.getPriceZoneIdForStore(conn, locationId);
		}

		getStrategies(dependentZoneInputDTO, inpStrategyMap, itemDataMap, productParentChildRelationMap, retLirMap,
				productListMap, chainId, dependentDivisionId, isZoneItem, zoneStoreRecFlag, leadInputDTO,
				leadZoneDivisionId, leadZoneId, locationListId, storeLocationListId, priceZoneId);

	}

	/**
	 * 
	 * @param dependentZoneInputDTO
	 * @param pricingEngineDAO
	 * @param inpStrategyMap
	 * @param itemDataMap
	 * @param productParentChildRelationMap
	 * @param retLirMap
	 * @param productListMap
	 * @param chainId
	 * @param dependentDivisionId
	 * @param isZoneItem
	 * @param zoneStoreRecFlag
	 * @param leadInputDTO
	 * @param leadZoneDivisionId
	 * @param leadZoneId
	 * @param locationListId
	 * @param storeLocationListId
	 * @param priceZoneId
	 * @throws OfferManagementException 
	 * @throws CloneNotSupportedException 
	 */
	public void getStrategies(PRStrategyDTO dependentZoneInputDTO,
			HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, Integer> productParentChildRelationMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, String chainId, int dependentDivisionId,
			boolean isZoneItem, PRZoneStoreReccommendationFlag zoneStoreRecFlag, PRStrategyDTO leadInputDTO,
			int leadZoneDivisionId, int leadZoneId, ArrayList<Integer> locationListId,
			ArrayList<Integer> storeLocationListId, int priceZoneId) throws OfferManagementException {
		HashMap<Integer, Integer> childParentMap = new HashMap<Integer, Integer>();
		PRStrategyDTO inputDTO = (leadZoneId > 0 ? leadInputDTO : dependentZoneInputDTO);
		int locationLevelId = inputDTO.getLocationLevelId();
		int locationId = inputDTO.getLocationId();
		int divisionId = 0;
		int ITEM_LIST_TEMP_REP_ID = 1001, VENDOR_TEMP_REP_ID = 1002, STATE_TEMP_REP_ID = 1003, CRITERIA_TEMP_REP_ID = 1004;
		try {

			for (Map.Entry<Integer, Integer> entry : productParentChildRelationMap.entrySet()) {
				childParentMap.put(entry.getValue(), entry.getKey());
			}
			for (PRItemDTO item : itemDataMap.values()) {
				PRStrategyDTO strategyForItem = new PRStrategyDTO();
				int productLevelId = Constants.ITEMLEVELID;
				int productId = item.getItemCode();

				// if(item.getItemCode() == 208654){
				// logger.debug("Stop Log");
				// }

				List<Integer> itemListIds = new ArrayList<Integer>();

				// Check list id of each item would have been found when it comes here
				if (item.getPriceCheckListId() != null && item.getPriceCheckListId() > 0)
					itemListIds.add(item.getPriceCheckListId());
				
				//check for secondary itemList obj
				if( item.getSecondaryPriceCheckList()!=null && item.getSecondaryPriceCheckList().getPriceCheckListId()>0)
					itemListIds.add(item.getSecondaryPriceCheckList().getPriceCheckListId());

				// Pick a Product and loop through location, e.g. check if item
				// level strategy is defined at store, zone, zone list,....
				// if not found go to item list, check if it is defined any of
				// the location, if not then pick then next product(segment) and
				// check if it is defined is any of the location, .....
				// Then go to Segment and locations, Segment List and Locations etc...

				productLevelId = Constants.ITEMLEVELID;
				productId = item.getItemCode();
				boolean allProductsChecked = false;
				boolean restoreNormalProdFlow = false;
				StrategyKey strategyKey;
				// Loops through product hierarchy. Item -> Segment -> Segment List -> Subcategory -> Subcategory List -> Category
				// -> Category List -> Portfolio -> Portfolio List -> Major Category -> Major category list -> All products
				while (strategyForItem.getStrategyId() == 0) {
					// String proKey = productLevelId + "-" + productId;
					boolean isStoreLocationListChecked = false;
					@SuppressWarnings("unused")
					boolean isZoneLevelChecked = false;
					boolean isLocationListChecked = false;
					boolean isDivisionLevelChecked = false;
					boolean isChainLevelChecked = false;
					int locationListCounter = 0;
					int storeLocationListCounter = 0;
					locationLevelId = inputDTO.getLocationLevelId();
					// locKey = inputDTO.getLocationLevelId() + "-" + locationId;
					// Based on UseLeadZone flag assign location Id using lead zone or Dependent Zone By Dinesh(08/22/2017)
					// Changes to get Strategy location id for the item based on Check list available for the Given Location.
					int strategyLocationId = 0;
					if (item.getUseLeadZoneStrategy() != null) {
						if (String.valueOf(Constants.NO).equals(item.getUseLeadZoneStrategy())) {
							strategyLocationId = dependentZoneInputDTO.getLocationId();
							divisionId = dependentDivisionId;
						} else if (String.valueOf(Constants.YES).equals(item.getUseLeadZoneStrategy())) {
							strategyLocationId = leadInputDTO.getLocationId();
							divisionId = leadZoneDivisionId;
						}
					}

					if (divisionId == 0) {
						divisionId = (leadZoneId > 0 ? leadZoneDivisionId : dependentDivisionId);
					}
					strategyKey = new StrategyKey(inputDTO.getLocationLevelId(),
							(strategyLocationId > 0 ? strategyLocationId : locationId), productLevelId, productId);
					// Loops through location hierarchy. Store -> Store List -> Zone -> Zone List -> Division -> Chain
					while (strategyForItem.getStrategyId() == 0) {

						// Traverse location->then each product, if it is item list / vendor / state
						// e.g. zone->sub cat, zone->cat, zone->maj cat, ... zoneList->sub cat, zoneList->Cat.
						// This is written to handle item list defined at any level e.g. category->item list, all products->item
						// list

						if (productLevelId == ITEM_LIST_TEMP_REP_ID) {
							strategyForItem = handleItemListVendorState(item, locationLevelId, strategyKey.locationId,
									inpStrategyMap, childParentMap, itemListIds, true, false, false, isZoneItem, productListMap, false);
						} else if (productLevelId == VENDOR_TEMP_REP_ID) {
							if (!isZoneItem)
								strategyForItem = handleItemListVendorState(item, locationLevelId,
										strategyKey.locationId, inpStrategyMap, childParentMap, itemListIds, false,
										true, false, isZoneItem, productListMap, false);
						} else if (productLevelId == STATE_TEMP_REP_ID) {
							if (!isZoneItem)
								strategyForItem = handleItemListVendorState(item, locationLevelId,
										strategyKey.locationId, inpStrategyMap, childParentMap, itemListIds, false,
										false, true, isZoneItem, productListMap, false);
						} else if(productLevelId == CRITERIA_TEMP_REP_ID) {
							if(item.getCriteriaId() > 0) {
//								logger.debug("Item with criteria: " + item.getItemCode());
								strategyForItem = handleItemListVendorState(item, locationLevelId,
										strategyKey.locationId, inpStrategyMap, childParentMap, itemListIds, false,
										false, false, isZoneItem, productListMap, true);	
							}
						} else {
							// Get all Strategies available for the product & location combination
							List<PRStrategyDTO> strategies = inpStrategyMap.get(strategyKey);

							// Check if strategy available for the product and location combination
							if (strategies != null) {
								if (isZoneItem) {
									strategyForItem = getStrategyOfZoneItem(productLevelId, item, strategies,
											itemListIds);
								} else {
									strategyForItem = getStrategyOfStoreItem(productLevelId, item, strategies,
											itemListIds, locationLevelId);
								}
							}

							// Still if strategy is not found for item
							if (strategyForItem.getStrategyId() == 0) {
								// Check against product list
								if (productListMap
										.get(strategyKey.productLevelId + "-" + strategyKey.productId) != null) {
									List<Integer> pListId = productListMap
											.get(strategyKey.productLevelId + "-" + strategyKey.productId);
									for (Integer pId : pListId) {
										// Get all Product List categories
										StrategyKey strategyKeyPrdList = new StrategyKey(strategyKey.locationLevelId,
												strategyKey.locationId, PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID, pId);
										if (inpStrategyMap.get(strategyKeyPrdList) != null) {
											List<PRStrategyDTO> prdListStrategies = inpStrategyMap
													.get(strategyKeyPrdList);
											if (isZoneItem) {
												strategyForItem = getStrategyOfZoneItem(productLevelId, item,
														prdListStrategies, itemListIds);
											} else {
												strategyForItem = getStrategyOfStoreItem(productLevelId, item,
														prdListStrategies, itemListIds, locationLevelId);
											}
										}
									}
								}
							}

							// Find if store level recommendation needs to be done,
							// Check if any strategy defined from Category/Zone to higher level has Store level
							// recommendation flag
							if (productLevelId == Constants.CATEGORYLEVELID || productLevelId == Constants.PORTFOLIO
									|| productLevelId == Constants.DEPARTMENTLEVELID
									|| productLevelId == Constants.ALLPRODUCTS) {
								// If a strategy is found and store recommendation flag is not determined
								if (strategyForItem.getStrategyId() > 0 && !zoneStoreRecFlag.isRecommendAtStoreLevel) {
									if (strategyForItem
											.getDsdRecommendationFlag() == PRConstants.DSD_RECOMMENDATION_STORE) {
										zoneStoreRecFlag.isRecommendAtStoreLevel = true;
									}
								}
							}
						}

						if (strategyForItem.getStrategyId() == 0) {
							// Change location levels on every iteration to traverse through location hierarchy
							if (isChainLevelChecked) {
								break;
							}

							if (locationLevelId == Constants.STORE_LEVEL_ID
									|| (locationLevelId == Constants.STORE_LIST_LEVEL_ID
											&& !isStoreLocationListChecked)) {
								locationLevelId = Constants.STORE_LIST_LEVEL_ID;
								if (storeLocationListCounter <= (storeLocationListId.size() - 1)) {
									// locKey = Constants.STORE_LIST_LEVEL_ID + "-" +
									// storeLocationListId.get(storeLocationListCounter);
									strategyKey.locationLevelId = Constants.STORE_LIST_LEVEL_ID;
									strategyKey.locationId = storeLocationListId.get(storeLocationListCounter);
									storeLocationListCounter++;
								} else {
									isStoreLocationListChecked = true;
								}
							} else if ((locationLevelId == Constants.STORE_LIST_LEVEL_ID
									|| locationLevelId == Constants.STORE_LEVEL_ID) && isStoreLocationListChecked) {
								locationLevelId = PRConstants.ZONE_LEVEL_TYPE_ID;
								// locKey = locationLevelId + "-" + priceZoneId;
								strategyKey.locationLevelId = locationLevelId;
								strategyKey.locationId = priceZoneId;
								isZoneLevelChecked = true;
							} else if (locationLevelId == PRConstants.ZONE_LEVEL_TYPE_ID
									|| (locationLevelId == PRConstants.ZONE_LIST_LEVEL_TYPE_ID
											&& !isLocationListChecked)) {
								locationLevelId = PRConstants.ZONE_LIST_LEVEL_TYPE_ID;
								if (locationListCounter <= (locationListId.size() - 1)) {
									// locKey = PRConstants.ZONE_LIST_LEVEL_TYPE_ID + "-" +
									// locationListId.get(locationListCounter);
									strategyKey.locationLevelId = PRConstants.ZONE_LIST_LEVEL_TYPE_ID;
									strategyKey.locationId = locationListId.get(locationListCounter);
									locationListCounter++;
								} else {
									isLocationListChecked = true;
								}
							} else if ((locationLevelId == PRConstants.ZONE_LIST_LEVEL_TYPE_ID
									|| locationLevelId == PRConstants.ZONE_LEVEL_TYPE_ID) && isLocationListChecked) {
								locationLevelId = Constants.DIVISION_LEVEL_ID;
								// locKey = locationLevelId + "-" + divisionId;
								strategyKey.locationLevelId = locationLevelId;
								strategyKey.locationId = divisionId;
								isDivisionLevelChecked = true;
							} else if (locationLevelId == Constants.DIVISION_LEVEL_ID && isDivisionLevelChecked) {
								locationLevelId = Constants.CHAIN_LEVEL_ID;
								// locKey = locationLevelId + "-" + chainId;
								strategyKey.locationLevelId = locationLevelId;
								strategyKey.locationId = Integer.parseInt(chainId);
								isChainLevelChecked = true;
							}
						}
					}
					// Change product levels on every iteration to iterate through product hierarchy
					// if (childParentMap.get(productLevelId) != null) {
					// After item, check for item list, vendor, state
					// This is included to support scenarios like Category->ItemList, All Products->ItemList
					// Temp no's used to diff between item list, vendor state

					if (productLevelId == Constants.ITEMLEVELID) {
						// Item List
						productLevelId = ITEM_LIST_TEMP_REP_ID;
					} else if (productLevelId == ITEM_LIST_TEMP_REP_ID) {
						// Vendor
						productLevelId = VENDOR_TEMP_REP_ID;
					} else if (productLevelId == VENDOR_TEMP_REP_ID) {
						// State
						productLevelId = STATE_TEMP_REP_ID;
					} else if (productLevelId == STATE_TEMP_REP_ID) {
						// Criteria
						productLevelId = CRITERIA_TEMP_REP_ID;
					} else if (productLevelId == CRITERIA_TEMP_REP_ID) {
						// Set to item, so that it starts from actual product
						// hierarchy
						restoreNormalProdFlow = true;
						productLevelId = Constants.ITEMLEVELID;
					}
					if (restoreNormalProdFlow) {
						if (childParentMap.get(productLevelId) != null) {
							productLevelId = childParentMap.get(productLevelId);
							if (productLevelId == Constants.SEGMENT_LEVEL_PRODUCT_LEVEL_ID) {
								productId = item.getSegmentProductId();
							} else if (productLevelId == Constants.SUBCATEGORYLEVELID)
								productId = item.getSubCatProductId();
							else if (productLevelId == Constants.RECOMMENDATIONUNIT)
								productId = item.getRecUnitProductId();
							else if (productLevelId == Constants.CATEGORYLEVELID)
								productId = item.getCategoryProductId();
							else if (productLevelId == Constants.PORTFOLIO)
								productId = item.getPortfolioProductId();
							else if (productLevelId == Constants.DEPARTMENTLEVELID)
								productId = item.getDeptProductId();
						} else if (!allProductsChecked) {
							productLevelId = Constants.ALLPRODUCTS;
							productId = 0;
							allProductsChecked = true;
						} else {
							break;
						}
					}
				}

				if (strategyForItem.getStrategyId() > 0) {
					item.setStrategyDTO(strategyForItem);
					// logger.debug("Strategy Set - " + item.getItemCode() +
					// "\t" +
					// strategyDTO.getStrategyId());
					// If atleast on item has a strategy assigned to it
					if (!isStrategyFound) {
						isStrategyFound = true;
						// logger.debug("Strategy Found turned on");
					}
				}

				// Store level recommendation is done, even if any one of the item in the category has vendor or state
				// or store level strategy
				if (strategyForItem.getStrategyId() > 0 && zoneStoreRecFlag.isCheckIfStoreLevelStrategyPresent) {
					findIfStoreLevelStrategyPresent(strategyForItem, zoneStoreRecFlag);
				}
			}
			if (isStrategyFound == true) {
				// Copy constraint and guidelines for min/max and locked price constraints
				copyLockMinMaxConstraint(itemDataMap);
				applySameStrategyForLigMember(retLirMap, itemDataMap);
			}

		} catch (Exception e) {
			logger.error("Error:", e);
			throw new OfferManagementException("Exception in getStrategiesForEachItem() " + e,
					RecommendationErrorCode.FIND_EACH_ITEM_STRATEGY);
		}

	}
	
	/**
	 * 
	 * @param item
	 * @param locationLevelId
	 * @param locationId
	 * @param inpStrategyMap
	 * @param childParentMap
	 * @param itemListIds
	 * @param isItemList
	 * @param isVendor
	 * @param isState
	 * @param isZoneItem
	 * @param productListMap
	 * @return strategy with price check list/state/vendor
	 */
	private PRStrategyDTO handleItemListVendorState(PRItemDTO item, int locationLevelId, int locationId,
			HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap, HashMap<Integer, Integer> childParentMap, 
			List<Integer> itemListIds, boolean isItemList, boolean isVendor, boolean isState, boolean isZoneItem,
			HashMap<String, ArrayList<Integer>> productListMap, boolean isCriteria) {
		int productLevelId = Constants.ITEMLEVELID;
		int productId = item.getItemCode();
		boolean allProductsChecked = false;
		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		
		while (strategyForItem.getStrategyId() == 0) {
			
			// Changes for handling product list level startegy when the product list is defined with price check list/state/vendor
			// Changes done by Pradeep on 08/29/2018
			if (productLevelId == Constants.ALLPRODUCTS) {

				// All products level
				strategyForItem = findStrategyForItemList(item, locationLevelId, locationId, productLevelId, productId,
						inpStrategyMap, itemListIds, isItemList, isVendor, isState, isZoneItem, isCriteria);

			} else {

				// Product level (segment -> sub category -> category -> department
				strategyForItem = findStrategyForItemList(item, locationLevelId, locationId, productLevelId, productId,
						inpStrategyMap, itemListIds, isItemList, isVendor, isState, isZoneItem, isCriteria);

				// Product list level
				if (strategyForItem.getStrategyId() == 0) {
					// Check against product list
					if (productListMap.get(productLevelId + "-" + productId) != null) {

						List<Integer> pListId = productListMap.get(productLevelId + "-" + productId);
						for (Integer pId : pListId) {
							int productListLevelId = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID;

							// Product level (segment list -> sub category list -> category list -> department list
							strategyForItem = findStrategyForItemList(item, locationLevelId, locationId,
									productListLevelId, pId, inpStrategyMap, itemListIds, isItemList, isVendor, isState,
									isZoneItem, isCriteria);
						}
					}
				}
			}
			
			
			if (childParentMap.get(productLevelId) != null) {
				productLevelId = childParentMap.get(productLevelId);
				if (productLevelId == Constants.SEGMENT_LEVEL_PRODUCT_LEVEL_ID) {
					productId = item.getSegmentProductId();
				} else if (productLevelId == Constants.SUBCATEGORYLEVELID)
					productId = item.getSubCatProductId();
				else if (productLevelId == Constants.RECOMMENDATIONUNIT)
					productId = item.getRecUnitProductId();
				else if (productLevelId == Constants.CATEGORYLEVELID)
					productId = item.getCategoryProductId();
				else if (productLevelId == Constants.PORTFOLIO)
					productId = item.getPortfolioProductId();
				else if (productLevelId == Constants.DEPARTMENTLEVELID)
					productId = item.getDeptProductId();
			} else if (!allProductsChecked) {
				productLevelId = Constants.ALLPRODUCTS;
				productId = 0;
				allProductsChecked = true;
			} else {
				break;
			}		
		}
		return strategyForItem;
	}
	
	
	/**
	 * 
	 * @param item
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param inpStrategyMap
	 * @param itemListIds
	 * @param isItemList
	 * @param isVendor
	 * @param isState
	 * @param isZoneItem
	 * @return strategy of given combination
	 */
	private PRStrategyDTO findStrategyForItemList(PRItemDTO item, int locationLevelId, int locationId,
			int productLevelId, int productId, HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap,
			List<Integer> itemListIds, boolean isItemList, boolean isVendor, boolean isState, 
			boolean isZoneItem, boolean isCriteria) {
		StrategyKey strategyKey = new StrategyKey(locationLevelId, locationId, productLevelId, productId);
		List<PRStrategyDTO> strategies = inpStrategyMap.get(strategyKey);
		List<PRStrategyDTO> strategiesToCheck = new ArrayList<PRStrategyDTO>();
		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		// Check if strategy available for the product and location combination
		if (strategies != null) {
			if (isItemList) {
				strategiesToCheck = getAllItemListStrategies(item, strategies);
			} else if (isVendor) {
				strategiesToCheck = getAllVendorStrategies(item, strategies);
			} else if (isState) {
				strategiesToCheck = getAllStateStrategies(item, strategies);
			} else if (isCriteria) {
//				logger.debug("strategy key: " + strategyKey.toString());
//				logger.debug("Item with criteria: " + item.getItemCode());
				strategiesToCheck = getAllCriteriaStrategies(item, strategies);
//				logger.debug("Strategies with criteria: " + strategiesToCheck.size());
			}

			if (isItemList && isZoneItem) {
				strategyForItem = getItemListStrategy(item, strategiesToCheck, itemListIds);
			} else if(isCriteria && isZoneItem) {
//				logger.debug("finding strategy for criteria item: " + item.getItemCode());
				strategyForItem = getCriteriaStrategy(item, strategiesToCheck);
//				logger.debug("strategy found: " + (strategyForItem.getStrategyId() > 0 ? "true" : "false"));
			} else {
				// Store level
				strategyForItem = getItemListOrVendorOrStateStrategyForStore(productLevelId, item, strategiesToCheck,
						itemListIds, locationLevelId);
			}
		}

		return strategyForItem;
	}
	
	public void getStrategiesAtStoreForEachItem(Connection conn, PRStrategyDTO inputStrategy, PricingEngineDAO pricingEngineDAO, 
			HashMap<StrategyKey, List<PRStrategyDTO>> inpStrategyMap, 
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore,
			HashMap<Integer, Integer> productParentChildRelationMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<String, ArrayList<Integer>> productListMap, String chainId, int divisionId,
			PRZoneStoreReccommendationFlag zoneStoreRecFlag, PRStrategyDTO leadInputDTO, int leadZoneDivisionId,int leadZoneId) throws OfferManagementException, GeneralException {

		logger.debug("Finding Strategy for Store Items is Started...");
		executionTimeLog = new ExecutionTimeLog(PRConstants.FIND_STORE_STRATEGY);
		zoneStoreRecFlag.isCheckIfStoreLevelStrategyPresent = true;
		for(Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> allStoresAndItsItems : itemDataMapStore.entrySet()){
			PRStrategyDTO inputDTO = new PRStrategyDTO();
			inputDTO.setLocationLevelId(Constants.STORE_LEVEL_ID);
			inputDTO.setLocationId(allStoresAndItsItems.getKey());
				
			getStrategiesForEachItem(conn, inputDTO, pricingEngineDAO, inpStrategyMap, allStoresAndItsItems.getValue(), 
					productParentChildRelationMap, retLirMap, productListMap, chainId, divisionId, false, 
					zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
				
			//Still if strategy is not found for the item, look in Zone Level Strategy
			HashMap<ItemKey, PRItemDTO> itemsToLookAtZoneStrategy = new HashMap<ItemKey, PRItemDTO> ();
			for (Map.Entry<ItemKey, PRItemDTO> storeAndItsItems : allStoresAndItsItems.getValue().entrySet()) {
				PRItemDTO storeItem = storeAndItsItems.getValue();
				if (storeItem.getStrategyDTO() == null || storeItem.getStrategyDTO().getStrategyId() < 1)
					itemsToLookAtZoneStrategy.put(storeAndItsItems.getKey(), storeItem);
			}
			
			
			if(itemsToLookAtZoneStrategy.size() > 0){				
				getStrategiesForEachItem(conn, inputStrategy, pricingEngineDAO, inpStrategyMap, itemsToLookAtZoneStrategy, 
						productParentChildRelationMap, retLirMap, productListMap, chainId, divisionId, true, 
						zoneStoreRecFlag, leadInputDTO, leadZoneDivisionId, leadZoneId);
				//Update the item in the item data map store again
				//for(PRItemDTO item :itemsToLookAtZoneStrategy.values()){
				for (Map.Entry<ItemKey, PRItemDTO> storeAndItsItems : allStoresAndItsItems.getValue().entrySet()) {
//					logger.debug("Picked Strategy from zone level for Store: "				
//							+ inputDTO.getLocationId() + ",Item: " + item.getItemCode() 
//							+ ",Strategy Dto: " + item.getStrategyId()
//							+ " as there is no Strategy found at Store Level" );
					PRItemDTO storeItem = storeAndItsItems.getValue();
					if(allStoresAndItsItems.getValue().get(storeAndItsItems.getKey()) != null)					
						allStoresAndItsItems.getValue().put(storeAndItsItems.getKey(), storeItem);	
				}							
			}				
		}
		
		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);
		logger.debug("Finding Strategy for Store Items is Completed...");
	}
	
	/**
	 * Copies guidelines and constraints from zone level strategy into store level strategy, when a store level strategy has min max constraint defined.
	 * Copies LIG constraint from zone level strategy into store level strategy, when a store level strategy has pre price/loc price constraints defined.
	 * @param strategyDTO			Zone Level strategy
	 * @param storeStrategyDTO		Store Level strategy
	 * @param pricingEngineDAO		Pricing Engine DAO
	 */
//	private void copyOtherGuidelinesAndConstraints(PRStrategyDTO strategyDTO, PRStrategyDTO storeStrategyDTO, PricingEngineDAO pricingEngineDAO) {
//		if(storeStrategyDTO.getConstriants().getMinMaxConstraint() != null){
//			if(strategyDTO.getGuidelines() != null){
//				PRGuidelinesDTO guidelinesDTO = strategyDTO.getGuidelines();
//				storeStrategyDTO.setGuidelines(guidelinesDTO);
//			}
//			if(strategyDTO.getConstriants() != null){
//				PRConstraintsDTO constraintsDTO = strategyDTO.getConstriants();
//				PRConstraintsDTO storeConstraintsDTO = storeStrategyDTO.getConstriants();
//				if(constraintsDTO.getRoundingConstraint() != null){
//					storeConstraintsDTO.setRoundingConstraint(constraintsDTO.getRoundingConstraint());
//				}
//				if(constraintsDTO.getThresholdConstraint() != null){
//					storeConstraintsDTO.setThresholdConstraint(constraintsDTO.getThresholdConstraint());
//				}
//				storeStrategyDTO.setConstriants(storeConstraintsDTO);
//			}
//		}else if(storeStrategyDTO.getConstriants().getPrePriceConstraint() != null || storeStrategyDTO.getConstriants().getLocPriceConstraint() != null){
//			if(strategyDTO.getGuidelines() != null){
//				PRGuidelinesDTO guidelinesDTO = strategyDTO.getGuidelines();
//				storeStrategyDTO.setGuidelines(guidelinesDTO);
//			}
//			if(strategyDTO.getConstriants() != null){
//				PRConstraintsDTO constraintsDTO = strategyDTO.getConstriants();
//				if(constraintsDTO.getLigConstraint() != null){
//					storeStrategyDTO.getConstriants().setLigConstraint(constraintsDTO.getLigConstraint());
//				}
//			}
//		}
//	}
	
	private void findIfStoreLevelStrategyPresent(PRStrategyDTO strategyForItem, PRZoneStoreReccommendationFlag zoneStoreRecFlag) {
		if (strategyForItem.getStateId() > 0 && !zoneStoreRecFlag.isStateLevelStrategyPresent)
			zoneStoreRecFlag.isStateLevelStrategyPresent = true;

		if (strategyForItem.getVendorId() > 0 && !zoneStoreRecFlag.isVendorLevelStrategyPresent)
			zoneStoreRecFlag.isVendorLevelStrategyPresent = true;

		// is there store level strategy defined
		if (strategyForItem.getLocationLevelId() == Constants.STORE_LEVEL_ID
				|| (strategyForItem.getLocationLevelId() == Constants.STORE_LIST_LEVEL_ID) 
				&& !zoneStoreRecFlag.isStoreLevelStrategyPresent)
			zoneStoreRecFlag.isStoreLevelStrategyPresent = true;
	}

	private PRStrategyDTO getStrategyOfZoneItem(int productLevelId, PRItemDTO item, List<PRStrategyDTO> strategies,
			List<Integer> itemAndItsCheckListIds) {
		// Check if strategy available for a item list
		// Same product-location combination may have different item list
		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		strategyForItem = getItemListStrategy(item, strategies, itemAndItsCheckListIds);
		/*for (PRStrategyDTO strategy : strategies) {
			// Ignore store level strategies
			if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() < 1
					&& strategyForItem.getStrategyId() == 0) {
				if (itemAndItsCheckListIds != null) {
					for (int itemListId : itemAndItsCheckListIds) {
						// Check if item is part of that item list
						if (itemListId == strategy.getPriceCheckListId()) {
							item.isCheckListLevelStrategyPresent = true;
							strategyForItem = strategy;
							break;
						}
					}
				}
			}
		}*/

		// If Strategy is not assigned already
		if (strategyForItem.getStrategyId() == 0) {
			// Get Strategy at product/location level i.e with
			// out item list, vendor, state
			for (PRStrategyDTO strategy : strategies) {
				// Ignore store level strategies
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() < 1 && strategy.getStateId() < 1 
						&& strategy.getCriteriaId() < 1
						&& strategyForItem.getStrategyId() == 0) {
					
					if (productLevelId == Constants.ITEMLEVELID){
						item.isItemLevelStrategyPresent = true;
						
						//For items with locked, min/max constraint, copy other guidelines and constraints from its higher level.
						//When there is a item level strategy, just mark and note the strategy id, so that higher level strategy is applied
						//later locked, min/max constraint will be added to this strategy
						if(isLocConstraintPresent(strategy) || isMinMaxConstraintPresent(strategy)){
							item.itemLevelStrategy = strategy;
						}else{
							strategyForItem = strategy;
						}
					}else {
						strategyForItem = strategy;
					}
				}
			}
		}
		return strategyForItem;
	}

	private PRStrategyDTO getItemListStrategy(PRItemDTO item, List<PRStrategyDTO> strategies, List<Integer> itemAndItsCheckListIds){
		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		boolean strategyfound = false;
		for (PRStrategyDTO strategy : strategies) {
			// Ignore store level strategies
			if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() < 1
					&& strategy.getCriteriaId() < 1
					&& strategyForItem.getStrategyId() == 0) {
				if (itemAndItsCheckListIds != null) {
					for (int itemListId : itemAndItsCheckListIds) {
						// Check if item is part of that item list
						//changes for AZ to give prefrence to min/max list if there is startegy defined 
						if (item.getSecondaryPriceCheckList() != null
								&& item.getSecondaryPriceCheckList().getPriceCheckListId() > 0) {
							if (item.getSecondaryPriceCheckList().getPriceCheckListId() == strategy
									.getPriceCheckListId()) {
								strategyForItem = strategy;
								item.setPriceCheckListId(item.getSecondaryPriceCheckList().getPriceCheckListId());
								item.setPriceCheckListTypeId(item.getSecondaryPriceCheckList().getPriceCheckListTypeId());
								item.setMinRetail(item.getSecondaryPriceCheckList().getMinRetail());
								item.setMaxRetail(item.getSecondaryPriceCheckList().getMaxRetail());
								item.setLockedRetail(item.getSecondaryPriceCheckList().getLockedRetail());
								item.setEndDate(item.getSecondaryPriceCheckList().getEndDate());
								strategyfound=true;
								break;
							}

						} else if (itemListId == strategy.getPriceCheckListId()) {
							item.isCheckListLevelStrategyPresent = true;
							strategyForItem = strategy;
							break;
						}
					}
					//if item has two price CheckList and if no startegy found for min max 
					// check if there is strategy defined for another item List 
					if (!strategyfound && item.getSecondaryPriceCheckList() != null
							&& item.getSecondaryPriceCheckList().getPriceCheckListId() > 0) {
						for (int itemListId : itemAndItsCheckListIds) {
							// Check if item is part of that item list

							if (itemListId == strategy.getPriceCheckListId()) {
								item.isCheckListLevelStrategyPresent = true;
								strategyForItem = strategy;
								break;
							}
						}
					}
				}
			}
		}
		return strategyForItem;
	}
	
	
	private PRStrategyDTO getCriteriaStrategy(PRItemDTO item, List<PRStrategyDTO> strategies) {
		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		for (PRStrategyDTO strategy : strategies) {
			// Ignore store level strategies
			if (strategy.getCriteriaId() > 0 && strategyForItem.getStrategyId() == 0) {
				if (strategy.getCriteriaId() == item.getCriteriaId() && (strategy.getPriceCheckListId() < 0
						|| (item.getPriceCheckListId() != null && strategy.getPriceCheckListId() == item.getPriceCheckListId()))) {
					strategyForItem = strategy;
					break;
				}
			}
		}
		return strategyForItem;
	}
	
	private List<PRStrategyDTO> getAllItemListStrategies(PRItemDTO item, List<PRStrategyDTO> strategies){
		List<PRStrategyDTO> itemListStrategies = new ArrayList<PRStrategyDTO>();
		for (PRStrategyDTO strategy : strategies) {
			if (strategy.getPriceCheckListId() > 0) {
				itemListStrategies.add(strategy);
			}
		}
		return itemListStrategies;
	}
	
	private List<PRStrategyDTO> getAllVendorStrategies(PRItemDTO item, List<PRStrategyDTO> strategies){
		List<PRStrategyDTO> vendorStrategies = new ArrayList<PRStrategyDTO>();
		for (PRStrategyDTO strategy : strategies) {
			if (strategy.getVendorId() > 0) {
				vendorStrategies.add(strategy);
			}
		}
		return vendorStrategies;
	}
	
	private List<PRStrategyDTO> getAllStateStrategies(PRItemDTO item, List<PRStrategyDTO> strategies){
		List<PRStrategyDTO> stateStrategies = new ArrayList<PRStrategyDTO>();
		for (PRStrategyDTO strategy : strategies) {
			if (strategy.getStateId() > 0) {
				stateStrategies.add(strategy);
			}
		}
		return stateStrategies;
	}
	
	
	private List<PRStrategyDTO> getAllCriteriaStrategies(PRItemDTO item, List<PRStrategyDTO> strategies){
		List<PRStrategyDTO> criteriaStrategies = new ArrayList<PRStrategyDTO>();
		for (PRStrategyDTO strategy : strategies) {
			if (strategy.getCriteriaId() > 0) {
				criteriaStrategies.add(strategy);
			}
		}
		return criteriaStrategies;
	}
	
	private boolean isLocConstraintPresent(PRStrategyDTO strategy) {
		boolean isLocConstraintPresent = false;
		PRConstraintsDTO constraintDTO = strategy.getConstriants();
		if (constraintDTO != null) {
			PRConstraintLocPrice locpriceConstraint = constraintDTO.getLocPriceConstraint();
			if (locpriceConstraint != null && locpriceConstraint.getValue() > 0) {
				isLocConstraintPresent = true;
			}
		}
		return isLocConstraintPresent;
	}
	
	private boolean isMinMaxConstraintPresent(PRStrategyDTO strategy) {
		boolean isMinMaxConstraintPresent = false;
		PRConstraintsDTO constraintDTO = strategy.getConstriants();
		if(constraintDTO != null){
			PRConstraintMinMax minMaxConstraint = constraintDTO.getMinMaxConstraint();
			if(minMaxConstraint != null){
				isMinMaxConstraintPresent = true;
			}
		}
		return isMinMaxConstraintPresent;
	}
	
	private void copyLockMinMaxConstraint(HashMap<ItemKey, PRItemDTO> itemDataMap) throws CloneNotSupportedException{
		//For items with locked, min/max constraint, copy other guidelines and constraints from its higher level.
		//When there is a item level strategy, just mark and note the strategy id, so that higher level strategy is applied
		//later locked, min/max constraint will be added to this strategy
		
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			// Look for item which has item level strategy with locked or min/max constraint
			if (itemDTO.isItemLevelStrategyPresent && itemDTO.itemLevelStrategy != null) {
				// Take a clone of the strategy
				PRStrategyDTO cloneStrategy = (PRStrategyDTO) itemDTO.getStrategyDTO().clone();

				// copy locked/min-max constraint
				if (itemDTO.itemLevelStrategy.getConstriants() != null) {
					if (itemDTO.itemLevelStrategy.getConstriants().getLocPriceConstraint() != null) {
						PRConstraintLocPrice locPriceConstraint = itemDTO.itemLevelStrategy.getConstriants().getLocPriceConstraint();
						cloneStrategy.getConstriants().setLocPriceConstraint(locPriceConstraint);
						cloneStrategy.setStrategyId(itemDTO.itemLevelStrategy.getStrategyId());
					}

					if (itemDTO.itemLevelStrategy.getConstriants().getMinMaxConstraint() != null) {
						PRConstraintMinMax minMaxConstraint = itemDTO.itemLevelStrategy.getConstriants().getMinMaxConstraint();
						cloneStrategy.getConstriants().setMinMaxConstraint(minMaxConstraint);
						cloneStrategy.setStrategyId(itemDTO.itemLevelStrategy.getStrategyId());
					}
				}
				//update with modified strategy
				itemDTO.setStrategyDTO(cloneStrategy);
			}
		}
	}
	
	private PRStrategyDTO getStrategyOfStoreItem(int productLevelId, PRItemDTO item, List<PRStrategyDTO> strategies,
			List<Integer> itemAndItsCheckListIds, int locationLevelId) {
		List<PRStrategyDTO> strategiesToCheck = new ArrayList<PRStrategyDTO>();
		PRStrategyDTO strategyForItem = new PRStrategyDTO();

		strategyForItem = getItemListOrVendorOrStateStrategyForStore(productLevelId, item, strategies,
				itemAndItsCheckListIds, locationLevelId);
		// Check if strategy exists with itemlist, vendor, state
//		for (PRStrategyDTO strategy : strategies) {
//			if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() > 0 && strategy.getStateId() > 0) {
//				strategiesToCheck.add(strategy);
//			}
//		}
//		strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//
//		if (strategyForItem.getStrategyId() == 0) {
//			strategiesToCheck = new ArrayList<PRStrategyDTO>();
//			// Check if strategy exists with itemlist, vendor
//			for (PRStrategyDTO strategy : strategies) {
//				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() > 0 && strategy.getStateId() < 1) {
//					strategiesToCheck.add(strategy);
//				}
//			}
//			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//		}
//
//		if (strategyForItem.getStrategyId() == 0) {
//			strategiesToCheck = new ArrayList<PRStrategyDTO>();
//			// Check if strategy exists with itemlist, state
//			for (PRStrategyDTO strategy : strategies) {
//				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() > 0) {
//					strategiesToCheck.add(strategy);
//				}
//			}
//			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//		}
//
//		if (strategyForItem.getStrategyId() == 0) {
//			strategiesToCheck = new ArrayList<PRStrategyDTO>();
//			// Check if strategy exists with vendor, state
//			for (PRStrategyDTO strategy : strategies) {
//				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() > 0 && strategy.getStateId() > 0) {
//					strategiesToCheck.add(strategy);
//				}
//			}
//			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//		}
//
//		if (strategyForItem.getStrategyId() == 0) {
//			strategiesToCheck = new ArrayList<PRStrategyDTO>();
//			// Check if strategy exists with vendor
//			for (PRStrategyDTO strategy : strategies) {
//				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() > 0 && strategy.getStateId() < 1) {
//					strategiesToCheck.add(strategy);
//				}
//			}
//			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//		}
//
//		if (strategyForItem.getStrategyId() == 0) {
//			strategiesToCheck = new ArrayList<PRStrategyDTO>();
//			// Check if strategy exists with state
//			for (PRStrategyDTO strategy : strategies) {
//				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() < 1 && strategy.getStateId() > 0) {
//					strategiesToCheck.add(strategy);
//				}
//			}
//			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//		}
//
//		if (strategyForItem.getStrategyId() == 0
//				&& (locationLevelId == Constants.STORE_LEVEL_ID || locationLevelId == Constants.STORE_LIST_LEVEL_ID)) {
//			strategiesToCheck = new ArrayList<PRStrategyDTO>();
//			// Itemlist
//			for (PRStrategyDTO strategy : strategies) {
//				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() < 1) {
//					strategiesToCheck.add(strategy);
//				}
//			}
//			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
//		}

		if (strategyForItem.getStrategyId() == 0
				&& (locationLevelId == Constants.STORE_LEVEL_ID || locationLevelId == Constants.STORE_LIST_LEVEL_ID)) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// None
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() < 1 && strategy.getStateId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}

		return strategyForItem;
	}

	private PRStrategyDTO getItemListOrVendorOrStateStrategyForStore(int productLevelId, PRItemDTO item, List<PRStrategyDTO> strategies,
			List<Integer> itemAndItsCheckListIds, int locationLevelId){
		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		List<PRStrategyDTO> strategiesToCheck = new ArrayList<PRStrategyDTO>();
		
		for (PRStrategyDTO strategy : strategies) {
			if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() > 0 && strategy.getStateId() > 0 && strategy.getCriteriaId() > 0) {
				strategiesToCheck.add(strategy);
			}
		}
		strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);

		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with itemlist, vendor and state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() > 0 && strategy.getStateId() > 0
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies,
					itemAndItsCheckListIds);
		}

		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with itemlist, state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() > 0 
						&& strategy.getCriteriaId() > 0) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with itemlist, state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() > 0 
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}

		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with vendor, state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() > 0 && strategy.getStateId() > 0 
						&& strategy.getCriteriaId() > 0) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with vendor, state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() > 0 && strategy.getStateId() > 0 
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with vendor
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() > 0 && strategy.getStateId() < 1 
						&& strategy.getCriteriaId() > 0) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with vendor
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() > 0 && strategy.getStateId() < 1 
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() < 1 && strategy.getStateId() > 0
						&& strategy.getCriteriaId() > 0) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Check if strategy exists with state
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() < 1 && strategy.getStateId() > 0
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Itemlist
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() < 1
						&& strategy.getCriteriaId() > 0) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Itemlist
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() < 1
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Itemlist
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() > 0 && strategy.getVendorId() < 1 && strategy.getStateId() < 1
						&& strategy.getCriteriaId() < 1) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		if (strategyForItem.getStrategyId() == 0) {
			strategiesToCheck = new ArrayList<PRStrategyDTO>();
			// Itemlist
			for (PRStrategyDTO strategy : strategies) {
				if (strategy.getPriceCheckListId() < 1 && strategy.getVendorId() < 1 && strategy.getStateId() < 1
						&& strategy.getCriteriaId() > 0) {
					strategiesToCheck.add(strategy);
				}
			}
			strategyForItem = findStrategyForStoreItem(strategiesToCheck, productLevelId, item, strategies, itemAndItsCheckListIds);
		}
		
		return strategyForItem;
	}
	
	private PRStrategyDTO findStrategyForStoreItem(List<PRStrategyDTO> strategiesToCheck, int productLevelId, PRItemDTO item,
			List<PRStrategyDTO> strategies, List<Integer> itemAndItsCheckListIds) {

		PRStrategyDTO strategyForItem = new PRStrategyDTO();
		if (strategiesToCheck.size() > 0) {
			for (PRStrategyDTO storeLevelStrategy : strategiesToCheck) {
				// If a strategy has item list/vendor/state. Then this strategy
				// must be picked
				// only when the item is belonging to item list/vendor/state

				boolean ignoreFurtherProcessing = false;
				// If there is item list level
				if (storeLevelStrategy.getPriceCheckListId() > 0) {
					// Check if it is part of item list
					if (itemAndItsCheckListIds != null) {
						for (int itemListId : itemAndItsCheckListIds) {
							if (itemListId == storeLevelStrategy.getPriceCheckListId()) {
								strategyForItem = storeLevelStrategy;
								break;
							}
						}
					}
					// If it is not part of item list, then ignore further
					// processing, as this strategy is not for the item
					if (strategyForItem.getStrategyId() == 0)
						ignoreFurtherProcessing = true;
				}

				if (!ignoreFurtherProcessing) {
					// If there is Vendor level strategy
					if (storeLevelStrategy.getVendorId() > 0) {
						if (item.getVendorId() == storeLevelStrategy.getVendorId()) {
							strategyForItem = storeLevelStrategy;
						} else {
							ignoreFurtherProcessing = true;
							strategyForItem = new PRStrategyDTO();
						}
					}
				}

				if (!ignoreFurtherProcessing) {
					// If there is State level strategy
					if (storeLevelStrategy.getStateId() > 0) {
						if (item.getStateId() == storeLevelStrategy.getStateId()) {
							strategyForItem = storeLevelStrategy;
						} else {
							ignoreFurtherProcessing = true;
							strategyForItem = new PRStrategyDTO();
						}
					}
				}
				
				if (!ignoreFurtherProcessing) {
					// If there is Criteria level strategy
					if (storeLevelStrategy.getCriteriaId() > 0) {
						if (item.getCriteriaId() == storeLevelStrategy.getCriteriaId()) {
							strategyForItem = storeLevelStrategy;
						} else {
							ignoreFurtherProcessing = true;
							strategyForItem = new PRStrategyDTO();
						}
					}
				}

				// Item is assigned with a strategy
				if (strategyForItem.getStrategyId() > 0) {
					// Determine if a item has itemlist -> Vendor -> state
					// (Preference in this way), and
					// same strategy is applied for all the lig members
					if (storeLevelStrategy.getPriceCheckListId() > 0)
						item.isCheckListLevelStrategyPresent = true;
					else if (storeLevelStrategy.getVendorId() > 0)
						item.isVendorLevelStrategyPresent = true;
					else if (storeLevelStrategy.getStateId() > 0)
						item.isStateLevelStrategyPresent = true;
				}

				if (!ignoreFurtherProcessing) {
					//strategyForItem = storeLevelStrategy;
					if (productLevelId == Constants.ITEMLEVELID){
						item.isItemLevelStrategyPresent = true;
						
						//For items with locked, min/max constraint, copy other guidelines and constraints from its higher level.
						//When there is a item level strategy, just mark and note the strategy id, so that higher level strategy is applied
						//later locked, min/max constraint will be added to this strategy
						if(isLocConstraintPresent(storeLevelStrategy) || isMinMaxConstraintPresent(storeLevelStrategy)){
							item.itemLevelStrategy = storeLevelStrategy;
						}else{
							strategyForItem = storeLevelStrategy;
						}
					}else {
						strategyForItem = storeLevelStrategy;
					}
				}

				// Don't look further
				if (strategyForItem.getStrategyId() > 0)
					break;
			}
		}
		return strategyForItem;
	}

	private void applySameStrategyForLigMember(HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<ItemKey, PRItemDTO> itemDataMap) {
		// Loop through each lig
		for (Map.Entry<Integer, List<PRItemDTO>> lirIdMap : retLirMap.entrySet()) {
			PRStrategyDTO strategyDTO = null;
			// Lig Map
			List<PRItemDTO> ligMembers = lirIdMap.getValue();

			// Check if item level strategy exists
			strategyDTO = getItemOrKVILevelStrategy(ligMembers, itemDataMap, "ITEM");

			// If there is no item level strategy, then look for KVI level strategy
			if (strategyDTO == null) {
				strategyDTO = getItemOrKVILevelStrategy(ligMembers, itemDataMap, "KVI");
			}

			// If there is no item level strategy, then look for Vendor level strategy
			if (strategyDTO == null) {
				strategyDTO = getItemOrKVILevelStrategy(ligMembers, itemDataMap, "VENDOR");
			}

			// If there is no item level strategy, then look for state level strategy
			if (strategyDTO == null) {
				strategyDTO = getItemOrKVILevelStrategy(ligMembers, itemDataMap, "STATE");
			}

			// If there is item level or kvi level present
			if (strategyDTO != null) {
				// Update all members & Lig item with item level
				// strategy
				//for (Map.Entry<Integer, List<PRItemDTO>> ligMembers : retLirMap.entrySet()) {
					for (PRItemDTO ligMember : ligMembers) {
						ItemKey itemKey = PRCommonUtil.getItemKey(ligMember);		
						if (itemDataMap.get(itemKey) != null) {
							PRItemDTO itemDTO = itemDataMap.get(itemKey);
							itemDTO.setStrategyDTO(strategyDTO);
							// logger.debug("Item Code: " + itemDTO.getItemCode() + ",Strategy Id: " +
							// strategyDTO.getStrategyId());
						}
					}
					ItemKey itemKey = PRCommonUtil.getItemKey(lirIdMap.getKey(), true);
					if (itemDataMap.get(itemKey) != null) {
						PRItemDTO itemDTO = itemDataMap.get(itemKey);
						itemDTO.setStrategyDTO(strategyDTO);
					}
					
//					if (itemDataMap.get(representingItem.getKey()) != null) {
//						PRItemDTO itemDTO = itemDataMap.get(representingItem.getKey());
//						itemDTO.setStrategyDTO(strategyDTO);
//						// logger.debug("Item Code: " + itemDTO.getItemCode() + ",Strategy Id: " +
//						// strategyDTO.getStrategyId());
//					}
				///}
			}
		}
	}

	private PRStrategyDTO getItemOrKVILevelStrategy(List<PRItemDTO> ligMembers, HashMap<ItemKey, PRItemDTO> itemDataMap,
			String key) {
		PRStrategyDTO strategyDTO = null;

		//for (Map.Entry<Integer, ArrayList<Integer>> representingItem : ligMap.entrySet()) {
			for (PRItemDTO ligMember : ligMembers) {
				ItemKey itemKey = PRCommonUtil.getItemKey(ligMember);				
				if (itemDataMap.get(itemKey) != null) {
					PRItemDTO itemDTO = itemDataMap.get(itemKey);
					if (key.equals("ITEM")) {
						if (itemDTO.isItemLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					} else if (key.equals("KVI")) {
						if (itemDTO.isCheckListLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					} else if (key.equals("VENDOR")) {
						if (itemDTO.isVendorLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					} else if (key.equals("STATE")) {
						if (itemDTO.isStateLevelStrategyPresent) {
							strategyDTO = itemDTO.getStrategyDTO();
						}
					}

				}
			}

			// Check representing item also (Strategy defined at lig level)
//			if (strategyDTO == null) {
//				if (itemDataMap.get(representingItem.getKey()) != null) {
//					PRItemDTO itemDTO = itemDataMap.get(representingItem.getKey());
//					if (key.equals("ITEM")) {
//						if (itemDTO.isItemLevelStrategyPresent) {
//							strategyDTO = itemDTO.getStrategyDTO();
//						}
//					} else if (key.equals("KVI")) {
//						if (itemDTO.isCheckListLevelStrategyPresent) {
//							strategyDTO = itemDTO.getStrategyDTO();
//						}
//					} else if (key.equals("VENDOR")) {
//						if (itemDTO.isVendorLevelStrategyPresent) {
//							strategyDTO = itemDTO.getStrategyDTO();
//						}
//					} else if (key.equals("STATE")) {
//						if (itemDTO.isStateLevelStrategyPresent) {
//							strategyDTO = itemDTO.getStrategyDTO();
//						}
//					}
//				}
//			}
		//}

		return strategyDTO;
	}

	public void copyPreLocMinMaxFromZoneToStore(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore) throws OfferManagementException {
		// Proceed if there is any recored in store item map
		try {
			// Loop through each store
			for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> storeItemMap : itemDataMapStore.entrySet()) {
				// Loop through each item in the store
				for (PRItemDTO storeItemDTO : storeItemMap.getValue().values()) {
					PRItemDTO zoneItemDTO = itemDataMap.get(PRCommonUtil.getItemKey(storeItemDTO));
					if (zoneItemDTO != null && zoneItemDTO.getStrategyDTO() != null) {
						// Check if corresponding item in itemDataMap has pre/loc/min-max
						// If it is pre or loc price, then copy the entire strategy,
//						if (zoneItemDTO.getStrategyDTO().getConstriants().getPrePriceConstraint() != null) {
//							// If there is pre-price constraint at store level, give preference to that
//							if (storeItemDTO.getStrategyDTO() == null || 
//									storeItemDTO.getStrategyDTO().getConstriants().getPrePriceConstraint() == null) {
//								storeItemDTO.setStrategyDTO(zoneItemDTO.getStrategyDTO());
//							}
//						} 
						//Check pre-price from item's property rather from strategy
						if(zoneItemDTO.getIsPrePriced() == 1){
							// If there is pre-price constraint at store level, give preference to that
							if(storeItemDTO.getIsPrePriced() == 0){
								storeItemDTO.setIsPrePriced(zoneItemDTO.getIsPrePriced());
							}
						}else if (zoneItemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() != null) {
							// If there is loc-price constraint at store level, give preference to that
							if (storeItemDTO.getStrategyDTO() == null || 
									storeItemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
								storeItemDTO.setStrategyDTO(zoneItemDTO.getStrategyDTO());
							}
						}
						 //If it is min/max just copy the min/max constraint alone
						else if (zoneItemDTO.getStrategyDTO().getConstriants().getMinMaxConstraint() != null) {
							// If there is min-max constraint at store level, give preference to that
							if (storeItemDTO.getStrategyDTO() == null || 
									storeItemDTO.getStrategyDTO().getConstriants().getMinMaxConstraint() == null) {
								PRStrategyDTO orgStrategy = storeItemDTO.getStrategyDTO();
								PRStrategyDTO cloneStrategy = (PRStrategyDTO) orgStrategy.clone();
//								PRConstraintMinMax cloneMinMax = (PRConstraintMinMax) 
//										zoneItemDTO.getStrategyDTO().getConstriants().getMinMaxConstraint().clone();
//								cloneStrategy.getConstriants().setMinMaxConstraint(cloneMinMax);
								cloneStrategy.getConstriants().setMinMaxConstraint(zoneItemDTO.getStrategyDTO().getConstriants().getMinMaxConstraint());
								storeItemDTO.setStrategyDTO(cloneStrategy);
								
//								storeItemDTO.getStrategyDTO().getConstriants()
//										.setMinMaxConstraint(zoneItemDTO.getStrategyDTO().getConstriants().getMinMaxConstraint());
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			throw new OfferManagementException("Exception in copyPreLocMinMaxFromZoneToStore() " + ex, 
					RecommendationErrorCode.COPY_PRE_LOC_MIN_MAX_TO_STORE);
		}
	}
	
	/***
	 * 
	 * @param conn
	 * @param strategyInput
	 * @param divisionIdOfZone
	 * @param strategyId 
	 * @return
	 * @throws OfferManagementException
	 */
	public List<Long> getAllActiveTempStrategies(Connection conn, PRStrategyDTO strategyInput, int divisionIdOfZone)
			throws OfferManagementException {

		List<Long> whatIfStratIDs = new ArrayList<Long>();
		StrategyDAO strategyDAO = new StrategyDAO();
		ExecutionTimeLog executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_TEMP_STRATEGIES);

		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_ALL_TEMP_STRATEGIES);

		whatIfStratIDs = strategyDAO.getAllActiveTempStrategies(conn, strategyInput, divisionIdOfZone);

		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);

		return whatIfStratIDs;
	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws OfferManagementException
	 */
	public Long getGlobalStrategy(Connection conn) throws OfferManagementException {

		Long globalStrategyId = null;
		StrategyDAO strategyDAO = new StrategyDAO();
		ExecutionTimeLog executionTimeLog = new ExecutionTimeLog(PRConstants.GET_GLOBAL_STRATEGY);

		executionTimeLog = new ExecutionTimeLog(PRConstants.GET_GLOBAL_STRATEGY);

		globalStrategyId = strategyDAO.getGlobalStrategy(conn);

		executionTimeLog.setEndTime();
		executionTimeLogs.add(executionTimeLog);

		return globalStrategyId;
	}

}
