package com.pristine.dataload.service;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.CostDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.vendor.VendorDAO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.VendorFileDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class StoreItemMapService {

	private static Logger logger = Logger.getLogger("StoreItemMapService");
	HashMap<String, Integer> storeIdsMap = null;
	HashMap<String, Integer> retailPriceZone = null;
	HashMap<String, String> storeInfoMap = null;

	public StoreItemMapService() {

	}

	public StoreItemMapService(HashMap<String, Integer> storeIdsMap, HashMap<String, Integer> retailPriceZone,
			HashMap<String, String> storeInfoMap) {
		this.storeIdsMap = storeIdsMap;
		this.retailPriceZone = retailPriceZone;
		this.storeInfoMap = storeInfoMap;
	}

	/**
	 * Iterate priceDataMap and insert/update/delete data in store_item_map
	 * based on record type
	 * 
	 * @throws GeneralException
	 */
	public void mergeIntoStoreItemMap(Connection conn, HashMap<ItemDetailKey, List<PriceAndCostDTO>> dataMap,
			HashMap<String, Long> vendorInfo, HashMap<ItemDetailKey, String> itemCodeMap, boolean costIndicator,Set<String> levelIdNotFound)
					throws GeneralException {
		List<RetailCostDTO> insertList = new ArrayList<RetailCostDTO>();
		List<RetailCostDTO> deleteList = new ArrayList<RetailCostDTO>();
		CostDAO costDao = new CostDAO();
		String itemCode = null;
		int noItemCode = 0;
		List<Long> warehouseVendors = costDao.getWarehouseVendors(conn);
		for (List<PriceAndCostDTO> costDTOList : dataMap.values()) {
			boolean noItemFlag = false;
			for (PriceAndCostDTO costDTO : costDTOList) {
				String upc = costDTO.getUpc();
				String itemNo = costDTO.getItemNo();
				String vendor = costDTO.getVendorNo();
				String ret_item_code = vendor + itemNo;
				ItemDetailKey itemDetailKey = new ItemDetailKey(upc, ret_item_code);
				itemCode = itemCodeMap.get(itemDetailKey);
				if (itemCode != null) {
					if (Constants.RECORD_TYPE_ADDED.equals(costDTO.getRecordType())
							|| Constants.RECORD_TYPE_UPDATED.equals(costDTO.getRecordType())) {
						RetailCostDTO retailCostDTO = new RetailCostDTO();
						updateValues(costDTO, retailCostDTO, insertList, itemCode, vendorInfo, warehouseVendors,levelIdNotFound);
					} else if (Constants.RECORD_TYPE_DELETED.equals(costDTO.getRecordType())) {
						RetailCostDTO retailCostDTO = new RetailCostDTO();
						updateValues(costDTO, retailCostDTO, deleteList, itemCode, vendorInfo, warehouseVendors,levelIdNotFound);
					}
				} else {
					noItemFlag = true;
				}
			}
			if (noItemFlag) {
				noItemCode++;
			}
		}

		logger.info("No of UPCs in price data file with no item code - " + noItemCode);

		// Insert/update/delete records in store_item_map
		costDao.mergeIntoStoreItemMap(conn, insertList, deleteList, costIndicator);
	}

	/**
	 * 
	 * @param costDTO
	 * @param retailCostDTO
	 * @param insertList
	 * @param itemCode
	 * @param vendorInfo
	 * @param warehouseVendors
	 */
	private void updateValues(PriceAndCostDTO costDTO, RetailCostDTO retailCostDTO, List<RetailCostDTO> insertList,
			String itemCode, HashMap<String, Long> vendorInfo, List<Long> warehouseVendors, Set<String> levelIdNotFound) {
		updateLevelId(costDTO, retailCostDTO);
		retailCostDTO.setItemcode(itemCode);
		retailCostDTO.setLevelTypeId(Integer.parseInt(costDTO.getSourceCode()));
		retailCostDTO.setVendorId(vendorInfo.get(costDTO.getVendorNo()));
		if (warehouseVendors.contains(retailCostDTO.getVendorId())) {
			retailCostDTO.setDistFlag("" + Constants.WAREHOUSE);
		} else {
			retailCostDTO.setDistFlag("" + Constants.DSD);
		}
		if(!Constants.EMPTY.equals(retailCostDTO.getLevelId()) && retailCostDTO.getLevelId() != null){
			insertList.add(retailCostDTO);
		}else{
			levelIdNotFound.add(costDTO.getZone());
		}
		// Adding store entries from zone level record.
		// Changed by Pradeep. on 05/05/2015
		if (Integer.parseInt(costDTO.getSourceCode()) == Constants.ZONE_LEVEL_TYPE_ID) {
			List<RetailCostDTO> storesList = new ArrayList<RetailCostDTO>();
			// Getting stores by zone number
			for (Map.Entry<String, String> entry : storeInfoMap.entrySet()) {
				if (entry.getValue().equals(costDTO.getZone())) {
					String storeNo = entry.getKey();
					RetailCostDTO retailCostDTOForStore = new RetailCostDTO();
					retailCostDTOForStore.setItemcode(itemCode);
					retailCostDTOForStore.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
					retailCostDTOForStore.setLevelId(storeIdsMap.get(storeNo).toString());
					retailCostDTOForStore.setVendorId(vendorInfo.get(costDTO.getVendorNo()));
					if (warehouseVendors.contains(retailCostDTOForStore.getVendorId())) {
						retailCostDTOForStore.setDistFlag("" + Constants.WAREHOUSE);
					} else {
						retailCostDTOForStore.setDistFlag("" + Constants.DSD);
					}
					storesList.add(retailCostDTOForStore);
				}
			}
			insertList.addAll(storesList);
		}
	}

	/**
	 * 
	 * @param costDTO
	 * @param retailCostDTO
	 */

	private void updateLevelId(PriceAndCostDTO costDTO, RetailCostDTO retailCostDTO) {
		if (Integer.parseInt(costDTO.getSourceCode()) == Constants.ZONE_LEVEL_TYPE_ID) {
			String zoneNumber = Integer.toString(Integer.parseInt(costDTO.getZone()));
			if (retailPriceZone.get(zoneNumber) != null) {
				String zoneId = retailPriceZone.get(zoneNumber).toString();
				retailCostDTO.setLevelId(zoneId);
			}
		} else if (Integer.parseInt(costDTO.getSourceCode()) == Constants.STORE_LEVEL_TYPE_ID) {
			if (storeIdsMap.get(costDTO.getZone()) != null) {
				String storeId = storeIdsMap.get(costDTO.getZone()).toString();
				retailCostDTO.setLevelId(storeId);
			}
		}
	}

	/**
	 * 
	 * @param conn
	 * @param dataMap
	 * @return
	 */
	public HashMap<String, Long> setupVendorInfo(Connection conn,
			HashMap<ItemDetailKey, List<PriceAndCostDTO>> dataMap) {
		// Update vendor info into the system.
		Collection<List<RetailCostDTO>> retailCostCol = new ArrayList<List<RetailCostDTO>>();
		for (List<PriceAndCostDTO> costList : dataMap.values()) {
			List<RetailCostDTO> retailCostList = new ArrayList<RetailCostDTO>();
			for (PriceAndCostDTO priceAndCostDTO : costList) {
				RetailCostDTO retailCostDTO = new RetailCostDTO();
				int vendorNo = Integer.parseInt(priceAndCostDTO.getVendorNo());
				if (vendorNo > 0) {
					String vendorName = "VENDOR-" + priceAndCostDTO.getVendorNo();
					retailCostDTO.setVendorName(vendorName);
					retailCostDTO.setVendorNumber(priceAndCostDTO.getVendorNo());
					retailCostList.add(retailCostDTO);
				}
			}
			retailCostCol.add(retailCostList);
		}

		HashMap<String, Long> vendorInfo = setupVendorInfo(conn, retailCostCol);

		return vendorInfo;
	}

	/**
	 * Insert/Update store/zone mapping with items
	 * 
	 * @param values
	 *            Contains the input with which mapping needs to be created
	 * @param itemCodeMap
	 *            Contains mapping between upc and item code
	 */
	public void mapItemsWithStoreForPrice(Connection conn, Collection<List<RetailPriceDTO>> retailPriceColln,
			HashMap<String, String> itemCodeMap, HashMap<String, Integer> storeIdMap,
			HashMap<String, Integer> priceZoneIdMap, Set<String> noItemCodeSet,
			HashMap<String, List<Integer>> deptZoneMap, HashMap<String, Long> vendorIdMap, HashMap<String, List<Integer>> storesForZoneMap) {
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();

		try {
			boolean useStoreItemMap = Boolean
					.parseBoolean(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
			if (useStoreItemMap) {

				HashMap<String, List<String>> zoneStoreMap = retailPriceDAO.getZoneStoreMapping(conn);
				long startTime = System.currentTimeMillis();
				findStorePriceAndSetupStoreItemMap(conn, retailPriceColln, itemCodeMap, storeIdMap, priceZoneIdMap,
						noItemCodeSet, deptZoneMap, vendorIdMap, zoneStoreMap, retailPriceDAO, useStoreItemMap);
				long endTime = System.currentTimeMillis();
				logger.info("mapItemsWithStoreForPrice() - time taken to setup store item map: " + (endTime - startTime)
						+ "ms");
			} else {
				long startTime = System.currentTimeMillis();
				retailPriceDAO.mapItemsWithStore(conn, retailPriceColln, itemCodeMap, storeIdMap, priceZoneIdMap,
						noItemCodeSet, deptZoneMap, vendorIdMap, useStoreItemMap, storesForZoneMap);
				long endTime = System.currentTimeMillis();
				logger.info("mapItemsWithStoreForPrice() - time taken to setup store item map: " + (endTime - startTime)
						+ "ms");
			}
		} catch (GeneralException | Exception ge) {
			logger.error("mapItemsWithStore() - Error ", ge);
		}
	}

	public void mapItemsWithStore(Connection conn, Collection<List<RetailCostDTO>> retailCostColln,
			HashMap<ItemDetailKey, String> itemCodeMap, HashMap<String, Integer> storeIdMap,
			HashMap<String, Integer> priceZoneIdMap, Set<String> noItemCodeSet,
			HashMap<String, List<Integer>> deptZoneMap, HashMap<String, Long> vendorIdMap, 
			HashMap<String, List<Integer>> storeZoneMap) {
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		RetailCostDAO retailCostDAO = new RetailCostDAO();
		try {
			boolean ignoreUpdateDistFlgFromRetItemCode = Boolean
					.parseBoolean(PropertyManager.getProperty("IGONORE_UPDATING_DIST_FLG_FROM_RET_ITEM_CODE", "FALSE"));
			boolean usePriceZoneMap = Boolean
					.parseBoolean(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
			if (usePriceZoneMap) {
				HashMap<String, List<String>> zoneStoreMap = retailPriceDAO.getZoneStoreMapping(conn);
				long mergeStartTime = System.currentTimeMillis();
				findStoreCostsAndSetupStoreItemMap(conn, retailCostColln, itemCodeMap, storeIdMap, priceZoneIdMap,
						noItemCodeSet, deptZoneMap, vendorIdMap, zoneStoreMap, ignoreUpdateDistFlgFromRetItemCode,
						retailCostDAO, usePriceZoneMap);
				long mergeEndTime = System.currentTimeMillis();
				logger.info(
						"Time taken for merge items with store_item_map - " + (mergeEndTime - mergeStartTime) + "ms");
			} else {

				long mergeStartTime = System.currentTimeMillis();
				retailCostDAO.mapItemsWithStore(conn, retailCostColln, itemCodeMap, storeIdMap, priceZoneIdMap,
						noItemCodeSet, deptZoneMap, vendorIdMap, ignoreUpdateDistFlgFromRetItemCode, usePriceZoneMap, storeZoneMap);
				long mergeEndTime = System.currentTimeMillis();
				logger.info(
						"Time taken for merge items with store_item_map - " + (mergeEndTime - mergeStartTime) + "ms");
			}
		} catch (GeneralException | Exception ge) {
			logger.error("mapItemsWithStore() - Error ", ge);
		}
	}

	/**
	 * 
	 * @param retailPriceColln
	 * @return map of all store store level data
	 * @throws CloneNotSupportedException
	 */
	private void findStorePriceAndSetupStoreItemMap(Connection conn, Collection<List<RetailPriceDTO>> retailPriceColln,
			HashMap<String, String> itemCodeMap, HashMap<String, Integer> storeIdMap,
			HashMap<String, Integer> priceZoneIdMap, Set<String> noItemCodeSet,
			HashMap<String, List<Integer>> deptZoneMap, HashMap<String, Long> vendorIdMap,
			HashMap<String, List<String>> zoneStoreMap, RetailPriceDAO retailPriceDAO, boolean usePriceZoneMap)
					throws CloneNotSupportedException {
		HashMap<String, List<RetailPriceDTO>> newPriceMap = new HashMap<>();
		int itemCounter = 0;
		int recordCounter = 0;
		for (List<RetailPriceDTO> priceList : retailPriceColln) {
			List<RetailPriceDTO> tempLst = new ArrayList<>();
			itemCounter++;
			String key = null;
			for (RetailPriceDTO priceDto : priceList) {
				key = priceDto.getUpc() + "-" + priceDto.getRetailerItemCode();
				if (zoneStoreMap.get(priceDto.getLevelId()) != null) {
					List<String> stores = zoneStoreMap.get(priceDto.getLevelId());
					for (String storeId : stores) {
						recordCounter++;
						RetailPriceDTO priceDtoNew = (RetailPriceDTO) priceDto.clone();
						priceDtoNew.setZoneNbr(priceDto.getLevelId());
						priceDtoNew.setLevelId(storeId);
						priceDtoNew.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
						tempLst.add(priceDtoNew);
					}
				}
			}
			newPriceMap.put(key, tempLst);

			if (itemCounter % Constants.BATCH_UPDATE_COUNT == 0) {
				retailPriceDAO.mapItemsWithStore(conn, newPriceMap.values(), itemCodeMap, storeIdMap, priceZoneIdMap,
						noItemCodeSet, deptZoneMap, vendorIdMap, usePriceZoneMap, null);
				logger.info("findStorePriceAndSetupStoreItemMap() - Total # of records processed: " + recordCounter);
				itemCounter = 0;
				newPriceMap.clear();
			}
		}

		if (itemCounter > 0) {
			retailPriceDAO.mapItemsWithStore(conn, newPriceMap.values(), itemCodeMap, storeIdMap, priceZoneIdMap,
					noItemCodeSet, deptZoneMap, vendorIdMap, usePriceZoneMap, null);
			logger.info("findStorePriceAndSetupStoreItemMap() - Total # of records processed: " + recordCounter);
			itemCounter = 0;
			newPriceMap.clear();
		}
	}

	/**
	 * 
	 * @param retailPriceColln
	 * @return map of all store store level data
	 * @throws CloneNotSupportedException
	 * @throws GeneralException
	 * @throws NumberFormatException
	 */
	private void findStoreCostsAndSetupStoreItemMap(Connection conn, Collection<List<RetailCostDTO>> retailCostColln,
			HashMap<ItemDetailKey, String> itemCodeMap, HashMap<String, Integer> storeIdMap,
			HashMap<String, Integer> priceZoneIdMap, Set<String> noItemCodeSet,
			HashMap<String, List<Integer>> deptZoneMap, HashMap<String, Long> vendorIdMap,
			HashMap<String, List<String>> zoneStoreMap, boolean ignoreUpdateDistFlgFromRetItemCode,
			RetailCostDAO retailCostDAO, boolean usePriceZoneMap) throws CloneNotSupportedException, NumberFormatException, GeneralException {
		HashMap<ItemDetailKey, List<RetailCostDTO>> newPriceMap = new HashMap<>();
		int itemCounter = 0;
		int recordCount = 0;
		for (List<RetailCostDTO> costList : retailCostColln) {
			itemCounter++;
			List<RetailCostDTO> tempLst = new ArrayList<>();
			ItemDetailKey key = null;
			for (RetailCostDTO costDto : costList) {
				key = new ItemDetailKey(costDto.getUpc(), costDto.getRetailerItemCode());
				if (zoneStoreMap.get(costDto.getLevelId()) != null) {
					List<String> stores = zoneStoreMap.get(costDto.getLevelId());
					for (String storeId : stores) {
						recordCount++;
						RetailCostDTO costDtoNew = (RetailCostDTO) costDto.clone();
						costDtoNew.setZoneNbr(costDto.getLevelId());
						costDtoNew.setLevelId(storeId);
						costDtoNew.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
						tempLst.add(costDtoNew);
					}
				}
			}
			newPriceMap.put(key, tempLst);

			if (itemCounter % Constants.BATCH_UPDATE_COUNT == 0) {
				retailCostDAO.mapItemsWithStore(conn, newPriceMap.values(), itemCodeMap, storeIdMap, priceZoneIdMap,
						noItemCodeSet, deptZoneMap, vendorIdMap, ignoreUpdateDistFlgFromRetItemCode, usePriceZoneMap, null);
				logger.info("findStorePriceAndSetupStoreItemMap() - Total # of records processed: " + recordCount);

				itemCounter = 0;
				newPriceMap.clear();
			}
		}

		if (itemCounter > 0) {
			retailCostDAO.mapItemsWithStore(conn, newPriceMap.values(), itemCodeMap, storeIdMap, priceZoneIdMap,
					noItemCodeSet, deptZoneMap, vendorIdMap, ignoreUpdateDistFlgFromRetItemCode, usePriceZoneMap, null);
			logger.info("findStorePriceAndSetupStoreItemMap() - Total # of records processed: " + recordCount);

			itemCounter = 0;
			newPriceMap.clear();
		}

	}

	/**
	 * 
	 * @param _Conn
	 * @param retailCostColln
	 * @return vendor id map
	 */
	public HashMap<String, Long> setupVendorInfo(Connection _Conn, Collection<List<RetailCostDTO>> retailCostColln) {
		List<VendorFileDTO> vendorList = new ArrayList<VendorFileDTO>();
		HashMap<String, Long> vendorIdMap = new HashMap<String, Long>();
		Set<String> vendorCodeSet = new HashSet<String>();
		VendorDAO vendorDAO = new VendorDAO(_Conn);
		for (List<RetailCostDTO> retailCostDTOList : retailCostColln) {
			for (RetailCostDTO retailCostDTO : retailCostDTOList) {
				if (!vendorCodeSet.contains(retailCostDTO.getVendorNumber()) && retailCostDTO.getVendorName() != null
						&& !retailCostDTO.getVendorName().isEmpty()) {
					vendorCodeSet.add(retailCostDTO.getVendorNumber());
				}
			}
		}
		vendorIdMap = vendorDAO.getVendorIdMap(vendorCodeSet);
		vendorCodeSet = new HashSet<String>();
		for (List<RetailCostDTO> retailCostDTOList : retailCostColln) {
			for (RetailCostDTO retailCostDTO : retailCostDTOList) {
				// check if the vendor code is already processed.
				if (!vendorIdMap.containsKey(retailCostDTO.getVendorNumber())
						&& !vendorCodeSet.contains(retailCostDTO.getVendorNumber())
						&& retailCostDTO.getVendorName() != null && !retailCostDTO.getVendorName().isEmpty()) {
					VendorFileDTO vendorDTO = new VendorFileDTO();
					vendorDTO.setVendorCode(retailCostDTO.getVendorNumber());
					vendorDTO.setVendorName(retailCostDTO.getVendorName());
					vendorList.add(vendorDTO);
					vendorCodeSet.add(retailCostDTO.getVendorNumber());
				}
			}
		}
		vendorIdMap.putAll(vendorDAO.insertVendor(vendorList));
		return vendorIdMap;
	}

	
	/**
	 * 
	 * @param conn
	 * @param vendorList
	 * @return vendor lookup
	 */
	public HashMap<String, Long> setupVendorLookup(Connection conn, List<VendorFileDTO> vendorList){
		Set<String> vendorCodeSet = new HashSet<String>();
		List<VendorFileDTO> vendorsToBeInserted = new ArrayList<VendorFileDTO>();
		HashMap<String, Long> vendorIdMap = new HashMap<String, Long>();
		VendorDAO vendorDAO = new VendorDAO(conn);
		for(VendorFileDTO vendor: vendorList){
			if (!vendorCodeSet.contains(vendor.getVendorCode())) {
				vendorCodeSet.add(vendor.getVendorCode());
			}
		}
		
		vendorIdMap = vendorDAO.getVendorIdMap(vendorCodeSet);
		
		for(VendorFileDTO vendor: vendorList){
			if (!vendorIdMap.containsKey(vendor.getVendorCode())) {
				vendorsToBeInserted.add(vendor);
			}
		}
		
		vendorIdMap.putAll(vendorDAO.insertVendor(vendorsToBeInserted));
		
		return vendorIdMap;
	}
	
	/**
	 * 
	 * @param _Conn
	 * @param retailPriceColln
	 * @return vendorIdMap
	 */

	public HashMap<String, Long> setupVendorInfoForPrice(Connection _Conn,
			Collection<List<RetailPriceDTO>> retailPriceColln) {
		List<VendorFileDTO> vendorList = new ArrayList<VendorFileDTO>();
		HashMap<String, Long> vendorIdMap = new HashMap<String, Long>();
		Set<String> vendorCodeSet = new HashSet<String>();
		VendorDAO vendorDAO = new VendorDAO(_Conn);
		for (List<RetailPriceDTO> retailPriceDTOList : retailPriceColln) {
			for (RetailPriceDTO retailCostDTO : retailPriceDTOList) {
				if (!vendorCodeSet.contains(retailCostDTO.getVendorNumber()) && retailCostDTO.getVendorName() != null
						&& !retailCostDTO.getVendorName().isEmpty()) {
					vendorCodeSet.add(retailCostDTO.getVendorNumber());
				}
			}
		}
		vendorIdMap = vendorDAO.getVendorIdMap(vendorCodeSet);
		vendorCodeSet = new HashSet<String>();
		for (List<RetailPriceDTO> retailPriceDTOList : retailPriceColln) {
			for (RetailPriceDTO retailPriceDTO : retailPriceDTOList) {
				// check if the vendor code is already processed.
				if (!vendorIdMap.containsKey(retailPriceDTO.getVendorNumber())
						&& !vendorCodeSet.contains(retailPriceDTO.getVendorNumber())
						&& retailPriceDTO.getVendorName() != null && !retailPriceDTO.getVendorName().isEmpty()) {
					VendorFileDTO vendorDTO = new VendorFileDTO();
					vendorDTO.setVendorCode(retailPriceDTO.getVendorNumber());
					vendorDTO.setVendorName(retailPriceDTO.getVendorName());
					vendorList.add(vendorDTO);
					vendorCodeSet.add(retailPriceDTO.getVendorNumber());
				}
			}
		}
		vendorIdMap.putAll(vendorDAO.insertVendor(vendorList));
		return vendorIdMap;
	}

}
