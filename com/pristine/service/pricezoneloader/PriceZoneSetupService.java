package com.pristine.service.pricezoneloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.PriceGroupAndCategoryKey;
import com.pristine.dto.offermgmt.ProductLocationMappingDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingKey;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;

public class PriceZoneSetupService {
	static Logger logger = Logger.getLogger("PriceZoneLoader");
	/**
	 * 
	 * @param productLocationMap
	 * @throws GeneralException 
	 */
	public HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> identifyDSDAndWhseZoneMapping(
			HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocationMap,
			HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap, 
			HashMap<Integer, Integer> primaryMatchingZone){

		
		// 1. Group DSD zones together with all the warehouse zones which are
		// matching
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> dsdProdLocMap = new HashMap<>();
		HashMap<Set<ProductStoreKey>, List<ProductLocationMappingDTO>> productStoreMap = new HashMap<>();
		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> prodLocMapEntry : productLocationMap
				.entrySet()) {
			Set<ProductStoreKey> categoryAndStoreSet = new HashSet<>();
			ProductLocationMappingDTO refZoneObj = prodLocMapEntry.getValue().get(0);
			if (refZoneObj.prcGrpCode.equals(Constants.GE_PRC_GRP_CD_DSD)) {
				dsdProdLocMap.put(prodLocMapEntry.getKey(), prodLocMapEntry.getValue());
			}
			for (ProductLocationMappingDTO productLocationMappingDTO : prodLocMapEntry.getValue()) {
				if (!productLocationMappingDTO.prcGrpCode.equals(Constants.GE_PRC_GRP_CD_DSD)
						&& !productLocationMappingDTO.prcGrpCode.equals(Constants.GE_PRC_GRP_CD_DSD1)) {

					ProductStoreKey productStoreKey = new ProductStoreKey(productLocationMappingDTO.productLevelId,
							productLocationMappingDTO.productId, productLocationMappingDTO.storeId);
					categoryAndStoreSet.add(productStoreKey);

				}
			}

			List<ProductLocationMappingDTO> temp = new ArrayList<>();			
			if (productStoreMap.containsKey(categoryAndStoreSet)) {
				temp = productStoreMap.get(categoryAndStoreSet);
			}
			temp.add(refZoneObj);
			productStoreMap.put(categoryAndStoreSet, temp);	
			
		}
		
		
		logger.info("identifyDSDAndWhseZoneMapping() - # of DSD zone mapping -> " + dsdProdLocMap.size());
		
		
		//Mapping Warehouse zones for DSD zones whose stores are nicely fitting into warehouse zones
		//If anyone of the store in a DSD zone spans into someother warehouse, DSD mapping will not happen.
		//All the stores under a DSD should fit in a warehouse zone 
		for(Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> dsdProdLocEntry: dsdProdLocMap.entrySet()){
			
			logger.debug("identifyDSDAndWhseZoneMapping() - Filtering stores...");
			Set<ProductStoreKey> prodStoreSet = new HashSet<>();
			for(ProductLocationMappingDTO productLocationMappingDTO: dsdProdLocEntry.getValue()){
				ProductStoreKey productStoreKey = new ProductStoreKey(productLocationMappingDTO.productLevelId,
						productLocationMappingDTO.productId, productLocationMappingDTO.storeId);
				
				prodStoreSet.add(productStoreKey);
			}
			
			logger.debug("identifyDSDAndWhseZoneMapping() - Filtering stores is completed");
			
			//Checking stores which are fitting in warehouse zones
			long startTime = System.currentTimeMillis();
			logger.debug("identifyDSDAndWhseZoneMapping() - Finding warehouse zones...");
			List<ProductLocationMappingDTO> warehouseZones = new ArrayList<>();
			for(Map.Entry<Set<ProductStoreKey>, List<ProductLocationMappingDTO>> prodStoreEntry: productStoreMap.entrySet()){
				if(prodStoreEntry.getKey().containsAll(prodStoreSet)){
					warehouseZones.addAll(prodStoreEntry.getValue());
				}
			}
			long endTime = System.currentTimeMillis();
			logger.debug("identifyDSDAndWhseZoneMapping() - # of Warehouse zones found -> " + warehouseZones.size());
			logger.debug("identifyDSDAndWhseZoneMapping() - Time taken to fine whse zones -> " + (endTime - startTime) + " ms.");
			
			
			startTime = System.currentTimeMillis();
			logger.debug("identifyDSDAndWhseZoneMapping() - Mapping warehouse zone...");

			ProductLocationMappingDTO parentProdLocDTO = null;
			int maxItemCount = 0;
			for(ProductLocationMappingDTO productLocationMappingDTO: warehouseZones){
				PriceGroupAndCategoryKey priceGroupAndCategoryKey = new PriceGroupAndCategoryKey(
						productLocationMappingDTO.prcGrpCode, productLocationMappingDTO.productId);
				if(prcCodeCountMap.containsKey(priceGroupAndCategoryKey)){
					int itemCount = prcCodeCountMap.get(priceGroupAndCategoryKey);
					if(maxItemCount == 0 || maxItemCount < itemCount){
						maxItemCount = itemCount;
						parentProdLocDTO = productLocationMappingDTO;
					}
				}	
			}
			
			logger.debug("identifyDSDAndWhseZoneMapping() - Mapping warehouse zone is completed");
			endTime = System.currentTimeMillis();
			
			logger.debug("identifyDSDAndWhseZoneMapping() - Time taken to map whse zones -> " + (endTime - startTime) + " ms.");
			
			// 3. Assign parent location for each DSD zone whereever applicable
			if(parentProdLocDTO != null){
				for(ProductLocationMappingDTO productLocationMappingDTO: dsdProdLocEntry.getValue()){
					
					//Check if current warehouse zone is a child of another warehouse zone.
					//If so, set parent as parent of current parent
					//based on PR_PRIMARY_MATCHING_ZONE_MAP
					//Otherwise, set current parent itself
					if(primaryMatchingZone.containsKey(parentProdLocDTO.locationId)){
						//Set parent as parent of parent
						int parentOfParent = primaryMatchingZone.get(parentProdLocDTO.locationId);
						productLocationMappingDTO.parentLocationId = parentOfParent;
						productLocationMappingDTO.parentLocationLevelId = parentProdLocDTO.locationLevelId;
					}else{
						//Set parent location id
						productLocationMappingDTO.parentLocationId = parentProdLocDTO.locationId;
						productLocationMappingDTO.parentLocationLevelId = parentProdLocDTO.locationLevelId;	
					}
				}
			}
		}
		
		return productLocationMap;
	}
	
	
	/**
	 * 
	 * @param productLocationMap
	 * @throws GeneralException
	 * @throws CloneNotSupportedException 
	 */
	public HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> groupZonesByMapping(
			HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocationMap,
			HashMap<Integer, Integer> primaryMatchingZone)
					throws GeneralException, CloneNotSupportedException {

		

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> prodLocEntry : productLocationMap
				.entrySet()) {
			ProductLocationMappingKey productLocationMappingKey = prodLocEntry.getKey();
			if (primaryMatchingZone.containsKey(productLocationMappingKey.getLocationId())
					&& productLocationMappingKey.getProductLevelId() != Constants.ALLPRODUCTS) {

				int parentLocationId = primaryMatchingZone.get(productLocationMappingKey.getLocationId());
				ProductLocationMappingKey productLocationMappingKeyNew = new ProductLocationMappingKey(
						productLocationMappingKey.getProductLevelId(), productLocationMappingKey.getProductId(),
						productLocationMappingKey.getLocationLevelId(), parentLocationId);
				if (productLocationMap.containsKey(productLocationMappingKeyNew)) {

					List<ProductLocationMappingDTO> existingStores = productLocationMap
							.get(productLocationMappingKeyNew);
					Set<Integer> stores = existingStores.stream().map(ProductLocationMappingDTO::getStoreId)
							.collect(Collectors.toSet());
					for (ProductLocationMappingDTO productLocationMappingDTO : prodLocEntry.getValue()) {
						ProductLocationMappingDTO productLocationMappingDTO2 = productLocationMappingDTO.clone();
						productLocationMappingDTO.parentLocationLevelId = productLocationMappingKeyNew.getLocationLevelId();
						productLocationMappingDTO.parentLocationId = productLocationMappingKeyNew.getLocationId();
						productLocationMappingDTO2.locationLevelId = productLocationMappingKeyNew.getLocationLevelId();
						productLocationMappingDTO2.locationId = productLocationMappingKeyNew.getLocationId();
						if (!stores.contains(productLocationMappingDTO2.storeId)) {
							existingStores.add(productLocationMappingDTO2);
						}
					}
				}
			}
		}
		
		return productLocationMap;
	}

	public class ProductStoreKey{
		int productLevelId;
		int productId;
		int storeId;
		
		public ProductStoreKey(int productLevelId, int productId, int storeId){
			this.productLevelId = productLevelId;
			this.productId = productId;
			this.storeId = storeId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + productId;
			result = prime * result + productLevelId;
			result = prime * result + storeId;
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
			ProductStoreKey other = (ProductStoreKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (productId != other.productId)
				return false;
			if (productLevelId != other.productLevelId)
				return false;
			if (storeId != other.storeId)
				return false;
			return true;
		}

		private PriceZoneSetupService getOuterType() {
			return PriceZoneSetupService.this;
		}

		
	}
	
	
}
