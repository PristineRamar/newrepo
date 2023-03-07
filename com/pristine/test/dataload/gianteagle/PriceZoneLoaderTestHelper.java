package com.pristine.test.dataload.gianteagle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.PriceGroupAndCategoryKey;
import com.pristine.dto.offermgmt.ProductLocationMappingDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingKey;

public class PriceZoneLoaderTestHelper {

	public static PriceGroupAndCategoryKey getPriceGroupAndCatKey(String prcGrpCd, int productId){
		PriceGroupAndCategoryKey priceGroupAndCategoryKey = new PriceGroupAndCategoryKey(prcGrpCd, productId);
		
		return priceGroupAndCategoryKey;
	}
	
	
	public static ProductLocationMappingDTO getProductLocationMapDTO(int productLevelId, int productId, int locationLevelId, int locationId,
			int storeId, String prcGrpCode){
		ProductLocationMappingDTO productLocationMappingDTO = new ProductLocationMappingDTO();
		
		
		productLocationMappingDTO.productLevelId = productLevelId;
		productLocationMappingDTO.productId = productId;
		productLocationMappingDTO.locationLevelId = locationLevelId;
		productLocationMappingDTO.locationId = locationId;
		productLocationMappingDTO.storeId = storeId;
		productLocationMappingDTO.prcGrpCode = prcGrpCode;
		
		return productLocationMappingDTO;
	}
	
	
	public static ProductLocationMappingDTO getProductLocationMapDTO(int productLevelId, int productId, int locationLevelId, int locationId,
			int storeId, String prcGrpCode, int parentLocationLevelId, int parentLocationId){
		ProductLocationMappingDTO productLocationMappingDTO = new ProductLocationMappingDTO();
		
		
		productLocationMappingDTO.productLevelId = productLevelId;
		productLocationMappingDTO.productId = productId;
		productLocationMappingDTO.locationLevelId = locationLevelId;
		productLocationMappingDTO.locationId = locationId;
		productLocationMappingDTO.storeId = storeId;
		productLocationMappingDTO.prcGrpCode = prcGrpCode;
		productLocationMappingDTO.parentLocationLevelId = parentLocationLevelId;
		productLocationMappingDTO.parentLocationId = parentLocationId;
		
		return productLocationMappingDTO;
	}
	
	
	public static HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> getProductLocationMap(List<ProductLocationMappingDTO> productLocList){
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocationMap = new HashMap<>();
		for (ProductLocationMappingDTO priceZoneMapDTO : productLocList) {
			ProductLocationMappingKey productLocationMappingKey = new ProductLocationMappingKey(
					priceZoneMapDTO.productLevelId, priceZoneMapDTO.productId, priceZoneMapDTO.locationLevelId,
					priceZoneMapDTO.locationId);
			List<ProductLocationMappingDTO> tempList;
			if (productLocationMap.get(productLocationMappingKey) != null) {
				tempList = productLocationMap.get(productLocationMappingKey);
			} else {
				tempList = new ArrayList<ProductLocationMappingDTO>();
			}
			tempList.add(priceZoneMapDTO);
			productLocationMap.put(productLocationMappingKey, tempList);
		}
		return productLocationMap;
	}
	
	
}
