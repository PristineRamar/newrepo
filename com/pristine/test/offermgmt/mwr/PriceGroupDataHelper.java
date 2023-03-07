package com.pristine.test.offermgmt.mwr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.offermgmt.PRConstants;

public class PriceGroupDataHelper {

	public static PRPriceGroupDTO getPriceGroupDTO(int itemCode, int priceGroupId, boolean isLir) {
		PRPriceGroupDTO priceGroupDTO = new PRPriceGroupDTO();
		priceGroupDTO.setPriceGroupId(priceGroupId);
		priceGroupDTO.setItemCode(itemCode);
		priceGroupDTO.setIsLig(isLir);

		return priceGroupDTO;
	}

	public static HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> setPriceGroupMap(
			HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> pgMap, PRPriceGroupDTO priceGroupDTO, boolean isLir,
			String pgName) {
		if (pgMap == null)
			pgMap = new HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>>();

		ItemKey itemKey = new ItemKey(priceGroupDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);
		if (isLir) {
			itemKey = new ItemKey(priceGroupDTO.getItemCode(), PRConstants.LIG_ITEM_INDICATOR);
		}

		HashMap<ItemKey, PRPriceGroupDTO> priceGroup = new HashMap<>();
		if (pgMap.containsKey(pgName)) {
			priceGroup = pgMap.get(pgName);
		}
		priceGroup.put(itemKey, priceGroupDTO);
		pgMap.put(pgName, priceGroup);

		return pgMap;
	}
	
	/**
	 * 
	 * @param itemCode
	 * @param priceGroupDTO
	 */
	public static void populateRelationListForSizeRelation(int itemCode, PRPriceGroupDTO priceGroupDTO) {
		PRPriceGroupRelatedItemDTO relatedItemDTO = new PRPriceGroupRelatedItemDTO();
		PRPriceGroupRelnDTO priceGroupRelnDTO = new PRPriceGroupRelnDTO();
		relatedItemDTO.setRelatedItemCode(itemCode);
		relatedItemDTO.setRelationType(PRConstants.SIZE_RELATION);
		relatedItemDTO.setPriceRelation(priceGroupRelnDTO);
		
		ArrayList<PRPriceGroupRelatedItemDTO> prList = new ArrayList<>();
		prList.add(relatedItemDTO);
		
		TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> prMap = new TreeMap<>();
		prMap.put(PRConstants.SIZE_RELATION, prList);
		
		priceGroupDTO.setRelationList(prMap);
	}
	
	
	/**
	 * 
	 * @param itemCode
	 * @param priceGroupDTO
	 */
	public static void populateRelationListForBrandRelation(int itemCode, PRPriceGroupDTO priceGroupDTO,
			double minValue, double maxValue, char valueType) {
		PRPriceGroupRelatedItemDTO relatedItemDTO = new PRPriceGroupRelatedItemDTO();
		PRPriceGroupRelnDTO priceGroupRelnDTO = new PRPriceGroupRelnDTO();
		priceGroupRelnDTO.setMinValue(minValue);
		priceGroupRelnDTO.setMaxValue(maxValue);
		priceGroupRelnDTO.setValueType(valueType);
		relatedItemDTO.setRelatedItemCode(itemCode);
		relatedItemDTO.setRelationType(PRConstants.BRAND_RELATION);
		relatedItemDTO.setPriceRelation(priceGroupRelnDTO);

		ArrayList<PRPriceGroupRelatedItemDTO> prList = new ArrayList<>();
		prList.add(relatedItemDTO);

		TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> prMap = new TreeMap<>();
		prMap.put(PRConstants.BRAND_RELATION, prList);

		priceGroupDTO.setRelationList(prMap);
	}

}
