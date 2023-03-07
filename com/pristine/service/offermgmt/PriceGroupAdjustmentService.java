package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRGuidelineBrand;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PriceGroupAdjustmentService {

	
	private static Logger logger = Logger.getLogger("RecommendationFlow");
	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 */
	public void adjustPriceGroupsByDiscontinuedItems(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(!itemDTO.isLir()) {
				if (itemDTO.getPgData() != null) {
					if (itemDTO.getPgData().getRelationList() != null) {
						NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = itemDTO.getPgData().getRelationList();
						navigableMap.forEach((brandSizeKey, priceGroupList) -> {
							if (brandSizeKey == PRConstants.SIZE_RELATION) {
								priceGroupList.forEach(relatedItem -> {
									adjustSizeRelatedItems(itemDTO, relatedItem, itemDataMap);	
								});
							} else if (brandSizeKey == PRConstants.BRAND_RELATION) {
								priceGroupList.forEach(relatedItem -> {
									adjustBrandRelatedItems(itemDTO, relatedItem, itemDataMap);	
								});
							}
						});
					}
				}
			}
		});
	}
	
	/**
	 * 
	 * @param itemDTO
	 * @param relatedItem
	 * @param itemDataMap
	 */
	private void adjustSizeRelatedItems(PRItemDTO itemDTO, PRPriceGroupRelatedItemDTO relatedItem, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		ItemKey relateItemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
		if(itemDataMap.containsKey(relateItemKey)) {
			PRItemDTO relatedItemDTO = itemDataMap.get(relateItemKey);
			if(relatedItemDTO.isNonMovingItem() || !relatedItemDTO.isActive() || !relatedItemDTO.isAuthorized()) {
				if(relatedItemDTO.getPgData() != null && relatedItemDTO.getPgData().getRelationList() != null) {
					NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = relatedItemDTO.getPgData().getRelationList();
					navigableMap.forEach((brandSizeKey, priceGroupList) -> {
						if (brandSizeKey == PRConstants.SIZE_RELATION) {
							itemDTO.getPgData().getRelationList().put(brandSizeKey, priceGroupList);
						}
					});
				}
			}
		}
	}
	
	/**
	 * 
	 * @param itemDTO
	 * @param relatedItem
	 * @param itemDataMap
	 */
	private void adjustBrandRelatedItems(PRItemDTO itemDTO, PRPriceGroupRelatedItemDTO relatedItem, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		ItemKey relateItemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
		if(itemDataMap.containsKey(relateItemKey)) {
			PRItemDTO relatedItemDTO = itemDataMap.get(relateItemKey);
			if(relatedItemDTO.isNonMovingItem() || !relatedItemDTO.isActive() || !relatedItemDTO.isAuthorized()) {
				logger.debug("Non moving or discontinued lead: " + relatedItemDTO.getItemCode() + ", for item code: " + itemDTO.getItemCode());
				if(relatedItemDTO.getPgData() != null && relatedItemDTO.getPgData().getRelationList() != null) {
					logger.debug("Non moving or discontinued lead has a lead. Establishing new relation: " + relatedItemDTO.getItemCode());
					NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = relatedItemDTO.getPgData().getRelationList();
					
					if (navigableMap.containsKey(PRConstants.BRAND_RELATION)) {
						ArrayList<PRPriceGroupRelatedItemDTO> priceGroupRelatedList = navigableMap
								.get(PRConstants.BRAND_RELATION);
						priceGroupRelatedList.forEach(newLeadItem -> {
							logger.debug("New lead found: " + newLeadItem.getRelatedItemCode() + ", for item code: "
									+ itemDTO.getItemCode());
							if (newLeadItem.getPriceRelation() != null && relatedItem.getPriceRelation() != null) {
								newLeadItem.addPriceRelations(newLeadItem.getPriceRelation());
								newLeadItem.addPriceRelations(relatedItem.getPriceRelation());
							}
							
							
							if (relatedItem.getRelatedItemBrandTier() != null
									&& !Constants.EMPTY.equals(relatedItem.getRelatedItemBrandTier())
									&& newLeadItem.getRelatedItemBrandTier() != null
									&& !Constants.EMPTY.equals(newLeadItem.getRelatedItemBrandTier())) {
								boolean isTierMatchingInGuideline = false;
								if (itemDTO.getStrategyDTO().getGuidelines().getBrandGuideline() != null) {
									for (PRGuidelineBrand brandGuideline : itemDTO.getStrategyDTO().getGuidelines()
											.getBrandGuideline()) {
										if (brandGuideline.getBrandTier2()
												.equals(relatedItem.getRelatedItemBrandTier())) {
											isTierMatchingInGuideline = true;
											break;
										}
									}
									if (isTierMatchingInGuideline) {
										logger.debug("Missing tier: " + relatedItem.getRelatedItemBrandTier()
												+ ", new tier: " + newLeadItem.getRelatedItemBrandTier());
										itemDTO.getMissingTierInfo().add(relatedItem.getRelatedItemBrandTier());
										itemDTO.getMissingTierInfo().add(newLeadItem.getRelatedItemBrandTier());
									}
								}
							}
						});
						itemDTO.getPgData().getRelationList().put(PRConstants.BRAND_RELATION, priceGroupRelatedList);
					}
				}
			}
		}
	}
}
