package com.pristine.service.offermgmt;

//import java.sql.Connection;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map.Entry;

//import org.apache.log4j.Logger;

//import com.pristine.dao.offermgmt.SubstituteDAO;
//import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.dto.offermgmt.PRSubstituteGroup;
//import com.pristine.dto.offermgmt.PRSubstituteItem;
//import com.pristine.exception.GeneralException;
//import com.pristine.exception.OfferManagementException;
//import com.pristine.util.Constants;
//import com.pristine.util.offermgmt.PRCommonUtil;
//import com.pristine.util.offermgmt.PRConstants;

public class SubstituteService {
//	private static Logger logger = Logger.getLogger("SubstituteService");
	
	/***
	 * Get all valid substitution groups
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param retLirMap
	 * @param itemDataMap
	 * @return
	 * @throws OfferManagementException
	 */
	/*public List<PRSubstituteGroup> getSubstituteGroups(Connection conn, int locationLevelId, int locationId,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<ItemKey, PRItemDTO> itemDataMap)
			throws OfferManagementException {
		List<PRSubstituteGroup> substituteGroup;
		SubstituteDAO substituteDAO = new SubstituteDAO();
		HashMap<Integer, PRSubstituteGroup> substituteGroups = new HashMap<Integer, PRSubstituteGroup>();
		List<Integer> retLirIds = new ArrayList<Integer>();
		List<Integer> nonLigItems = new ArrayList<Integer>();
		try {
			//All ret lir ids
			retLirIds.addAll(retLirMap.keySet());
			
			//All non lig's
			for (PRItemDTO itemDTO : itemDataMap.values()) {
				if (!itemDTO.isLir() && itemDTO.getRetLirId() < 1)
					nonLigItems.add(itemDTO.getItemCode());
			}
			
			if (retLirIds.size() > 0 || nonLigItems.size() > 0)
				substituteGroups = substituteDAO.getSubstituteItems(conn, locationLevelId, locationId, retLirIds,
						nonLigItems);
			// Remove substitution group, if the group item is not part of the substitution group
			substituteGroup = filterSubstituteItems(substituteGroups, itemDataMap);
			logger.info("No of valid substitution groups:" + substituteGroup.size());
		} catch (Exception | GeneralException ex) {
			logger.error("Exeption in filterSubstituteItems()" + ex);
			throw new OfferManagementException("Exception in filterSubstituteItems() " + ex,
					RecommendationErrorCode.SUBSTITUTION_SERVICE);
		}
		return substituteGroup;
	}*/
	
	/***
	 * Remove substitution group if the lead item is not authorized or none of the item is authorised
	 * Remove items from substitution group if the item is not authorized
	 * Remove susbtitution group if lead item is not part of substitution group
	 * @param substituteGroups
	 * @return
	 * @throws OfferManagementException
	 */
	/*private List<PRSubstituteGroup> filterSubstituteItems(HashMap<Integer, PRSubstituteGroup> substituteGroups,
			HashMap<ItemKey, PRItemDTO> itemDataMap) throws OfferManagementException {
		List<PRSubstituteGroup> substituteGroup = new ArrayList<PRSubstituteGroup>();
		try {

			// Remove substitution group, if the lead item is not part of the
			// substitution group
			// Loop substitute groups
			for (Entry<Integer, PRSubstituteGroup> subsGroupEntry : substituteGroups.entrySet()) {
				PRSubstituteGroup subsGroup = subsGroupEntry.getValue();
				boolean isLeadItemPresentInSubsGroup = false, isLeadItemIsAuthorized = false;
				int groupItemId = subsGroup.getGroupItemId();
				int groupItemType = subsGroup.getGroupItemType();
				ItemKey leadItemKey;
				List<PRSubstituteItem> subtituteItems = new ArrayList<PRSubstituteItem>();

				if (groupItemType == Constants.PRODUCT_LEVEL_ID_LIG)
					leadItemKey = new ItemKey(groupItemId, PRConstants.LIG_ITEM_INDICATOR);
				else
					leadItemKey = new ItemKey(groupItemId, PRConstants.NON_LIG_ITEM_INDICATOR);

				// All  items
				// Remove if items are not authorized to sell in
				// the zone
				for (PRSubstituteItem subsItem : subsGroup.getSubtituteItems()) {
					//pick non lead items
					ItemKey itemAKey, itemBKey;
					
					if (subsItem.getItemAType() == Constants.PRODUCT_LEVEL_ID_LIG)
						itemAKey = new ItemKey(subsItem.getItemAId(), PRConstants.LIG_ITEM_INDICATOR);
					else
						itemAKey = new ItemKey(subsItem.getItemAId(), PRConstants.NON_LIG_ITEM_INDICATOR);
					
					if (subsItem.getItemBType() == Constants.PRODUCT_LEVEL_ID_LIG)
						itemBKey = new ItemKey(subsItem.getItemBId(), PRConstants.LIG_ITEM_INDICATOR);
					else
						itemBKey = new ItemKey(subsItem.getItemBId(), PRConstants.NON_LIG_ITEM_INDICATOR);
					
					//Both are authorized to sell
					if (itemDataMap.get(itemAKey) != null && itemDataMap.get(itemBKey) != null)
						subtituteItems.add(subsItem);
				}

				// All substitute items
				for (PRSubstituteItem subsItem : subsGroup.getSubtituteItems()) {
					// Item present in group
					if ((subsItem.getItemBId() == groupItemId && subsItem.getItemBType() == groupItemType)
							|| (subsItem.getItemAId() == groupItemId && subsItem.getItemAType() == groupItemType)) {
						isLeadItemPresentInSubsGroup = true;
						break;
					}
				}
				if (itemDataMap.get(leadItemKey) != null)
					isLeadItemIsAuthorized = true;

				// Ignore if lead item is not authorized to sell in the zone
				// or if all the dependents items are not authorized to sell in the zone
				if (isLeadItemIsAuthorized && isLeadItemPresentInSubsGroup && subtituteItems.size() > 0) {
					subsGroup.getSubtituteItems().clear();
					subsGroup.setSubtituteItems(subtituteItems);
					substituteGroup.add(subsGroup);
				}
			}
		} catch (Exception ex) {
			logger.error("Exeption in filterSubstituteItems()" + ex);
			throw new OfferManagementException("Exception in filterSubstituteItems() " + ex,
					RecommendationErrorCode.SUBSTITUTION_SERVICE);
		}
		return substituteGroup;
	}*/
	
	/***
	 * Update if an item is part of substitute or not in the itemDataMap
	 * @param substituteGroups
	 * @param itemDataMap
	 * @param retLirMap
	 */
	/*public void updateSubstituteFlag(List<PRSubstituteGroup> substituteGroups, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		List<ItemKey> subsItemKey = new ArrayList<ItemKey>();
		HashSet<ItemKey> subsLeadItems = new HashSet<ItemKey>();
		
		for (PRSubstituteGroup subsGroup : substituteGroups) {
			ItemKey itemKey;
			if(subsGroup.getGroupItemType()  == Constants.PRODUCT_LEVEL_ID_LIG)
				itemKey = new ItemKey(subsGroup.getGroupItemId(), PRConstants.LIG_ITEM_INDICATOR);
			else
				itemKey = new ItemKey(subsGroup.getGroupItemId(), PRConstants.NON_LIG_ITEM_INDICATOR);
			subsLeadItems.add(itemKey);
		}
		
		// each substitution group
		for (PRSubstituteGroup subsGroup : substituteGroups) {
			// Each substitute item
			for (PRSubstituteItem subsItem : subsGroup.getSubtituteItems()) {
				ItemKey itemKey;

				// Ret lir id
				if (subsItem.getItemAType() == Constants.PRODUCT_LEVEL_ID_LIG) {
					itemKey = PRCommonUtil.getItemKey(subsItem.getItemAId(), true);
					subsItemKey.add(itemKey);
				} else {
					itemKey = PRCommonUtil.getItemKey(subsItem.getItemAId(), false);
					subsItemKey.add(itemKey);
				}

				// Ret lir id
				if (subsItem.getItemBType() == Constants.PRODUCT_LEVEL_ID_LIG) {
					itemKey = PRCommonUtil.getItemKey(subsItem.getItemBId(), true);
					subsItemKey.add(itemKey);
				} else {
					itemKey = PRCommonUtil.getItemKey(subsItem.getItemBId(), false);
					subsItemKey.add(itemKey);
				}
			}
		}

		// Update flag for lig members and non-lig's
		for (ItemKey itemKey : subsItemKey) {
			boolean isLeadItem = false;
			if (itemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
				if(subsLeadItems.contains(itemKey))
					isLeadItem = true;
				
				// All lig members
				if (retLirMap.get(itemKey.getItemCodeOrRetLirId()) != null) {
					//update lig
					ItemKey ligItemKey = PRCommonUtil.getItemKey(itemKey.getItemCodeOrRetLirId(), true);
					itemDataMap.get(ligItemKey).setIsPartOfSubstituteGroup(true);
					for (PRItemDTO ligMem : retLirMap.get(itemKey.getItemCodeOrRetLirId())) {
						ItemKey ligMemKey = PRCommonUtil.getItemKey(ligMem);
						if (itemDataMap.get(ligMemKey) != null){
							itemDataMap.get(ligMemKey).setIsPartOfSubstituteGroup(true);
							itemDataMap.get(ligMemKey).setIsSubstituteLeadItem(isLeadItem);
						}
					}
				}
			} else {
				if (itemDataMap.get(itemKey) != null) {
					if(subsLeadItems.contains(itemKey))
						isLeadItem = true;
					itemDataMap.get(itemKey).setIsPartOfSubstituteGroup(true);
					itemDataMap.get(itemKey).setIsSubstituteLeadItem(isLeadItem);
				}
			}
		}
	}*/
}
