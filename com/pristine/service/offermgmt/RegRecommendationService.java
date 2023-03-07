package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.offermgmt.PRCommonUtil;

public class RegRecommendationService {
	private static Logger logger = Logger.getLogger("RegRecommendationService");
	
	// Find if cur retail across store is same
	public void findIsCurRetailIsSameAcrossStores(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore) throws OfferManagementException {
		HashMap<ItemKey, List<MultiplePrice>> storeItemMap = new HashMap<ItemKey, List<MultiplePrice>>();

		try {
			ItemKey itemKey = null;
			List<MultiplePrice> storeItemPrices = new ArrayList<MultiplePrice>();
			// Convert storeItems to a hashmap with itemcode as key and all its
			// store level cur retail as value in a list
			for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> allStoresAndItsItems : itemDataMapStore.entrySet()) {
				for (Map.Entry<ItemKey, PRItemDTO> storeItems : allStoresAndItsItems.getValue().entrySet()) {
					// ignore lig
					PRItemDTO storeItem = storeItems.getValue();
					itemKey = storeItems.getKey();
					if (!storeItem.isLir()) {
						// If item is already present
						if (storeItemMap.get(itemKey) != null) {
							storeItemPrices = storeItemMap.get(itemKey);
							MultiplePrice mp = PRCommonUtil.getMultiplePrice(storeItem.getRegMPack(),
									storeItem.getRegPrice(), storeItem.getRegMPrice());
							storeItemPrices.add(mp);
						} else {
							storeItemPrices = new ArrayList<MultiplePrice>();
							MultiplePrice mp = PRCommonUtil.getMultiplePrice(storeItem.getRegMPack(),
									storeItem.getRegPrice(), storeItem.getRegMPrice());
							storeItemPrices.add(mp);
							storeItemMap.put(itemKey, storeItemPrices);
						}
					}
				}
			}

			//Loop each zone item
			for (Map.Entry<ItemKey, PRItemDTO> zoneItems : itemDataMap.entrySet()) {
				boolean isCurRetailSameAcrossStores = true;
				HashSet<MultiplePrice> storePrices = new HashSet<MultiplePrice>();
				//Get all its store items
				PRItemDTO zoneItem = zoneItems.getValue();
				ItemKey zoneItemKey = zoneItems.getKey();
				//It item present in any one of the store(only lig's ignored here)
				if(storeItemMap.get(zoneItemKey) != null){
					//Loop each store prices
					for (MultiplePrice storePrice : storeItemMap.get(zoneItemKey)) {
						if(storePrice != null){
							storePrices.add(storePrice);
						}
						else{
							//even if one of the store doesn't have price, then add Instance of multiple price
							//so that there will be more than one price in the hash set
							//and its marked as store has diff price
							//storePrices.add(new MultiplePrice(0, 0d));
						}
					}
				}
				//if there are more than one distinct price
				if(storePrices.size() > 1)
					isCurRetailSameAcrossStores  = false;
				zoneItem.setIsCurRetailSameAcrossStores(isCurRetailSameAcrossStores);
			}
		} catch (Exception e) {
			logger.error("Exception in findIsCurRetailIsSameAcrossStores()");
			throw new OfferManagementException("Error in findIsCurRetailIsSameAcrossStores() - " + e,
					RecommendationErrorCode.FIND_IF_CUR_RETAIL_SAME_ACROSS_STORES);
		}
	}
}
