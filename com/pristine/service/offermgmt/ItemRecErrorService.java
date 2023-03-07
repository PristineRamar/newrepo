package com.pristine.service.offermgmt;

import java.util.HashMap;
import java.util.Map;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;

public class ItemRecErrorService {

	
	public void setErrorCodeForZoneItems(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails) {
		setErrorCodeForItems(itemDataMap, leadZoneDetails);
	}

	public void setErrorCodeForStoreItems(HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemDataMapStore,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails) {
		for (Map.Entry<Integer, HashMap<ItemKey, PRItemDTO>> allStoresAndItsItems : itemDataMapStore.entrySet()) {
			setErrorCodeForItems(allStoresAndItsItems.getValue(), leadZoneDetails);
		}
	}
	
	private void setErrorCodeForItems(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails) {
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			if (!itemDTO.isLir()) {
				// If there is lead zone guideline
				// and there is no lead zone price, then don't recommend
				if (itemDTO.getStrategyDTO() != null && itemDTO.getStrategyDTO().getGuidelines() != null
						&& itemDTO.getStrategyDTO().getGuidelines().getLeadZoneGuideline() != null) {
					PricingEngineService pricingEngineService = new PricingEngineService();
					PRRecommendation leadZoneItem = pricingEngineService.getLeadZoneItem(itemDTO, leadZoneDetails);

					// The item is not present in lead zone
					if (leadZoneItem == null || leadZoneItem.getRecRegPriceObj() == null) {
						itemDTO.setIsRecError(true);
						itemDTO.setErrorButRecommend(false);
						itemDTO.getRecErrorCodes().add(ItemRecommendationStatus.NO_LEAD_ZONE_PRICE.getStatusCode());
					}
				} else {
					// Check if strategy present
					if (itemDTO.getStrategyDTO() == null) {
						itemDTO.setIsRecError(true);
						// Don't recommend for these items
						itemDTO.setErrorButRecommend(false);
						itemDTO.getRecErrorCodes().add(ItemRecommendationStatus.NO_STRATEGY.getStatusCode());
					}

					// Check if current retail available
					if (itemDTO.getRegPrice() == null || itemDTO.getRegPrice() == 0) {
						itemDTO.setIsRecError(true);
						// Don't recommend for these items
						itemDTO.setErrorButRecommend(false);
						itemDTO.getRecErrorCodes().add(ItemRecommendationStatus.CUR_RETAIL_UNAVAILABLE.getStatusCode());
					}

					// Check if current cost available
					if (itemDTO.getListCost() == null || itemDTO.getListCost() == 0) {
						itemDTO.setIsRecError(true);
						itemDTO.setErrorButRecommend(false);
						itemDTO.getRecErrorCodes().add(ItemRecommendationStatus.CUR_COST_UNAVAILABLE.getStatusCode());
					}
				}

			}
		}
	}
	
	public void updateErrorStatusAsNotRecommended(PRItemDTO itemDTO) {
		//When strategy what-if is done at price check list level, then non price check list
		//item are not recommended, when this non price check list item is part of relation
		//then there will be infinite loop, as the related item is not recommended and item
		//dependent on it will keep waiting for the related item to be recommended, if these
		//non price check list item are marked as error, then the dependent item will 
		//depend on related item
		
		itemDTO.setIsRecError(true);
		// Don't recommend for these items
		itemDTO.setErrorButRecommend(false);
		itemDTO.getRecErrorCodes().add(ItemRecommendationStatus.ITEM_NOT_CONSIDERED_FOR_RECOMMENDATION.getStatusCode());
	}
}
