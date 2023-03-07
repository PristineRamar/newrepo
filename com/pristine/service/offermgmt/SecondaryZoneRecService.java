package com.pristine.service.offermgmt;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.util.offermgmt.PRCommonUtil;

public class SecondaryZoneRecService {
	private static Logger logger = Logger.getLogger("SecondaryZoneRecService");
	/**
	 * 
	 * @param itemDataMap
	 */
	public void applyRecommendationForSecondaryZones(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, boolean isUpdateRec) {
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (!itemDTO.isLir()) {
				if (itemDTO.getRegPrice() != null && itemDTO.getRecommendedRegPrice() != null) {
					double deltaPrice = getPriceDelta(itemDTO);
					if (itemDTO.getSecondaryZones() != null && itemDTO.getSecondaryZones().size() > 0) {
						itemDTO.getSecondaryZones().forEach(secZoneDTO -> {
							double newRegPrice = secZoneDTO.getCurrentRegPrice().price + deltaPrice;
							MultiplePrice recommendedPriceForSecZone = new MultiplePrice(
									secZoneDTO.getCurrentRegPrice().multiple, newRegPrice);
							if(isUpdateRec) {
								secZoneDTO.setOverrideRegPrice(recommendedPriceForSecZone);
							} else {
								secZoneDTO.setRecommendedRegPrice(recommendedPriceForSecZone);	
							}
						});
						itemDTO.setSecondaryZoneRecPresent(true);
						logger.debug("Item with secondary zone recommendation: " + itemDTO.getItemCode());
					}
				}
			}
		});
	}

	/**
	 * 
	 * @param itemDTO
	 * @return diff unit price between current and new price
	 */
	private double getPriceDelta(PRItemDTO itemDTO) {
		MultiplePrice currPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		double currUnitPrice = PRCommonUtil.getUnitPrice(currPrice, true);
		double recUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRecommendedRegPrice(), true);
		double deltaPrice = recUnitPrice - currUnitPrice;
		return deltaPrice;
	}
}
