package com.pristine.service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.offermgmt.PRCommonUtil;

public class LowPriceFlagSetupService {

	
	/**
	 * 
	 * @param priceList
	 * @param itemCodeMap
	 * @param zoneIdMap
	 * @param skippedRetailerItemcodes
	 * @param calendarId
	 * @return items with low price flags
	 * @throws GeneralException
	 * @throws Exception
	 */
	public List<GiantEaglePriceDTO> prepareItemsWithLowPriceFlags(List<GiantEaglePriceDTO> priceList,
			HashMap<ItemDetailKey, String> itemCodeMap, HashMap<String, Integer> zoneIdMap,
			Set<String> skippedRetailerItemcodes, HashMap<String, List<String>> upcMap, int calendarId,
			HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap,
			HashMap<Integer, Integer> itemCodeCategoryMap, String dateStr)
					throws GeneralException, Exception {

		List<GiantEaglePriceDTO> finalList = new ArrayList<>();

		for (GiantEaglePriceDTO giantEaglePriceDTO : priceList) {

			
			if(isExpiredNLPEndDate(dateStr, giantEaglePriceDTO.getNEW_LOW_PRC_END_DTE())){
				giantEaglePriceDTO.setNEW_LOW_PRC_END_DTE("");
				giantEaglePriceDTO.setNEW_LOW_PRC_FG("N");
			}
			
			if(isExpiredNLPEndDate(dateStr, giantEaglePriceDTO.getLOW_PRC_END_DTE())){
				giantEaglePriceDTO.setLOW_PRC_END_DTE("");
				giantEaglePriceDTO.setLOW_PRC_FG("N");
			}
			
			if (upcMap.get(giantEaglePriceDTO.getRITEM_NO()) == null) {

				skippedRetailerItemcodes.add(giantEaglePriceDTO.getRITEM_NO());
				continue;

			} else {

				List<String> upcs = upcMap.get(giantEaglePriceDTO.getRITEM_NO());

				for (String upc : upcs) {

					GiantEaglePriceDTO giantEaglePriceDTOCloned = (GiantEaglePriceDTO) giantEaglePriceDTO.clone();

					giantEaglePriceDTOCloned.setUpc(PrestoUtil.castUPC(upc, false));

					ItemDetailKey itemDetailKey = new ItemDetailKey(giantEaglePriceDTOCloned.getUpc(),
							giantEaglePriceDTOCloned.getRITEM_NO());

					if (itemCodeMap.containsKey(itemDetailKey)) {

						giantEaglePriceDTOCloned.setItemCode(Integer.parseInt(itemCodeMap.get(itemDetailKey)));

					} else {
						skippedRetailerItemcodes.add(giantEaglePriceDTO.getRITEM_NO());
						continue;
					}

					if (zoneIdMap.containsKey(giantEaglePriceDTOCloned.getZoneNumber())) {

						giantEaglePriceDTOCloned.setLoctionLevelId(Constants.ZONE_LEVEL_ID);
						giantEaglePriceDTOCloned.setLocationId(zoneIdMap.get(giantEaglePriceDTOCloned.getZoneNumber()));

					} else {
						skippedRetailerItemcodes.add(giantEaglePriceDTO.getRITEM_NO());
						continue;
					}

					giantEaglePriceDTOCloned.setCalendarId(calendarId);

					
					// Changes for creating record at warehouse zone level for DSD zones
					if(itemCodeCategoryMap.get(giantEaglePriceDTOCloned.getItemCode()) != null){
						int productId = itemCodeCategoryMap.get(giantEaglePriceDTOCloned.getItemCode());
						if (dsdAndWhseZoneMap.containsKey(productId)) {
							HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
							if (zoneMap.containsKey(giantEaglePriceDTOCloned.getZoneNumber())) {
								String whseZone = zoneMap.get(giantEaglePriceDTOCloned.getZoneNumber());
								if (zoneIdMap.get(whseZone) != null) {
									
									GiantEaglePriceDTO giantEaglePriceDTOWhseZone = (GiantEaglePriceDTO) giantEaglePriceDTOCloned
											.clone();
									
									giantEaglePriceDTOWhseZone.setLoctionLevelId(Constants.ZONE_LEVEL_ID);
									giantEaglePriceDTOWhseZone.setLocationId(zoneIdMap.get(whseZone));
									
									finalList.add(giantEaglePriceDTOWhseZone);
								}
							}
						}	
					}
					
					finalList.add(giantEaglePriceDTOCloned);

				}
			}
		}

		return finalList;
	}
	
	
	private boolean isExpiredNLPEndDate(String weekStart, String nlpDate){
		boolean isExpiredNLPEndDate = false;
		
		LocalDate currentDate = LocalDate.parse(weekStart, PRCommonUtil.getDateFormatter());
		
		if (nlpDate != null && !Constants.EMPTY.equals(nlpDate)) {
			
			LocalDate newLowPriceEndDate = LocalDate.parse(nlpDate, PRCommonUtil.getDateFormatter());
			if(newLowPriceEndDate.isBefore(currentDate)){
				isExpiredNLPEndDate = true;
			}
		}
		
		return isExpiredNLPEndDate;
	}
	
}
