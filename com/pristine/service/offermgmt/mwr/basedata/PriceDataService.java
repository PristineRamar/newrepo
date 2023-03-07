package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;

public class PriceDataService {

	/**
	 * 
	 * @param conn
	 * @param locationId
	 * @param chainId
	 * @param itemDataMap
	 * @param priceAndStrategyZoneNos
	 * @param allWeekCalendarDetails
	 * @param storeList
	 * @param startDate
	 * @return zone level price map
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, RetailPriceDTO>> getZonePriceHistory(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails,
			List<Integer> storeList, List<PRItemDTO> authorizedItems, List<String> priceAndStrategyZoneNos,
			RecommendationInputDTO recommendationInputDTO, HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory) throws GeneralException {
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory =new HashMap<Integer, HashMap<String,RetailPriceDTO>>();

		if (recommendationInputDTO.isPriceTestZone()) {
			itemZonePriceHistory = new PricingEngineService().getItemZonePriceHistory(
					recommendationInputDTO.getChainId(), recommendationInputDTO.getTempLocationID(), itemPriceHistory);
		} else {
			itemZonePriceHistory = new PricingEngineService().getItemZonePriceHistory(
					recommendationInputDTO.getChainId(), recommendationInputDTO.getLocationId(), itemPriceHistory);
		}

		return itemZonePriceHistory;
	}

	
	public HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> getPriceHistory(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails,
			List<Integer> storeList, List<PRItemDTO> authorizedItems, List<String> priceAndStrategyZoneNos,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = new PricingEngineDAO()
				.getPriceHistory(conn, recommendationInputDTO.getChainId(), allWeekCalendarDetails, calDTO, calDTO,
						itemDataMap, priceAndStrategyZoneNos, storeList, false);
		return itemPriceHistory;
	}
	
	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param priceAndStrategyZoneNos
	 * @param storeList
	 * @param recommendationInputDTO
	 * @return latest price data
	 * @throws GeneralException
	 */
	public LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> getLatestPriceData(
			Connection conn, HashMap<ItemKey, PRItemDTO> itemDataMap, List<String> priceAndStrategyZoneNos,
			List<Integer> storeList, RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		RetailCalendarDTO calDTO = new RetailCalendarDAO().getCalendarId(conn, recommendationInputDTO.getBaseWeek(),
				Constants.CALENDAR_WEEK);

		LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap = new PricingEngineDAO()
				.getLatestPriceDataOptimized(conn, recommendationInputDTO.getChainId(), calDTO, MultiWeekRecConfigSettings.getMwrCostHistory(), itemDataMap,
						priceAndStrategyZoneNos, storeList);

		return priceDataMap;
	}

}
