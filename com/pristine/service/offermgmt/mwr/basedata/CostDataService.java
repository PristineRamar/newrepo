package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
//import org.apache.log4j.Logger;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;

public class CostDataService {

	//private static Logger logger = Logger.getLogger("CostDataService");
	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param storeList
	 * @param priceAndStrategyZoneNos
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getCostData(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, List<Integer> storeList, List<String> priceAndStrategyZoneNos,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, RecommendationInputDTO recommendationInputDTO)
					throws GeneralException {

		int costHistory = MultiWeekRecConfigSettings.getMwrCostHistory();

		Set<Integer> nonCachedItemCodeSet = new HashSet<Integer>();
		for (PRItemDTO item : itemDataMap.values()) {
			if (!item.isLir()) {
				nonCachedItemCodeSet.add(item.getItemCode());
			}
		}

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(conn);

		// Get non-cached item's zone and store cost history
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostMap = retailCostServiceOptimized
				.getCostHistory(recommendationInputDTO.getChainId(), calDTO, costHistory, allWeekCalendarDetails,
						nonCachedItemCodeSet, priceAndStrategyZoneNos, storeList);

		return itemCostMap;
	}
	
	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getFutureCostData(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, List<Integer> storeList, List<String> priceAndStrategyZoneNos,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, RecommendationInputDTO recommendationInputDTO)
					throws GeneralException {
		//Config added to decide noOfWeeks for which Future cost has to be fetched
		int weeksTofetchFutureCost = MultiWeekRecConfigSettings.getFutureCostWeeksToFetch();
	
		Set<Integer> nonCachedItemCodeSet = new HashSet<Integer>();
		for (PRItemDTO item : itemDataMap.values()) {
			if (!item.isLir()) {
				nonCachedItemCodeSet.add(item.getItemCode());
			}
		}

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(conn);
			// get the current week end date to pass to cost query for fetching future cost
			// data
			String weekendDate = DateUtil.localDateToString(
					LocalDate.parse(recommendationInputDTO.getBaseWeek(),
							DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT)).plusDays(6),
					Constants.APP_DATE_FORMAT);
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostMap =retailCostServiceOptimized.getFutureCost(recommendationInputDTO.getChainId(), calDTO,weeksTofetchFutureCost,
					allWeekCalendarDetails,nonCachedItemCodeSet, priceAndStrategyZoneNos, storeList,weekendDate);
		

		return itemCostMap;
	}

}
