package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.LocationCompetitorMapDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;

public class CompDataService {
	private static Logger logger = Logger.getLogger("CompDataService");

	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param itemDataMap
	 * @return competitor map
	 * @throws GeneralException
	 */
	public HashMap<Integer, LocationKey> getCompetitors(Connection conn,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, RecommendationInputDTO recommendationInputDTO)
					throws GeneralException {

		LocationCompetitorMapDAO locationCompetitorMapDAO = new LocationCompetitorMapDAO();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		if (recommendationInputDTO.isPriceTestZone()) {
			// Gets competitor ids for price test its going to use the zone Id for maximum stores in storelist
			compIdMap = locationCompetitorMapDAO.getCompetitors(conn, recommendationInputDTO.getLocationLevelId(),
					recommendationInputDTO.getTempLocationID(), recommendationInputDTO.getProductLevelId(),
					recommendationInputDTO.getProductId());

		} else {
			// Gets competitor ids
			compIdMap = locationCompetitorMapDAO.getCompetitors(conn, recommendationInputDTO.getLocationLevelId(),
					recommendationInputDTO.getLocationId(), recommendationInputDTO.getProductLevelId(),
					recommendationInputDTO.getProductId());
		}
		// new PricingEngineService().addDistinctCompStrId(itemDataMap, compIdMap);

		addDistinctCompetitorsFromStrategy(compIdMap, strategyMap);
		
		return compIdMap;
	}

	/**
	 * 
	 * @param compIdMap
	 * @param strategyMap
	 */
	private void addDistinctCompetitorsFromStrategy(HashMap<Integer, LocationKey> compIdMap,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap) {

		Set<LocationKey> distinctCompIds = new HashSet<>();

		for (Map.Entry<StrategyKey, List<PRStrategyDTO>> strategyEntry : strategyMap.entrySet()) {
			for (PRStrategyDTO prStrategyDTO : strategyEntry.getValue()) {
				if (prStrategyDTO.getGuidelines().getPiGuideline() != null) {
					if (prStrategyDTO.getGuidelines().getPiGuideline().size() > 0) {
						for (PRGuidelinePI guidelinePI : prStrategyDTO.getGuidelines().getPiGuideline()) {
							if (guidelinePI.getCompStrId() > 0) {
								LocationKey lk = new LocationKey(Constants.STORE_LEVEL_ID, guidelinePI.getCompStrId());
								distinctCompIds.add(lk);
							}
						}
					}
				}
			}
		}

		int dummyKey = 1;
		// Add to compIdMap
		for (LocationKey locationKey : distinctCompIds) {
			if (compIdMap.get((dummyKey)) == null) {
				compIdMap.put(dummyKey, locationKey);
				dummyKey = dummyKey + 1;
			}
		}
	}

	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param competitorMap
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 * @throws Exception
	 */
	public HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> getLatestCompPriceData(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, LocationKey> competitorMap,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException, Exception {

		HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompDataMap = new HashMap<>();
		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		int compHistory = MultiWeekRecConfigSettings.getMwrCompHistory();

		for (Map.Entry<Integer, LocationKey> compEntry: competitorMap.entrySet()) {
			LocationKey locationKey = compEntry.getValue();
			PRStrategyDTO tempDTO = new PRStrategyDTO();
			tempDTO.copy(inputDTO);
			tempDTO.setLocationLevelId(locationKey.getLocationLevelId());
			tempDTO.setLocationId(locationKey.getLocationId());
			tempDTO.setChainId(locationKey.getChainId());
			// Sets current comp price, previous comp price, comp price change indicator for items in
//			logger.info("Getting latest competitor data for comp store:" + locationKey.toString() + " is Started...");
//			logger.info("Comp str type:" + compEntry.getKey());
			// pricingEngineDAO.getCompPriceData(conn, tempDTO, calDTO, null, compHistory, itemDataMap);

			HashMap<Integer, CompetitiveDataDTO> priceData = new PricingEngineDAO().getLatestCompPriceData(conn,
					tempDTO, recommendationInputDTO.getBaseWeek(), compHistory * 7);

			latestCompDataMap.put(locationKey, priceData);

//			logger.info("Getting latest competitor data for comp store:" + locationKey.toString() + " is Completed...");
		}

		return latestCompDataMap;
	}

	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param competitorMap
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 * @throws Exception
	 */
	public HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> getPreviousCompPriceData(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, LocationKey> competitorMap,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException, Exception {

		HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompDataMap = new HashMap<>();
		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		int compHistory = MultiWeekRecConfigSettings.getMwrCompHistory();

		for (LocationKey locationKey : competitorMap.values()) {
			PRStrategyDTO tempDTO = new PRStrategyDTO();
			tempDTO.copy(inputDTO);
			tempDTO.setLocationLevelId(locationKey.getLocationLevelId());
			tempDTO.setLocationId(locationKey.getLocationId());
			tempDTO.setChainId(locationKey.getChainId());
			// Sets current comp price, previous comp price, comp price change indicator for items in
//			logger.info("Getting latest competitor data for comp store:" + locationKey.toString() + " is Started...");
			// pricingEngineDAO.getCompPriceData(conn, tempDTO, calDTO, null, compHistory, itemDataMap);

			HashMap<Integer, CompetitiveDataDTO> priceData = new PricingEngineDAO().getPreviousCompPriceData(conn,
					tempDTO, recommendationInputDTO.getBaseWeek(), compHistory * 7);

			previousCompDataMap.put(locationKey, priceData);

//			logger.info("Getting latest competitor data for comp store:" + locationKey.toString() + " is Completed...");
		}

		return previousCompDataMap;
	}

	/**
	 * 
	 * @param conn
	 * @param strategyMap
	 * @param recommendationInputDTO
	 * @return latest price map for multi comp
	 * @throws OfferManagementException
	 */
	public void getMultiCompLatestPriceMap(Connection conn,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<ItemKey, PRItemDTO> itemDataMap, 
			RecommendationInputDTO recommendationInputDTO)
					throws OfferManagementException {
		/*** Retrieve Competition Data for Multiple Competitor ***/
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		logger.info("Retrieving of Competition Data for Multiple Competitor is Started...");
		long tStartTime = System.currentTimeMillis();
		new PricingEngineWS().applyMultiCompRetails(conn, strategyMap, curCalDTO,
				recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(), itemDataMap);
		long tEndTime = System.currentTimeMillis();
		logger.info("^^^ Time -- Get Multi Comp Price(getLatestCompDataForMultiComp) --> "
				+ ((tEndTime - tStartTime) / 1000) + " s ^^^");
		/*** Retrieve Competition Data for Multiple Competitor ***/

	}

	
	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public void setupMultiCompRetails(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, Integer> compSetting)
			throws GeneralException {
		itemDataMap.forEach((itemKey, itemDTO) -> {
			HashMap<LocationKey, MultiplePrice> compMap = itemDTO.getAllCompPrice();

			compSetting.forEach((order, compChainId) -> {
				if (order == 1) {
					for (Map.Entry<LocationKey, MultiplePrice> compEntry : compMap.entrySet()) {
						if (compEntry.getKey().getChainId() == compChainId) {
							itemDTO.setComp1Retail(compEntry.getValue());
							itemDTO.setComp1StrId(compEntry.getKey().getLocationId());
						}
					}
				} else if (order == 2) {
					for (Map.Entry<LocationKey, MultiplePrice> compEntry : compMap.entrySet()) {
						if (compEntry.getKey().getChainId() == compChainId) {
							itemDTO.setComp2Retail(compEntry.getValue());
							itemDTO.setComp2StrId(compEntry.getKey().getLocationId());
						}
					}
				} else if (order == 3) {
					for (Map.Entry<LocationKey, MultiplePrice> compEntry : compMap.entrySet()) {
						if (compEntry.getKey().getChainId() == compChainId) {
							itemDTO.setComp3Retail(compEntry.getValue());
							itemDTO.setComp3StrId(compEntry.getKey().getLocationId());
						}
					}
				} else if (order == 4) {
					for (Map.Entry<LocationKey, MultiplePrice> compEntry : compMap.entrySet()) {
						if (compEntry.getKey().getChainId() == compChainId) {
							itemDTO.setComp4Retail(compEntry.getValue());
							itemDTO.setComp4StrId(compEntry.getKey().getLocationId());
						}
					}
				} else if (order == 5) {
					for (Map.Entry<LocationKey, MultiplePrice> compEntry : compMap.entrySet()) {
						if (compEntry.getKey().getChainId() == compChainId) {
							itemDTO.setComp5Retail(compEntry.getValue());
							itemDTO.setComp5StrId(compEntry.getKey().getLocationId());
						}
					}
				}
			});
		});
	}
}
