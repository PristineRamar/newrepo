package com.pristine.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
//import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class RetailCostServiceOptimized {
	private static Logger logger = Logger.getLogger("RetailCostServiceOptimized");
	
	private Connection connection = null;
//	private static final String GET_RETAIL_COST = "SELECT ITEM_CODE, LEVEL_TYPE_ID, "
//			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO=LEVEL_ID AND COMP_CHAIN_ID = ?) "
//			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ZONE_NUM=LEVEL_ID) "
//			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, "
//			+ "LIST_COST, DEAL_COST, VIP_COST, " + " TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE "
//			+ "  FROM SYNONYM_RETAIL_COST_INFO " + "  WHERE CALENDAR_ID = ? " + "  AND ITEM_CODE IN (%s) "
//			+ "  AND (%LOCATION_SUBQUERY%)";
	
	private static final String GET_RETAIL_COST = "SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, "
			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE "
			+ " CAST_DATA(COMP_STR_NO," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ " )= " 
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ ")"
			+ " AND COMP_CHAIN_ID = ?) "
			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE "
			+ "CAST_DATA(ZONE_NUM," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ " )= "
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ "))"
			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, "
			+ "LIST_COST, DEAL_COST, VIP_COST, " + " TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, "
			+ "DEAL_COST, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE "
			//NU:: 1st Dec 2016, GE has one more additional cost
			+ ", LIST_COST_2"
			//+ "  FROM SYNONYM_RETAIL_COST_INFO " + "  WHERE CALENDAR_ID = ? " + "  AND ITEM_CODE IN (%s) "
			//NU:: 2nd Feb 2018, get allowance cost and flag to use in recommendation for GE
			+ ", ALLOWANCE_COST, LONG_TERM_FLAG, CWAC_CORE_COST,NIPO_BASE_COST,CWAC_BASE_COST "
			+ "  FROM SYN_RETAIL_COST_INFO " + "  WHERE CALENDAR_ID = ? " + "  AND ITEM_CODE IN (%s) "
			+ "  AND (%LOCATION_SUBQUERY%)";
	
	private static final String GET_RETAIL_COST_ALL = "SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, "
			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE "
			+ " CAST_DATA(COMP_STR_NO," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ " )= " 
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ ")"
			+ " AND COMP_CHAIN_ID = ?) "
			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE "
			+ "CAST_DATA(ZONE_NUM," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ " )= "
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ "))"
			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, "
			+ "LIST_COST, DEAL_COST, VIP_COST, " + " TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, "
			+ "DEAL_COST, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE "
			+ ", LIST_COST_2"
			+ ", ALLOWANCE_COST, LONG_TERM_FLAG "
			+ "  FROM SYN_RETAIL_COST_INFO " + "  WHERE CALENDAR_ID = ? " + "  AND ITEM_CODE IN (%s) "
			+ "  AND (%LOCATION_SUBQUERY%)";	
	
	private static final String GET_FUTURE_COST = "SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, "
			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE "
			+ " CAST_DATA(COMP_STR_NO," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ " )= " 
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ ")"
			+ " AND COMP_CHAIN_ID = ?) "
			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE "
			+ "CAST_DATA(ZONE_NUM," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ " )= "
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ "))"
			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, "
			+ "LIST_COST, DEAL_COST, VIP_COST, " + " TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, "
			+ "DEAL_COST, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE "
			+ ", LIST_COST_2"
			+ ", ALLOWANCE_COST, LONG_TERM_FLAG, CWAC_CORE_COST,NIPO_BASE_COST,CWAC_BASE_COST "
			+ "  FROM SYN_RETAIL_COST_INFO  WHERE  EFF_LIST_COST_DATE > TO_DATE(?, 'MM/dd/yyyy') AND CALENDAR_ID = ?  AND ITEM_CODE IN (%s) "
			+ "  AND (%LOCATION_SUBQUERY%)";
	
	public RetailCostServiceOptimized(Connection connection){
		this.connection = connection;
	}
	
	/**
	 * Returns latest cost info for input product and location combination
	 * @throws GeneralException 
	 */
	public HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> getRetailCost(int calendarId,
			int chainId, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, boolean fetchStoreData,
			Set<Integer> itemCodeSet) throws GeneralException {

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveRetailCost(costDataMap, calendarId, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, values);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveRetailCost(costDataMap, calendarId, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, values);
		}

		return costDataMap;
	}
	
	
	/**
	 * Returns latest cost info for input product and location combination
	 * @throws GeneralException 
	 */
	public HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> getRetailCost(
		int calendarId,	int chainId, boolean fetchZoneData, 
		boolean fetchStoreData,	Set<Integer> itemCodeSet) throws GeneralException {

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveRetailCost(costDataMap, calendarId, chainId, fetchZoneData, fetchStoreData, values);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveRetailCost(costDataMap, calendarId, chainId, fetchZoneData, fetchStoreData, values);
		}

		return costDataMap;
	}	
	
	
	
	private void retrieveRetailCost(HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,
			int calendarId, int chainId, boolean fetchStoreData, List<String> priceAndStrategyZoneNos,
			List<Integer> priceZoneStores, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			String sql = GET_RETAIL_COST;

			String pzStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			sql = sql.replaceAll("%LOCATION_SUBQUERY%",
					getLocationSubQuery(fetchStoreData, priceAndStrategyZoneNos, pzStores));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			
			//logger.info("GET_RETAIL_COST:" + sql);
			statement = connection.prepareStatement(sql);
			
			int counter = 0;
			statement.setInt(++counter, chainId);
			statement.setInt(++counter, calendarId);
			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 3, values);
			statement.setFetchSize(2000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				populateCostToMapFromDB(resultSet, costDataMap);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_RETAIL_COST " + e);
			throw new GeneralException("Exception in retrieveRetailCost()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}


	private void retrieveRetailCost(HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,
			int calendarId, int chainId, boolean fetchZoneData, boolean fetchStoreData, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String sql = GET_RETAIL_COST_ALL;
			
			sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(fetchZoneData , fetchStoreData));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			statement = connection.prepareStatement(sql);

			int counter = 0;
			statement.setInt(++counter, chainId);
			statement.setInt(++counter, calendarId);

			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 3, values);

			statement.setFetchSize(2000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				populateCostToMapFromDB(resultSet, costDataMap);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_RETAIL_COST " + e);
			throw new GeneralException( "Exception in retrieveRetailCost()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	
	private String getLocationSubQuery(boolean fetchStoreData, List<String> priceAndStrategyZoneNos, String priceZoneStores) {
		StringBuffer subQuery = new StringBuffer();
		String strPriceAndStrategyZones = "";
		for (String psz : priceAndStrategyZoneNos) {
			strPriceAndStrategyZones = strPriceAndStrategyZones + ",'" + PrestoUtil.castZoneNumber(psz) + "'";
		}
		strPriceAndStrategyZones = strPriceAndStrategyZones.substring(1);
		
		if (fetchStoreData) {
			subQuery.append("(LEVEL_TYPE_ID = 2 AND LEVEL_ID IN ( SELECT CAST_DATA(COMP_STR_NO,"
					+ Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) + ")"
					+ " FROM COMPETITOR_STORE WHERE COMP_STR_ID IN ( " +
					"" + priceZoneStores + "))) ");
			subQuery.append("OR (LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (" + strPriceAndStrategyZones + ")) ");
		} else {
			//subQuery.append("(LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (" + strPriceAndStrategyZones + ")) ");
			subQuery.append("(LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (" + strPriceAndStrategyZones + ")) ");
		}
		subQuery.append("OR (LEVEL_TYPE_ID = 0) ");
		return subQuery.toString();
	}

	private String getLocationSubQuery(boolean fetchZoneData, boolean fetchStoreData) {
		StringBuffer subQuery = new StringBuffer();
		
		subQuery.append(" LEVEL_TYPE_ID = 0 ");

		if (fetchZoneData)
			subQuery.append(" OR LEVEL_TYPE_ID = 1");

		if (fetchStoreData)
			subQuery.append(" OR LEVEL_TYPE_ID = 2");
		
		return subQuery.toString();
	}	
	
	public RetailCostDTO findCostForStore(HashMap<RetailPriceCostKey, RetailCostDTO> costMap,
			RetailPriceCostKey storeKey, RetailPriceCostKey zoneKey, RetailPriceCostKey chainKey) {
		RetailCostDTO storeLevelCost = new RetailCostDTO();
		if (costMap.get(storeKey) != null) {
			storeLevelCost.copy(costMap.get(storeKey));
		} else if (costMap.get(zoneKey) != null) {
			storeLevelCost.copy(costMap.get(zoneKey));
		} else if (costMap.get(chainKey) != null){
			storeLevelCost.copy(costMap.get(chainKey));
		} else {
			storeLevelCost = null;
		}
		return storeLevelCost;
	}

	public RetailCostDTO findCostForZone(HashMap<RetailPriceCostKey, RetailCostDTO> costMap,
			RetailPriceCostKey zoneKey, RetailPriceCostKey chainKey) {
		RetailCostDTO zoneLevelCost = new RetailCostDTO();
		if (costMap.get(zoneKey) != null) {
			zoneLevelCost.copy(costMap.get(zoneKey));
		} else if (costMap.get(chainKey) != null){
			zoneLevelCost.copy(costMap.get(chainKey));
		}
		return zoneLevelCost;
	}

	/**
	 * output -> HashMap<ItemCode, HashMap<WeekStartDate, RetailCostDTO>>
	 * 
	 * @param connection
	 * @param chainId
	 * @param startWeekCalDTO
	 * @param noOfCostHistory
	 * @param allWeekCalendarDetails
	 * @param itemCodeList
	 * @param priceAndStrategyZoneNos
	 * @param priceZoneStores
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getCostHistory(Integer chainId,
			RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, Set<Integer> itemCodeList,
			List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores) throws GeneralException {

		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costHistoryMap = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();

		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails,
				startWeekCalDTO.getStartDate(), noOfCostHistory);

		if (itemCodeList.size() > 0) {
			// Get cost for each week
			for (RetailCalendarDTO curCalDTO : retailCalendarList) {
				// Get cost detail of item for a week for all locations
				HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = getRetailCost(
						curCalDTO.getCalendarId(), chainId, priceAndStrategyZoneNos, priceZoneStores, false,
						itemCodeList);
				populateCostMap(costDataMap, costHistoryMap, curCalDTO);
			}
		}

		return costHistoryMap;
	}
	
	/**
	 * Get the latest cost for all non-cached zone items
	 * @param itemCostHistory
	 * @param nonCachedItemCodeSet
	 * @param itemDataMap
	 * @param chainId
	 * @param zoneId
	 * @param startWeekCalDTO
	 * @param noOfCostHistory
	 * @param allWeekCalendarDetails
	 * @param currentCostItemCodeSet 
	 * @param futureCostItemSet 
	 * @throws GeneralException
	 */
	public void getLatestCostOfZoneItems(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<ItemKey, PRItemDTO> itemDataMap, Integer chainId, int zoneId, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {
		
		getLatestCost(itemCostHistory, nonCachedItemCodeSet, itemDataMap, chainId, zoneId, 0, startWeekCalDTO, noOfCostHistory,
				allWeekCalendarDetails);
	}

	/**
	 * Get the latest cost for all non-cached store items
	 * @param itemCostHistory
	 * @param nonCachedItemCodeSet
	 * @param itemStoreDataMap
	 * @param chainId
	 * @param zoneId
	 * @param startWeekCalDTO
	 * @param noOfCostHistory
	 * @param allWeekCalendarDetails
	 * @throws GeneralException
	 */
	public void getLatestCostOfStoreItems(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemStoreDataMap, Integer chainId, int zoneId, RetailCalendarDTO startWeekCalDTO,
			int noOfCostHistory, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		// Loop each stores
		for (Entry<Integer, HashMap<ItemKey, PRItemDTO>> storeItemMap : itemStoreDataMap.entrySet()) {
			getLatestCost(itemCostHistory, nonCachedItemCodeSet, storeItemMap.getValue(), chainId, zoneId, storeItemMap.getKey(), startWeekCalDTO,
					noOfCostHistory, allWeekCalendarDetails);
		}
	}
	
	private void getLatestCost(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<ItemKey, PRItemDTO> itemDataMap, Integer chainId, int zoneId, int storeId, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfCostHistory);
	

		for (Integer nonCachedItemCode : nonCachedItemCodeSet) {
			// go from latest week
			for (RetailCalendarDTO curCalDTO : retailCalendarList) {
				ItemKey itemKey = new ItemKey(nonCachedItemCode, PRConstants.NON_LIG_ITEM_INDICATOR);

				if (itemDataMap.get(itemKey) != null) {
					PRItemDTO itemDTO = itemDataMap.get(itemKey);

					RetailCostDTO retailCostDTO = getCostDTO(chainId, zoneId, storeId, itemDTO, itemCostHistory, curCalDTO);

					if (retailCostDTO != null) {
						populateCurrentCost(itemDTO, retailCostDTO);
						if(itemDTO.getSecondaryZones() != null && itemDTO.getSecondaryZones().size() > 0) {
							itemDTO.getSecondaryZones().forEach(secZone -> {
								RetailCostDTO secZoneRetailCostDTO = getCostDTO(chainId, secZone.getPriceZoneId(),
										storeId, itemDTO, itemCostHistory, curCalDTO);
								if (secZoneRetailCostDTO != null) {
									secZone.setListCost(
											Double.valueOf(secZoneRetailCostDTO.getListCostMinusLongTermAllowances()));
								}
							});
						}
						break;
					}
				}
			}
		}
		
	}
	
	public void getMinimumCostOfStoreItems(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemStoreDataMap, Integer chainId, int zoneId, RetailCalendarDTO startWeekCalDTO,
			int noOfCostHistory, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		// Loop each stores
		for (Entry<Integer, HashMap<ItemKey, PRItemDTO>> storeItemMap : itemStoreDataMap.entrySet()) {
			getMinimumCost(itemCostHistory, nonCachedItemCodeSet, storeItemMap.getValue(), chainId, zoneId, storeItemMap.getKey(), startWeekCalDTO,
					noOfCostHistory, allWeekCalendarDetails);
		}
	}
	
	private void getMinimumCost(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<ItemKey, PRItemDTO> itemDataMap, Integer chainId, int zoneId, int storeId, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfCostHistory);

		for (Integer nonCachedItemCode : nonCachedItemCodeSet) {
			
			ItemKey itemKey = new ItemKey(nonCachedItemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
			
			if (itemDataMap.get(itemKey) != null) {
				PRItemDTO itemDTO = itemDataMap.get(itemKey);
				float minDealCost = 0.0f;
			
				// check min cost week by week
				for (RetailCalendarDTO curCalDTO : retailCalendarList) {

					RetailCostDTO retailCostDTO = getCostDTO(chainId, zoneId, storeId, itemDTO, itemCostHistory, curCalDTO);
					
					if (retailCostDTO != null && retailCostDTO.getDealCost() > 0.0f) {
						if(minDealCost > 0.0f){
							if(minDealCost > retailCostDTO.getDealCost()){
								minDealCost = retailCostDTO.getDealCost();
							}
						} else {
							minDealCost = retailCostDTO.getDealCost();
						}
					}
				}
				itemDTO.setMinDealCost(Double.valueOf(minDealCost));
			}
		}
	}
	
	private RetailCostDTO getCostDTO(int chainId, int zoneId, int storeId, PRItemDTO itemDTO,
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costMap, RetailCalendarDTO calDTO) {

		RetailCostDTO costDTO = null;
		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, zoneId);
		RetailPriceCostKey storeKey = new RetailPriceCostKey(Constants.STORE_LEVEL_TYPE_ID, storeId);

		zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, itemDTO.getPriceZoneId());
		// logger.debug("Actual Zone Id of item:" + item.getPriceZoneId());
		if (costMap.get(itemDTO.getItemCode()) != null) {
			HashMap<String, List<RetailCostDTO>> allWeekCostMap = costMap.get(itemDTO.getItemCode());
			List<RetailCostDTO> costList = allWeekCostMap.get(calDTO.getStartDate());
			if (costList != null) {
				HashMap<RetailPriceCostKey, RetailCostDTO> tempCostKeyMap = new HashMap<RetailPriceCostKey, RetailCostDTO>();
				for (RetailCostDTO retailCostDTO : costList) {
					RetailPriceCostKey retailPriceCostKey = new RetailPriceCostKey(retailCostDTO.getLevelTypeId(),
							Integer.valueOf(retailCostDTO.getLevelId()));
					tempCostKeyMap.put(retailPriceCostKey, retailCostDTO);
				}

				if (storeId > 0) {
					costDTO = findCostForStore(tempCostKeyMap, storeKey, zoneKey, chainKey);
				} else {
					costDTO = findCostForZone(tempCostKeyMap, zoneKey, chainKey);
				}
			}
		}

		return costDTO;
	}
	
	private void populateCurrentCost(PRItemDTO itemDTO, RetailCostDTO zoneLevelCost) {
		// itemDTO.setListCost(Double.valueOf(zoneLevelCost.getListCost()));
		// 08/15/2018 PK::Changes for including long term allowances and long term scan back
		itemDTO.setListCost(Double.valueOf(zoneLevelCost.getListCostMinusLongTermAllowances()));
		itemDTO.setListCostEffDate(zoneLevelCost.getEffListCostDate());
		if (zoneLevelCost.getVipCost() > 0)
			itemDTO.setVipCost(Double.valueOf(zoneLevelCost.getVipCost()));
		if(zoneLevelCost.getDealCost() > 0) {
			//TODO:: since there is no future deal cost, latest deal cost is taken
			//as recommended week's deal cost
			itemDTO.setRecWeekDealCost(Double.valueOf(zoneLevelCost.getDealCost()));
		}
		
		//Set sale cost
		Double recWeekSaleCost = (itemDTO.getRecWeekDealCost() != null && itemDTO.getRecWeekDealCost() > 0) ? itemDTO.getRecWeekDealCost()
				: itemDTO.getListCost();
		itemDTO.setRecWeekSaleCost(recWeekSaleCost);
		itemDTO.setCwacCoreCost(zoneLevelCost.getCwacCoreCost());
		itemDTO.setNipoBaseCost(zoneLevelCost.getNipoBaseCost());
		itemDTO.setCwagBaseCost(zoneLevelCost.getCwagBaseCost());
	}

	
	public void getPreviousCostOfZoneItems(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<ItemKey, PRItemDTO> itemDataMap, Integer chainId, int zoneId, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, double costChangeThreshold) throws GeneralException {
		populatePreviousCost(itemCostHistory, nonCachedItemCodeSet, itemDataMap, chainId, zoneId, 0, startWeekCalDTO, noOfCostHistory,
				allWeekCalendarDetails, costChangeThreshold);
	}

	public void getPreviousCostOfStoreItems(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<Integer, HashMap<ItemKey, PRItemDTO>> itemStoreDataMap, Integer chainId, int zoneId, RetailCalendarDTO startWeekCalDTO,
			int noOfCostHistory, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, double costChangeThreshold) throws GeneralException {

		// Loop each stores
		for (Entry<Integer, HashMap<ItemKey, PRItemDTO>> storeItemMap : itemStoreDataMap.entrySet()) {
			populatePreviousCost(itemCostHistory, nonCachedItemCodeSet, storeItemMap.getValue(), chainId, zoneId, storeItemMap.getKey(),
					startWeekCalDTO, noOfCostHistory, allWeekCalendarDetails, costChangeThreshold);
		}
	}
	
	private void populatePreviousCost(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory, Set<Integer> nonCachedItemCodeSet,
			HashMap<ItemKey, PRItemDTO> itemDataMap, Integer chainId, int zoneId, int storeId, RetailCalendarDTO startWeekCalDTO, int noOfCostHistory,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, double costChangeThreshold) throws GeneralException {

		boolean isMarkAsReview = Boolean.parseBoolean(PropertyManager.getProperty("MARK_AS_REVIEW_IF_COST_CHANGE", "FALSE"));
		
		DecimalFormat df = new DecimalFormat("######.##");

		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, startWeekCalDTO.getStartDate(),
				noOfCostHistory);

		// loop all non-cached items
		for (Integer nonCachedItemCode : nonCachedItemCodeSet) {
			ItemKey itemKey = new ItemKey(nonCachedItemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
			PRItemDTO itemDTO = itemDataMap.get(itemKey);
			if (itemDTO != null) {
				boolean isPreviousCostSet = false;
				// loop from latest week
				for (RetailCalendarDTO curCalDTO : retailCalendarList) {

					RetailCostDTO retailCostDTO = getCostDTO(chainId, zoneId, storeId, itemDTO, itemCostHistory, curCalDTO);

					if (retailCostDTO != null) {

						double preListCost = Double.valueOf(retailCostDTO.getListCostMinusLongTermAllowances());
						double preVipCost = Double.valueOf(retailCostDTO.getVipCost());
						
						// Make sure there is current cost
						if (itemDTO.getListCost() != null) {
							//15,June,2021 if the future cost is present for an item then use that to find the cost Change
							double futureListCost= itemDTO.getFutureListCost()!=null &&itemDTO.getFutureListCost()>0 ?itemDTO.getFutureListCost():0;
							
							double costChange;
							if (futureListCost > 0)
								costChange = new Double(df.format(Math.abs(itemDTO.getListCost() - futureListCost)));
							else
								costChange = new Double(df.format(Math.abs(itemDTO.getListCost() - preListCost)));

							// 15th Mar 2017, previously previous cost was found based on reset date,
							// that triggered price change for every week considering there is cost increase
							// as reset date is not set.

							// now the previous cost considered, when the cost change in not reflected
							// in the price. for e.g. if there is a price change after the cost change
							// it will be assumed that the cost change is reflected

							if ((futureListCost!=0 && futureListCost != itemDTO.getListCost() && costChange > costChangeThreshold) 
									||(preListCost != itemDTO.getListCost() && costChange > costChangeThreshold)) {
								boolean isConsiderAsCostChange = false;
								
								// when there is no list cost effective date, consider the week 
								// in which the cost changed as cost effective date
								String listCostEffDate = (itemDTO.getListCostEffDate() != null ? itemDTO.getListCostEffDate() : curCalDTO.getStartDate());
								
								// when there is no reg retail effective date, but there is cost change
								if (itemDTO.getCurRegPriceEffDate() == null || (itemDTO.getCurRegPriceEffDate() == null && futureListCost>0)) {
									isConsiderAsCostChange = true;
								} else {
									Date curRegPriceEffDate = DateUtil.toDate(itemDTO.getCurRegPriceEffDate());
									if (futureListCost > 0) {
										if (itemDTO.getFutureCostEffDate() != null) {

											if (curRegPriceEffDate
													.equals(DateUtil.toDate(itemDTO.getFutureCostEffDate()))
													|| curRegPriceEffDate
															.after(DateUtil.toDate(itemDTO.getFutureCostEffDate()))) {
												isConsiderAsCostChange = false;
											} else if (curRegPriceEffDate
													.before(DateUtil.toDate(itemDTO.getFutureCostEffDate()))) {
												isConsiderAsCostChange = true;
											}
										}
									} else {
										
										// if reg effective date is equal or after cost effective date
										if (curRegPriceEffDate.equals(DateUtil.toDate(listCostEffDate))
												|| curRegPriceEffDate.after(DateUtil.toDate(listCostEffDate))) {
											isConsiderAsCostChange = false;
										} else if (curRegPriceEffDate.before(DateUtil.toDate(listCostEffDate))) {
											isConsiderAsCostChange = true;
										}
									}
								}

								if (isConsiderAsCostChange) {
									itemDTO.setPreListCost(preListCost);
									
									if (futureListCost > 0) {
										if (itemDTO.getListCost() < futureListCost) {
											itemDTO.setCostChgIndicator(-1);
										} else if (itemDTO.getListCost() > futureListCost) {
											itemDTO.setCostChgIndicator(+1);
										}

										if (itemDTO.getCostChgIndicator() != 0 && isMarkAsReview) {
											itemDTO.setIsMarkedForReview(String.valueOf(Constants.YES));
										}

									} else
									{
										// 19th May 2016, set mark for review, when there is cost change for CVS
										if (itemDTO.getListCost() < itemDTO.getPreListCost()) {
											itemDTO.setCostChgIndicator(-1);
										} else if (itemDTO.getListCost() > itemDTO.getPreListCost()) {
											itemDTO.setCostChgIndicator(+1);
										}

										if (itemDTO.getCostChgIndicator() != 0 && isMarkAsReview) {
											itemDTO.setIsMarkedForReview(String.valueOf(Constants.YES));
										}
										// Set previous vip cost
										if (itemDTO.getVipCost() != null && preVipCost > 0) {
											double vipCostChange = new Double(
													df.format(Math.abs(itemDTO.getVipCost() - preVipCost)));
											if (preVipCost != itemDTO.getVipCost()
													&& vipCostChange > costChangeThreshold) {

												itemDTO.setPreVipCost(preVipCost);

												if (itemDTO.getVipCost() < itemDTO.getPreVipCost()) {
													itemDTO.setVipCostChgIndicator(-1);
												} else if (itemDTO.getVipCost() > itemDTO.getPreVipCost()) {
													itemDTO.setVipCostChgIndicator(+1);
												}

												if (itemDTO.getVipCostChgIndicator() != 0 && isMarkAsReview) {
													itemDTO.setIsMarkedForReview(String.valueOf(Constants.YES));
												}
											}
										}
									}
									isPreviousCostSet = true;
									break;
								}
							}
						}
					}
				}
				// If there is no previous cost set, then stay with current cost
				if (!isPreviousCostSet) {
					itemDTO.setPreListCost(itemDTO.getListCost());
					itemDTO.setPreVipCost(itemDTO.getVipCost());
				}
			}
		}
	}
	
	/**
	 * Function to  popualte the future Cost if present
	 * @param itemDTO
	 * @param zoneLevelCost
	 */
	private void populateFutureCost(PRItemDTO itemDTO, RetailCostDTO zoneLevelCost) {

		itemDTO.setFutureListCost(Double.valueOf(zoneLevelCost.getListCostMinusLongTermAllowances()));
		itemDTO.setFutureCostEffDate(zoneLevelCost.getEffListCostDate());
		
	}

	/**
	 * 
	 * @param chainId
	 * @param calDTO
	 * @param costHistory
	 * @param allWeekCalendarDetails 
	 * @param nonCachedItemCodeSet
	 * @param priceAndStrategyZoneNos
	 * @param storeList
	 * @param itemCostMap
	 * @param weekendDate
	 * @return 
	 * @throws GeneralException 
	 */
	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getFutureCost(int chainId,
			RetailCalendarDTO startWeekCalDTO, int weeksTofetchFutureCost,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, Set<Integer> itemCodeList,
			List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, String weekendDate)
			throws GeneralException {

		List<RetailCalendarDTO> futureCalendarList = PRCommonUtil.getFutureCalendars(allWeekCalendarDetails,
				startWeekCalDTO.getStartDate(), weeksTofetchFutureCost);
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costHistoryMap = new HashMap<Integer, HashMap<String, List<RetailCostDTO>>>();

		if (itemCodeList.size() > 0) {
			// Get cost for each week
			for (RetailCalendarDTO futureCalDTO : futureCalendarList) {
				// Get cost detail of item for a week for all locations
				HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = getFutureCost(
						futureCalDTO.getCalendarId(), chainId, priceAndStrategyZoneNos, priceZoneStores, false,
						itemCodeList, weekendDate);
				populateCostMap(costDataMap, costHistoryMap, futureCalDTO);
			}
		}

		return costHistoryMap;
	}

	/**
	 * 
	 * @param calendarId
	 * @param chainId
	 * @param priceAndStrategyZoneNos
	 * @param priceZoneStores
	 * @param fetchStoreData
	 * @param itemCodeSet
	 * @param weekendDate
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> getFutureCost(int calendarId,
			int chainId, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, boolean fetchStoreData,
			Set<Integer> itemCodeSet, String weekendDate) throws GeneralException {

		HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveFutureCost(costDataMap, calendarId, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, values,weekendDate);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveFutureCost(costDataMap, calendarId, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, values,weekendDate);
		}

		return costDataMap;
	}

	/**
	 * 
	 * @param costDataMap
	 * @param calendarId
	 * @param chainId
	 * @param fetchStoreData
	 * @param priceAndStrategyZoneNos
	 * @param priceZoneStores
	 * @param values
	 * @param weekEndDate
	 * @throws GeneralException
	 */
	private void retrieveFutureCost(HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,
			int calendarId, int chainId, boolean fetchStoreData, List<String> priceAndStrategyZoneNos,
			List<Integer> priceZoneStores, Object[] values, String weekEndDate) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			String sql = GET_FUTURE_COST;
			
				String pzStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(fetchStoreData, priceAndStrategyZoneNos, pzStores));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			//logger.debug("GET_FUTURE_COST qry :" + sql);
			statement = connection.prepareStatement(sql);

			int counter = 0;
			statement.setInt(++counter, chainId);
			statement.setString(++counter,weekEndDate);
			statement.setInt(++counter, calendarId);
			PristineDBUtil.setValues(statement, ++counter, values);

			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				populateCostToMapFromDB(resultSet,costDataMap);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_FUTURE_COST:  " + e);
			throw new GeneralException( "Exception in retrieveFutureCost()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		
	}

 /**
  * 
  * @param resultSet
  * @param costDataMap
  * @throws SQLException
  */
	private void populateCostToMapFromDB(ResultSet resultSet,
			HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap) throws SQLException {
	
		int itemCode = resultSet.getInt("ITEM_CODE");
		int levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
		int levelId = resultSet.getInt("LEVEL_ID");
		RetailCostDTO retailCostDTO = new RetailCostDTO();
		retailCostDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
		retailCostDTO.setListCost(resultSet.getFloat("LIST_COST"));
		retailCostDTO.setFinalListCost(resultSet.getFloat("LIST_COST_2"));
		retailCostDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
		retailCostDTO.setVipCost(resultSet.getFloat("VIP_COST"));
		retailCostDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
		retailCostDTO.setDealStartDate(resultSet.getString("DEAL_START_DATE"));
		retailCostDTO.setDealEndDate(resultSet.getString("DEAL_END_DATE"));
		retailCostDTO.setLevelTypeId(levelTypeId);
		retailCostDTO.setLevelId(String.valueOf(levelId));
		retailCostDTO.setAllowanceAmount(resultSet.getFloat("ALLOWANCE_COST"));

		String longTermFlag = resultSet.getString("LONG_TERM_FLAG");
		if (longTermFlag != null && String.valueOf(Constants.YES).equalsIgnoreCase(longTermFlag)) {
			retailCostDTO.setLongTermFlag(longTermFlag);
		} else {
			retailCostDTO.setLongTermFlag(String.valueOf(Constants.NO));
		}

		retailCostDTO.setCwacCoreCost(resultSet.getDouble("CWAC_CORE_COST"));
		retailCostDTO.setNipoBaseCost(resultSet.getDouble("NIPO_BASE_COST"));
		retailCostDTO.setCwagBaseCost(resultSet.getDouble("CWAC_BASE_COST"));

		RetailPriceCostKey costKey = new RetailPriceCostKey(levelTypeId, levelId);
		HashMap<RetailPriceCostKey, RetailCostDTO> itemRec = null;
		if (costDataMap.get(itemCode) != null)
			itemRec = costDataMap.get(itemCode);
		else
			itemRec = new HashMap<RetailPriceCostKey, RetailCostDTO>();
		// Conisder most recent future cost
		if (itemRec.containsKey(costKey)) {
			RetailCostDTO costDTO = itemRec.get(costKey);
			if (DateUtil.stringToLocalDate(costDTO.getEffListCostDate(), Constants.APP_DATE_FORMAT).compareTo(
					DateUtil.stringToLocalDate(retailCostDTO.getEffListCostDate(), Constants.APP_DATE_FORMAT)) > 0) {
				itemRec.put(costKey, retailCostDTO);
			}
		} else {
			itemRec.put(costKey, retailCostDTO);
		}
		costDataMap.put(itemCode, itemRec);
		
	}
	
	/**
	 * 
	 * @param costDataMap
	 * @param costHistoryMap
	 * @param calDTO
	 */
	private void populateCostMap(HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costHistoryMap, RetailCalendarDTO calDTO) {
		// Go through each item
		for (Entry<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> itemCostData : costDataMap.entrySet()) {

			int itemCode = itemCostData.getKey();
			// Go through each location's cost
			for (Entry<RetailPriceCostKey, RetailCostDTO> costData : itemCostData.getValue().entrySet()) {
				RetailCostDTO retailcostDTO = costData.getValue();
				// Keep each week cost in the output map
				HashMap<String, List<RetailCostDTO>> locationCost = new HashMap<String, List<RetailCostDTO>>();
				if (costHistoryMap.get(itemCode) != null) {
					locationCost = costHistoryMap.get(itemCode);
				}
				List<RetailCostDTO> retailCostDTOs = new ArrayList<RetailCostDTO>();
				if (locationCost.get(calDTO.getStartDate()) != null) {
					retailCostDTOs = locationCost.get(calDTO.getStartDate());
				}
				retailCostDTOs.add(retailcostDTO);
				locationCost.put(calDTO.getStartDate(), retailCostDTOs);
				costHistoryMap.put(itemCode, locationCost);
			}
		}
	}

	public void getFutureCostOfZoneItems(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostMap,
			Set<Integer> nonCachedItemCodeSet, HashMap<ItemKey, PRItemDTO> itemDataMap, int chainId, int zoneId,
			RetailCalendarDTO startWeekCalDTO, int weeksTofetchFutureCost,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		getFutureCost(futureCostMap, nonCachedItemCodeSet, itemDataMap, chainId, zoneId, 0, startWeekCalDTO,
				weeksTofetchFutureCost, allWeekCalendarDetails);

	}

	private void getFutureCost(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostMap,
			Set<Integer> nonCachedItemCodeSet, HashMap<ItemKey, PRItemDTO> itemDataMap, int chainId, int zoneId,
			int storeId, RetailCalendarDTO startWeekCalDTO, int weeksTofetchFutureCost,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) throws GeneralException {

		List<RetailCalendarDTO> futureCalendarList = PRCommonUtil.getFutureCalendars(allWeekCalendarDetails,
				startWeekCalDTO.getStartDate(), weeksTofetchFutureCost);

		for (Integer nonCachedItemCode : nonCachedItemCodeSet) {
			// go from latest week
			for (RetailCalendarDTO futureCalDTO : futureCalendarList) {

				ItemKey itemKey = new ItemKey(nonCachedItemCode, PRConstants.NON_LIG_ITEM_INDICATOR);

				if (itemDataMap.get(itemKey) != null) {
					PRItemDTO itemDTO = itemDataMap.get(itemKey);

					RetailCostDTO retailCostDTO = getCostDTO(chainId, zoneId, storeId, itemDTO, futureCostMap,
							futureCalDTO);

					if (retailCostDTO != null) {
						populateFutureCost(itemDTO, retailCostDTO);
						if (itemDTO.getSecondaryZones() != null && itemDTO.getSecondaryZones().size() > 0) {
							itemDTO.getSecondaryZones().forEach(secZone -> {
								RetailCostDTO secZoneRetailCostDTO = getCostDTO(chainId, secZone.getPriceZoneId(),
										storeId, itemDTO, futureCostMap, futureCalDTO);
								if (secZoneRetailCostDTO != null) {
									secZone.setFutureListCost(
											Double.valueOf(secZoneRetailCostDTO.getListCostMinusLongTermAllowances()));
								}
							});
						}
						break;
					}
				}
			}
		}
	}
	
}
