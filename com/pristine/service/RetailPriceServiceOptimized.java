package com.pristine.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

//import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class RetailPriceServiceOptimized {
	private static Logger logger = Logger.getLogger("RetailPriceService");
	
	private Connection connection = null;
//	private static final String GET_RETAIL_PRICE = "SELECT ITEM_CODE, LEVEL_TYPE_ID, "
//			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO=LEVEL_ID AND COMP_CHAIN_ID = ?) "
//			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ZONE_NUM=LEVEL_ID) "
//			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, "
//			+ "REG_PRICE, REG_QTY, REG_M_PRICE, " + "TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, SALE_PRICE, SALE_QTY, SALE_M_PRICE "
//			+ "FROM SYNONYM_RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? " + "AND ITEM_CODE IN (%s) "
//			+ "AND (%LOCATION_SUBQUERY%)";
	
	private static final String GET_RETAIL_PRICE = "SELECT ITEM_CODE, LEVEL_TYPE_ID, "
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
			+ "REG_PRICE, REG_QTY, REG_M_PRICE, " + "TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, SALE_PRICE, SALE_QTY, SALE_M_PRICE, CORE_RETAIL "
			//+ "FROM SYNONYM_RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? " + "AND ITEM_CODE IN (%s) "
			+ "FROM SYN_RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? " + "AND ITEM_CODE IN (%s) "
			+ "AND (%LOCATION_SUBQUERY%)";
	
	private static final String GET_RETAIL_PRICE_MULTIPLE_WEEKS = "SELECT RPI.CALENDAR_ID, TO_CHAR(RC.START_DATE,'MM/dd/yyyy') AS WEEK_START_DATE, "
			+ " ITEM_CODE, LEVEL_TYPE_ID, "
			+ "(CASE WHEN LEVEL_TYPE_ID = 2 THEN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE "
			+ " CAST_DATA(COMP_STR_NO," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ " )= CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("STORE_NUMBER_LENGTH")) 
			+ ")"
			+ " AND COMP_CHAIN_ID = ?) "
			+ "WHEN LEVEL_TYPE_ID = 1 THEN (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE "
			+ "CAST_DATA(ZONE_NUM," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ " )= "
			+ " CAST_DATA(LEVEL_ID," + Integer.parseInt(PropertyManager.getProperty("ZONE_NUMBER_LENGTH")) 
			+ "))"
			+ "ELSE (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE COMP_CHAIN_ID = LEVEL_ID) END) LEVEL_ID, "
			+ "REG_PRICE, REG_QTY, REG_M_PRICE, " + "TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, SALE_PRICE, SALE_QTY, SALE_M_PRICE "
			//+ "FROM SYNONYM_RETAIL_PRICE_INFO RPI "
			+ "FROM SYN_RETAIL_PRICE_INFO RPI "
			+ "LEFT JOIN RETAIL_CALENDAR RC ON RPI.CALENDAR_ID = RC.CALENDAR_ID "
			+ "WHERE RPI.CALENDAR_ID IN (%CALENDAR_IDS%) " + "AND ITEM_CODE IN (%s) "
			+ "AND (%LOCATION_SUBQUERY%)";
	
	public RetailPriceServiceOptimized() {
		
	}
			
	public RetailPriceServiceOptimized(Connection connection){
		this.connection = connection;
	}
	
	/**
	 * Returns price info for input product and location combination 
	 * @throws GeneralException 
	 */
	public HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> getRetailPrice(int calendarId, int chainId,
			List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, boolean fetchStoreData,
			Set<Integer> itemCodeSet) throws GeneralException {
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceDataMap = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveRetailPrice(priceDataMap, calendarId, chainId, fetchStoreData, priceAndStrategyZoneNos,
						priceZoneStores, values);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveRetailPrice(priceDataMap, calendarId, chainId, fetchStoreData, priceAndStrategyZoneNos,
					priceZoneStores, values);
		}

		return priceDataMap;
	}
	
	public HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> getRetailPrice(HashSet<Integer> calendarIdToFetch, int chainId,
			List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, boolean fetchStoreData, Set<Integer> itemCodeSet)
			throws GeneralException {
		HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceDataMap = new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
		int limitcount = 0;
		Set<Integer> itemCodeSubset = new HashSet<Integer>();
		for (Integer itemCode : itemCodeSet) {
			itemCodeSubset.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeSubset.toArray();
				retrieveRetailPrice(priceDataMap, calendarIdToFetch, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, values);
				itemCodeSubset.clear();
			}
		}
		if (itemCodeSubset.size() > 0) {
			Object[] values = itemCodeSubset.toArray();
			retrieveRetailPrice(priceDataMap, calendarIdToFetch, chainId, fetchStoreData, priceAndStrategyZoneNos, priceZoneStores, values);
		}

		return priceDataMap;
	}
	
	private void retrieveRetailPrice(HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceDataMap,
			int calendarId, int chainId, boolean fetchStoreData, List<String> priceAndStrategyZoneNos,
			List<Integer> priceZoneStores, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		//PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		try {
			String sql = GET_RETAIL_PRICE;
			
			String pzStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			//List<String> pzStoreNumbers = pricingEngineDAO.getStoreNumber(connection, pzStores);
			//String storeNumbers = PRCommonUtil.getCommaSeperatedStringFromStrArray(pzStoreNumbers);
			//String stZones = PRCommonUtil.getCommaSeperatedStringFromStrArray(priceAndStrategyZoneNos);
			
			//sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(fetchStoreData, stZones, pzStores));
			sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(fetchStoreData, priceAndStrategyZoneNos, pzStores));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			
//			logger.debug("GET_RETAIL_PRICE:" + sql);
			statement = connection.prepareStatement(sql);

			int counter = 0;
			statement.setInt(++counter, chainId);
			statement.setInt(++counter, calendarId);

			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 3, values);

//			if (fetchStoreData) {
//				statement.setInt(++counter, priceZoneId);
//				statement.setInt(++counter, priceZoneId);
//			} else {
//				statement.setInt(++counter, priceZoneId);
//			}
			statement.setFetchSize(2000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_CODE");
				int levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
				int levelId = resultSet.getInt("LEVEL_ID");
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
				retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
				retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
				retailPriceDTO.setRegEffectiveDate(resultSet.getString("EFF_REG_START_DATE"));
				retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
				retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
				retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
				retailPriceDTO.setCoreRetail(resultSet.getDouble("CORE_RETAIL"));
				RetailPriceCostKey costKey = new RetailPriceCostKey(levelTypeId, levelId);
				HashMap<RetailPriceCostKey, RetailPriceDTO> itemRec = null;
				if (priceDataMap.get(itemCode) != null)
					itemRec = priceDataMap.get(itemCode);
				else
					itemRec = new HashMap<RetailPriceCostKey, RetailPriceDTO>();
				itemRec.put(costKey, retailPriceDTO);
				priceDataMap.put(itemCode, itemRec);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_RETAIL_PRICE " + e);
			throw new GeneralException( "Exception in retrieveRetailPrice()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	private void retrieveRetailPrice(HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceDataMap, HashSet<Integer> calendarIdToFetch,
			int chainId, boolean fetchStoreData, List<String> priceAndStrategyZoneNos, List<Integer> priceZoneStores, Object... values)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			String sql = GET_RETAIL_PRICE_MULTIPLE_WEEKS;

			String pzStores = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			sql = sql.replaceAll("%LOCATION_SUBQUERY%", getLocationSubQuery(false, priceAndStrategyZoneNos, pzStores));
			sql = sql.replaceAll("%CALENDAR_IDS%", PRCommonUtil.getCommaSeperatedStringFromIntSet(calendarIdToFetch));
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));

			List<Integer> items = new ArrayList<>();
			for (int i = 0; i < values.length; i++) {
				items.add((Integer)values[i]);
			}
			
//			logger.debug("Item codes: " + PRCommonUtil.getCommaSeperatedStringFromIntArray(items));
			
//			logger.debug("GET_RETAIL_PRICE:" + sql);
			statement = connection.prepareStatement(sql);

			int counter = 0;
			statement.setInt(++counter, chainId);
			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 2, values);

			statement.setFetchSize(50000);
			
			
			resultSet = statement.executeQuery();

//			logger.debug("Price data retrieved" + resultSet.getFetchSize());
			
			while (resultSet.next()) {
				
				int itemCode = resultSet.getInt("ITEM_CODE");
				RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
				String weekStartDate = resultSet.getString("WEEK_START_DATE");
				retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
				retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
				retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
				retailPriceDTO.setRegEffectiveDate(resultSet.getString("EFF_REG_START_DATE"));
				retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
				retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
				retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
				retailPriceDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
				retailPriceDTO.setLevelId(String.valueOf(resultSet.getInt("LEVEL_ID")));
				
				HashMap<String, List<RetailPriceDTO>> itemWeekPrice = new HashMap<String, List<RetailPriceDTO>>();
				if (priceDataMap.get(itemCode) != null) {
					itemWeekPrice = priceDataMap.get(itemCode);
				}
				
				List<RetailPriceDTO> retailPriceDTOs = new ArrayList<RetailPriceDTO>();
				if (itemWeekPrice.get(weekStartDate) != null) {
					retailPriceDTOs = itemWeekPrice.get(weekStartDate);
				}  
				retailPriceDTOs.add(retailPriceDTO);
				itemWeekPrice.put(weekStartDate, retailPriceDTOs);
				
				priceDataMap.put(itemCode, itemWeekPrice);
			}
		} catch (SQLException e) {
			logger.error("Error while executing retrieveRetailPrice " + e);
			throw new GeneralException("Exception in retrieveRetailPrice()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Returns location sub query for input location level and location
	 */
//	private String getLocationSubQuery(boolean fetchStoreData, String priceAndStrategyZones, String priceZoneStores) {
//		StringBuffer subQuery = new StringBuffer();
////		if (fetchStoreData) {
////			subQuery.append("(LEVEL_TYPE_ID = 2 AND LEVEL_ID IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?)) ");
////			subQuery.append("OR (LEVEL_TYPE_ID = 1 AND LEVEL_ID = (SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?)) ");
////		} else {
////			subQuery.append("(LEVEL_TYPE_ID = 1 AND LEVEL_ID = (SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?)) ");
////		}
//		if (fetchStoreData) {
//			subQuery.append("(LEVEL_TYPE_ID = 2 AND LEVEL_ID IN ( SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_STR_ID IN ( " +
//					"" + priceZoneStores + "))) ");
//			subQuery.append("OR (LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (" + priceAndStrategyZones + ")) ");
//		} else {
//			subQuery.append("(LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (" + priceAndStrategyZones + ")) ");
//		}
//		subQuery.append("OR (LEVEL_TYPE_ID = 0) ");
//		return subQuery.toString();
//	}
	
	private String getLocationSubQuery(boolean fetchStoreData, List<String> priceAndStrategyZoneNos, String priceZoneStores) {
		StringBuffer subQuery = new StringBuffer();
		String strPriceAndStrategyZones = "";
		for (String psz : priceAndStrategyZoneNos) {
			strPriceAndStrategyZones = strPriceAndStrategyZones + ",'" + PrestoUtil.castZoneNumber(psz) + "'";
		}
		strPriceAndStrategyZones = strPriceAndStrategyZones.substring(1);
//		logger.debug("*************Zones: " + strPriceAndStrategyZones);
		if (fetchStoreData) {
			subQuery.append("(LEVEL_TYPE_ID = 2 AND LEVEL_ID IN (SELECT CAST_DATA(COMP_STR_NO,"
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
	
	public RetailPriceDTO findPriceForStore(HashMap<RetailPriceCostKey, RetailPriceDTO> priceMap,
			RetailPriceCostKey storeKey, RetailPriceCostKey zoneKey, RetailPriceCostKey chainKey) {
		RetailPriceDTO storeLevelPrice = new RetailPriceDTO();
		if (priceMap.get(storeKey) != null) {
			storeLevelPrice.copy(priceMap.get(storeKey));
		} else if (priceMap.get(zoneKey) != null) {
			storeLevelPrice.copy(priceMap.get(zoneKey));
		} else if(priceMap.get(chainKey) != null){
			storeLevelPrice.copy(priceMap.get(chainKey));
		} else{ 
		}
		return storeLevelPrice;
	}

	public RetailPriceDTO findPriceForZone(HashMap<RetailPriceCostKey, RetailPriceDTO> priceMap,
			RetailPriceCostKey zoneKey, RetailPriceCostKey chainKey) {
		RetailPriceDTO zoneLevelPrice = new RetailPriceDTO();
		if (priceMap.get(zoneKey) != null) {
//			logger.debug("findPriceForZone :: Zone level price : zoneKey  : " + zoneKey.levelId);
			zoneLevelPrice.copy(priceMap.get(zoneKey));
		} else if (priceMap.get(chainKey) != null){
//			logger.debug("findPriceForZone :: Chain level price : chainKey  : " + chainKey.levelId);
			zoneLevelPrice.copy(priceMap.get(chainKey));
		}
		else {
//			logger.debug("findPriceForZone :: Price not found for this item ");
		}
		return zoneLevelPrice;
	}
	
}
