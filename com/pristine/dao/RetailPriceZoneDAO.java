package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class RetailPriceZoneDAO {
	static Logger logger = Logger.getLogger("RetailPriceZoneDAO");
	
	public static final String GET_RETAIL_PRICE_ZONE = "SELECT PRICE_ZONE_ID, PRIMARY_COMP_STR_ID, SECONDARY_COMP_STR_ID_1, SECONDARY_COMP_STR_ID_2, SECONDARY_COMP_STR_ID_3,GLOBAL_ZONE,ZONE_TYPE " +
													   "FROM RETAIL_PRICE_ZONE WHERE ZONE_NUM = ?";
	
	public static final String GET_PRICE_INDEX_LOCATION = "SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRIMARY_COMP_STR_ID, "
														+ " SECONDARY_COMP_STR_ID_1, SECONDARY_COMP_STR_ID_2, "
														+ " SECONDARY_COMP_STR_ID_3 FROM PRICE_INDEX_LOCATION WHERE "
														+ " LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?" ;
			   

	public static final String GET_STORES_IN_ZONE = "SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ?";
	
	public static final String GET_STORES_IN_GLOBAL_ZONE = "SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.PRICE_ZONE_ID_3 = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ?";
	
	public static final String GET_STORES_FOR_LOCATION = "SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS "
												  + " WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') ";
	
	public static final String GET_STORE_IDS_IN_ZONE = "SELECT CS.COMP_STR_ID FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ? AND CS.ACTIVE_INDICATOR='Y' AND CS.CLOSE_DATE IS NULL";
	//Added condition for checking active indicator and store close date  for FF on 10/11/2022
	public static final String GET_STORES_FOR_GLOBAL_ZONE = "SELECT CS.COMP_STR_ID FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.PRICE_ZONE_ID_3 = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ? AND CS.ACTIVE_INDICATOR='Y' AND CS.CLOSE_DATE IS NULL";
	
	public static final String GET_STORE_ZONE_MAPPING = "SELECT CS.COMP_STR_NO, RPZ.PRICE_ZONE_ID FROM COMPETITOR_STORE CS, "
			+ "RETAIL_PRICE_ZONE RPZ WHERE "
			+ " CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID AND CS.COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE "
			+ "PRESTO_SUBSCRIBER = 'Y') AND RPZ.ACTIVE_INDICATOR = 'Y' AND CS.ACTIVE_INDICATOR = 'Y'";
	
	public static final String GET_DISTINCT_LIST = "SELECT DISTINCT(PRICE_CHECK_LIST_ID) FROM LOCATION_COMPETITOR_MAP";
	
	public static final String GET_KVI_LIST_FOR_ZONE = "SELECT PRICE_CHECK_LIST_ID FROM LOCATION_COMPETITOR_MAP WHERE BASE_LOCATION_LEVEL_ID = 6 AND BASE_LOCATION_ID = ?";
	
	public static final String GET_ZONES_UNDER_SUBSCRIBER = "SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') ORDER BY PRICE_ZONE_ID";
	
	public static final String GET_PRIMARY_COMPETITOR = "SELECT PRIMARY_COMP_STR_ID FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?";
	
	public static final String GET_ZONE_DIVISION_MAP = "SELECT PRICE_ZONE_ID, DIVISION_ID FROM ( " + 
														" SELECT PRICE_ZONE_ID, DIVISION_ID, ROW_NUMBER() OVER (PARTITION BY PRICE_ZONE_ID ORDER BY COUNT(*) DESC) ROW_NUMBER " + 
														" FROM COMPETITOR_STORE " + 
														" WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') " +
														" %ZONE_ID_CONDITION% " +
														" GROUP BY PRICE_ZONE_ID, DIVISION_ID " + 
														" ) WHERE ROW_NUMBER = 1 " + 
														" ORDER BY PRICE_ZONE_ID";
	
	public static final String GET_DIVISION_FOR_ZONE = "SELECT PRICE_ZONE_ID, DIVISION_ID FROM ( " + 
														" SELECT PRICE_ZONE_ID, DIVISION_ID, ROW_NUMBER() OVER (PARTITION BY PRICE_ZONE_ID ORDER BY COUNT(*) DESC) ROW_NUMBER " + 
														" FROM COMPETITOR_STORE " + 
														" WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') " +
														" AND PRICE_ZONE_ID = ? " + 
														" GROUP BY PRICE_ZONE_ID, DIVISION_ID " + 
														" ) WHERE ROW_NUMBER = 1 ";
	
	public static final String GET_DIVISION_FOR_ZONE_FROM_LOC_MAP = "SELECT DISTINCT(DIVISION_ID) AS DIVISION_ID FROM PR_PRODUCT_LOCATION_MAPPING PLM, "
			+ " PR_PRODUCT_LOCATION_STORE PLS, COMPETITOR_STORE CS WHERE PRODUCT_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? "
			+ " AND CS.COMP_STR_ID = PLS.STORE_ID AND PLM.PRODUCT_LOCATION_MAPPING_ID = PLS.PRODUCT_LOCATION_MAPPING_ID";
	
	public static final String GET_ZONE_ID_MAP = "SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE";
	
	
	private static final String GET_ZONE_NUMBER_FOR_STORE_ID = "SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID IN "
															 + "	(SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = ?)";
	
	
	private static final String GET_ZONE_NUMBER_FROM_RETAIL_PRICE_ZONE = "SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE";
	
	private static final String INSERT_INTO_RETAIL_PRICE_ZONE = "INSERT INTO RETAIL_PRICE_ZONE (PRICE_ZONE_ID, NAME, ZONE_NUM, ZONE_TYPE, ACTIVE_INDICATOR) "
			+ "VALUES(PRICE_ZONE_ID_SEQ.NEXTVAL,?,?,?,'Y')";
	
	private static final String UPDATE_RETAIL_PRICE_ZONE_ACTIVE_IND = "UPDATE RETAIL_PRICE_ZONE SET ACTIVE_INDICATOR ='Y', ZONE_TYPE = ?, NAME = ? WHERE ZONE_NUM = ? ";
	
	
	public static final String ZONE_STORE_MAPPING = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_ID FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ " + 
													"WHERE CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID"; 
	
	public static final String GET_STORES_IN_ZONE_FOR_AHOLD = "SELECT CS.COMP_STR_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.PRICE_ZONE_ID_2 = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID = ?";
	
	public static final String GET_STORES_IN_ZONE_FROM_PLM = "SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT DISTINCT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN (SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE LOCATION_ID = ? ))";
	
	public static final String GET_RETAIL_PRICE_ZONE_FOR_AHOLD = "SELECT PRICE_ZONE_ID, PRIMARY_COMP_STR_ID, SECONDARY_COMP_STR_ID_1, SECONDARY_COMP_STR_ID_2, SECONDARY_COMP_STR_ID_3 " +
			   "FROM RETAIL_PRICE_ZONE WHERE ZONE_NUM = ?";
	
	public static final String GET_IS_GLOBAL_ZONE = "SELECT GLOBAL_ZONE FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID=?";

	public static final String GET_PRICE_TEST_ZONES = "SELECT PRICE_ZONE_ID,ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE ZONE_TYPE='T'";
	
	public static final String CHECK_AND_GET_CHILD_ZONES = "SELECT PRICE_ZONE_ID,  RPZ.ZONE_NUM, PRIMARY_COMP_STR_ID, SECONDARY_COMP_STR_ID_1, SECONDARY_COMP_STR_ID_2, SECONDARY_COMP_STR_ID_3,GLOBAL_ZONE,ZONE_TYPE "
														 + " FROM RETAIL_PRICE_ZONE RPZ "
														 + "WHERE RPZ.PRICE_ZONE_ID IN ( "
														 + "SELECT DISTINCT PLM.LOCATION_ID FROM PR_PRODUCT_LOCATION_MAPPING PLM "
														 + "LEFT JOIN PR_PRODUCT_LOCATION_STORE PLS ON PLM.PRODUCT_LOCATION_MAPPING_ID = PLS.PRODUCT_LOCATION_MAPPING_ID "
														 + "WHERE PLS.STORE_ID IN ( "
														 + "SELECT DISTINCT COMP_STR_ID FROM COMPETITOR_STORE "
														 + "WHERE ACTIVE_INDICATOR = 'Y' AND PRICE_ZONE_ID = ? AND COMP_CHAIN_ID = ? ) AND PLM.LOCATION_LEVEL_ID = ? ) "
														 + "AND RPZ.ACTIVE_INDICATOR = 'Y' AND RPZ.ZONE_TYPE IN ('W','V') AND GLOBAL_ZONE = 'N'";
	public static final String GET_ACTIVE_ZONES = "SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ACTIVE_INDICATOR='Y'";
	
	
	
	public PriceZoneDTO getRetailPriceZone(Connection conn, String zoneNum) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    PriceZoneDTO priceZoneDTO = null;
		try{
			statement = conn.prepareStatement(GET_RETAIL_PRICE_ZONE);
	        statement.setString(1, zoneNum);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	priceZoneDTO = new PriceZoneDTO();
	        	priceZoneDTO.setPriceZoneId(resultSet.getInt("PRICE_ZONE_ID"));
	        	priceZoneDTO.setPrimaryCompetitor(resultSet.getInt("PRIMARY_COMP_STR_ID"));
	        	priceZoneDTO.setSecComp1(resultSet.getInt("SECONDARY_COMP_STR_ID_1"));
	        	priceZoneDTO.setSecComp2(resultSet.getInt("SECONDARY_COMP_STR_ID_2"));
	        	priceZoneDTO.setSecComp3(resultSet.getInt("SECONDARY_COMP_STR_ID_3"));
				if (resultSet.getString("GLOBAL_ZONE").equals("Y")) {
					priceZoneDTO.setGlobalZone(true);
				} else
					priceZoneDTO.setGlobalZone(false);
				
				if (resultSet.getString("ZONE_TYPE").equals("T")) {
					priceZoneDTO.setTestZone(true);
				} else
					priceZoneDTO.setTestZone(false);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_PRICE_ZONE");
			throw new GeneralException("Error while executing GET_RETAIL_PRICE_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceZoneDTO;
	}
	
	/**
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @return Object that contains Primary, sec competitors for given location
	 * @throws GeneralException
	 */
	public PriceZoneDTO getPriceIndexLocation(Connection conn, int locationLevelId,
			int locationId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    PriceZoneDTO priceZoneDTO = null;
		try{
			statement = conn.prepareStatement(GET_PRICE_INDEX_LOCATION);
	        statement.setInt(1, locationLevelId);
	        statement.setInt(2, locationId);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	priceZoneDTO = new PriceZoneDTO();
	        	priceZoneDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
	        	priceZoneDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
	        	priceZoneDTO.setPrimaryCompetitor(resultSet.getInt("PRIMARY_COMP_STR_ID"));
	        	priceZoneDTO.setSecComp1(resultSet.getInt("SECONDARY_COMP_STR_ID_1"));
	        	priceZoneDTO.setSecComp2(resultSet.getInt("SECONDARY_COMP_STR_ID_2"));
	        	priceZoneDTO.setSecComp3(resultSet.getInt("SECONDARY_COMP_STR_ID_3"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PRICE_INDEX_LOCATION");
			throw new GeneralException("Error while executing GET_PRICE_INDEX_LOCATION", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceZoneDTO;
	}
	
	public List<String> getStoresInZone(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<String> storeList = new ArrayList<String>();
		try{
			statement = conn.prepareStatement(GET_STORES_IN_ZONE);
	        statement.setInt(1, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getString("COMP_STR_NO"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORES_IN_ZONE");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
	
	/**
	 * 
	 * @param conn
	 * @param chainId
	 * @return LIST of stores for given chain id
	 * @throws GeneralException
	 */
	public List<String> getStoresForLocation(Connection conn, int locationLevelId, int locationId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<String> storeList = new ArrayList<String>();
		try{
			StringBuilder sb = new StringBuilder();
			sb.append(GET_STORES_FOR_LOCATION);
			if(locationLevelId == Constants.DIVISION_LEVEL_ID){
				sb.append(" AND DIVISION_ID = " + locationId);
			}else if(locationLevelId == Constants.REGION_LEVEL_ID){
				sb.append(" AND REGION_ID = " + locationId);
			}else if(locationLevelId == Constants.DISTRICT_LEVEL_ID){
				sb.append(" AND DISTRICT_ID = " + locationId);
			}
			statement = conn.prepareStatement(sb.toString());
	        //statement.setInt(1, locationId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getString("COMP_STR_NO"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORES_IN_ZONE");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
	
	public List<Integer> getStoreIdsInZone(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<Integer> storeList = new ArrayList<Integer>();
		try{
			statement = conn.prepareStatement(GET_STORE_IDS_IN_ZONE);
	        statement.setInt(1, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getInt("COMP_STR_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORE_IDS_IN_ZONE");
			throw new GeneralException("Error while executing GET_STORE_IDS_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
	
	public HashMap<String, Integer> getStoreZoneMapping(Connection conn) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    HashMap<String, Integer> storeZoneMap = new HashMap<String, Integer>();
		try{
			statement = conn.prepareStatement(GET_STORE_ZONE_MAPPING);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeZoneMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("PRICE_ZONE_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORE_ZONE_MAPPING");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeZoneMap;
	}
	/**
	 * 
	 * @param conn
	 * @param priceZoneId
	 * @return
	 * @throws GeneralException
	 */
	public PriceZoneDTO getPriceZoneInfo(Connection conn, int priceZoneId) throws GeneralException{
		String sql = "SELECT ZONE_NUM, NAME FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		PriceZoneDTO pzDTO = null;
		try{
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, priceZoneId);
			rs = stmt.executeQuery();
			if(rs.next()){
				pzDTO = new PriceZoneDTO();
				pzDTO.setPriceZoneNum(rs.getString("ZONE_NUM"));
				pzDTO.setPriceZoneName(rs.getString("NAME"));
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving price zone info map - " + ex.getMessage());
			throw new GeneralException("Error when retrieving price zone info map - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return pzDTO;
	}
	
	public int getKviListForZone(Connection conn, int priceZoneId){
		int priceCheckListId = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_DISTINCT_LIST);
			rs = stmt.executeQuery();
			int count = 0;
			while(rs.next()){
				priceCheckListId = rs.getInt("PRICE_CHECK_LIST_ID");
				count++;
			}
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
			if(count > 1){
				stmt = conn.prepareStatement(GET_KVI_LIST_FOR_ZONE);
				stmt.setInt(1, priceZoneId);
				rs = stmt.executeQuery();
				if(rs.next()){
					priceCheckListId = rs.getInt("PRICE_CHECK_LIST_ID");
				}
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving price check list id - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return priceCheckListId;
	}
	
	/**
	 * Returns list of price_zone_id under presto subscriber
	 * @param conn	Connection
	 * @return
	 */
	public ArrayList<Integer> getZonesUnderSubscriber(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> locationIdList = new ArrayList<Integer>();
		try{
			stmt = conn.prepareStatement(GET_ZONES_UNDER_SUBSCRIBER);
			rs = stmt.executeQuery();
			while(rs.next()){
				locationIdList.add(rs.getInt("PRICE_ZONE_ID"));
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving zones under presto subscriber - " + ex.getMessage());
			throw new GeneralException("Error when retrieving zones under presto subscriber - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.info("Number of zones - " + locationIdList.size());
		return locationIdList;
	}
	
	/**
	 * Returns primary competitor for the given price_zone_id
	 * @param conn	Connection
	 * @return
	 */
	public int getPrimaryCompetitor(Connection conn, int locationId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int primaryCompId = 0;
		try{
			stmt = conn.prepareStatement(GET_PRIMARY_COMPETITOR);
			stmt.setInt(1, locationId);
			rs = stmt.executeQuery();
			if(rs.next()){
				primaryCompId = rs.getInt("PRIMARY_COMP_STR_ID");
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving primary comp str id - " + ex.getMessage());
			throw new GeneralException("Error when retrieving primary comp str id - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return primaryCompId;
	}
	
	/**
	 * Returns a map that has price zone id as key and division id as value
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Integer> getZoneDivisionMap(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, Integer> zoneDivisionMap = new HashMap<Integer, Integer>();
		try{
			String sql = GET_ZONE_DIVISION_MAP;
			if(priceZoneId > 0)
				sql = sql.replaceAll("%ZONE_ID_CONDITION%", "AND PRICE_ZONE_ID = ?");
			else
				sql = sql.replaceAll("%ZONE_ID_CONDITION%", "");
			stmt = conn.prepareStatement(sql);
			if(priceZoneId > 0)
				stmt.setInt(1, priceZoneId);
			
			rs = stmt.executeQuery();
			while(rs.next()){
				zoneDivisionMap.put(rs.getInt("PRICE_ZONE_ID"), rs.getInt("DIVISION_ID"));
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving zone to division mapping - " + ex.getMessage());
			throw new GeneralException("Error when retrieving zone to division mapping - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneDivisionMap;
	}
	
	/**
	 * Returns division id for the input zone
	 * @param conn
	 * @param priceZoneId
	 * @return
	 * @throws GeneralException
	 */
	public int getDivisionIdForZone(Connection conn, int priceZoneId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int divisionId = 0;
		try {
			boolean takeDivisionIdFromLocMap = Boolean.parseBoolean(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
			if (takeDivisionIdFromLocMap) {
				int defaultProdId = Integer.parseInt(PropertyManager.getProperty("DEFAULT_PRODUCT_ID_IN_PROD_LOC_MAPPING", "-1"));
				stmt = conn.prepareStatement(GET_DIVISION_FOR_ZONE_FROM_LOC_MAP);
				stmt.setInt(1, defaultProdId);
				stmt.setInt(2, Constants.ZONE_LEVEL_ID);
				stmt.setInt(3, priceZoneId);
			} else {
				stmt = conn.prepareStatement(GET_DIVISION_FOR_ZONE);
				stmt.setInt(1, priceZoneId);
			}

			rs = stmt.executeQuery();
			if (rs.next()) {
				divisionId = rs.getInt("DIVISION_ID");
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving division id for zone - " + ex.getMessage());
			throw new GeneralException("Error when retrieving division id for zone - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return divisionId;
	}
	
	
	public HashMap<String, Integer> getZoneIdMap(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try{
			stmt = conn.prepareStatement(GET_ZONE_ID_MAP);
			rs = stmt.executeQuery();
			while(rs.next()){
				zoneIdMap.put(PrestoUtil.castZoneNumber(rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		}catch(SQLException ex){
			logger.error("Error when retrieving zone id map - " + ex.getMessage());
			throw new GeneralException("Error when retrieving zone id map - " + ex.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}
	
	/**
	 * Gives zone number for given store
	 * @param conn
	 * @param compStrId
	 * @return zone number
	 * @throws GeneralException
	 */
	
	public PriceZoneDTO getZoneDetailForStore(Connection conn, int compStrId) throws GeneralException{
		PriceZoneDTO zoneInfo = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_ZONE_NUMBER_FOR_STORE_ID);
			int counter = 0;
			statement.setInt(++counter, compStrId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				zoneInfo = new PriceZoneDTO();
				zoneInfo.setPriceZoneNum( resultSet.getString("ZONE_NUM"));
				zoneInfo.setPriceZoneId(resultSet.getInt("PRICE_ZONE_ID"));
			}
		}
		catch(SQLException sqlE){
			logger.error("Error while getting zone number for store id", sqlE);
			throw new GeneralException("Error while getting zone number for store id", sqlE);
		}
		return zoneInfo;
	}
	/**
	 * Get zone number from retail price zone
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashSet<String> getZoneNumFromRetailPriceZone(Connection conn) throws GeneralException{
		HashSet<String> zoneNumList = new HashSet<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_ZONE_NUMBER_FROM_RETAIL_PRICE_ZONE);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				zoneNumList.add(resultSet.getString("ZONE_NUM"));
			}
		}
		catch(SQLException sqlE){
			logger.error("Error while getting zone number", sqlE);
			throw new GeneralException("Error while getting zone number", sqlE);
		}
		return zoneNumList;
		
	}
	public int insertintoRetialPriceZone(Connection conn, List<ZoneDTO> insertZoneNumList) throws GeneralException {

		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;
		try {
			statement = conn.prepareStatement(INSERT_INTO_RETAIL_PRICE_ZONE);

			for(ZoneDTO zoneDTO: insertZoneNumList) {
				
					statement.setString(1, zoneDTO.getMktAreaDscr());
					statement.setString(2, zoneDTO.getActualZoneNum());
					statement.setString(3, zoneDTO.getZoneType());
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						// logger.debug("Total count of records processed: " +
						// itemNoInBatch);
						int[] count = statement.executeBatch();
						totalInsertCnt = totalInsertCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
		} catch (Exception e) {
			logger.error("Error in insertintoRetialPriceZone()  - " + e.toString());
			throw new GeneralException("Error in insertintoRetialPriceZone()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalInsertCnt;
	}
	
	public int updateRPZActiveIndicator(Connection conn, List<ZoneDTO> updateZoneNumList) throws GeneralException {
		PreparedStatement statement = null;
		int totalInsertCnt = 0;
		try {
			String sql = UPDATE_RETAIL_PRICE_ZONE_ACTIVE_IND;
			statement = conn.prepareStatement(sql);
			for (ZoneDTO zoneDTO : updateZoneNumList) {
				int counter = 0;
				statement.setString(++counter, zoneDTO.getZoneType());
				statement.setString(++counter, zoneDTO.getMktAreaDscr());
				statement.setString(++counter, zoneDTO.getActualZoneNum());
				statement.addBatch();
			}
			int[] count = statement.executeBatch();
			totalInsertCnt = totalInsertCnt + count.length;
			statement.clearBatch();
		} catch (SQLException e) {
			logger.error("Error while executing UPDATE_LIKE_ITEM_ID");
			throw new GeneralException("Error while executing UPDATE_LIKE_ITEM_ID " + e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalInsertCnt;
	}
	
	public void updateActiveIndicatorFlag(Connection conn) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE RETAIL_PRICE_ZONE SET ACTIVE_INDICATOR ='N'");
		int count = PristineDBUtil.executeUpdate(conn, sb, "Update RETAIL PRICE ZONE");
		logger.info("No of ROWS updated " + count);
	}
	
	
	public HashMap<String, List<Integer>> getZoneStoreMap(Connection conn) throws GeneralException{
		HashMap<String, List<Integer>> zoneStoreMap = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(ZONE_STORE_MAPPING);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				
				String zoneNum = resultSet.getString("ZONE_NUM");
				int storeId = resultSet.getInt("COMP_STR_ID");
				if(zoneStoreMap.containsKey(zoneNum)){
					List<Integer> stores = zoneStoreMap.get(zoneNum);
					stores.add(storeId);
					zoneStoreMap.put(zoneNum, stores);
				}else{
					List<Integer> stores = new ArrayList<>();
					stores.add(storeId);
					zoneStoreMap.put(zoneNum, stores);
				}
			}
		}catch(SQLException sqlE){
			logger.error("Error while executing ZONE_STORE_MAPPING query");
			throw new GeneralException("Error while executing ZONE_STORE_MAPPING query " + sqlE);
		}
		return zoneStoreMap;
	}
	
	public List<String> getStoresInZoneFromForecastTable(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<String> storeList = new ArrayList<String>();
		try{
			statement = conn.prepareStatement(GET_STORES_IN_ZONE_FOR_AHOLD);
	        statement.setInt(1, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getString("COMP_STR_NO"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORES_IN_ZONE");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
	
	public List<String> getStoresInZoneFromProductLocationMapTable(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<String> storeList = new ArrayList<String>();
		try{
			statement = conn.prepareStatement(GET_STORES_IN_ZONE_FROM_PLM);
	        statement.setInt(1, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getString("COMP_STR_NO"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORES_IN_ZONE_PLM");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE_PLM", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
	
	public PriceZoneDTO getRetailPriceZoneUsingForecastTable(Connection conn, String zoneNum) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    PriceZoneDTO priceZoneDTO = null;
		try{
			statement = conn.prepareStatement(GET_RETAIL_PRICE_ZONE_FOR_AHOLD);
	        statement.setString(1, zoneNum);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	priceZoneDTO = new PriceZoneDTO();
	        	priceZoneDTO.setPriceZoneId(resultSet.getInt("PRICE_ZONE_ID"));
	        	priceZoneDTO.setPrimaryCompetitor(resultSet.getInt("PRIMARY_COMP_STR_ID"));
	        	priceZoneDTO.setSecComp1(resultSet.getInt("SECONDARY_COMP_STR_ID_1"));
	        	priceZoneDTO.setSecComp2(resultSet.getInt("SECONDARY_COMP_STR_ID_2"));
	        	priceZoneDTO.setSecComp3(resultSet.getInt("SECONDARY_COMP_STR_ID_3"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_PRICE_ZONE");
			throw new GeneralException("Error while executing GET_RETAIL_PRICE_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceZoneDTO;
	}
	
	/**
	 * 
	 * @param conn
	 * @param priceZoneId
	 * @return stores from global zone
	 * @throws GeneralException
	 */
	public List<Integer> getStoreIdsInGlobalZone(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<Integer> storeList = new ArrayList<Integer>();
		try{
			statement = conn.prepareStatement(GET_STORES_FOR_GLOBAL_ZONE);
	        statement.setInt(1, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getInt("COMP_STR_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORES_FOR_GLOBAL_ZONE");
			throw new GeneralException("Error while executing GET_STORE_IDS_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
	/**
	 * Returns true if its a global zone for the input zone
	 * @param conn
	 * @param locationId
	 * @return
	 * @throws GeneralException
	 */
	public boolean getIsGlobalZone(Connection conn, int locationId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		boolean isGlobalZone = false;
		try {
				stmt = conn.prepareStatement(GET_IS_GLOBAL_ZONE);
				stmt.setInt(1, locationId);
				
			rs = stmt.executeQuery();
			
			if (rs.next()) {
				isGlobalZone = String.valueOf(Constants.YES).equals(rs.getString("GLOBAL_ZONE"));
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving its a global zone for the input zone - " + ex.getMessage());
			throw new GeneralException("Error when retrieving its a global zone for the input zone " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return isGlobalZone;
	}
	public List<String> getStoresInGlobalZone(Connection conn, int priceZoneId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<String> storeList = new ArrayList<String>();
		try{
			statement = conn.prepareStatement(GET_STORES_IN_GLOBAL_ZONE);
	        statement.setInt(1, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeList.add(resultSet.getString("COMP_STR_NO"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORES_IN_ZONE");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}
/**
 * This function with get only  PriceTest zones from RETAIL_PRICE_INFO
 * @param conn
 * @return
 * @throws GeneralException
 */
	public HashMap<Integer,Integer> getPriceTestZones(Connection conn) throws GeneralException {
	
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, Integer> testZoneList = new HashMap<Integer, Integer>();
		try {
			statement = conn.prepareStatement(GET_PRICE_TEST_ZONES);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				testZoneList.put(resultSet.getInt("PRICE_ZONE_ID"),resultSet.getInt("ZONE_NUM"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_STORES_IN_ZONE");
			throw new GeneralException("Error while executing GET_STORES_IN_ZONE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return testZoneList;
	}

public List<PriceZoneDTO> checkAndGetChildZonesIfPresent(Connection conn, int priceZoneId, String chainId) {
	List<PriceZoneDTO> output = new ArrayList<>();
	
	PreparedStatement statement = null;
	ResultSet resultSet = null;
	try {
		statement = conn.prepareStatement(CHECK_AND_GET_CHILD_ZONES);
		statement.setInt(1, priceZoneId);
		statement.setInt(2, Integer.parseInt(chainId));
		statement.setInt(3, Constants.ZONE_LEVEL_ID);
		
		resultSet = statement.executeQuery();
		while (resultSet.next()) {
			PriceZoneDTO priceZoneDTO = new PriceZoneDTO();
        	priceZoneDTO.setPriceZoneId(resultSet.getInt("PRICE_ZONE_ID"));
        	priceZoneDTO.setPrimaryCompetitor(resultSet.getInt("PRIMARY_COMP_STR_ID"));
        	priceZoneDTO.setSecComp1(resultSet.getInt("SECONDARY_COMP_STR_ID_1"));
        	priceZoneDTO.setSecComp2(resultSet.getInt("SECONDARY_COMP_STR_ID_2"));
        	priceZoneDTO.setSecComp3(resultSet.getInt("SECONDARY_COMP_STR_ID_3"));
        	priceZoneDTO.setPriceZoneNum(resultSet.getString("ZONE_NUM"));
			if (resultSet.getString("GLOBAL_ZONE").equals("Y")) {
				priceZoneDTO.setGlobalZone(true);
			} else
				priceZoneDTO.setGlobalZone(false);
			
			if (resultSet.getString("ZONE_TYPE").equals("T")) {
				priceZoneDTO.setTestZone(true);
			} else
				priceZoneDTO.setTestZone(false);
			
			output.add(priceZoneDTO);
		}
	} catch (SQLException e) {
		logger.error("Error while executing checkAndGetChildZonesIfPresent: "+e.getMessage());
	} finally {
		PristineDBUtil.close(resultSet);
		PristineDBUtil.close(statement);
	}

	
	return output;
}


	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, String> getZoneIdAndNoMap(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, String> zoneIdMap = new HashMap<Integer, String>();
		try {
			stmt = conn.prepareStatement(GET_ACTIVE_ZONES);
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put(rs.getInt("PRICE_ZONE_ID"), rs.getString("ZONE_NUM"));
			}
		} catch (SQLException ex) {
			logger.error("zoneIdAndNoMap ()-Error when retrieving zone id map - " + ex.getMessage());
			throw new GeneralException("zoneIdAndNoMap-Error when retrieving zone id map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}
	 
}
