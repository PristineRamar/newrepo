package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class LocationCompetitorMapDAO implements IDAO{
	static Logger logger = Logger.getLogger("LocationCompetitorMapDAO");
	
	private String GET_PA_USER_LOC_COMP_DETAILS = "SELECT LCM.LOCATION_COMPETITOR_MAP_ID, USER_ID, BASE_LOCATION_LEVEL_ID, BASE_LOCATION_ID, COMP_LOCATION_LEVEL_ID, COMP_LOCATION_ID, " +
										 		  "PRODUCT_LEVEL_ID, PRODUCT_ID, PRICE_CHECK_LIST_ID FROM PA_USER_LOCATION_MAP ULM, LOCATION_COMPETITOR_MAP LCM " + 
										 		  "WHERE LCM.PRICING_ALERT_ENABLED = 'Y' AND LCM.LOCATION_COMPETITOR_MAP_ID = ULM.LOCATION_COMPETITOR_MAP_ID";
	
	private String GET_PA_USER_DETAILS = "SELECT DISTINCT USER_ID, PRODUCT_LEVEL_ID, PRODUCT_ID FROM PA_USER_LOCATION_MAP ";
	
	private String GET_LOCATION_COMPETITOR_DETAILS = "SELECT LOCATION_COMPETITOR_MAP_ID, BASE_LOCATION_LEVEL_ID, BASE_LOCATION_ID, COMP_LOCATION_LEVEL_ID, COMP_LOCATION_ID, PRICE_CHECK_LIST_ID, " +
										  			 "COMP_LOCATION_TYPES_ID FROM LOCATION_COMPETITOR_MAP WHERE PRICING_ALERT_ENABLED = 'Y'"; 
	
	private String GET_COMPETITOR_FOR_LOCATION = "SELECT C.COMP_LOCATION_LEVEL_ID,C.COMP_LOCATION_ID,C.COMP_LOCATION_TYPES_ID,"
			+ " CS.COMP_CHAIN_ID FROM LOCATION_COMPETITOR_MAP C JOIN COMPETITOR_STORE CS ON C.COMP_LOCATION_ID=CS.COMP_STR_ID "
			+ " WHERE BASE_LOCATION_LEVEL_ID = ? AND  BASE_LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";

	private String GET_COMPETITOR_FOR_LOCATION_PRODUCT = "SELECT C.COMP_LOCATION_LEVEL_ID,C.COMP_LOCATION_ID,C.COMP_LOCATION_TYPES_ID,"
			+ " CS.COMP_CHAIN_ID FROM LOCATION_COMPETITOR_MAP C JOIN COMPETITOR_STORE CS ON C.COMP_LOCATION_ID=CS.COMP_STR_ID "
			+ " WHERE BASE_LOCATION_LEVEL_ID = ? AND BASE_LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	
	public ArrayList<LocationCompetitorMapDTO> getPAUserDetails(Connection connection) throws GeneralException{
		ArrayList<LocationCompetitorMapDTO> processList = new ArrayList<LocationCompetitorMapDTO>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = connection.prepareStatement(GET_PA_USER_LOC_COMP_DETAILS);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	LocationCompetitorMapDTO dto = new LocationCompetitorMapDTO();
	        	dto.setUserId(resultSet.getString("USER_ID"));
	        	dto.setLocationCompetitorMapId(resultSet.getInt("LOCATION_COMPETITOR_MAP_ID"));
	        	dto.setBaseLocationLevelId(resultSet.getInt("BASE_LOCATION_LEVEL_ID"));
	        	dto.setBaseLocationId(resultSet.getInt("BASE_LOCATION_ID"));
	        	dto.setCompLocationLevelId(resultSet.getInt("COMP_LOCATION_LEVEL_ID"));
	        	dto.setCompLocationId(resultSet.getInt("COMP_LOCATION_ID"));
	        	dto.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
	        	dto.setProductId(resultSet.getInt("PRODUCT_ID"));
	        	dto.setPriceCheckListId(resultSet.getInt("PRICE_CHECK_LIST_ID"));
	        	processList.add(dto);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PA_USER_LOC_COMP_DETAILS");
			throw new GeneralException("Error while executing GET_PA_USER_LOC_COMP_DETAILS" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return processList;
	}
	
	public ArrayList<LocationCompetitorMapDTO> getPAUserDetailsOnly(Connection connection) throws GeneralException{
		ArrayList<LocationCompetitorMapDTO> processList = new ArrayList<LocationCompetitorMapDTO>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = connection.prepareStatement(GET_PA_USER_DETAILS);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	LocationCompetitorMapDTO dto = new LocationCompetitorMapDTO();
	        	dto.setUserId(resultSet.getString("USER_ID"));
	        	dto.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
	        	dto.setProductId(resultSet.getInt("PRODUCT_ID"));
	        	processList.add(dto);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PA_USER_DETAILS");
			throw new GeneralException("Error while executing GET_PA_USER_DETAILS" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return processList;
	}
	
	public ArrayList<LocationCompetitorMapDTO> getLocationCompetitorDetails(Connection connection) throws GeneralException{
		ArrayList<LocationCompetitorMapDTO> processList = new ArrayList<LocationCompetitorMapDTO>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = connection.prepareStatement(GET_LOCATION_COMPETITOR_DETAILS);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	LocationCompetitorMapDTO dto = new LocationCompetitorMapDTO();
	        	dto.setLocationCompetitorMapId(resultSet.getInt("LOCATION_COMPETITOR_MAP_ID"));
	        	dto.setBaseLocationLevelId(resultSet.getInt("BASE_LOCATION_LEVEL_ID"));
	        	dto.setBaseLocationId(resultSet.getInt("BASE_LOCATION_ID"));
	        	dto.setCompLocationLevelId(resultSet.getInt("COMP_LOCATION_LEVEL_ID"));
	        	dto.setCompLocationId(resultSet.getInt("COMP_LOCATION_ID"));
	        	dto.setPriceCheckListId(resultSet.getInt("PRICE_CHECK_LIST_ID"));
	        	dto.setCompLocationTypeId(resultSet.getInt("COMP_LOCATION_TYPES_ID"));
	        	processList.add(dto);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_LOCATION_COMPETITOR_DETAILS");
			throw new GeneralException("Error while executing GET_LOCATION_COMPETITOR_DETAILS" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return processList;
	}
	
	/**
	 * Return competitor for location-product if present, If not present returns competitor for location
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, LocationKey> getCompetitors(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId)
			throws GeneralException {
		HashMap<Integer, LocationKey> compData = new HashMap<Integer, LocationKey>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_COMPETITOR_FOR_LOCATION);
			int counter = 0;
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, locationId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				//LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, resultSet.getInt("COMP_LOCATION_ID"));
				LocationKey locationKey = new LocationKey(resultSet.getInt("COMP_LOCATION_LEVEL_ID"), resultSet.getInt("COMP_LOCATION_ID"));
				locationKey.setChainId(resultSet.getInt("COMP_CHAIN_ID"));
				int compLocationTypesId = resultSet.getInt("COMP_LOCATION_TYPES_ID");
				Character compType = decodeCompLocationType(compLocationTypesId);
				//compData.put(compType, resultSet.getInt("COMP_LOCATION_ID"));
				compData.put(compLocationTypesId, locationKey);
			}

			resultSet.close();
			statement.close();

			statement = conn.prepareStatement(GET_COMPETITOR_FOR_LOCATION_PRODUCT);
			counter = 0;
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, productLevelId);
			statement.setInt(++counter, productId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				//LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, resultSet.getInt("COMP_LOCATION_ID"));
				LocationKey locationKey = new LocationKey(resultSet.getInt("COMP_LOCATION_LEVEL_ID"), resultSet.getInt("COMP_LOCATION_ID"));
				locationKey.setChainId(resultSet.getInt("COMP_CHAIN_ID"));
				int compLocationTypesId = resultSet.getInt("COMP_LOCATION_TYPES_ID");
			//	Character compType = decodeCompLocationType(compLocationTypesId);
				//compData.put(compType, resultSet.getInt("COMP_LOCATION_ID"));
				compData.put(compLocationTypesId, locationKey);
			}
		} catch (SQLException e) {
			logger.error("Error while retrieving competition info " + e);
			throw new GeneralException("Error while retrieving competition info " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return compData;
	}
	
	public Character decodeCompLocationType(int compLocationTypesId){
		switch(compLocationTypesId){
			case 1:
				return PRConstants.COMP_TYPE_PRIMARY;
			case 2:
				return PRConstants.COMP_TYPE_SECONDARY;
			case 3:
				return PRConstants.COMP_TYPE_TERTIARY;
			case 4:
				return PRConstants.COMP_TYPE_FOUR;
			default:
				return PRConstants.COMP_TYPE_NONE;
		}
	}
	
	public HashMap<Integer, Integer> getCompChainId(Connection conn, Set<Integer> compStrIds) throws GeneralException {
		HashMap<Integer, Integer> compChainIdMap = new HashMap<>();
		String qry = "SELECT COMP_STR_ID, COMP_CHAIN_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN ("
				+ PRCommonUtil.getCommaSeperatedStringFromIntSet(compStrIds) + ")";

		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int compStrId = resultSet.getInt("COMP_STR_ID");
				int compChainId = resultSet.getInt("COMP_CHAIN_ID");
				compChainIdMap.put(compStrId, compChainId);
			}
		} catch (SQLException e) {
			logger.error("Error while retrieving competition info " + e);
			throw new GeneralException("Error while retrieving competition info " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return compChainIdMap;
	}
	
	public HashMap<Integer, Integer> getMultiCompSetting(Connection conn) throws GeneralException {
		HashMap<Integer, Integer> multiCompSetting = new HashMap<>();
		String qry = "SELECT COMP_ORDER, COMP_CHAIN_ID FROM PR_MUTLI_COMP_CONFIG";

		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int order = resultSet.getInt("COMP_ORDER");
				int compChainId = resultSet.getInt("COMP_CHAIN_ID");
				multiCompSetting.put(order, compChainId);
			}
		} catch (SQLException e) {
			logger.error("Error while retrieving competition info " + e);
			throw new GeneralException("Error while retrieving competition info " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return multiCompSetting;
	}
}