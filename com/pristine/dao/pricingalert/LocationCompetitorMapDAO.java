package com.pristine.dao.pricingalert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class LocationCompetitorMapDAO implements IDAO{
	static Logger logger = Logger.getLogger("LocationCompetitorMapDAO");
	
	private String GET_PA_USER_LOC_COMP_DETAILS = "SELECT LCM.LOCATION_COMPETITOR_MAP_ID, USER_ID, BASE_LOCATION_LEVEL_ID, BASE_LOCATION_ID, COMP_LOCATION_LEVEL_ID, COMP_LOCATION_ID, COMP_LOCATION_TYPES_ID, " +
										 		  "PRODUCT_LEVEL_ID, PRODUCT_ID, PRICE_CHECK_LIST_ID FROM PA_USER_LOCATION_MAP ULM, LOCATION_COMPETITOR_MAP LCM " + 
										 		  "WHERE LCM.PRICING_ALERT_ENABLED = 'Y' AND LCM.LOCATION_COMPETITOR_MAP_ID = ULM.LOCATION_COMPETITOR_MAP_ID ORDER BY COMP_LOCATION_TYPES_ID";
	
	private String GET_PA_USER_DETAILS = "SELECT DISTINCT USER_ID, PRODUCT_LEVEL_ID, PRODUCT_ID FROM PA_USER_LOCATION_MAP ";
	
	private String GET_LOCATION_COMPETITOR_DETAILS = "SELECT LOCATION_COMPETITOR_MAP_ID, BASE_LOCATION_LEVEL_ID, BASE_LOCATION_ID, COMP_LOCATION_LEVEL_ID, COMP_LOCATION_ID, COMP_LOCATION_TYPES_ID, PRICE_CHECK_LIST_ID " +
										  			 "FROM LOCATION_COMPETITOR_MAP WHERE PRICING_ALERT_ENABLED = 'Y' ORDER BY COMP_LOCATION_TYPES_ID"; 

	private String GET_OTHER_COMPETITOR_INFO = "SELECT COMP_LOCATION_LEVEL_ID, COMP_LOCATION_ID FROM LOCATION_COMPETITOR_MAP WHERE BASE_LOCATION_LEVEL_ID = ? AND BASE_LOCATION_ID = ? AND COMP_LOCATION_TYPES_ID = ?";
	
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
	        	dto.setCompLocationTypeId(resultSet.getInt("COMP_LOCATION_TYPES_ID"));
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
	
	public LocationCompetitorMapDTO getOtherCompetitorInfo(Connection connection, LocationCompetitorMapDTO inputDTO){
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    LocationCompetitorMapDTO otherCompInfo = null;
		try{
			statement = connection.prepareStatement(GET_OTHER_COMPETITOR_INFO);
			statement.setInt(1, inputDTO.getBaseLocationLevelId());
			statement.setInt(2, inputDTO.getBaseLocationId());
			if(inputDTO.getCompLocationTypeId() == Constants.COMP_LOCATION_TYPE_PRIMARY)
				statement.setInt(3, Constants.COMP_LOCATION_TYPE_SECONDARY);
			else
				statement.setInt(3, Constants.COMP_LOCATION_TYPE_PRIMARY);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				otherCompInfo = new LocationCompetitorMapDTO();
				otherCompInfo.setCompLocationLevelId(resultSet.getInt("COMP_LOCATION_LEVEL_ID"));
				otherCompInfo.setCompLocationId(resultSet.getInt("COMP_LOCATION_ID"));
			}
		}catch (SQLException e){
			logger.error("Error while executing GET_OTHER_COMPETITOR_INFO " + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return otherCompInfo;
	}
}
