package com.pristine.dao.pricingalert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.pricingalert.AlertTypesDto;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class AlertTypesDAO implements IDAO{
	static Logger logger = Logger.getLogger("AlertTypesDAO");
	
	private String GET_ALERT_TYPES = "SELECT PA_ALERT_TYPES_ID, NAME, DISPLAY_NAME, DESCRIPTION, TECHNICAL_CODE, " +
									 " BASE_CUR_DATA_LOOKUP_RANGE, BASE_PRE_DATA_LOOKUP_RANGE, BASE_FUT_DATA_LOOKUP_RANGE, " +
									 " COMP_CUR_DATA_LOOKUP_RANGE, COMP_PRE_DATA_LOOKUP_RANGE FROM PA_ALERT_TYPES WHERE ENABLED = 'N'";
	
	public ArrayList<AlertTypesDto> getAlertTypes(Connection connection) throws GeneralException{
		ArrayList<AlertTypesDto> alertTypeList = new ArrayList<AlertTypesDto>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = connection.prepareStatement(GET_ALERT_TYPES);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	AlertTypesDto alertTypeDTO = new AlertTypesDto();
	        	alertTypeDTO.setAlertTypeId(resultSet.getInt("PA_ALERT_TYPES_ID"));
	        	alertTypeDTO.setName(resultSet.getString("NAME"));
	        	alertTypeDTO.setDisplayName(resultSet.getString("DISPLAY_NAME"));
	        	alertTypeDTO.setDescription(resultSet.getString("DESCRIPTION"));
	        	alertTypeDTO.setTechnicalCode(resultSet.getString("TECHNICAL_CODE"));
	        	alertTypeDTO.setBaseCurDataRange(resultSet.getString("BASE_CUR_DATA_LOOKUP_RANGE"));
	        	alertTypeDTO.setBasePreDataRange(resultSet.getString("BASE_PRE_DATA_LOOKUP_RANGE"));
	        	alertTypeDTO.setBaseFutDataRange(resultSet.getString("BASE_FUT_DATA_LOOKUP_RANGE"));
	        	alertTypeDTO.setCompCurDataRange(resultSet.getString("COMP_CUR_DATA_LOOKUP_RANGE"));
	        	alertTypeDTO.setCompPreDataRange(resultSet.getString("COMP_PRE_DATA_LOOKUP_RANGE"));
	        	alertTypeList.add(alertTypeDTO);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ALERT_TYPES");
			throw new GeneralException("Error while executing GET_ALERT_TYPES" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return alertTypeList;
	}
}
