package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.LocationDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class RetailDivisionDAO {
	static Logger logger = Logger.getLogger("RetailDivisionDAO");

	private static final String GET_ALL_DIVISION = "SELECT ID, NAME, DESCRIPTION FROM RETAIL_DIVISION RD WHERE RD.ACTIVE_INDICATOR = 'Y'";

	public List<LocationDTO> getAllDivision(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		LocationDTO locationDTO = null;
		List<LocationDTO> divisions = new ArrayList<LocationDTO>();
		try {
			stmt = conn.prepareStatement(GET_ALL_DIVISION);
			stmt.setFetchSize(1000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				locationDTO = new LocationDTO();
				locationDTO.levelId = Constants.DIVISION_LEVEL_ID;
				locationDTO.locationId = rs.getInt("ID");
				locationDTO.name = rs.getString("NAME");
				locationDTO.desc = rs.getString("DESCRIPTION");
				divisions.add(locationDTO);
			}
		} catch (SQLException e) {
			logger.error("Error in getAllDivision() - " + e);
			throw new GeneralException("Error in getAllDivision() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return divisions;
	}
}
