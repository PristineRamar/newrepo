package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class RetailCalendarPromoDAO {
	static Logger logger = Logger.getLogger("RetailCalendarPromoDAO");

	private static final String GET_FULL_RETAIL_CALENDAR_PROMO = "SELECT CAL_YEAR,ROW_TYPE,ACTUAL_NO, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ "TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE,CALENDAR_ID,SPECIAL_DAY_ID,WEEK_NO " 
			+ "FROM RETAIL_CALENDAR_PROMO RC";

	public List<RetailCalendarDTO> getFullRetailCalendarPromo(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		List<RetailCalendarDTO> calendarDetails = new ArrayList<RetailCalendarDTO>();
		try {
			stmt = conn.prepareStatement(GET_FULL_RETAIL_CALENDAR_PROMO);
			stmt.setFetchSize(100000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				calendar.setEndDate(rs.getString("END_DATE"));
				calendar.setWeekNo(rs.getInt("WEEK_NO"));
				calendar.setSpecialDayId(rs.getInt("SPECIAL_DAY_ID"));
				calendar.setRowType(rs.getString("ROW_TYPE"));
				calendar.setCalYear(rs.getInt("CAL_YEAR"));
				calendar.setActualNo(rs.getInt("ACTUAL_NO"));
				calendarDetails.add(calendar);
			}
		} catch (SQLException e) {
			logger.error("Error in getFullRetailCalendarPromo() - " + e);
			throw new GeneralException("Error in getFullRetailCalendarPromo() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendarDetails;
	}
}
