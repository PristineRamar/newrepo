package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.dataload.gianteagle.GEHouseholdDTO;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.util.PristineDBUtil;

public class GEHouseholdDAO {

	static Logger logger = Logger.getLogger(" GEHouseholdDAO");

	private static final String UPDATE_HOUSEHOLDATA = "UPDATE CUSTOMER_LOYALTY_INFO SET BLOCK_CODE= ?, ZIP_CODE = ?"
			+ " WHERE HOUSEHOLD_NUMBER = ?";

	public int updateHouseHoldInfo(Connection conn, List<GEHouseholdDTO> processedList) throws SQLException {
		int[] status = null;
		PreparedStatement stmt = null;
		int updateCount = 0;

		logger.info("updateHouseHoldInfo()-Insert Begin");
		try {

			logger.info("processing count:" + processedList.size());

			stmt = conn.prepareStatement(UPDATE_HOUSEHOLDATA);
			for (GEHouseholdDTO hhData : processedList) {

				int counter = 0;
				stmt.setString(++counter, hhData.getBlockcode());
				stmt.setString(++counter, hhData.getZipcode());
				stmt.setInt(++counter, hhData.getHouseholdno());

				stmt.addBatch();

			}
			status = stmt.executeBatch();

		} catch (SQLException exception) {

			logger.error("Error when inserting householdInfo - " + exception);
		} finally {
			// stmt.close();
			PristineDBUtil.close(stmt);
			// conn.close();
		}
		if (status != null) {
			updateCount = status.length;
		}
		logger.info("updateHouseHoldInfo()- update Complete for  :" + updateCount + " records");
		return updateCount;
	}

}
