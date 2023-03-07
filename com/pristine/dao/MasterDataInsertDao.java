package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.masterDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;

public class MasterDataInsertDao implements IDAO {

	private static Logger logger = Logger.getLogger("MasterDataInsertDao");

	public void writeItemsToDB(Connection conn, List<masterDataDTO> finalList) throws GeneralException, SQLException {

		PreparedStatement statement = null;

		String INSERT_INTO_MASTER_DATA = "INSERT INTO MASTER_DATA(STORE,ZONE,ZONE_NAME,COUNTRY_CODE,PREDICTED,PRIMARY_DC,"
				+ "ITEM_CODE,RECOMMENDATION_UNIT,PART_NUMBER,HP_SF_FLAG,RETAIL_EFFECTIVE_DATE,DIY_RETAIL,CORE_RETAIL,VDP_RETAIL,"
				+ "\"" + "LEVEL" + "\" ,"
				+ "TOTAL_PRICE_CHANGE,APPROVER,APPROVER_NAME,CE_FLAG,STORE_LOCK_EXPIRY_FLAG,FILE_NAME) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {

			statement = conn.prepareStatement(INSERT_INTO_MASTER_DATA);

			
			int itemNoInBatch = 0;
			for (masterDataDTO masterDataDTO : finalList) {

				int counter = 0;

				statement.setString(++counter, masterDataDTO.getStore());
				statement.setString(++counter, masterDataDTO.getZone());
				statement.setString(++counter, masterDataDTO.getZoneName());
				statement.setString(++counter, masterDataDTO.getCountryCode());
				statement.setString(++counter, masterDataDTO.getPredicted());
				statement.setString(++counter, masterDataDTO.getPrimaryDc());
				statement.setInt(++counter, masterDataDTO.getItemCode());
				statement.setString(++counter, masterDataDTO.getRecommendationUnit());
				statement.setString(++counter, masterDataDTO.getPartNumber());
				statement.setString(++counter, masterDataDTO.getHpsfFlag());
				statement.setString(++counter, masterDataDTO.getRetaileffdate());
				statement.setString(++counter, masterDataDTO.getDiyRetail());
				statement.setString(++counter, masterDataDTO.getCoreRetail());
				statement.setString(++counter, masterDataDTO.getVdpRetail());
				statement.setString(++counter, masterDataDTO.getLevel());
				statement.setString(++counter, masterDataDTO.getTotalPriceChange());
				statement.setString(++counter, masterDataDTO.getApprover());
				statement.setString(++counter, masterDataDTO.getApproverName());
				statement.setString(++counter, masterDataDTO.getCeFlag());
				statement.setString(++counter, masterDataDTO.getStoreLockExpiryFlag());
				statement.setString(++counter, masterDataDTO.getFileName());
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.LIMIT_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}

			}

			if (itemNoInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error in writeItemsToDB()" + e.getMessage());
			throw new GeneralException("Error in writeItemsToDB() " + e.toString(), e);
		} finally {
			statement.close();

		}
	}

}
