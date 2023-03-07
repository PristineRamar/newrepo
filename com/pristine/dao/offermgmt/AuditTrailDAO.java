package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;


import com.pristine.dto.offermgmt.AuditTrailDetailDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class AuditTrailDAO {
	private static Logger logger = Logger.getLogger("AuditTrailDAO");

	private static final String INSERT_INTO_AUDIT_TRAIL_DETAIL = "INSERT INTO AUDIT_TRAIL_DETAIL(AUDIT_TRAIL_ID, "
			+ "LOCATION_ID, LOCATION_LEVEL_ID, PRODUCT_ID, PRODUCT_LEVEL_ID, AUDIT_KEY_1, STATUS_TYPE, DETAILS, AUDIT_DATE, USER_ID,"
			+ "AUDIT_TYPE,AUDIT_SUB_TYPE,SUB_STATUS_TYPE ,TOTAL_EXPORTED_ITEMS, TOTAL_EXPORTED_STORES, EXPORT_TIME) "
			+ "VALUES(AUDIT_TRAIL_SEQUENCE.NEXTVAL,?,?,?,?,?,?,?,SYSDATE,?,?,?,?,?,?,?) ";

	private static String INSERT_INTO_AUDIT_TRAIL_WEEKLY_DETAIL = "INSERT INTO AUDIT_TRAIL_DETAIL(AUDIT_TRAIL_ID, "
			+ "LOCATION_ID, LOCATION_LEVEL_ID, PRODUCT_ID, PRODUCT_LEVEL_ID, AUDIT_KEY_1, DETAILS, AUDIT_DATE, USER_ID,"
			+ "AUDIT_TYPE,AUDIT_SUB_TYPE,SUB_STATUS_TYPE,AUDIT_KEY_3) "
			+ "VALUES(AUDIT_TRAIL_SEQUENCE.NEXTVAL,?,?,?,?,?,?,SYSDATE,?,?,?,?,?) ";

	public void insertAuditTrailDetail(Connection connection, List<AuditTrailDetailDTO> auditTrailDetailList)
			throws GeneralException {
		PreparedStatement statement = null;
		// Loop each auditTrailDetailList to get their list of values

		try {
			String query = new String(INSERT_INTO_AUDIT_TRAIL_DETAIL);
			logger.debug(query);
			statement = connection.prepareStatement(query);
			int itemNoInBatch = 0;
			for (AuditTrailDetailDTO auditTrailDetailDTO : auditTrailDetailList) {
				int counter = 0;

				statement.setLong(++counter, auditTrailDetailDTO.getLocationId());
				statement.setLong(++counter, auditTrailDetailDTO.getLocationLevelId());
				statement.setLong(++counter, auditTrailDetailDTO.getProductId());
				statement.setLong(++counter, auditTrailDetailDTO.getProductLevelId());
				if(auditTrailDetailDTO.getAuditKey1() != 0L) {
					statement.setLong(++counter, auditTrailDetailDTO.getAuditKey1());
				}
				else {
					statement.setNull(++counter, Types.INTEGER);
				}
				/* statement.setInt(++counter, auditTrailDetailDTO.getAuditTrailType()); */
				if(auditTrailDetailDTO.getStatusType() > 0) {
					statement.setInt(++counter, auditTrailDetailDTO.getStatusType());
				}else {
					statement.setNull(++counter, Types.INTEGER);
				}
				statement.setString(++counter, null);
				statement.setString(++counter, auditTrailDetailDTO.getUserId());
				statement.setInt(++counter, auditTrailDetailDTO.getAuditType());
				statement.setInt(++counter, auditTrailDetailDTO.getAuditSubType());
				statement.setInt(++counter, auditTrailDetailDTO.getAuditSubStatus());
				
				if(auditTrailDetailDTO.getExportItemCount() > 0) {
					statement.setInt(++counter, auditTrailDetailDTO.getExportItemCount());
				}else {
					statement.setNull(++counter, Types.INTEGER);
				}
				if(auditTrailDetailDTO.getExportStoreCount() > 0) {
					statement.setInt(++counter, auditTrailDetailDTO.getExportStoreCount());
				}else {
					statement.setNull(++counter, Types.INTEGER);
				}
				if(auditTrailDetailDTO.getExportTime() > 0) {
					statement.setInt(++counter, auditTrailDetailDTO.getExportTime());
				}else {
					statement.setNull(++counter, Types.INTEGER);
				}
				
				statement.addBatch();
				itemNoInBatch++;
				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}

			}
			if (itemNoInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			
			PristineDBUtil.commitTransaction(connection, "Audit Trial");
		} catch (SQLException e) {
			logger.error("Error in insertAuditTrailDetail()" + e.toString(), e);
			throw new GeneralException("Error in insertAuditTrailDetail() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void insertAuditTrailWeeklyDeatils(Connection connection, List<AuditTrailDetailDTO> auditTrailDetailList)
			throws GeneralException {
		PreparedStatement statement = null;

		// Loop each auditTrailDetailList to get their list of values
		try {
			/*
			 * String query = new String(INSERT_INTO_AUDIT_TRAIL_WEEKLY_DETAIL);
			 * logger.debug(query);
			 */
			statement = connection.prepareStatement(INSERT_INTO_AUDIT_TRAIL_WEEKLY_DETAIL);

			int itemNoInBatch = 0;
			for (AuditTrailDetailDTO auditTrailDetailDTO : auditTrailDetailList) {
				int counter = 0;

				statement.setLong(++counter, auditTrailDetailDTO.getLocationId());
				statement.setLong(++counter, auditTrailDetailDTO.getLocationLevelId());
				statement.setLong(++counter, auditTrailDetailDTO.getProductId());
				statement.setLong(++counter, auditTrailDetailDTO.getProductLevelId());

				statement.setLong(++counter, auditTrailDetailDTO.getAuditKey1());
				statement.setString(++counter, null);
				statement.setString(++counter, auditTrailDetailDTO.getUserId());
				statement.setInt(++counter, auditTrailDetailDTO.getAuditType());

				statement.setInt(++counter, auditTrailDetailDTO.getAuditSubType());
				statement.setInt(++counter, auditTrailDetailDTO.getAuditSubStatus());
				statement.setInt(++counter, auditTrailDetailDTO.getAuditKey3());
				String str = "  loc: " + auditTrailDetailDTO.getLocationId() + " locLvelId:  "
						+ auditTrailDetailDTO.getLocationLevelId() + " productId:  "
						+ auditTrailDetailDTO.getProductId() + " prodLvlId:   "
						+ auditTrailDetailDTO.getProductLevelId() + "AuditKey1(RunId):  "
						+ auditTrailDetailDTO.getAuditKey1() + " userId: " + auditTrailDetailDTO.getUserId()
						+ "AuditSubType:  " + auditTrailDetailDTO.getAuditType() + "AuditSubType:  "
						+ auditTrailDetailDTO.getAuditSubType() + "AuditSubStatus:  "
						+ auditTrailDetailDTO.getAuditSubStatus() + "auditKey3(calId):  "
						+ auditTrailDetailDTO.getAuditKey3();
				logger.info("ItemDetails:  " + str);
				statement.addBatch();
				itemNoInBatch++;
				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					try {
						statement.executeBatch();

						statement.clearBatch();
						itemNoInBatch = 0;
						PristineDBUtil.commitTransaction(connection, "commiting INSERT INTO AUDIT_TRAIL_DETAIL");
					} catch (Exception ex) {
						PristineDBUtil.rollbackTransaction(connection,
								" Error commiting INSERT INTO AUDIT_TRAIL_DETAIL");
						logger.error("Error in INSERT INTO AUDIT_TRAIL_DETAIL " + ex);
					}
				}
			}

			try {
				if (itemNoInBatch > 0) {
					statement.executeBatch();

					statement.clearBatch();
					itemNoInBatch = 0;
					PristineDBUtil.commitTransaction(connection, "commiting INSERT INTO AUDIT_TRAIL_DETAIL");
				}
			} catch (Exception ex) {
				PristineDBUtil.rollbackTransaction(connection, " Error commiting INSERT INTO AUDIT_TRAIL_DETAIL");
				logger.error("Error in INSERT INTO PR_QUARTER_REC_STATUS " + ex);
			}
		} catch (Exception ex) {

			logger.error("Exception while executing function  insertAuditTrailWeeklyDeatils() " + ex);
		}

	}

}
