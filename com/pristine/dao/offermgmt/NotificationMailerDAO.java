package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.NotificationDetailDTO;
import com.pristine.dto.offermgmt.NotificationHeaderDTO;
import com.pristine.dto.offermgmt.NotificationMailerDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class NotificationMailerDAO {
	private static Logger logger = Logger.getLogger("NotificationMailerDAO");
	private static final String GET_USERDETAILS_FROM_NOTIFICATION_ANALYSIS = "SELECT URS.ROLE_DESC AS APPROVER_ROLE_NAME, RPZ.ZONE_NUM, PG.NAME, PG.PRODUCT_ID, "
			+ "UR.ROLE_DESC, UDS.FIRST_NAME || ' ' || UDS.LAST_NAME AS USER_NAME, "
			+ "UD.E_MAIL, ND.USER_ID, NH.NOTIFICATION_ID, NH.NOTIFICATION_TYPE, "
			+ "NH.NOTIFICATION_KEY1, TO_CHAR(NH.NOTIFICATION_TIME,?) "
			+ "AS NOTIFICATION_TIME FROM NOTIFICATION_HEADER NH "
			+ "LEFT JOIN NOTIFICATION_DETAIL ND ON NH.NOTIFICATION_ID = ND.NOTIFICATION_ID "
			+ "LEFT JOIN USER_DETAILS UD ON ND.USER_ID = UD.USER_ID "
			+ "LEFT JOIN PR_QUARTER_REC_HEADER PR ON PR.RUN_ID = NH.NOTIFICATION_KEY1 AND "
			+ "PR.LOCATION_LEVEL_ID = ? LEFT JOIN RETAIL_PRICE_ZONE RPZ ON RPZ.PRICE_ZONE_ID = PR.LOCATION_ID "
			+ "LEFT JOIN  PRODUCT_GROUP PG ON PG.PRODUCT_ID = PR.PRODUCT_ID AND PG.PRODUCT_LEVEL_ID = ? "
			+ "LEFT JOIN USER_ROLE UR ON NH.NOTIFICATION_KEY2 = UR.ROLE_ID "
			+ " LEFT JOIN USER_DETAILS UDS ON PR.STATUS_BY=UDS.USER_ID "
			+ " LEFT JOIN USER_ROLE URS ON PR.APPROVAL_ROLE_ID = URS.ROLE_ID"
			+ " LEFT JOIN USER_CONFIG_SETTING   UCF  ON UCF.USER_ID=ND.USER_ID"
			+ " WHERE NH.IS_NOTIFICATION_SEND = ? AND UCF.email_notification='Y' AND ND.IS_MAIL_SEND = ? "
			+ " AND NH.NOTIFICATION_TIME > SYSDATE -(?/24) AND UDS.ACTIVE_INDICATOR = 'Y' ";
			
	private static final String UPDATE_NOTIFICATION_DETAILS = "UPDATE NOTIFICATION_DETAIL SET IS_MAIL_SEND = ? WHERE USER_ID = ? "
			+ "AND NOTIFICATION_ID = ? ";
	private static final String UPDATE_NOTIFICATION_HEADER = "UPDATE NOTIFICATION_HEADER SET IS_NOTIFICATION_SEND = ? "
			+ "WHERE NOTIFICATION_ID = ? ";

	/**
	 * Get all the user details and their values
	 * 
	 * @param connection
	 * @return
	 * @throws GeneralException
	 */
	public List<NotificationMailerDTO> getUserDetails(Connection connection) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<NotificationMailerDTO> notificationMailerList = new ArrayList<NotificationMailerDTO>();
		int preferredRecProdLevelId = Integer.parseInt(
				PropertyManager.getProperty("PREFERRED_REC_PROD_LEVEL", String.valueOf(Constants.CATEGORYLEVELID)));
		try {
			int counter = 0;
			String query = new String(GET_USERDETAILS_FROM_NOTIFICATION_ANALYSIS);
			statement = connection.prepareStatement(query);
			logger.info("getUserDetails query :" + query);
			logger.info("parameters:" + "1. " + Constants.APP_DATE_TIME_FORMAT + "; 2. "
					+ PRConstants.ZONE_LEVEL_TYPE_ID + "3., " + preferredRecProdLevelId + " 4,5. " + Constants.NO + " "
					+ ";6. " + Integer.parseInt(PropertyManager.getProperty("DIFFERENCE_IN_NOTIFICATION_TIME")));

			statement.setString(++counter, Constants.APP_DATE_TIME_FORMAT);
			statement.setInt(++counter, PRConstants.ZONE_LEVEL_TYPE_ID);
			statement.setInt(++counter, preferredRecProdLevelId);
			statement.setString(++counter, String.valueOf(Constants.NO));
			statement.setString(++counter, String.valueOf(Constants.NO));
			statement.setInt(++counter, Integer.parseInt(PropertyManager.getProperty("DIFFERENCE_IN_NOTIFICATION_TIME")));
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				NotificationMailerDTO notificationMailerDTO = new NotificationMailerDTO();
				notificationMailerDTO.setmailId(resultSet.getString("E_MAIL"));
				notificationMailerDTO.setNotificationId(resultSet.getLong("NOTIFICATION_ID"));
				notificationMailerDTO.setNotificationType(resultSet.getInt("NOTIFICATION_TYPE"));
				notificationMailerDTO.setProductName(resultSet.getString("NAME"));
				notificationMailerDTO.setUserId(resultSet.getString("USER_ID"));
				notificationMailerDTO.setZoneNumber(resultSet.getString("ZONE_NUM"));
				notificationMailerDTO.setRunId(resultSet.getInt("NOTIFICATION_KEY1"));
				notificationMailerDTO.setNotificationTime(resultSet.getString("NOTIFICATION_TIME"));
				notificationMailerDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				notificationMailerDTO.setUserRollName(
						resultSet.getString("ROLE_DESC") == null ? Constants.EMPTY : resultSet.getString("ROLE_DESC"));
				notificationMailerDTO.setUserName(
						resultSet.getString("USER_NAME") == null ? Constants.EMPTY : resultSet.getString("USER_NAME"));
				notificationMailerDTO
						.setApproverRoleName(resultSet.getString("APPROVER_ROLE_NAME") == null ? Constants.EMPTY
								: resultSet.getString("APPROVER_ROLE_NAME"));

				notificationMailerList.add(notificationMailerDTO);
			}
		} catch (SQLException e) {
			logger.error("Error while executing getUserDetails() " + e);
			throw new GeneralException("Error while executing getUserDetails() " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return notificationMailerList;
	}

	/**
	 * Update Is mail send status in notification detail
	 * 
	 * @param connection
	 * @param userDetails
	 * @throws GeneralException
	 */
	public int updateIsEMailSendStatus(Connection connection, List<NotificationDetailDTO> userDetails) throws GeneralException {
		PreparedStatement statement = null;
		int updateStatus = 0;
		try {
			int itemNoInBatch = 0;
			statement = connection.prepareStatement(UPDATE_NOTIFICATION_DETAILS);
			for (NotificationDetailDTO notificationDetailDTO : userDetails) {
				int counter = 0;

				statement.setString(++counter, String.valueOf(notificationDetailDTO.getIsEMailSend()));
				statement.setString(++counter, notificationDetailDTO.getUserId());
				statement.setLong(++counter, notificationDetailDTO.getNotificationId());

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
				updateStatus = 1;
			}
			logger.info("Updated is_mail_send value in notification_detail");

		} catch (SQLException e) {
			logger.error("Error in updateIsEMailSendStatus()" + e.toString(), e);
			throw new GeneralException("Error in updateIsEMailSendStatus() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return updateStatus;
	}

	/**
	 * Update is notification send status
	 * 
	 * @param connection
	 * @param userDetails
	 * @throws GeneralException
	 */
	public void updateIsNotificationSendStatus(Connection connection, List<NotificationHeaderDTO> notificationIdList) throws GeneralException {
		PreparedStatement statement = null;
		try {
			int itemNoInBatch = 0;
			statement = connection.prepareStatement(UPDATE_NOTIFICATION_HEADER);
			for (NotificationHeaderDTO notificationHeaderDTO : notificationIdList) {
				int counter = 0;
				statement.setString(++counter, String.valueOf(notificationHeaderDTO.getIsNotificationSend()));
				statement.setLong(++counter, notificationHeaderDTO.getNotificationId());
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
			logger.info("Updated Is_notification_send value in Notification_header");
		} catch (SQLException e) {
			logger.error("Error in updateIsNotificationSendStatus()" + e.toString(), e);
			throw new GeneralException("Error in updateIsNotificationSendStatus() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
}
