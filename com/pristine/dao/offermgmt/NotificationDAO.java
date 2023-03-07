package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.NotificationHeaderDTO;
import com.pristine.dto.offermgmt.NotificationMailerDTO;
import com.pristine.dto.offermgmt.NotificationDetailDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.UserTaskDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
public class NotificationDAO {

	private static Logger logger = Logger.getLogger("NotificationDAO");
	// Get values from GET_PR_RECOMMENDATION_RUN_HEADER
	private static final String GET_PR_RECOMMENDATION_RUN_HEADER = "SELECT RUN_ID, PRODUCT_LEVEL_ID,"
			+ " PRODUCT_ID, PWL.WORKFLOW_LEVEL AS CURRENT_WF_LEVEL, PRH.STATUS_ROLE AS CURRENT_ROLE_ID, "
			+ " PRH.APPROVAL_ROLE_ID, PWLA.WORKFLOW_LEVEL AS APPROVAL_WF_LEVEL "
			+ " FROM PR_QUARTER_REC_HEADER PRH LEFT JOIN PR_WORKFLOW_LOOKUP PWL ON PWL.ROLE_ID = PRH.STATUS_ROLE "
			+ " LEFT JOIN PR_WORKFLOW_LOOKUP PWLA ON PWLA.ROLE_ID = PRH.APPROVAL_ROLE_ID "
			+ " WHERE RUN_ID IN(%QUERY_CONDITION%)";
	// return user_id using run_id value
	private static final String GET_USER_ID = "SELECT URM.USER_ID, UT.VALUE_TYPE, UT.VALUE, URM.ROLE_ID, PWL.WORKFLOW_LEVEL, "
			+ " PWL.ENABLE_NOTIFICATION, PWL.NOTIFY_EMAIL FROM USER_TASK UT "
			+ " INNER JOIN USER_ROLE_MAP URM ON URM.USER_ID = UT.USER_ID "
			+ " INNER JOIN USER_DETAILS UD ON UD.USER_ID = UT.USER_ID "
			+ " LEFT JOIN PR_WORKFLOW_LOOKUP PWL ON URM.ROLE_ID = PWL.ROLE_ID "
			+ " WHERE UT.TASK_ID IN (%TASKIDS%) AND UD.ACTIVE_INDICATOR = 'Y' ";
	//AND URM.DEFAULT_ROLE = 'Y'
	// insert into notification_header
	private static final String INSERT_INTO_NOTIFICATION_HEADER = "INSERT INTO NOTIFICATION_HEADER "
			+ "(NOTIFICATION_ID, MODULE_ID,NOTIFICATION_TYPE, NOTIFICATION_KEY1,NOTIFICATION_KEY2,NOTIFICATION_KEY3, NOTIFICATION_KEY4, NOTIFICATION_TIME) VALUES (?,?,?,?,?,?,?,SYSDATE)";
	// insert notification_detail
	private static final String INSERT_INTO_NOTIFICATION_DETAILS = "INSERT INTO NOTIFICATION_DETAIL"
			+ "(NOTIFICATION_DETAIL_ID, NOTIFICATION_ID, USER_ID, IS_MAIL_SEND, IS_NEW) VALUES (NOTIFICATION_DETAIL_SEQUENCE.NEXTVAL, ?, ?, ?, ?)";
	// get notification_id_sequence no
	private static final String GET_NOTIFICATION_ID_SEQUENCE = "SELECT NOTIFICATION_ID_SEQUENCE.NEXTVAL AS NOTIFICATION_ID FROM DUAL";

	static final String GET_SYSTEM_ADMINISTRATOR = "SELECT USER_ID FROM USER_ROLE_MAP WHERE ROLE_ID = ?";

	
	private static final String GET_PARENT_PRODUCT_MAP = "SELECT DISTINCT PRODUCT_LEVEL_ID, PRODUCT_ID FROM "
			+ "(SELECT PRODUCT_LEVEL_ID, PRODUCT_ID FROM PRODUCT_GROUP_RELATION_REC PGR START WITH PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
			+ "CONNECT BY PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND PRIOR   PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) WHERE PRODUCT_LEVEL_ID IN (%s)";
	
	
	static final String GET_USER_CONFIG_SETTINGS = "SELECT USER_ID, PUSH_NOTIFICATION, EMAIL_NOTIFICATION FROM USER_CONFIG_SETTING";
	
	// get values from recommendation_run_header
	/**
	 * To get values related to a run_id where multiple runID values as input
	 * 
	 * @param connection
	 * @param notificationDetail
	 * @return
	 * @throws GeneralException
	 */
	public List<PRRecommendationRunHeader> getNotificationDetails(Connection connection, List<NotificationHeaderDTO> notificationDetail)
			throws GeneralException {
		
		List<PRRecommendationRunHeader> prRecommendationRunHeaderList = new ArrayList<PRRecommendationRunHeader>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<Long> runIds = new ArrayList<Long>();
		for (NotificationHeaderDTO notificationHeaderDTO : notificationDetail) {
			long runId = notificationHeaderDTO.getNotificationKey1();
			if(!runIds.contains(runId)) {
				runIds.add(runId);
			}
		}
		String runIdList = PRCommonUtil.getCommaSeperatedStringFromLongArray(runIds);
		logger.info("runIds: " + runIdList);

		try {
			String query = new String(GET_PR_RECOMMENDATION_RUN_HEADER);
			query = query.replaceAll("%QUERY_CONDITION%", runIdList);
			statement = connection.prepareStatement(query);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				PRRecommendationRunHeader prRecommendationRunHeader = new PRRecommendationRunHeader();
				prRecommendationRunHeader.setRunId(resultSet.getLong("RUN_ID"));
				prRecommendationRunHeader.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				prRecommendationRunHeader.setProductId(resultSet.getInt("PRODUCT_ID"));
				prRecommendationRunHeader.setCurrentStatusWorkflowLevel(resultSet.getInt("CURRENT_WF_LEVEL"));
				prRecommendationRunHeader.setCurrentStatusRoleId(resultSet.getInt("CURRENT_ROLE_ID"));
				prRecommendationRunHeader.setApprovalRoleId(resultSet.getInt("APPROVAL_ROLE_ID"));
				prRecommendationRunHeader.setApprovalWorkflowLevel(resultSet.getInt("APPROVAL_WF_LEVEL"));
				prRecommendationRunHeaderList.add(prRecommendationRunHeader);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getNotificationDetails " + e.toString(), e);
			throw new GeneralException("Error while executing getNotificationDetails " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return prRecommendationRunHeaderList;

	}

	/**
	 * Get all the user_id list for the given parameters
	 * 
	 * @param connection
	 * @param locationList
	 * @param productList
	 * @return
	 * @throws GeneralException
	 */
	public List<UserTaskDTO> getUserIds(Connection connection) throws GeneralException {
		List<UserTaskDTO> userTaskDTOs = new ArrayList<UserTaskDTO>();
		List<Integer> analysisTaskId = new ArrayList<Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			/*String[] prAnalysisRoleId = PropertyManager.getProperty("PR_ANALYSIS_ROLE_ID").split(",");
			for (String roleId : prAnalysisRoleId) {
				analysisRoleId.add(Integer.parseInt(roleId));
			}*/
			//String roleIdValue = PRCommonUtil.getCommaSeperatedStringFromIntArray(analysisRoleId);
//			statement.setString(++counter, roleIdValue);
			
			String[] notificationTaskId = PropertyManager.getProperty("NOTIFICATION_SERVICE_TASK_ID").split(",");
			for (String taskID : notificationTaskId) {
				analysisTaskId.add(Integer.parseInt(taskID));
			}
			String taskIdValues = PRCommonUtil.getCommaSeperatedStringFromIntArray(analysisTaskId);
//			statement.setString(++counter,taskIdValues);

			String query = new String(GET_USER_ID);
			//query = query.replaceAll("%ROLEIDS%", roleIdValue);
			query = query.replaceAll("%TASKIDS%", taskIdValues);
			statement = connection.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				UserTaskDTO userTaskDTO = new UserTaskDTO();
				userTaskDTO.setUserId(resultSet.getString("USER_ID"));
				userTaskDTO.setValueType(resultSet.getString("VALUE_TYPE"));
				userTaskDTO.setValue(resultSet.getString("VALUE"));
				userTaskDTO.setRoleId(resultSet.getInt("ROLE_ID"));
				userTaskDTO.setWorkflowLevel(resultSet.getInt("WORKFLOW_LEVEL"));
				if (String.valueOf(Constants.YES).equals(resultSet.getString("ENABLE_NOTIFICATION"))) {
					userTaskDTO.setNotificationEnabled(true);
				} else {
					userTaskDTO.setNotificationEnabled(false);
				}
				
				if (String.valueOf(Constants.YES).equals(resultSet.getString("NOTIFY_EMAIL"))) {
					userTaskDTO.setEmailEnabled(true);
				} else {
					userTaskDTO.setEmailEnabled(false);
				}
				
				userTaskDTOs.add(userTaskDTO);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getUserIds() " + e.toString(), e);
			throw new GeneralException("Error while executing getUserIds() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return userTaskDTOs;
	}

	/**
	 * Insert into notification header
	 * 
	 * @param connection
	 * @param notificationId
	 * @param runId
	 * @throws GeneralException
	 */
	public void insertNotificationHeader(Connection connection, List<NotificationHeaderDTO> notificationHeaders) throws GeneralException {
		PreparedStatement statement = null;
		// Loop each RunId to get their list of values

		try {

			statement = connection.prepareStatement(INSERT_INTO_NOTIFICATION_HEADER);
			int itemNoInBatch = 0;
			for (NotificationHeaderDTO notificationHeader : notificationHeaders) {
				// Get the notification sequence value
				long notificationId = getNotificationIdSequence(connection);
				notificationHeader.setNotificationId(notificationId);
				int counter = 0;
				statement.setLong(++counter, notificationId);
				statement.setInt(++counter, notificationHeader.getModuleId());
				statement.setLong(++counter, notificationHeader.getNotificationTypeId());
				statement.setLong(++counter, notificationHeader.getNotificationKey1());
				statement.setLong(++counter, notificationHeader.getNotificationKey2());
				statement.setLong(++counter, notificationHeader.getNotificationKey3());
				statement.setLong(++counter, notificationHeader.getNotificationKey4());
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
		} catch (SQLException e) {
			logger.error("Error in insertNotificationHeader()" + e.toString(), e);
			throw new GeneralException("Error in insertNotificationHeader() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Insert into notification header
	 * 
	 * @param connection
	 * @param notificationId
	 * @param runId
	 * @throws GeneralException
	 */
	public long insertNotificationHeaderV2(Connection connection, NotificationHeaderDTO notificationHeader) throws GeneralException {
		PreparedStatement statement = null;
		long notificationId = 0;
		// Loop each RunId to get their list of values

		try {
			statement = connection.prepareStatement(INSERT_INTO_NOTIFICATION_HEADER);
			int itemNoInBatch = 0;
				// Get the notification sequence value
				notificationId = getNotificationIdSequence(connection);
				notificationHeader.setNotificationId(notificationId);
				int counter = 0;
				statement.setLong(++counter, notificationId);
				statement.setInt(++counter, notificationHeader.getModuleId());
				statement.setLong(++counter, notificationHeader.getNotificationTypeId());
				statement.setLong(++counter, notificationHeader.getNotificationKey1());
				statement.setLong(++counter, notificationHeader.getNotificationKey2());
				statement.addBatch();
				itemNoInBatch++;
				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}


			if (itemNoInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			notificationId = 0;
			logger.error("Error in insertNotificationHeader()" + e.toString(), e);
			throw new GeneralException("Error in insertNotificationHeader() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
		
		return notificationId;
	}	
	
	/**
	 * Insert user_id along with the notificationId
	 * 
	 * @param connection
	 * @param notificationHeaderDTOs
	 * @param userList
	 * @throws GeneralException
	 */
	public void insertNotificationDetails(Connection connection, List<NotificationDetailDTO> notificationDetailDTOs) throws GeneralException {
		PreparedStatement statement = null;
		try {
			int itemNoInBatch = 0;
			statement = connection.prepareStatement(INSERT_INTO_NOTIFICATION_DETAILS);
			for (NotificationDetailDTO notificationDetailDTO : notificationDetailDTOs) {
				
				int counter = 0;
				statement.setLong(++counter, notificationDetailDTO.getNotificationId());
				statement.setString(++counter, notificationDetailDTO.getUserId());
				statement.setString(++counter, String.valueOf(notificationDetailDTO.getIsEMailSend()));
				statement.setString(++counter, String.valueOf(notificationDetailDTO.getIsNotificationSend()));
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

		} catch (SQLException e) {
			logger.error("Error in insertNotificationDetails()" + e.toString(), e);
			throw new GeneralException("Error in insertNotificationDetails() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Get notification_id_sequence number
	 * 
	 * @param connection
	 * @throws GeneralException
	 */
	public long getNotificationIdSequence(Connection connection) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		long sequence = 0;
		try {
			String query = new String(GET_NOTIFICATION_ID_SEQUENCE);
			statement = connection.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				sequence = (resultSet.getLong("NOTIFICATION_ID"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_NOTIFICATION_ID_SEQUENCE " + e.toString(), e);
			throw new GeneralException("Error while executing GET_NOTIFICATION_ID_SEQUENCE " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return sequence;

	}

	/**
	 * 
	 * @param connection
	 * @return
	 * @throws GeneralException
	 */
	public List<String> getSystemAdministrator(Connection connection) throws GeneralException {
		List<String> systemAdminUserIds = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_SYSTEM_ADMINISTRATOR);
			statement = connection.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, Integer.parseInt(PropertyManager.getProperty("SYSTEM_ADMINISTRATOR_ROLE_ID")));

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String userId = resultSet.getString("USER_ID");
				systemAdminUserIds.add(userId);
			}

		} catch (SQLException e) {
			logger.error("Error while executing GET_SYSTEM_ADMINISTRATOR " + e.toString(), e);
			throw new GeneralException("Error while executing GET_SYSTEM_ADMINISTRATOR " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return systemAdminUserIds;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param chiildProducts
	 * @return parent and child product map 
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<ProductKey>> getParentProducts(Connection conn, Set<ProductKey> chiildProducts)
			throws GeneralException {
		HashMap<ProductKey, List<ProductKey>> parentProductMap = new HashMap<>();
		for (ProductKey productKey : chiildProducts) {
			getParentProducts(conn, productKey, parentProductMap);
		}
		return parentProductMap;
	}
	
	/**
	 * 
	 * @param conn
	 * @param productKey
	 * @param parentProductMap
	 * @throws GeneralException 
	 */
	private void getParentProducts(Connection conn, ProductKey productKey, 
			HashMap<ProductKey, List<ProductKey>> parentProductMap) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String preferredParentProductLevels = PropertyManager.getProperty("NOTIFICATION_PREF_PARENT_PRODUCTS",
				Constants.DEPARTMENTLEVELID + "," + Constants.CATEGORYLEVELID);
		try {
			
			String sql = GET_PARENT_PRODUCT_MAP.replaceAll("%s", preferredParentProductLevels);
			statement = conn.prepareStatement(sql);
			int counter = 0;
			statement.setInt(++counter, productKey.getProductLevelId());
			statement.setInt(++counter, productKey.getProductId());
			resultSet = statement.executeQuery();
			while(resultSet.next()) {
				ProductKey parentProductKey = new ProductKey(resultSet.getInt("PRODUCT_LEVEL_ID"), resultSet.getInt("PRODUCT_ID"));
				List<ProductKey> tempList = new ArrayList<>();
				if(parentProductMap.containsKey(productKey)) {
					tempList = parentProductMap.get(productKey);
				}
				tempList.add(parentProductKey);
				parentProductMap.put(productKey, tempList);
			}
		} catch (SQLException sqlE) {
			logger.error("getParentProducts() - Error while geting parent child mapping for  " + productKey.toString(),
					sqlE);
			throw new GeneralException(
					"getParentProducts() - Error while geting parent child mapping for  " + productKey.toString(),
					sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * 
	 * @param conn
	 * @return user config settings
	 * @throws GeneralException
	 */
	public HashMap<String, UserTaskDTO> getUserConfigSettings(Connection conn) throws GeneralException {
		HashMap<String, UserTaskDTO> userConfigSettings = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_USER_CONFIG_SETTINGS);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				UserTaskDTO userTaskDTO = new UserTaskDTO();
				userTaskDTO.setUserId(resultSet.getString("USER_ID"));
				if (String.valueOf(Constants.YES).equals(resultSet.getString("EMAIL_NOTIFICATION"))) {
					userTaskDTO.setEmailEnabled(true);
				} else {
					userTaskDTO.setEmailEnabled(false);
				}

				if (String.valueOf(Constants.YES).equals(resultSet.getString("PUSH_NOTIFICATION"))) {
					userTaskDTO.setNotificationEnabled(true);
				} else {
					userTaskDTO.setNotificationEnabled(false);
				}

				userConfigSettings.put(userTaskDTO.getUserId(), userTaskDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("getUserConfigSettings() - Error while geting user config settings ", sqlE);
			throw new GeneralException("getUserConfigSettings() - Error while geting user config settings ", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return userConfigSettings;
	}

	/*public List<NotificationMailerDTO> getWorkFlowList(Connection connection) throws GeneralException {
		List<NotificationMailerDTO> prRecommendationRunHeaderList = new ArrayList<NotificationMailerDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT ROLE_ID, WORKFLOW_LEVEL, NOTIFY_EMAIL, ENABLE_NOTIFICATION FROM PR_WORKFLOW_LOOKUP ");
			statement = connection.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				NotificationMailerDTO prRecommendationRunHeader = new NotificationMailerDTO();
				prRecommendationRunHeader.setWorkFlowLevel(resultSet.getInt("WORKFLOW_LEVEL"));
				prRecommendationRunHeader.setRoleId(resultSet.getInt("ROLE_ID"));
				prRecommendationRunHeader.setNotifyEmail(resultSet.getString("NOTIFY_EMAIL"));
				prRecommendationRunHeader.setEnableNotif(resultSet.getString("ENABLE_NOTIFICATION"));
				prRecommendationRunHeaderList.add(prRecommendationRunHeader);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getNotificationDetails " + e.toString(), e);
			throw new GeneralException("Error while executing getNotificationDetails " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return prRecommendationRunHeaderList;
	}*/
	
}
