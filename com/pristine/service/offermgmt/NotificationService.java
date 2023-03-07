package com.pristine.service.offermgmt;

import java.sql.Connection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.NotificationDAO;
import com.pristine.dto.offermgmt.NotificationHeaderDTO;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.NotificationDetailDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.UserTaskDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.RecommendationStatusLookup;
import com.pristine.lookup.offermgmt.UserRoleIdLookup;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class NotificationService {
	private static Logger logger = Logger.getLogger("NotificationService");
	NotificationDAO notificationDAO = new NotificationDAO();

	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;

	public int addNotificationsBatch(Connection conn, List<NotificationDetailInputDTO> notificationDetailInputDTOs, boolean sendEmail)
			throws GeneralException, SQLException {
		this.conn = conn;
		int result = 0;
		if (notificationDetailInputDTOs.size() > 0) {
			result = addNotifications(notificationDetailInputDTOs, false, sendEmail);
		}
		return result;
	}

	public int addNotificationsUI(List<NotificationDetailInputDTO> notificationDetailInputDTOs) throws GeneralException, SQLException {
		int result = 0;
		if (notificationDetailInputDTOs.size() > 0) {
			result = addNotifications(notificationDetailInputDTOs, true, true);
		}
		return result;
	}

	/**
	 * To get the NotificationAnalysis information
	 * 
	 * @param connection
	 * @param notificationDetail
	 * @throws GeneralException
	 * @throws SQLException
	 */
	private int addNotifications(List<NotificationDetailInputDTO> notificationDetailInputDTOs, boolean isOnline, boolean sendEmail)
			throws GeneralException, SQLException {
		int result = 0;
		HashMap<Long, PRRecommendationRunHeader> runHeaderMap= new HashMap<Long, PRRecommendationRunHeader>();

		List<NotificationHeaderDTO> notificationHeaderDTOs = new ArrayList<NotificationHeaderDTO>();
		List<String> systemAdminUserIds = new ArrayList<String>();
		try {

			if (isOnline)
				initializeForWS();

			convertObject(notificationDetailInputDTOs, notificationHeaderDTOs);

			// Get values by passing RunIds
			List<PRRecommendationRunHeader> prRecommendationRunHeader = notificationDAO.getNotificationDetails(getConnection(),
					notificationHeaderDTOs);
			
			logger.debug("Header size: " + prRecommendationRunHeader.size());
			
			HashMap<String, UserTaskDTO> userConfigSettings = notificationDAO.getUserConfigSettings(getConnection());
			
			logger.debug("userConfigSettings size: " + userConfigSettings.size());
			
			Set<ProductKey> distinctProducts = getDistinctProducts(prRecommendationRunHeader);
			
			HashMap<ProductKey, List<ProductKey>> parentProductMap = notificationDAO.getParentProducts(getConnection(), distinctProducts);
			
			// get the userId for the given string of product or location list
			HashMap<Long, List<UserTaskDTO>> userList = getUserIDList(getConnection(), prRecommendationRunHeader,
					parentProductMap);
			
			logger.debug("userList size: " + userList.size());
			
			prRecommendationRunHeader.forEach(header -> {
				runHeaderMap.put(header.getRunId(), header);
			});

			// Get system admin user id list
			for (NotificationHeaderDTO notificationHeaderDTO : notificationHeaderDTOs) {
				
				if(runHeaderMap.containsKey(notificationHeaderDTO.getNotificationKey1()))
				{
					notificationHeaderDTO.setNotificationKey2(
							runHeaderMap.get(notificationHeaderDTO.getNotificationKey1()).getCurrentStatusRoleId());
				}
				int notificationType = notificationHeaderDTO.getNotificationTypeId();
				if (notificationType == PRConstants.ERROR_REC || notificationType == PRConstants.NO_PREDICTIONS
						|| notificationType == PRConstants.NO_NEW_PRICES || notificationType == PRConstants.NO_RECOMMENDATION) {
					systemAdminUserIds = notificationDAO.getSystemAdministrator(getConnection());
					break;
				}
			}

			// insert into notification header table
			notificationDAO.insertNotificationHeader(getConnection(), notificationHeaderDTOs);
			// Set values into notification detailDTO
			List<NotificationDetailDTO> notificationDetailDTOs = getNotificationDetailDTOValues(notificationHeaderDTOs, userList, systemAdminUserIds);
			
			logger.debug("notificationDetailDTOs size: " + notificationDetailDTOs.size());
			
			boolean isNotificationStrictModeEnabled = Boolean
					.parseBoolean(PropertyManager.getProperty("NOTIFICATION_STIRCT_MAILING", "FALSE"));
			if(isNotificationStrictModeEnabled) {
				controlNotificationsByWorkflowV2(prRecommendationRunHeader, notificationDetailDTOs,runHeaderMap);	
			}
			
			
			List<NotificationDetailDTO> notificationsFilteredByUserPref = filterNotificationsByUserPref(
					notificationDetailDTOs, userConfigSettings);
			
			
			logger.debug("notificationsFilteredByUserPref size: " + notificationsFilteredByUserPref.size());
			
			// Insert into notification details table
			notificationDAO.insertNotificationDetails(getConnection(), notificationsFilteredByUserPref);

			boolean sendMailEnabled =  false;
			if (sendMailEnabled) {
				new NotificationMailer().sendNotificationMail(getConnection());
				PristineDBUtil.commitTransaction(getConnection(), "Commit Notifications");
			}
		} catch (GeneralException | Exception e) {
			   result = -1;
			   logger.error("Error in notificationAnalysis()" + e.toString(), e);
			   if (isOnline) {
			    PristineDBUtil.rollbackTransaction(getConnection(), "Rollback Notifications");
			   }
			   throw new GeneralException("Error in notificationAnalysis() " + e.toString(), e);
			  } finally {
			   if (isOnline) {
			    PristineDBUtil.close(getConnection());
			   }
			  }
		return result;
	}

	private void convertObject(List<NotificationDetailInputDTO> sourceObject, List<NotificationHeaderDTO> destObject) {
		// Convert NotificationDetailUIDTO to NotificationDetailDTO
		for (NotificationDetailInputDTO notificationDetailUIDTO : sourceObject) {
			NotificationHeaderDTO notificationDetailDTO = new NotificationHeaderDTO();
			notificationDetailDTO.setModuleId(notificationDetailUIDTO.getModuleId());
			notificationDetailDTO.setNotificationKey1(notificationDetailUIDTO.getNotificationKey1());
			notificationDetailDTO.setNotificationTypeId(notificationDetailUIDTO.getNotificationTypeId());
			destObject.add(notificationDetailDTO);
		}
	}
	
	/**
	 * This method updates email send status for each user. 
	 * If sending email to a user is inappropriate as per  workflow, this will disable the email notification 
	 */
	private void controlNotificationsByWorkflowV2(
			List<PRRecommendationRunHeader> prRecommendationRunHeader,
			List<NotificationDetailDTO> notificationDetailDTOs,HashMap<Long, PRRecommendationRunHeader>runHeader) {

		notificationDetailDTOs.forEach(notification -> {
			
			PRRecommendationRunHeader header = runHeader.get(notification.getRunId());
			//logger.info("notification.getRunId(): "  +notification.getRunId());
			int currentWorkFlowLevel = header.getCurrentStatusWorkflowLevel();
			//logger.info("currentWorkFlowLevel: "  +currentWorkFlowLevel);//9
			int currentRoleId = header.getCurrentStatusRoleId();
			//logger.info("currentRoleId: "  +currentRoleId);//2
			int approvalWorkFlowLevel = header.getApprovalWorkflowLevel();
			//logger.info("approvalWorkFlowLevel: "  +approvalWorkFlowLevel);//3
			
			//logger.info("notification.getNotificationTypeId(): " + notification.getNotificationTypeId());
			//logger.info("notification.getUserTaskDTO().getWorkflowLevel(): " + notification.getUserTaskDTO().getWorkflowLevel());
			
			//Notification to Pricing Analyst 
			if (notification.getNotificationTypeId() == RecommendationStatusLookup.RECOMMENDED.getStatusId()) {
				if (notification.getUserTaskDTO().getWorkflowLevel() == 1
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} 
			
			else if (notification.getNotificationTypeId() == RecommendationStatusLookup.REVIEW_COMPLETED
					.getStatusId()) {
				//Complete review, Analyst. Notification to Category Manager
				if (currentRoleId == UserRoleIdLookup.PRICING_ANALYST.getRoleId()) {
					if (notification.getUserTaskDTO().getWorkflowLevel() == 2 && notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				}
				else if (currentRoleId == UserRoleIdLookup.PRICING_MANAGER.getRoleId()) {
					if(((notification.getUserTaskDTO().getWorkflowLevel() == 1) || 
							(notification.getUserTaskDTO().getWorkflowLevel() == 2))
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				}
				//Complete review, others. Notification to next users in workflow order
				else if(currentRoleId == UserRoleIdLookup.PRESTO_APPLICATION_ADMINISTRATOR.getRoleId()) {
					if ((notification.getUserTaskDTO().getWorkflowLevel() == approvalWorkFlowLevel+1)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				} else if(approvalWorkFlowLevel == currentWorkFlowLevel) {
					if ((notification.getUserTaskDTO().getWorkflowLevel() == 1
							|| notification.getUserTaskDTO().getWorkflowLevel() == currentWorkFlowLevel + 1)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				} else {
					//Notification to pricing analyst is constant
					if ((notification.getUserTaskDTO().getWorkflowLevel() == 1
							|| notification.getUserTaskDTO().getWorkflowLevel() == currentWorkFlowLevel + 1)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				}
			} 
			//	Depending on the role that rejects, notification is sent to all lower-level roles
			else if (notification.getNotificationTypeId() == RecommendationStatusLookup.REVIEW_REJECTED
					.getStatusId()) {
				if (currentRoleId == UserRoleIdLookup.PRESTO_APPLICATION_ADMINISTRATOR.getRoleId()) {
					if ((notification.getUserTaskDTO().getWorkflowLevel() < approvalWorkFlowLevel)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				} else if ((notification.getUserTaskDTO().getWorkflowLevel() < currentWorkFlowLevel)
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} 
			//Notifications to Pricing Analyst, Category Manager
			else if (notification.getNotificationTypeId() == RecommendationStatusLookup.APPROVED
					.getStatusId()) {
				logger.debug("email enable? : " + notification.getUserTaskDTO().isEmailEnabled());
				if(((notification.getUserTaskDTO().getWorkflowLevel() == 1) || 
						(notification.getUserTaskDTO().getWorkflowLevel() == 2))
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
				logger.debug("email: " + notification.getIsEMailSend());
				logger.debug("user: " + notification.getUserId() +" role: "+ notification.getUserTaskDTO().getRoleId());
			} 
			//Notifications to Pricing Analyst, Category Manager
			else if (notification.getNotificationTypeId() == RecommendationStatusLookup.EMERGENCY_APPROVED
					.getStatusId()) {
				if(((notification.getUserTaskDTO().getWorkflowLevel() == 1) || 
						(notification.getUserTaskDTO().getWorkflowLevel() == 2))
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} 
			//Notifications to Pricing Analyst, Category Manager
			else if (notification.getNotificationTypeId() == RecommendationStatusLookup.EXPORTED.getStatusId()
					|| notification.getNotificationTypeId() == RecommendationStatusLookup.EXPORTED_PARTIALLY
							.getStatusId()) {
				if(((notification.getUserTaskDTO().getWorkflowLevel() == 1) || 
						(notification.getUserTaskDTO().getWorkflowLevel() == 2))
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}				
				logger.debug("user: " + notification.getUserId() +" role: "+ notification.getUserTaskDTO().getRoleId());
				logger.debug("email: " + notification.getIsEMailSend());
			}
			if (notification.getUserTaskDTO().isNotificationEnabled()
					&& notification.getUserTaskDTO().getWorkflowLevel() == approvalWorkFlowLevel+1) {
				notification.setIsNotificationSend(Constants.YES);
			} else {
				notification.setIsNotificationSend(Constants.NO);
			}
		});
	}

	/**
	 * Retrieve userId for the given parameters
	 * 
	 * @param connection
	 * @param prRecommendationRunHeaderList
	 * @return
	 * @throws GeneralException
	 */
	private HashMap<Long, List<UserTaskDTO>> getUserIDList(Connection connection, 
			List<PRRecommendationRunHeader> prRecommendationRunHeaderList, HashMap<ProductKey, List<ProductKey>> parentProductMap)
			throws GeneralException {
		HashMap<Long, List<UserTaskDTO>> prRecommendationRunHeaderMap = new HashMap<Long, List<UserTaskDTO>>();

		// Get List of User_id and their values from User_Task
		List<UserTaskDTO> userTaskDTOList = notificationDAO.getUserIds(connection);

		// Group by user id and location list and product list as value
		for (PRRecommendationRunHeader prRecommendationRunHeader : prRecommendationRunHeaderList) {
			HashSet<String> userIds = new HashSet<>();
			List<UserTaskDTO> userList = new ArrayList<>();
			
			String allProductsId = "\"p-l-i\":\"" + Constants.ALLPRODUCTS + "\"";
			String productList = "\"p-l-i\":\"" + prRecommendationRunHeader.productLevelId + "\",\"p-i\":\"" + prRecommendationRunHeader.productId
					+ "\"";

			ProductKey productKey = new ProductKey(prRecommendationRunHeader.productLevelId, prRecommendationRunHeader.productId);
			List<ProductKey> parentList = null;
			if(parentProductMap.containsKey(productKey)) {
				parentList = parentProductMap.get(productKey);
			}
			
			for (UserTaskDTO userTaskDTO : userTaskDTOList) {
				String UserId = userTaskDTO.getUserId();
				String UserRoleMap=String.valueOf(userTaskDTO.getRoleId());
				if (userTaskDTO.getValue() != null) {
					if ((userTaskDTO.getValue().contains(productList)) || (userTaskDTO.getValue().contains(allProductsId))) {
						if(!userIds.contains(UserId+"-"+UserRoleMap)) {
							userIds.add(UserId+"-"+UserRoleMap);
							userList.add(userTaskDTO);
						}
					}
					
					if(parentList != null) {
						for(ProductKey parentProd: parentList) {
							String parentProdStr = "\"p-l-i\":\"" + parentProd.getProductLevelId() + "\",\"p-i\":\"" + parentProd.getProductId()
									+ "\"";
							if (userTaskDTO.getValue().contains(parentProdStr)) {
								if(!userIds.contains(UserId+"-"+UserRoleMap)) {
									userIds.add(UserId+"-"+UserRoleMap);
									userList.add(userTaskDTO);
								}
							}
						}
					}
				}
			}

			prRecommendationRunHeaderMap.put(prRecommendationRunHeader.getRunId(), userList);
		}
		return prRecommendationRunHeaderMap;
	}

	private List<NotificationDetailDTO> getNotificationDetailDTOValues(List<NotificationHeaderDTO> notificationHeaderDTOs,
			HashMap<Long, List<UserTaskDTO>> userList, List<String> systemAdminUserIds) {
		List<NotificationDetailDTO> notificationDetailDTOs = new ArrayList<NotificationDetailDTO>();
		for (NotificationHeaderDTO notificationHeaderDTO : notificationHeaderDTOs) {
			int notificationType = notificationHeaderDTO.getNotificationTypeId();
			if (notificationType == PRConstants.ERROR_REC || notificationType == PRConstants.NO_PREDICTIONS
					|| notificationType == PRConstants.NO_NEW_PRICES || notificationType == PRConstants.NO_RECOMMENDATION) {
				for (String userId : systemAdminUserIds) {
					NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
					notificationDetailDTO.setNotificationId(notificationHeaderDTO.getNotificationId());
					notificationDetailDTO.setUserId(userId);
					notificationDetailDTO.setNotificationTypeId(notificationType);
					notificationDetailDTO.setIsNotificationSend(Constants.YES);
					notificationDetailDTO.setIsEMailSend(Constants.NO);
					notificationDetailDTOs.add(notificationDetailDTO);
				}
			} else {
				for (UserTaskDTO user : userList.get(notificationHeaderDTO.getNotificationKey1())) {
					NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
					notificationDetailDTO.setNotificationId(notificationHeaderDTO.getNotificationId());
					notificationDetailDTO.setUserId(user.getUserId());
					notificationDetailDTO.setUserTaskDTO(user);
					notificationDetailDTO.setNotificationTypeId(notificationType);
					notificationDetailDTO.setRunId(notificationHeaderDTO.getNotificationKey1());
					notificationDetailDTO.setIsNotificationSend(Constants.YES);
					notificationDetailDTO.setIsEMailSend(Constants.NO);
					notificationDetailDTOs.add(notificationDetailDTO);
				}
			}
		}
		return notificationDetailDTOs;
	}

	/**
	 * Delete all the duplicate values inside the list
	 * 
	 * @param userIdValues
	 * @return
	 */
	// private HashMap<Long, List<String>> deleteDuplicateUserId(HashMap<Long,
	// List<String>> userIdValues) {
	// HashMap<Long, List<String>> distinctUserIdMap = new HashMap<Long,
	// List<String>>();
	//
	// for (Map.Entry<Long, List<String>> UserIdEntry : userIdValues.entrySet())
	// {
	// List<String> distinctUsedId = new ArrayList<>();
	// HashSet<String> hashSet = new HashSet<String>();
	//
	// long runId = UserIdEntry.getKey();
	// List<String> usedId = UserIdEntry.getValue();
	// hashSet.addAll(usedId);
	// distinctUsedId.addAll(hashSet);
	// distinctUserIdMap.put(runId, distinctUsedId);
	// }
	// return distinctUserIdMap;
	// }

	/**
	 * Initializes connection. Used when program is accessed through webservice
	 */
	protected void initializeForWS() {
		setConnection(getDSConnection());
		System.out.println("Connection : " + getConnection());
	}

	/**
	 * Returns Connection from datasource
	 * 
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));
		;
		try {
			if (ds == null) {
				initContext = new InitialContext();
				envContext = (Context) initContext.lookup("java:/comp/env");
				ds = (DataSource) envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		} catch (NamingException exception) {
			logger.error("Error when creating connection from datasource " + exception.toString());
		} catch (SQLException exception) {
			logger.error("Error when creating connection from datasource " + exception.toString());
		}
		return connection;
	}

	protected Connection getConnection() {
		return conn;
	}

	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes connection
	 */
	protected void initialize() {
		setConnection();
	}
	
	private Set<ProductKey> getDistinctProducts(List<PRRecommendationRunHeader> allProducts){
		Set<ProductKey> distinctProducts = new HashSet<>();
		allProducts.forEach(p -> {
			ProductKey productKey = new ProductKey(p.getProductLevelId(), p.getProductId());
			distinctProducts.add(productKey);
		});
		return distinctProducts;
	}

	
	/**
	 * This method updates email send status for each user. 
	 * If sending email to a user is inappropriate as per  workflow, this will disable the email notification 
	 */
	private void controlNotificationsByWorkflow(
			List<PRRecommendationRunHeader> prRecommendationRunHeader,
			List<NotificationDetailDTO> notificationDetailDTOs,HashMap<Long, PRRecommendationRunHeader>runHeader) {

		//HashMap<Long, PRRecommendationRunHeader> runHeader = new HashMap<>();

		/*
		 * prRecommendationRunHeader.forEach(header -> {
		 * runHeader.put(header.getRunId(), header); });
		 */

		notificationDetailDTOs.forEach(notification -> {
			PRRecommendationRunHeader header = runHeader.get(notification.getRunId());
			int currentWorkFlowLevel = header.getCurrentStatusWorkflowLevel();
			int currentRoleId = header.getCurrentStatusRoleId();
			int approvalWorkFlowLevel = header.getApprovalWorkflowLevel();
			if (notification.getNotificationTypeId() == RecommendationStatusLookup.RECOMMENDED.getStatusId()) {
				if(notification.getUserTaskDTO().getWorkflowLevel() == 1 && notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} else if (notification.getNotificationTypeId() == RecommendationStatusLookup.REVIEW_COMPLETED
					.getStatusId()) {
				if(currentRoleId == UserRoleIdLookup.PRESTO_APPLICATION_ADMINISTRATOR.getRoleId()) {
					if ((notification.getUserTaskDTO().getWorkflowLevel() <= approvalWorkFlowLevel)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				} else if(approvalWorkFlowLevel == currentWorkFlowLevel) {
					if ((notification.getUserTaskDTO().getWorkflowLevel() <= currentWorkFlowLevel)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				} else {
					if ((notification.getUserTaskDTO().getWorkflowLevel() <= currentWorkFlowLevel
							|| notification.getUserTaskDTO().getWorkflowLevel() == currentWorkFlowLevel + 1)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				}
			} else if (notification.getNotificationTypeId() == RecommendationStatusLookup.REVIEW_REJECTED
					.getStatusId()) {
				if (currentRoleId == UserRoleIdLookup.PRESTO_APPLICATION_ADMINISTRATOR.getRoleId()) {
					if ((notification.getUserTaskDTO().getWorkflowLevel() <= approvalWorkFlowLevel)
							&& notification.getUserTaskDTO().isEmailEnabled()) {
						notification.setIsEMailSend(Constants.NO);
					} else {
						notification.setIsEMailSend(Constants.YES);
					}
				} else if ((notification.getUserTaskDTO().getWorkflowLevel() <= currentWorkFlowLevel)
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} else if (notification.getNotificationTypeId() == RecommendationStatusLookup.APPROVED
					.getStatusId()) {
				if ((notification.getUserTaskDTO().getWorkflowLevel() <= approvalWorkFlowLevel)
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} else if (notification.getNotificationTypeId() == RecommendationStatusLookup.EMERGENCY_APPROVED
					.getStatusId()) {
				if (notification.getUserTaskDTO().getWorkflowLevel() <= approvalWorkFlowLevel
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			} else if (notification.getNotificationTypeId() == RecommendationStatusLookup.EXPORTED.getStatusId()
					|| notification.getNotificationTypeId() == RecommendationStatusLookup.EXPORTED_PARTIALLY
							.getStatusId()) {
				if (notification.getUserTaskDTO().getWorkflowLevel() <= approvalWorkFlowLevel
						&& notification.getUserTaskDTO().isEmailEnabled()) {
					notification.setIsEMailSend(Constants.NO);
				} else {
					notification.setIsEMailSend(Constants.YES);
				}
			}
			if (notification.getUserTaskDTO().isNotificationEnabled()
					&& notification.getUserTaskDTO().getWorkflowLevel() <= approvalWorkFlowLevel) {
				notification.setIsNotificationSend(Constants.YES);
			} else {
				notification.setIsNotificationSend(Constants.NO);
			}
		});
	}
	
	/**
	 * 
	 * @param allNotifications
	 * @param userConfigSettings
	 * @return
	 */
	private List<NotificationDetailDTO> filterNotificationsByUserPref(List<NotificationDetailDTO> allNotifications,
			HashMap<String, UserTaskDTO> userConfigSettings) {
		List<NotificationDetailDTO> notificationsFiltered = new ArrayList<>();
		allNotifications.forEach(notification -> {
			if (userConfigSettings.containsKey(notification.getUserId())) {
				UserTaskDTO userTaskDTO = userConfigSettings.get(notification.getUserId());
				if (userTaskDTO.isEmailEnabled() && notification.getIsEMailSend() == Constants.NO) {
					notification.setIsEMailSend(Constants.NO);
				}

				if (userTaskDTO.isNotificationEnabled() && notification.getIsNotificationSend() == Constants.YES) {
					notification.setIsNotificationSend(Constants.YES);
				}

				if (userTaskDTO.isEmailEnabled() || userTaskDTO.isNotificationEnabled()) {
					if (notification.getIsEMailSend() == Constants.NO
							|| notification.getIsNotificationSend() == Constants.YES) {
						if (!userTaskDTO.isEmailEnabled()) {
							// set it as O to not send email to user if user has disabled email notification 
							notification.setIsEMailSend('O');
						}
						if (!userTaskDTO.isNotificationEnabled()) {
							// set it as Y to not send  push  notification  to user if user has disabled push notification
							notification.setIsRead('Y');
						}else
							notification.setIsRead('N');
							
						notificationsFiltered.add(notification);
					}
				}
			}
		});
		return notificationsFiltered;
	}
}
