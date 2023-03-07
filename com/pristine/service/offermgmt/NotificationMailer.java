package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.NotificationMailerDAO;
import com.pristine.dto.offermgmt.NotificationDetailDTO;
import com.pristine.dto.offermgmt.NotificationHeaderDTO;
import com.pristine.dto.offermgmt.NotificationMailerDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.RecommendationStatusLookup;
import com.pristine.service.email.EmailService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class NotificationMailer {
	private Connection connection = null;
	private static Logger logger = Logger.getLogger("NotificationMailer");
	NotificationMailerDAO notificationMailerDAO = new NotificationMailerDAO();

	public static void main(String[] args) {
		NotificationMailer notificationMailer = new NotificationMailer();
		PropertyConfigurator.configure("log4j-notification-mailer.properties");
		PropertyManager.initialize("recommendation.properties");
		notificationMailer.intialSetup();
		try {
			notificationMailer.sendNotificationMail(notificationMailer.connection);
			PristineDBUtil.commitTransaction(notificationMailer.connection, "Commit Transaction");
		} catch (GeneralException | Exception e) {
			logger.error("Error in notificationMailer" + e);
			PristineDBUtil.rollbackTransaction(notificationMailer.connection, "RollBack Transction " + e);
			e.printStackTrace();
		} finally {

			PristineDBUtil.close(notificationMailer.connection);

		}
	}

	// StringBuilder stringBuilder = new StringBuilder();
	public void sendNotificationMail(Connection connection) throws GeneralException, Exception {
		logger.info("sendNotificationMail() is started");
		// Get user details to send notification mail
		List<NotificationDetailDTO> oldUserIdList = new ArrayList<NotificationDetailDTO>();
		List<NotificationMailerDTO> userDetails = notificationMailerDAO.getUserDetails(connection);
		List<NotificationMailerDTO> sortedByLatestProductAndZone = getLatestProductAndZoneList(userDetails,
				oldUserIdList);
		List<NotificationMailerDTO> sortedUserDetails = getLatestDateTimeRunIds(sortedByLatestProductAndZone);
		// Group by userId
		HashMap<String, List<NotificationMailerDTO>> groupedByUserId = groupByUserId(sortedUserDetails);
		logger.info("Sending email to each user");
		// Send email to each users
		HashMap<Long, List<NotificationDetailDTO>> userDetailsList = sendEmailToUsers(groupedByUserId);
		// Get list to update status in notification_detail
		List<NotificationDetailDTO> notificationDetailList = getNotificationDetailList(userDetailsList, oldUserIdList);

		// Update notification detail
		notificationMailerDAO.updateIsEMailSendStatus(connection, notificationDetailList);
		// Get details to update in notification_header
		List<NotificationHeaderDTO> isNotificationSend = getIsNotificationSendList(userDetailsList, oldUserIdList);

		// Update notification header
		notificationMailerDAO.updateIsNotificationSendStatus(connection, isNotificationSend);
	}

	/**
	 * Sort by latest time to get latest notificationType for each UserId.
	 * 
	 * @param userDetails
	 * @return
	 */
	private List<NotificationMailerDTO> getLatestDateTimeRunIds(List<NotificationMailerDTO> userDetails) {
		List<NotificationMailerDTO> sortedUserDetails = new ArrayList<NotificationMailerDTO>();
		HashMap<String, List<NotificationMailerDTO>> groupByUserId = new HashMap<String, List<NotificationMailerDTO>>();
		// Group by user id if it is not error notification type
		for (NotificationMailerDTO notificationMailerDTOList : userDetails) {
			int notificationType = notificationMailerDTOList.getNotificationType();
			// If notificationType falls into any of error category add them
			// directly in list
			if (notificationType == PRConstants.ERROR_REC || notificationType == PRConstants.NO_PREDICTIONS
					|| notificationType == PRConstants.NO_NEW_PRICES
					|| notificationType == PRConstants.NO_RECOMMENDATION) {
				sortedUserDetails.add(notificationMailerDTOList);
			} else {
				// Group by User id
				List<NotificationMailerDTO> tList = new ArrayList<NotificationMailerDTO>();
				if (groupByUserId.get(notificationMailerDTOList.getUserId()) != null) {
					tList = groupByUserId.get(notificationMailerDTOList.getUserId());
				}
				tList.add(notificationMailerDTOList);
				groupByUserId.put(notificationMailerDTOList.getUserId(), tList);
			}
		}
		// Loop each User Id
		for (Map.Entry<String, List<NotificationMailerDTO>> userIdEntry : groupByUserId.entrySet()) {
			HashMap<Long, List<NotificationMailerDTO>> groupByRunID = new HashMap<Long, List<NotificationMailerDTO>>();
			List<NotificationMailerDTO> groupedByUserIdList = userIdEntry.getValue();
			// Group by RunId number
			for (NotificationMailerDTO notificationMailerDTO : groupedByUserIdList) {
				List<NotificationMailerDTO> tList = new ArrayList<NotificationMailerDTO>();
				if (groupByRunID.get(notificationMailerDTO.getRunId()) != null) {
					tList = groupByRunID.get(notificationMailerDTO.getRunId());
				}
				tList.add(notificationMailerDTO);
				groupByRunID.put(notificationMailerDTO.getRunId(), tList);
			}
			// Loop each run id
			for (Map.Entry<Long, List<NotificationMailerDTO>> runIDEntry : groupByRunID.entrySet()) {
				List<NotificationMailerDTO> groupedByRunIdList = runIDEntry.getValue();
				// Sort by datetime and get only latest time value only for each
				// runId
				sortByDateTime(groupedByRunIdList);
				for (NotificationMailerDTO notificationMailerDTO : groupedByRunIdList) {
					sortedUserDetails.add(notificationMailerDTO);
					break;
				}
			}
		}

		return sortedUserDetails;

	}

	/**
	 * 
	 * @param isMailSend
	 * @param notificationDetailList
	 * @return
	 */
	private List<NotificationDetailDTO> getNotificationDetailList(HashMap<Long, List<NotificationDetailDTO>> isMailSend,
			List<NotificationDetailDTO> notProcesssedList) {
		List<NotificationDetailDTO> notificationDetailList = new ArrayList<NotificationDetailDTO>();
		for (Map.Entry<Long, List<NotificationDetailDTO>> notificationIdEntry : isMailSend.entrySet()) {
			List<NotificationDetailDTO> notificationId = notificationIdEntry.getValue();
			for (NotificationDetailDTO notificationDetailDTOs : notificationId) {
				NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
				notificationDetailDTO.setNotificationId(notificationDetailDTOs.getNotificationId());
				notificationDetailDTO.setUserId(notificationDetailDTOs.getUserId());
				notificationDetailDTO.setIsEMailSend(notificationDetailDTOs.getIsEMailSend());
				notificationDetailList.add(notificationDetailDTO);
			}
		}
		// Loop not processed list and set the values.
		for (NotificationDetailDTO notificationDetailDTOList : notProcesssedList) {
			NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
			notificationDetailDTO.setNotificationId(notificationDetailDTOList.getNotificationId());
			notificationDetailDTO.setUserId(notificationDetailDTOList.getUserId());
			notificationDetailDTO.setIsEMailSend(notificationDetailDTOList.getIsEMailSend());
			notificationDetailList.add(notificationDetailDTO);
		}
		return notificationDetailList;

	}

	/**
	 * Get list of notification header details to update
	 * 
	 * @param isMailSend
	 * @return
	 */
	public List<NotificationHeaderDTO> getIsNotificationSendList(HashMap<Long, List<NotificationDetailDTO>> isMailSend,
			List<NotificationDetailDTO> notProcesssedList) {
		List<NotificationHeaderDTO> notificationHeaderDTOs = new ArrayList<NotificationHeaderDTO>();

		HashMap<Long, List<NotificationDetailDTO>> groupByNotificationId = new HashMap<Long, List<NotificationDetailDTO>>();
		// Function check condition and update isNotificationSend status
		// according to mail status
		for (Map.Entry<Long, List<NotificationDetailDTO>> notificationIdEntry : isMailSend.entrySet()) {
			NotificationHeaderDTO notificationHeaderDTO = new NotificationHeaderDTO();
			notificationHeaderDTO.setNotificationId(notificationIdEntry.getKey());
			notificationHeaderDTO.setIsNotificationSend(Constants.YES);
			List<NotificationDetailDTO> notificationDetailDTOs = notificationIdEntry.getValue();
			// For any one user id haven't send a email for particular
			// notification id set notification status as N
			for (NotificationDetailDTO notificationDetailDTO : notificationDetailDTOs) {
				if (notificationDetailDTO.getIsEMailSend() == Constants.NO) {
					notificationHeaderDTO.setIsNotificationSend(Constants.NO);
					break;
				}
			}
			notificationHeaderDTOs.add(notificationHeaderDTO);
		}
		// Loop each Not processed List and Group by Notification Id and set
		// isNotificationSend status
		for (NotificationDetailDTO notificationDetailDTO : notProcesssedList) {
			List<NotificationDetailDTO> notificationIdList = new ArrayList<NotificationDetailDTO>();
			if (groupByNotificationId.get(notificationDetailDTO.getNotificationId()) != null) {
				notificationIdList = groupByNotificationId.get(notificationDetailDTO.getNotificationId());
			}
			notificationIdList.add(notificationDetailDTO);
			groupByNotificationId.put(notificationDetailDTO.getNotificationId(), notificationIdList);
		}
		// For each Notification id set IsNotificationSend values
		for (Map.Entry<Long, List<NotificationDetailDTO>> notificationIdMap : groupByNotificationId.entrySet()) {
			NotificationHeaderDTO notificationHeaderDTO = new NotificationHeaderDTO();
			notificationHeaderDTO.setNotificationId(notificationIdMap.getKey());
			notificationHeaderDTO.setIsNotificationSend(Constants.YES);
			notificationHeaderDTOs.add(notificationHeaderDTO);
		}
		return notificationHeaderDTOs;

	}

	/**
	 * Group by User ID
	 * 
	 * @param userDetails
	 * @return
	 * @throws Exception
	 */
	private HashMap<String, List<NotificationMailerDTO>> groupByUserId(Collection<NotificationMailerDTO> userDetails)
			throws Exception {
		HashMap<String, List<NotificationMailerDTO>> groupByUserID = new HashMap<String, List<NotificationMailerDTO>>();

		try {
			for (NotificationMailerDTO notificationMailerDTO : userDetails) {
				List<NotificationMailerDTO> userIdList = new ArrayList<NotificationMailerDTO>();

				if (groupByUserID.get(notificationMailerDTO.getUserId()) != null) {
					userIdList = groupByUserID.get(notificationMailerDTO.getUserId());
				}
				userIdList.add(notificationMailerDTO);
				groupByUserID.put(notificationMailerDTO.getUserId(), userIdList);
			}
			return groupByUserID;

		} catch (Exception ex) {
			logger.error("Error while executing groupByEmailID() " + ex);
			throw new Exception("Error in grouping Email ID items - " + ex);
		}

	}

	/**
	 * Send email for user
	 * 
	 * @param userIDMap
	 * @throws Exception
	 * @throws GeneralException
	 */
	private HashMap<Long, List<NotificationDetailDTO>> sendEmailToUsers(
			HashMap<String, List<NotificationMailerDTO>> userIDMap) throws Exception, GeneralException {

		HashMap<Long, List<NotificationDetailDTO>> groupByNotificationId = new HashMap<Long, List<NotificationDetailDTO>>();

		String fromAddr = PropertyManager.getProperty("MAIL.FROMADDR");
		String envName = PropertyManager.getProperty("MAIL.ENVIRONMENT");
		// Grouping by Notification Type by looping each EMail Id and adding
		// content to send mail

		for (Map.Entry<String, List<NotificationMailerDTO>> userIDListMap : userIDMap.entrySet()) {
			HashMap<String, List<NotificationMailerDTO>> groupByNotificationTypeList = new HashMap<String, List<NotificationMailerDTO>>();
			//HashMap<String, String> notificationeAndUsernameMap = new HashMap<String, String>();
			StringBuilder sb = new StringBuilder();
			List<String> EMailId = new ArrayList<String>();
			// Get header Content
			mailHeader(sb);
			String mailId = null;
			StringBuilder subject = new StringBuilder("Presto Notification ");
			if (envName != null) {
				
				subject.append(" - " + envName);
			}

		
			// Group notification types
			List<NotificationMailerDTO> userIdValues = userIDListMap.getValue();
			// Group by notification type
			try {
				for (NotificationMailerDTO notificationMailerDTO : userIdValues) {
					// Get the emailId values
					mailId = notificationMailerDTO.getmailId();
					// Group by Notification type
					List<NotificationMailerDTO> notificationTypeList = new ArrayList<NotificationMailerDTO>();
					String key = notificationMailerDTO.getNotificationType() + "-"
							+ notificationMailerDTO.getUserRollName() + "-" + notificationMailerDTO.getUserName();
					if (groupByNotificationTypeList.get(key) != null) {
						notificationTypeList = groupByNotificationTypeList.get(key);
					}
					notificationTypeList.add(notificationMailerDTO);
					groupByNotificationTypeList.put(key, notificationTypeList);
				}
				mailBody(sb, groupByNotificationTypeList);

			} catch (Exception ex) {
				logger.error("Error while executing sendEmailToUsers()", ex);
				throw new Exception("Error while executing sendEmailToUsers()", ex);

			}
			// Get the email id from string
			logger.info("# Adding mail id: " + mailId);
			EMailId.add(mailId);
			// Send Email...
			// boolean isEmailSend = true;
			boolean isMailIdNull = true;
			boolean isEmailSend = false;
			if (mailId != null) {
				logger.info("# users to send mail" + EMailId.size());
				isEmailSend = EmailService.sendEmailAsHTML(fromAddr, EMailId, null, null, subject.toString(), sb.toString(), null, false);
				isMailIdNull = false;
			}
			for (NotificationMailerDTO notificationMailerDTO : userIdValues) {
				NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
				List<NotificationDetailDTO> notificationIdList = new ArrayList<NotificationDetailDTO>();
				notificationDetailDTO.setNotificationId(notificationMailerDTO.getNotificationId());
				notificationDetailDTO.setUserId(notificationMailerDTO.getUserId());
				if (isEmailSend || isMailIdNull) {
					notificationDetailDTO.setIsEMailSend(Constants.YES);
				} else if (!isEmailSend) {
					notificationDetailDTO.setIsEMailSend(Constants.NO);
				}
				if (groupByNotificationId.get(notificationMailerDTO.getNotificationId()) != null) {
					notificationIdList = groupByNotificationId.get(notificationMailerDTO.getNotificationId());
				}
				notificationIdList.add(notificationDetailDTO);
				groupByNotificationId.put(notificationMailerDTO.getNotificationId(), notificationIdList);
			}

		}
		logger.info("Email Send to listed users");

		return groupByNotificationId;
	}

	/**
	 * 
	 * @param sb
	 * @param groupByNotificationTypeList
	 * @return
	 */
	private StringBuilder mailBody(StringBuilder sb,
			HashMap<String, List<NotificationMailerDTO>> groupByNotificationTypeList) {
		String notificationMailerLink = PropertyManager.getProperty("NOTIFICATION_MAILER_LINK");
		// Form mail content
		for (Map.Entry<String, List<NotificationMailerDTO>> notificationTypeListMap : groupByNotificationTypeList
				.entrySet()) {
			HashMap<String, List<NotificationMailerDTO>> groupByProductName = new HashMap<String, List<NotificationMailerDTO>>();
			int notificationTypeId = Integer.parseInt(notificationTypeListMap.getKey().split("-")[0]);
			String userRoleName = Constants.EMPTY;
			String userName = Constants.EMPTY;
			
			if (notificationTypeListMap.getKey().split("-").length > 1) {
				userRoleName = notificationTypeListMap.getKey().split("-")[1];
			}
			if (notificationTypeListMap.getKey().split("-").length > 2) {
				userName = notificationTypeListMap.getKey().split("-")[2];
			}

			String notificationTypeName = getNotificationName(notificationTypeId, userRoleName, userName);
			sb.append("<br>");
			sb.append(notificationTypeName);
			sb.append("<br>");
			List<NotificationMailerDTO> notificationTypeValues = notificationTypeListMap.getValue();

			// Group by Product name
			for (NotificationMailerDTO notificationMailerDTO : notificationTypeValues) {
				List<NotificationMailerDTO> productNameList = new ArrayList<NotificationMailerDTO>();
				String prodAndApprName=notificationMailerDTO.getProductName()+";"+notificationMailerDTO.getApproverRoleName();
				if (groupByProductName.get(prodAndApprName) != null) {
					productNameList = groupByProductName.get(prodAndApprName);
				}
				productNameList.add(notificationMailerDTO);
				groupByProductName.put(prodAndApprName, productNameList);

			}

			// Loop each product name and add zone numbers
			for (Map.Entry<String, List<NotificationMailerDTO>> finalEntryMap : groupByProductName.entrySet()) {
				List<String> zoneNumbers = new ArrayList<String>();
				List<String> recIds = new ArrayList<String>();
				List<Long> collectionOfRunID = new ArrayList<Long>();
				String productName = Constants.EMPTY;
				String approvalName=Constants.EMPTY;
				
				if (finalEntryMap.getKey().split(";").length > 0) {
					productName = finalEntryMap.getKey().split(";")[0];
				}
				if (finalEntryMap.getKey().split(";").length > 1) {
					approvalName = finalEntryMap.getKey().split(";")[1];
				}

				List<NotificationMailerDTO> zoneNumber = finalEntryMap.getValue();
				// Looping each zone to add zone number
				for (NotificationMailerDTO notificationMailerDTO : zoneNumber) {
					long runId = notificationMailerDTO.getRunId();
					String zoneNum = notificationMailerDTO.getZoneNumber();
					// 03/31/2021 - Changes by Imran - Include parameter FM=Y - to identify navigation from mail
					String zoneNumberValue = "<a href =" + notificationMailerLink + runId + "&FM=Y>" + zoneNum + "</a>";
					String recIDValue = String.valueOf(runId);
					zoneNumbers.add(zoneNumberValue);
					recIds.add(recIDValue);
					// adding all the run id values
					collectionOfRunID.add(notificationMailerDTO.getRunId());
				}
				String productNameList = getUnderScoreSeperatedStringFromIntArray(collectionOfRunID);
				// Forming link to product name by adding all the related run id
				sb.append("<a href =" + notificationMailerLink + productNameList + "&FM=Y>" + productName + "</a>(");
				String zoneList = PRCommonUtil.getCommaSeperatedStringFromStrArray(zoneNumbers);
				String recIDList = PRCommonUtil.getCommaSeperatedStringFromStrArray(recIds);
				sb.append("Zones: ");
				sb.append(zoneList);
				sb.append(" Rec Id: ");
				sb.append(recIDList);
				sb.append(") ");
				if (notificationTypeId == RecommendationStatusLookup.REVIEW_COMPLETED.getStatusId()) {
					sb.append("Approval Role: " + approvalName);
				}
				sb.append("<br>");

			}

		}
		sb.append("<br>");
		sb.append("-Presto Support team");
		sb.append("<br>");
		sb.append("<br>");
		sb.append(Constants.MAIL_CONFIDENTIAL_NOTE);
		sb.append("</font></body>");

		return sb;

	}

	/**
	 * Set header values to string builder
	 * 
	 * @param stringBuilder
	 * @return
	 */
	private StringBuilder mailHeader(StringBuilder stringBuilder) {

		stringBuilder.append("<body><font face=\"Calibri\"><font color= green><font size=\"6\">Presto</font></font>");
		stringBuilder.append("<br>");
		stringBuilder.append("______________________________________________________________________________");
		stringBuilder.append("<br>");

		return stringBuilder;

	}

	/**
	 * Get the notification type Values
	 * 
	 * @param notificationType @return()
	 */
	public String getNotificationName(int notificationType, String userRoleName, String userName) {
		String notificationTypeName = null;
		switch (notificationType) {
		case 1:
			if (!Constants.EMPTY.equals(userRoleName))
				notificationTypeName = "Recommendation completed by " + userRoleName
						+ " for the following product and location combinations";
			else
				notificationTypeName = "Recommendation completed for the following product and location combinations";
			break;
		case 2:
			notificationTypeName = "Review in Progress for the following product and location combinations";
			break;
		case 3:
			notificationTypeName = "Review Completed by " + userName + " (" + userRoleName
					+ ") for the following product and location combinations";
			break;
		case 4:
			notificationTypeName = "Review Rejected by "+ userName + " (" +userRoleName
					+ ") for the following product and location combinations";
			break;
		case 5:
			notificationTypeName = "Error for the following product and location combinations";
			break;
		case 6:
			notificationTypeName = "Prices Approved by " + userName + " (" +userRoleName
					+ ") for the following product and location combinations";
			break;
		case 7:
			notificationTypeName = "Exported partially by " + userRoleName
					+ " for the following product and location combinations";
			break;
		case 8:
			notificationTypeName = "Exported for the following product and location combinations";
			break;
		case 9:
			notificationTypeName = "Effective Price Date is overridden for following product and location combinations";
			break;
		case 10:
			notificationTypeName = "Emergency approved by " + userName + " (" +userRoleName
					+ ") for the following product and location combinations";
			break;
		}
		return notificationTypeName;

	}

	/**
	 * Get underScore separated string
	 * 
	 * @param input
	 * @return
	 */
	public String getUnderScoreSeperatedStringFromIntArray(List<Long> input) {
		String output = "";

		if (input != null && input.size() > 0) {
			for (Long d : input) {
				output = output + "_" + d;
			}
			output = output.substring(1);
		}

		return output;
	}

	public void intialSetup() {
		initialize();
	}

	protected void setConnection() {
		if (connection == null) {
			try {
				connection = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.connection = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}

	/**
	 * Sort by date and time
	 * 
	 * @param notificationMailerDTO
	 */
	public void sortByDateTime(List<NotificationMailerDTO> notificationMailerDTO) {
		final SimpleDateFormat sdf = new SimpleDateFormat("MM/DD/yyyy HH:mm:ss");

		Collections.sort(notificationMailerDTO, new Comparator<NotificationMailerDTO>() {
			public int compare(final NotificationMailerDTO notificationMailerDTO1,
					final NotificationMailerDTO notificationMailerDTO2) {
				Date existingDate;
				Date newDate;
				int compare = 0;

				try {

					if (notificationMailerDTO1.getNotificationTime() == null
							&& notificationMailerDTO2.getNotificationTime() == null) {
						compare = 0;
					} else if (notificationMailerDTO1.getNotificationTime() == null
							&& notificationMailerDTO2.getNotificationTime() != null) {
						compare = -1;
					} else if (notificationMailerDTO1.getNotificationTime() != null
							&& notificationMailerDTO2.getNotificationTime() == null) {
						compare = 1;
					} else {
						existingDate = sdf.parse(notificationMailerDTO2.getNotificationTime());
						newDate = sdf.parse(notificationMailerDTO1.getNotificationTime());
						compare = existingDate.compareTo(newDate);
					}
				} catch (Exception ex) {
					logger.error("Error while executing sortByDateTime() " + ex);
					try {
						throw new OfferManagementException("Error in sortByDateTime() - " + ex,
								RecommendationErrorCode.GENERAL_EXCEPTION);
					} catch (OfferManagementException e) {
						e.printStackTrace();
					}
				}
				return compare;
			}
		});
	}

	/**
	 * 
	 * @param userDetails
	 * @param oldUserIdList
	 * @return
	 */
	private List<NotificationMailerDTO> getLatestProductAndZoneList(List<NotificationMailerDTO> userDetails,
			List<NotificationDetailDTO> oldUserIdList) {
		List<NotificationMailerDTO> sortedLatestRunids = new ArrayList<NotificationMailerDTO>();
		HashMap<Integer, List<NotificationMailerDTO>> groupByProductId = new HashMap<Integer, List<NotificationMailerDTO>>();
		// Group By Product Name
		for (NotificationMailerDTO notificationMailerDTO : userDetails) {

			List<NotificationMailerDTO> productNameList = new ArrayList<NotificationMailerDTO>();

			if (groupByProductId.get(notificationMailerDTO.getProductId()) != null) {
				productNameList = groupByProductId.get(notificationMailerDTO.getProductId());
			}
			productNameList.add(notificationMailerDTO);
			groupByProductId.put(notificationMailerDTO.getProductId(), productNameList);

		}
		for (Map.Entry<Integer, List<NotificationMailerDTO>> productNameMap : groupByProductId.entrySet()) {
			HashMap<String, List<NotificationMailerDTO>> groupByZoneNum = new HashMap<String, List<NotificationMailerDTO>>();
			List<NotificationMailerDTO> productIdList = productNameMap.getValue();
			// Group By Zone Numbers
			for (NotificationMailerDTO notificationMailerDTO : productIdList) {
				List<NotificationMailerDTO> zoneNumList = new ArrayList<NotificationMailerDTO>();

				if (groupByZoneNum.get(notificationMailerDTO.getZoneNumber()) != null) {
					zoneNumList = groupByZoneNum.get(notificationMailerDTO.getZoneNumber());
				}
				zoneNumList.add(notificationMailerDTO);
				groupByZoneNum.put(notificationMailerDTO.getZoneNumber(), zoneNumList);
			}
			for (Map.Entry<String, List<NotificationMailerDTO>> zoneNumberMap : groupByZoneNum.entrySet()) {
				List<NotificationMailerDTO> zoneNumberList = zoneNumberMap.getValue();
				// Sort By date and time and get only latest values for each
				// zone number
				sortByDateTime(zoneNumberList);
				int count = 0;
				long notificationId = 0;

				long notificationKey1 = 0;
				for (NotificationMailerDTO notificationMailerDTO : zoneNumberList) {
					long notificationType = notificationMailerDTO.getNotificationType();
					// If Notification Type is error, no prediction, no new
					// price or no recommendation then it must be added
					// to the list
					if (notificationType == PRConstants.ERROR_REC || notificationType == PRConstants.NO_PREDICTIONS
							|| notificationType == PRConstants.NO_NEW_PRICES
							|| notificationType == PRConstants.NO_RECOMMENDATION) {
						// TO get only latest error notification add the values
						// in list according to latest Run id.
						if (count == 0 || notificationKey1 == notificationMailerDTO.getRunId()) {
							sortedLatestRunids.add(notificationMailerDTO);
							notificationKey1 = notificationMailerDTO.getRunId();
							
						}else {
							NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
							notificationDetailDTO.setNotificationId(notificationMailerDTO.getNotificationId());
							notificationDetailDTO.setUserId(notificationMailerDTO.getUserId());
							notificationDetailDTO.setIsEMailSend(Constants.YES);
							oldUserIdList.add(notificationDetailDTO);
						}
					} else if (count == 0 || notificationId == notificationMailerDTO.getNotificationId()) {
						notificationId = notificationMailerDTO.getNotificationId();
						sortedLatestRunids.add(notificationMailerDTO);
						count++;
					} else {
						NotificationDetailDTO notificationDetailDTO = new NotificationDetailDTO();
						notificationDetailDTO.setNotificationId(notificationMailerDTO.getNotificationId());
						notificationDetailDTO.setUserId(notificationMailerDTO.getUserId());
						notificationDetailDTO.setIsEMailSend(Constants.YES);
						oldUserIdList.add(notificationDetailDTO);
					}
				}
			}
		}
		return sortedLatestRunids;

	}
}
