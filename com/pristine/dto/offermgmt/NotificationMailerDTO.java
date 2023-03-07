package com.pristine.dto.offermgmt;

public class NotificationMailerDTO {

	private String productName = "";
	private String mailId = "";
	private String zoneNumber = null;
	private int notificationType = 0;
	private String userId = "";
	private long notificationId = 0;
	private long runId = 0;
	private String notificationTime = null;
	private int productId = 0;
	private String userRollName = null;
	private String userName = null;
	private String approverRoleName;

	

	public String getApproverRoleName() {
		return approverRoleName;
	}

	public void setApproverRoleName(String approverRoleName) {
		this.approverRoleName = approverRoleName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserRollName() {
		return userRollName;
	}

	public void setUserRollName(String userRollName) {
		this.userRollName = userRollName;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public String getmailId() {
		return mailId;
	}

	public void setmailId(String mailId) {
		this.mailId = mailId;
	}

	public String getZoneNumber() {
		return zoneNumber;
	}

	public void setZoneNumber(String zoneNumber) {
		this.zoneNumber = zoneNumber;
	}

	public int getNotificationType() {
		return notificationType;
	}

	public void setNotificationType(int notificationType) {
		this.notificationType = notificationType;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public long getNotificationId() {
		return notificationId;
	}

	public void setNotificationId(long notificationId) {
		this.notificationId = notificationId;
	}

	public long getRunId() {
		return runId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}

	public String getNotificationTime() {
		return notificationTime;
	}

	public void setNotificationTime(String notificationTime) {
		this.notificationTime = notificationTime;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}
	
}
