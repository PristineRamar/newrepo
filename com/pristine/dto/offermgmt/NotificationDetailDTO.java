package com.pristine.dto.offermgmt;

public class NotificationDetailDTO {
	private long notificationDetailId;
	private long notificationId;
	private String userId;
	private char isEMailSend;
	private char isNotificationSend;
	private int notificationTypeId;
	private UserTaskDTO userTaskDTO;
	private long runId;
	private char isRead;
	public long getNotificationId() {
		return notificationId;
	}
	public void setNotificationId(long notificationId) {
		this.notificationId = notificationId;
	}
	public long getNotificationDetailId() {
		return notificationDetailId;
	}
	public void setNotificationDetailId(long notificationDetailId) {
		this.notificationDetailId = notificationDetailId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public char getIsEMailSend() {
		return isEMailSend;
	}
	public void setIsEMailSend(char isEMailSend) {
		this.isEMailSend = isEMailSend;
	}
	public char getIsNotificationSend() {
		return isNotificationSend;
	}
	public void setIsNotificationSend(char isNotificationSend) {
		this.isNotificationSend = isNotificationSend;
	}
	public int getNotificationTypeId() {
		return notificationTypeId;
	}
	public void setNotificationTypeId(int notificationTypeId) {
		this.notificationTypeId = notificationTypeId;
	}
	public UserTaskDTO getUserTaskDTO() {
		return userTaskDTO;
	}
	public void setUserTaskDTO(UserTaskDTO userTaskDTO) {
		this.userTaskDTO = userTaskDTO;
	}
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public char getIsRead() {
		return isRead;
	}
	public void setIsRead(char isRead) {
		this.isRead = isRead;
	}
	
}

