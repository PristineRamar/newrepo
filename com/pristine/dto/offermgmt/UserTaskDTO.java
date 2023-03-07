package com.pristine.dto.offermgmt;

public class UserTaskDTO {
	private String userId = null;
	private String valueType = null;
	private String value = null;
	private String combinedValue;
	private int roleId;
	private int workflowLevel;
	private boolean isNotificationEnabled;
	private boolean isEmailEnabled; 
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getValueType() {
		return valueType;
	}
	public void setValueType(String valueType) {
		this.valueType = valueType;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getCombinedValue() {
		return combinedValue;
	}
	public void setCombinedValue(String combinedValue) {
		this.combinedValue = combinedValue;
	}
	public int getRoleId() {
		return roleId;
	}
	public void setRoleId(int roleId) {
		this.roleId = roleId;
	}
	public int getWorkflowLevel() {
		return workflowLevel;
	}
	public void setWorkflowLevel(int workflowLevel) {
		this.workflowLevel = workflowLevel;
	}
	public boolean isNotificationEnabled() {
		return isNotificationEnabled;
	}
	public void setNotificationEnabled(boolean isNotificationEnabled) {
		this.isNotificationEnabled = isNotificationEnabled;
	}
	public boolean isEmailEnabled() {
		return isEmailEnabled;
	}
	public void setEmailEnabled(boolean isEmailEnabled) {
		this.isEmailEnabled = isEmailEnabled;
	}
}
