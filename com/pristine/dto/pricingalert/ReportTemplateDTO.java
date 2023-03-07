package com.pristine.dto.pricingalert;

public class ReportTemplateDTO {
	private String userId;
	private String alertIdForUser;
	private String excelTemplateName;
	
	private String columnKeys;
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getAlertIdForUser() {
		return alertIdForUser;
	}
	public void setAlertIdForUser(String alertIdForUser) {
		this.alertIdForUser = alertIdForUser;
	}
	public String getExcelTemplateName() {
		return excelTemplateName;
	}
	public void setExcelTemplateName(String excelTemplateName) {
		this.excelTemplateName = excelTemplateName;
	}
	public String getColumnKeys() {
		return columnKeys;
	}
	public void setColumnKeys(String columnKeys) {
		this.columnKeys = columnKeys;
	}
}
